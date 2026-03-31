import ipaddress
import re
import socket
import time
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


URL_PATTERN = re.compile(
    r"https?://[A-Za-z0-9\-._~:/?#\[\]@!$&'()*+,;=%]+",
    re.IGNORECASE,
)
URL_TRAILING_PUNCT = ".,!?;:)]}>\"'，。！？；：）】》」"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}
MAX_SNIPPET_CHARS = 1200
MAX_TOTAL_WAIT_SECONDS = 8.0


def extract_first_url(text: str) -> str:
    if not text:
        return ""
    match = URL_PATTERN.search(str(text))
    if not match:
        return ""
    return match.group(0).rstrip(URL_TRAILING_PUNCT).strip()


def _is_public_ip(ip_str: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip_str)
    except ValueError:
        return False

    if hasattr(ip_obj, "is_global"):
        return bool(ip_obj.is_global)

    return not any(
        [
            ip_obj.is_private,
            ip_obj.is_loopback,
            ip_obj.is_link_local,
            ip_obj.is_multicast,
            ip_obj.is_reserved,
            ip_obj.is_unspecified,
        ]
    )


def is_safe_public_url(url: str) -> bool:
    try:
        parsed = urlparse(str(url).strip())
    except Exception:
        return False

    if parsed.scheme not in ("http", "https"):
        return False

    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        return False
    if host in LOCAL_HOSTS:
        return False

    try:
        ipaddress.ip_address(host)
        return _is_public_ip(host)
    except ValueError:
        pass

    try:
        infos = socket.getaddrinfo(
            host,
            parsed.port or (443 if parsed.scheme == "https" else 80),
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror:
        # DNS 无法解析时交给后续 fetch 阶段处理，不在安全阶段直接拒绝。
        return True
    except Exception:
        return False

    for item in infos:
        sockaddr = item[4]
        if not sockaddr:
            continue
        ip = sockaddr[0]
        if not _is_public_ip(ip):
            return False

    return True


def fetch_html(url: str, timeout=(3, 6), max_bytes: int = 2_000_000) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    }

    with requests.get(url, headers=headers, timeout=timeout, stream=True, allow_redirects=True) as resp:
        resp.raise_for_status()
        content_type = (resp.headers.get("Content-Type") or "").lower()
        if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
            raise ValueError("non_html")

        chunks = []
        size = 0
        for chunk in resp.iter_content(chunk_size=8192):
            if not chunk:
                continue
            size += len(chunk)
            if size > max_bytes:
                raise ValueError("too_large")
            chunks.append(chunk)

    raw = b"".join(chunks)
    encoding = "utf-8"
    if hasattr(resp, "encoding") and resp.encoding:
        encoding = resp.encoding
    try:
        return raw.decode(encoding, errors="ignore")
    except Exception:
        return raw.decode("utf-8", errors="ignore")


def _clean_text(text: str) -> str:
    if not text:
        return ""
    lines = []
    for ln in str(text).splitlines():
        normalized = re.sub(r"\s+", " ", ln).strip()
        if normalized:
            lines.append(normalized)
    return "\n".join(lines).strip()


def _extract_title(soup: BeautifulSoup, fallback_title: str = "") -> str:
    candidates = [
        soup.select_one("#activity-name"),
        soup.select_one("meta[property='og:title']"),
        soup.select_one("h1"),
    ]
    for node in candidates:
        if not node:
            continue
        if getattr(node, "name", "") == "meta":
            value = _clean_text(node.get("content", ""))
        else:
            value = _clean_text(node.get_text(" ", strip=True))
        if value:
            return value
    return _clean_text(fallback_title)


def _pick_main_node(soup: BeautifulSoup):
    selectors = [
        "#js_content",
        "article",
        "main",
        "[role='main']",
        "#content",
        ".article",
        ".post",
        ".entry-content",
        ".news-content",
    ]
    best_node = None
    best_len = 0
    for selector in selectors:
        node = soup.select_one(selector)
        if not node:
            continue
        txt = _clean_text(node.get_text("\n", strip=True))
        txt_len = len(txt)
        if txt_len > best_len:
            best_len = txt_len
            best_node = node

    if best_node is not None:
        return best_node
    return soup.body or soup


def extract_main_text(html: str, url: str) -> tuple[str, str]:
    soup = BeautifulSoup(html or "", "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()

    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower()
    default_title = _clean_text(soup.title.get_text(" ", strip=True)) if soup.title else ""

    if "mp.weixin.qq.com" in host:
        title = _extract_title(soup, default_title)
        wx_content = soup.select_one("#js_content")
        if wx_content:
            content = _clean_text(wx_content.get_text("\n", strip=True))
            return title, content

    title = _extract_title(soup, default_title)
    content_node = _pick_main_node(soup)
    content = _clean_text(content_node.get_text("\n", strip=True))
    return title, content


def build_link_context(text: str) -> dict:
    result = {
        "ok": False,
        "url": "",
        "title": "",
        "snippet": "",
        "error_code": "no_url",
        "error_message": "",
    }

    url = extract_first_url(text)
    if not url:
        result["error_message"] = "未检测到链接。"
        return result

    result["url"] = url

    if not is_safe_public_url(url):
        result["error_code"] = "unsafe_url"
        result["error_message"] = "链接不安全或属于内网地址，已拒绝访问。"
        return result

    started = time.monotonic()
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    try:
        html = fetch_html(url=url, timeout=(3, 5), max_bytes=2_000_000)
    except requests.Timeout:
        result["error_code"] = "timeout"
        result["error_message"] = "链接抓取超时。"
        return result
    except ValueError as ve:
        code = str(ve).strip().lower()
        if code == "too_large" and "mp.weixin.qq.com" in host:
            # 微信文章页面可能包含较多内联资源，给一次受控重试机会。
            try:
                html = fetch_html(url=url, timeout=(3, 5), max_bytes=6_000_000)
            except requests.Timeout:
                result["error_code"] = "timeout"
                result["error_message"] = "链接抓取超时。"
                return result
            except ValueError as retry_ve:
                retry_code = str(retry_ve).strip().lower()
                if retry_code == "too_large":
                    result["error_code"] = "too_large"
                    result["error_message"] = "页面内容过大，已中止读取。"
                elif retry_code == "non_html":
                    result["error_code"] = "non_html"
                    result["error_message"] = "链接不是可解析的 HTML 页面。"
                else:
                    result["error_code"] = "parse_error"
                    result["error_message"] = f"链接解析失败: {retry_ve}"
                return result
            except requests.RequestException as retry_rexc:
                result["error_code"] = "fetch_failed"
                result["error_message"] = f"链接抓取失败: {retry_rexc}"
                return result
            except Exception as retry_exc:
                result["error_code"] = "fetch_failed"
                result["error_message"] = f"链接抓取失败: {retry_exc}"
                return result
        else:
            if code == "non_html":
                result["error_code"] = "non_html"
                result["error_message"] = "链接不是可解析的 HTML 页面。"
            elif code == "too_large":
                result["error_code"] = "too_large"
                result["error_message"] = "页面内容过大，已中止读取。"
            else:
                result["error_code"] = "parse_error"
                result["error_message"] = f"链接解析失败: {ve}"
            return result
    except requests.RequestException as rexc:
        result["error_code"] = "fetch_failed"
        result["error_message"] = f"链接抓取失败: {rexc}"
        return result
    except Exception as exc:
        result["error_code"] = "fetch_failed"
        result["error_message"] = f"链接抓取失败: {exc}"
        return result

    elapsed = time.monotonic() - started
    if elapsed > MAX_TOTAL_WAIT_SECONDS:
        result["error_code"] = "timeout"
        result["error_message"] = "链接读取超时（超过8秒）。"
        return result

    try:
        title, content = extract_main_text(html, url)
    except Exception as exc:
        result["error_code"] = "parse_error"
        result["error_message"] = f"正文提取失败: {exc}"
        return result

    content = _clean_text(content)
    if len(content) < 40:
        result["error_code"] = "content_empty"
        result["error_message"] = "未提取到足够正文内容。"
        return result

    result["ok"] = True
    result["title"] = _clean_text(title) or "未提取到标题"
    result["snippet"] = content[:MAX_SNIPPET_CHARS]
    result["error_code"] = ""
    result["error_message"] = ""
    return result

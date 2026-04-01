import ipaddress
import json
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
DYNAMIC_RENDER_DOMAINS = ("wallstreetcn.com", "cls.cn")
DYNAMIC_FETCH_MIN_TIMEOUT_SECONDS = 6.0
DYNAMIC_FETCH_MAX_TIMEOUT_SECONDS = 10.0
WS_CN_API_TEMPLATE = "https://api-one-wscn.awtmt.com/apiv1/content/articles/{article_id}?extract=1"


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


def _extract_wscn_article_id(url: str) -> str:
    parsed = urlparse(url or "")
    host = (parsed.hostname or "").lower().strip(".")
    if not (host == "wallstreetcn.com" or host.endswith(".wallstreetcn.com")):
        return ""
    m = re.search(r"/articles/(\d+)", parsed.path or "")
    return m.group(1) if m else ""


def _fetch_wallstreetcn_article_api(url: str) -> tuple[str, str, str]:
    article_id = _extract_wscn_article_id(url)
    if not article_id:
        return "", "", "not_wallstreetcn_article"

    api_url = WS_CN_API_TEMPLATE.format(article_id=article_id)
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json,text/plain,*/*",
        "Referer": url,
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=(3, 6))
        resp.raise_for_status()
        payload = resp.json()
    except requests.RequestException as rexc:
        return "", "", f"wscn_api_fetch_failed:{rexc}"
    except Exception as exc:
        return "", "", f"wscn_api_parse_failed:{exc}"

    if payload.get("code") != 20000:
        return "", "", f"wscn_api_bad_code:{payload.get('code')}"

    data = payload.get("data") or {}
    title = _clean_text(data.get("title") or "")
    content_html = data.get("content") or data.get("content_short") or ""
    if not isinstance(content_html, str):
        content_html = str(content_html)

    soup = BeautifulSoup(content_html, "html.parser")
    for tag in soup(["script", "style", "noscript", "iframe", "svg"]):
        tag.decompose()
    content = _clean_text(soup.get_text("\n", strip=True))
    if len(content) < 40:
        return title, "", "wscn_api_content_empty"

    return title, content, ""


def _iter_json_strings(obj):
    if isinstance(obj, dict):
        for _, value in obj.items():
            yield from _iter_json_strings(value)
    elif isinstance(obj, list):
        for item in obj:
            yield from _iter_json_strings(item)
    elif isinstance(obj, str):
        value = _clean_text(obj)
        if value:
            yield value


def _extract_from_json_ld(soup: BeautifulSoup) -> tuple[str, str]:
    title = ""
    best_content = ""
    scripts = soup.select("script[type='application/ld+json']")
    for script in scripts:
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except Exception:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_type = str(node.get("@type", "")).lower()
            if any(k in node_type for k in ("article", "news", "report", "post")):
                node_title = _clean_text(node.get("headline") or node.get("name") or "")
                if node_title and not title:
                    title = node_title
                candidate = _clean_text(
                    node.get("articleBody")
                    or node.get("description")
                    or node.get("text")
                    or node.get("content")
                    or ""
                )
                if len(candidate) > len(best_content):
                    best_content = candidate
    return title, best_content


def _extract_from_next_data(soup: BeautifulSoup) -> tuple[str, str]:
    node = soup.select_one("script#__NEXT_DATA__")
    if not node:
        return "", ""

    raw = (node.string or node.get_text() or "").strip()
    if not raw:
        return "", ""

    try:
        payload = json.loads(raw)
    except Exception:
        return "", ""

    title = ""
    best_content = ""
    content_keys = {"articleBody", "content", "description", "summary", "body", "text", "markdown", "detail"}
    title_keys = {"title", "headline", "name"}

    def _walk(obj):
        nonlocal title, best_content
        if isinstance(obj, dict):
            local_title = ""
            for tk in title_keys:
                if tk in obj and isinstance(obj[tk], str):
                    val = _clean_text(obj[tk])
                    if val:
                        local_title = val
                        if not title:
                            title = val
                        break

            for ck in content_keys:
                if ck in obj and isinstance(obj[ck], str):
                    val = _clean_text(obj[ck])
                    if len(val) > len(best_content):
                        best_content = val
                        if local_title:
                            title = local_title

            for _, value in obj.items():
                _walk(value)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)

    _walk(payload)
    return title, best_content


def _extract_structured_article(soup: BeautifulSoup) -> tuple[str, str]:
    t1, c1 = _extract_from_json_ld(soup)
    if len(c1) >= 40:
        return t1, c1

    t2, c2 = _extract_from_next_data(soup)
    if len(c2) >= 40:
        return t2 or t1, c2

    return t1 or t2, c1 if len(c1) >= len(c2) else c2


def _is_dynamic_domain(host: str) -> bool:
    host = (host or "").lower().strip(".")
    if not host:
        return False
    for domain in DYNAMIC_RENDER_DOMAINS:
        if host == domain or host.endswith("." + domain):
            return True
    return False


def _fetch_dynamic_text(url: str, timeout_sec: float = 8.0) -> tuple[str, str]:
    try:
        import asyncio
        import concurrent.futures
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
    except Exception:
        return "", "dynamic_unavailable"

    cfg = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        word_count_threshold=20,
        excluded_tags=["nav", "footer", "header", "aside", "script", "style", "noscript"],
        process_iframes=False,
        magic=True,
    )

    async def _run():
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url=url, config=cfg)
            if result.success and result.markdown:
                raw = result.markdown.fit_markdown or result.markdown.raw_markdown or ""
                cleaned = _clean_text(raw)
                if cleaned:
                    return cleaned, ""
            return "", _clean_text(getattr(result, "error_message", "") or "dynamic_failed")

    def _runner():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(_run())
        finally:
            loop.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_runner)
        try:
            return future.result(timeout=max(1.0, float(timeout_sec)))
        except concurrent.futures.TimeoutError:
            return "", "dynamic_timeout"
        except Exception as exc:
            return "", f"dynamic_failed:{exc}"


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
    raw_soup = BeautifulSoup(html or "", "html.parser")
    structured_title, structured_content = _extract_structured_article(raw_soup)

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

    if len(structured_content) >= 40:
        return _clean_text(structured_title) or _extract_title(soup, default_title), _clean_text(structured_content)

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

    # wallstreetcn 专用 API 优先，稳定绕过动态渲染页面正文提取失败问题
    wscn_title, wscn_content, wscn_err = _fetch_wallstreetcn_article_api(url)
    if wscn_content:
        print(f"[URL_PREPROCESS] wallstreetcn API 命中: {url}")
        elapsed = time.monotonic() - started
        if elapsed > MAX_TOTAL_WAIT_SECONDS:
            result["error_code"] = "timeout"
            result["error_message"] = "链接读取超时（超过8秒）。"
            return result
        result["ok"] = True
        result["title"] = wscn_title or "未提取到标题"
        result["snippet"] = wscn_content[:MAX_SNIPPET_CHARS]
        result["error_code"] = ""
        result["error_message"] = ""
        return result

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
        remaining = MAX_TOTAL_WAIT_SECONDS - (time.monotonic() - started)
        dynamic_error = ""
        if _is_dynamic_domain(host):
            # 动态站点给独立预算，避免被静态抓取耗时挤压导致误超时。
            dynamic_budget = max(
                DYNAMIC_FETCH_MIN_TIMEOUT_SECONDS,
                min(DYNAMIC_FETCH_MAX_TIMEOUT_SECONDS, remaining + 3.0),
            )
            dynamic_text, dynamic_error = _fetch_dynamic_text(url, timeout_sec=dynamic_budget)
            dynamic_text = _clean_text(dynamic_text)
            if len(dynamic_text) >= 40:
                content = dynamic_text

        if len(content) < 40:
            result["error_code"] = "content_empty"
            if dynamic_error:
                result["error_message"] = f"未提取到足够正文内容（动态提取失败: {dynamic_error}）。"
            else:
                result["error_message"] = "未提取到足够正文内容。"
            return result

    result["ok"] = True
    result["title"] = _clean_text(title) or "未提取到标题"
    result["snippet"] = content[:MAX_SNIPPET_CHARS]
    result["error_code"] = ""
    result["error_message"] = ""
    return result

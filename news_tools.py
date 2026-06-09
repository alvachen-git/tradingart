from __future__ import annotations

import hashlib
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Iterable, List, Mapping, Sequence, Tuple
from urllib.parse import urlencode

try:
    import requests
except Exception:  # pragma: no cover - used only in minimal test environments
    requests = None

try:
    from langchain_core.tools import tool
except Exception:  # pragma: no cover - used only in minimal test environments
    class _SimpleTool:
        def __init__(self, func):
            self.func = func
            self.__name__ = getattr(func, "__name__", "tool")

        def __call__(self, *args, **kwargs):
            return self.func(*args, **kwargs)

        def invoke(self, args):
            if isinstance(args, dict):
                return self.func(**args)
            return self.func(args)

    def tool(func=None, **_kwargs):
        if func is None:
            return lambda wrapped: _SimpleTool(wrapped)
        return _SimpleTool(func)


CLS_UPDATE_TELEGRAPH_URL = "https://www.cls.cn/nodeapi/updateTelegraphList"
CLS_TELEGRAPH_URL = "https://www.cls.cn/nodeapi/telegraphList"
CLS_ROLL_LIST_URL = "https://www.cls.cn/v1/roll/get_roll_list"
EASTMONEY_FAST_NEWS_URL = "https://np-weblist.eastmoney.com/comm/web/getFastNewsList"
REQUEST_TIMEOUT = (2, 5)
BEIJING_TZ = timezone(timedelta(hours=8))

NEWS_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
}

CLS_HEADERS = {
    **NEWS_HEADERS,
    "Referer": "https://www.cls.cn/telegraph",
    "Origin": "https://www.cls.cn",
}

QUERY_STOPWORDS = (
    "请", "帮我", "帮", "查", "查询", "分析", "影响", "什么", "如何", "怎么", "有没有", "是否",
    "最近", "最新", "消息", "新闻", "快讯", "相关", "关于", "市场", "事件", "可能", "一下",
)

FINANCE_KEYWORDS = (
    "美联储", "fomc", "fed", "加息", "降息", "利率", "实际利率", "美元", "美债", "黄金", "白银",
    "金银", "贵金属", "原油", "铜", "a股", "港股", "美股", "纳指", "半导体", "机器人",
)


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _duration_ms(started_at: float) -> int:
    return int((time.perf_counter() - started_at) * 1000)


def _log_news_source(
    source: str,
    status: str,
    http_code: Any = "",
    duration_ms: int = 0,
    fallback: bool = False,
    error: str = "",
) -> None:
    clean_error = _safe_text(error).replace("\n", " ")[:180]
    print(
        f"[news-tool] source={source} status={status} "
        f"http_code={http_code or ''} duration_ms={duration_ms} "
        f"fallback={str(bool(fallback)).lower()} error={clean_error}"
    )


def _response_status_from_exception(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    return _safe_text(getattr(response, "status_code", ""))


def _request_json(
    source: str,
    url: str,
    params: Sequence[Tuple[str, str]] | Mapping[str, str],
    headers: Mapping[str, str],
    fallback: bool,
) -> Mapping[str, Any]:
    if requests is None:
        raise RuntimeError("requests is not installed")

    started_at = time.perf_counter()
    http_code: Any = ""
    try:
        response = requests.get(url, params=params, headers=dict(headers), timeout=REQUEST_TIMEOUT)
        http_code = getattr(response, "status_code", "")
        response.raise_for_status()
        data = response.json()
        _log_news_source(source, "ok", http_code=http_code, duration_ms=_duration_ms(started_at), fallback=fallback)
        return data if isinstance(data, Mapping) else {}
    except Exception as exc:
        _log_news_source(
            source,
            "error",
            http_code=http_code or _response_status_from_exception(exc),
            duration_ms=_duration_ms(started_at),
            fallback=fallback,
            error=f"{type(exc).__name__}: {exc}",
        )
        raise


def _build_cls_sign(query_string: str) -> str:
    sha1_value = hashlib.sha1(query_string.encode("utf-8")).hexdigest()
    return hashlib.md5(sha1_value.encode("utf-8")).hexdigest()


def _build_cls_signed_params(params: Mapping[str, Any]) -> List[Tuple[str, str]]:
    sorted_items = sorted((str(key), _safe_text(value)) for key, value in params.items() if value is not None)
    query_string = urlencode(sorted_items)
    return sorted_items + [("sign", _build_cls_sign(query_string))]


def _format_timestamp(value: Any) -> str:
    raw = _safe_text(value)
    if not raw:
        return ""
    try:
        number = float(raw)
        if number > 10_000_000_000:
            number /= 1000
        return datetime.fromtimestamp(number, tz=BEIJING_TZ).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return raw[:16]


def _normalize_rows(rows: Iterable[Mapping[str, Any]], limit: int) -> List[dict]:
    out: List[dict] = []
    for row in rows:
        title = _safe_text(row.get("标题") or row.get("title") or row.get("news_title"))
        content = _safe_text(row.get("内容") or row.get("content") or row.get("summary") or row.get("digest") or title)
        publish_time = _safe_text(row.get("发布时间") or row.get("showTime") or row.get("time") or row.get("date"))
        link = _safe_text(row.get("链接") or row.get("url") or row.get("uniqueUrl") or row.get("code"))
        if not title and content:
            title = content[:32]
        if not content:
            content = title
        if not title and not content:
            continue
        out.append({"标题": title, "内容": content, "发布时间": publish_time, "链接": link})
        if len(out) >= limit:
            break
    return out


def _build_cls_request_candidates(limit: int) -> List[Tuple[str, str, Sequence[Tuple[str, str]], Mapping[str, str]]]:
    rn = str(max(5, min(int(limit), 50)))
    now_ts = str(int(time.time()))
    roll_v1_params = _build_cls_signed_params(
        {
            "app": "CailianpressWeb",
            "category": "",
            "last_time": now_ts,
            "os": "web",
            "refresh_type": "1",
            "rn": rn,
            "sv": "8.4.6",
        }
    )
    update_params = _build_cls_signed_params(
        {
            "appName": "CailianpressWeb",
            "hasFirstVipArticle": "1",
            "os": "web",
            "sv": "8.7.9",
        }
    )

    stale_glanceway_params: List[Tuple[str, str]] = [
        ("app", "CailianpressWeb"),
        ("os", "web"),
        ("refresh_type", "1"),
        ("rn", rn),
        ("sv", "8.4.6"),
    ]

    legacy_params: List[Tuple[str, str]] = [
        ("app", "CailianpressWeb"),
        ("last_time", now_ts),
        ("os", "web"),
        ("rn", rn),
        ("sv", "7.7.5"),
    ]
    legacy_query = urlencode(legacy_params)
    signed_legacy_params = legacy_params + [("sign", _build_cls_sign(legacy_query))]

    return [
        ("cls_roll_v1", CLS_ROLL_LIST_URL, roll_v1_params, CLS_HEADERS),
        ("cls_update_telegraph", CLS_UPDATE_TELEGRAPH_URL, update_params, CLS_HEADERS),
        ("cls_stale_telegraph_list", CLS_TELEGRAPH_URL, stale_glanceway_params, CLS_HEADERS),
        ("cls_legacy_signed", CLS_TELEGRAPH_URL, signed_legacy_params, CLS_HEADERS),
    ]


def _fetch_cls_telegraph_direct(limit: int = 20) -> List[dict]:
    last_error: Exception | None = None
    data: Mapping[str, Any] = {}
    for source_name, url, params, headers in _build_cls_request_candidates(limit):
        try:
            data = _request_json(source_name, url, params, headers, fallback=False)
            break
        except Exception as exc:
            last_error = exc
            continue
    else:
        if last_error is not None:
            raise last_error

    payload = data.get("data") if isinstance(data.get("data"), Mapping) else {}
    raw_rows = payload.get("roll_data") or payload.get("list") or payload.get("items") or []
    if not isinstance(raw_rows, list):
        raw_rows = []

    rows = []
    for item in raw_rows:
        if not isinstance(item, Mapping):
            continue
        rows.append(
            {
                "标题": item.get("title"),
                "内容": item.get("content") or item.get("summary"),
                "发布时间": _format_timestamp(item.get("ctime") or item.get("time") or item.get("rtime")),
                "链接": item.get("url") or item.get("share_url"),
            }
        )

    normalized = _normalize_rows(rows, limit=limit)
    if not normalized:
        _log_news_source("cls", "empty", fallback=False)
    return normalized


def _fetch_eastmoney_fast_news_direct(limit: int = 20) -> List[dict]:
    params = {
        "client": "web",
        "biz": "web_724",
        "fastColumn": "102",
        "sortEnd": "",
        "pageSize": str(max(5, min(int(limit), 100))),
        "req_trace": str(int(time.time() * 1000)),
    }
    data = _request_json("eastmoney", EASTMONEY_FAST_NEWS_URL, params, NEWS_HEADERS, fallback=True)
    payload = data.get("data") if isinstance(data.get("data"), Mapping) else {}
    raw_rows = payload.get("fastNewsList") or payload.get("list") or []
    if not isinstance(raw_rows, list):
        raw_rows = []

    rows = []
    for item in raw_rows:
        if not isinstance(item, Mapping):
            continue
        code = _safe_text(item.get("code"))
        link = item.get("url") or item.get("uniqueUrl") or (f"https://finance.eastmoney.com/a/{code}.html" if code else "")
        rows.append(
            {
                "标题": item.get("title"),
                "内容": item.get("summary") or item.get("content") or item.get("title"),
                "发布时间": item.get("showTime") or item.get("time"),
                "链接": link,
            }
        )

    normalized = _normalize_rows(rows, limit=limit)
    if not normalized:
        _log_news_source("eastmoney", "empty", fallback=True)
    return normalized


def standardize_columns(rows: Any) -> List[dict]:
    if rows is None:
        return []
    if isinstance(rows, list):
        return _normalize_rows([row for row in rows if isinstance(row, Mapping)], limit=len(rows) or 20)
    if hasattr(rows, "to_dict"):
        try:
            records = rows.to_dict("records")
            return _normalize_rows([row for row in records if isinstance(row, Mapping)], limit=len(records) or 20)
        except Exception:
            return []
    return []


def _query_keywords(query: str) -> List[str]:
    text = _safe_text(query).lower()
    if not text:
        return []

    out: List[str] = []
    for keyword in FINANCE_KEYWORDS:
        if keyword.lower() in text and keyword.lower() not in out:
            out.append(keyword.lower())

    cleaned = text
    for stopword in QUERY_STOPWORDS:
        cleaned = cleaned.replace(stopword.lower(), " ")
    for token in re.split(r"[\s,，。！？?、:：;；()（）]+", cleaned):
        token = token.strip().lower()
        if not token:
            continue
        if re.fullmatch(r"[a-z]{3,}", token) or 2 <= len(token) <= 12:
            if token not in out:
                out.append(token)
        if len(out) >= 8:
            break
    return out


def _filter_rows(rows: List[dict], query: str) -> Tuple[List[dict], bool]:
    if not query:
        return rows[:5], True

    keywords = _query_keywords(query)
    if not keywords:
        return rows[:5], False

    matched = []
    for row in rows:
        searchable = f"{row.get('标题', '')} {row.get('内容', '')}".lower()
        if any(keyword in searchable for keyword in keywords):
            matched.append(row)
    return (matched[:5], True) if matched else (rows[:5], False)


def format_news(rows: Any, limit: int = 10) -> str:
    normalized = standardize_columns(rows)[:limit]
    if not normalized:
        return "暂无相关新闻。"

    news_text = []
    for row in normalized:
        time_str = _safe_text(row.get("发布时间"))[:16]
        title = _safe_text(row.get("标题"))
        content = _safe_text(row.get("内容"))
        if len(content) > 90:
            content = f"{content[:90]}..."
        if content and title and content != title:
            item = f"- [{time_str}] {title}\n  {content}"
        else:
            item = f"- [{time_str}] {title or content}"
        news_text.append(item)

    return "\n".join(news_text)


def _format_source_output(source_name: str, rows: List[dict], query: str) -> str:
    selected_rows, exact_match = _filter_rows(rows, query)
    if not selected_rows:
        return ""
    if query and exact_match:
        header = f"来自 {source_name} 关于 '{query}' 的消息："
    elif query:
        header = f"{source_name} 未找到精确匹配，返回最新快讯供参考："
    else:
        header = f"{source_name} 最新头条："
    return f"{header}\n{format_news(selected_rows, limit=5)}"


def _get_financial_news_text(query: str = "") -> str:
    clean_query = _safe_text(query)
    print(f"[*] AI 正在检索新闻: {clean_query if clean_query else '宏观快讯'} ...")

    source_name = ""
    rows: List[dict] = []
    try:
        rows = _fetch_cls_telegraph_direct(limit=30)
        source_name = "财联社电报"
    except Exception as exc:
        print(f"[news-tool] source=cls action=fallback_to_eastmoney reason={type(exc).__name__}: {str(exc)[:160]}")

    if not rows:
        try:
            rows = _fetch_eastmoney_fast_news_direct(limit=30)
            source_name = "东方财富快讯"
        except Exception as exc:
            print(f"[news-tool] source=eastmoney action=all_sources_failed reason={type(exc).__name__}: {str(exc)[:160]}")

    if not rows:
        return "所有新闻接口暂时不可用，或未检索到可用内容。建议稍后再试。"

    output = _format_source_output(source_name, rows, clean_query)
    if output:
        return output
    return "未检索到匹配新闻。"


@tool
def get_financial_news(query: str = ""):
    """
    财经新闻检索工具。

    参数:
    - query: 搜索关键词，例如 "白银", "黄金", "原油", "贵州茅台"。
    """
    return _get_financial_news_text(query)


if __name__ == "__main__":
    print(_get_financial_news_text("黄金"))

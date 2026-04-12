from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Dict, List

from market_tools import get_market_snapshot, get_recent_price_series


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _parse_news_lines(raw: Any, source_name: str) -> List[Dict[str, Any]]:
    lines = [ln.strip() for ln in _safe_text(raw).splitlines() if ln.strip()]
    events: List[Dict[str, Any]] = []
    for idx, line in enumerate(lines[:20]):
        clean = line.replace("**", "").replace("- ", "").strip()
        if len(clean) < 6:
            continue
        events.append(
            {
                "event_id": f"{source_name}_{idx + 1}",
                "source": source_name,
                "title": clean[:120],
                "content": clean[:260],
                "timestamp": "",
            }
        )
    return events


def _extract_query_keywords(query: str) -> List[str]:
    text = _safe_text(query)
    if not text:
        return []

    zh_tokens = re.findall(r"[\u4e00-\u9fff]{2,6}", text)
    en_tokens = re.findall(r"[A-Za-z]{3,}", text.lower())
    stopwords = {
        "请",
        "给出",
        "分析",
        "影响",
        "什么",
        "如何",
        "以及",
        "并且",
        "可能",
        "市场",
        "事件",
        "概率",
        "冲突",
        "排序",
        "report",
        "analysis",
        "with",
        "from",
        "this",
        "that",
        "about",
        "event",
        "market",
    }

    merged = zh_tokens + en_tokens
    out: List[str] = []
    for tok in merged:
        t = tok.strip().lower()
        if not t or t in stopwords:
            continue
        if t not in out:
            out.append(t)
        if len(out) >= 10:
            break
    return out


def _matches_keywords(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    low = _safe_text(text).lower()
    return any(k in low for k in keywords)


def _try_load_news_from_tool(query: str) -> List[Dict[str, Any]]:
    try:
        from news_tools import get_financial_news

        raw = get_financial_news.invoke({"query": query})
        return _parse_news_lines(raw, "新闻工具")
    except Exception:
        return []


def _try_load_news_from_akshare(query: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    try:
        import akshare as ak

        dfs = []
        try:
            df1 = ak.stock_info_global_cls()
            if df1 is not None and not df1.empty:
                dfs.append(("财联社", df1.head(30)))
        except Exception:
            pass

        try:
            df2 = ak.stock_info_global_em()
            if df2 is not None and not df2.empty:
                dfs.append(("东方财富", df2.head(30)))
        except Exception:
            pass

        keywords = _extract_query_keywords(query)
        fallback_pool: List[Dict[str, Any]] = []
        for src, df in dfs:
            cols = [str(c) for c in df.columns]
            title_col = None
            time_col = None
            content_col = None

            for c in cols:
                lc = c.lower()
                if title_col is None and ("标题" in c or "title" in lc):
                    title_col = c
                if content_col is None and ("内容" in c or "content" in lc or "digest" in lc):
                    content_col = c
                if time_col is None and ("时间" in c or "time" in lc or "date" in lc):
                    time_col = c

            if title_col is None and len(cols) > 0:
                title_col = cols[0]

            for i, (_, row) in enumerate(df.iterrows()):
                title = _safe_text(row.get(title_col)) if title_col else ""
                content = _safe_text(row.get(content_col)) if content_col else title
                ts = _safe_text(row.get(time_col)) if time_col else ""
                merged = f"{title} {content}".lower()
                if len(title) < 6 and len(content) < 8:
                    continue
                item = {
                    "event_id": f"{src}_{i + 1}",
                    "source": src,
                    "title": (title or content)[:120],
                    "content": content[:260],
                    "timestamp": ts[:32],
                }
                if _matches_keywords(merged, keywords):
                    events.append(item)
                else:
                    fallback_pool.append(item)
                if len(events) >= 16:
                    break
            if len(events) >= 16:
                break

        if not events and fallback_pool:
            events.extend(fallback_pool[:12])
    except Exception:
        return []
    return events


def _try_load_news(query: str, use_external_news: bool) -> List[Dict[str, Any]]:
    if not use_external_news:
        return []

    events = _try_load_news_from_tool(query)
    if events:
        return events

    fallback = _try_load_news_from_akshare(query)
    if fallback:
        return fallback

    return [
        {
            "event_id": "news_unavailable",
            "source": "新闻降级",
            "title": "新闻数据暂不可用",
            "content": "外部新闻源暂时无可用内容，报告将基于现有市场与假设进行推演。",
            "timestamp": "",
        }
    ]


def _try_load_market_context(query: str, analysis_horizon: str) -> Dict[str, Any]:
    horizon_days_map = {"intraday": 3, "swing": 10, "weekly": 20}
    days = horizon_days_map.get(_safe_text(analysis_horizon).lower(), 10)
    snapshot = {}
    series = {}
    errors: List[str] = []

    try:
        snapshot = get_market_snapshot(query)
    except Exception as exc:
        errors.append(f"snapshot_error: {exc}")

    try:
        series = get_recent_price_series(query, days=days)
    except Exception as exc:
        errors.append(f"series_error: {exc}")

    return {"snapshot": snapshot, "series": series, "errors": errors}


def ingest_event_timeline(
    query: str,
    analysis_horizon: str = "swing",
    use_external_news: bool = True,
) -> Dict[str, Any]:
    market_ctx = _try_load_market_context(query, analysis_horizon)
    news_events = _try_load_news(query, use_external_news)
    timeline: List[Dict[str, Any]] = []
    now = datetime.now().isoformat()

    snapshot = market_ctx.get("snapshot")
    if snapshot:
        timeline.append(
            {
                "event_id": "market_snapshot",
                "source": "市场工具",
                "title": "市场快照",
                "content": _safe_text(snapshot)[:500],
                "timestamp": now,
            }
        )

    series = market_ctx.get("series")
    if series:
        timeline.append(
            {
                "event_id": "market_series",
                "source": "市场工具",
                "title": "近期价格序列",
                "content": _safe_text(series)[:500],
                "timestamp": now,
            }
        )

    timeline.extend(news_events)

    return {
        "timeline": timeline,
        "market_context": market_ctx,
        "ingest_meta": {
            "query": query,
            "analysis_horizon": analysis_horizon,
            "use_external_news": bool(use_external_news),
            "ingested_at": now,
            "event_count": len(timeline),
            "market_errors": market_ctx.get("errors", []),
        },
    }

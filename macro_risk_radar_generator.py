from __future__ import annotations

import argparse
import html
import json
import os
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import pandas as pd
from sqlalchemy import text

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except Exception:  # pragma: no cover
    go = None
    make_subplots = None


CHANNEL_CODE = "macro_risk_radar"
CHANNEL_NAME = "宏观周报"
DEFAULT_PREVIEW_PATH = "preview_macro_risk_radar.html"
DEFAULT_EVENT_WINDOW_DAYS = 7
DEFAULT_CHART_LOOKBACK_DAYS = 90
FED_CHART_LOOKBACK_DAYS = 120
CPI_NFP_LOOKBACK_DAYS = 180
EQUITY_BOND_LOOKBACK_DAYS = 365
EQUITY_BOND_RANK_LOOKBACK_DAYS = 365 * 3
CN_CORE_INDEX_CODE = "000300.SH"  # 沪深300，作为A股核心估值代表

DEFAULT_NEWS_QUERY = (
    "macro economy inflation labor market cpi nonfarm payroll pmi gdp retail sales housing "
    "federal reserve treasury yield balance sheet liquidity fiscal policy trade "
    "geopolitical conflict war sanctions tariff middle east ukraine"
)

FRESHNESS_THRESHOLD_BY_FREQ = {"D": 7, "W": 21, "M": 45, "Q": 120}
REQUIRED_ANALYSIS_KEYS = (
    "overview",
    "yield_curve_comment",
    "gold_silver_comment",
    "cpi_nfp_comment",
    "fed_policy_comment",
    "macro_news_comment",
    "allocation_advice_comment",
)

# Official BLS schedules (ET) used to avoid heuristic date guessing.
# Source:
# - CPI: https://www.bls.gov/schedule/news_release/cpi.htm
# - Employment Situation: https://www.bls.gov/schedule/news_release/empsit.htm
BLS_CPI_RELEASE_DATES: List[date] = [
    date(2025, 12, 18),
    date(2026, 1, 13),
    date(2026, 2, 13),
    date(2026, 3, 11),
    date(2026, 4, 10),
    date(2026, 5, 12),
    date(2026, 6, 10),
    date(2026, 7, 14),
    date(2026, 8, 12),
    date(2026, 9, 11),
    date(2026, 10, 14),
    date(2026, 11, 10),
    date(2026, 12, 10),
]

BLS_NFP_RELEASE_DATES: List[date] = [
    date(2025, 12, 16),
    date(2026, 1, 9),
    date(2026, 2, 11),
    date(2026, 3, 6),
    date(2026, 4, 3),
    date(2026, 5, 8),
    date(2026, 6, 5),
    date(2026, 7, 2),
    date(2026, 8, 7),
    date(2026, 9, 4),
    date(2026, 10, 2),
    date(2026, 11, 6),
    date(2026, 12, 4),
]


def _invoke_tool(tool_obj: Any, payload: Dict[str, Any]) -> str:
    try:
        if hasattr(tool_obj, "invoke"):
            return str(tool_obj.invoke(payload) or "").strip()
        return str(tool_obj(**payload) or "").strip()
    except Exception as exc:
        return f"tool_error: {exc}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_date(value: Any) -> Optional[date]:
    if value in (None, "", "None"):
        return None
    try:
        ts = pd.to_datetime(value, errors="coerce")
    except Exception:
        ts = pd.NaT
    if pd.isna(ts):
        txt = str(value).strip()
        if re.fullmatch(r"\d{8}", txt):
            try:
                return datetime.strptime(txt, "%Y%m%d").date()
            except Exception:
                return None
        return None
    return ts.date()


def _calc_change(latest: Optional[float], previous: Optional[float]) -> float:
    if latest is None or previous is None:
        return 0.0
    return float(latest) - float(previous)


def _compact_lines(text: str, limit: int = 10) -> List[str]:
    out: List[str] = []
    for raw in str(text or "").splitlines():
        line = re.sub(r"\s+", " ", str(raw or "")).strip(" -*\t")
        if not line:
            continue
        out.append(line)
        if len(out) >= max(1, int(limit)):
            break
    return out


def _clean_news_title(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""
    s = s.replace("**", "")
    s = re.sub(r"[📌📰⚠️✅⭐•]+", " ", s)
    s = re.sub(r"^\d+[).、\s]*", "", s)
    s = re.sub(r"^[-*]\s*", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _dedupe_news_items(items: List[Dict[str, str]], limit: int = 10) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    seen: set[str] = set()
    for item in items:
        title = _clean_news_title(item.get("title", ""))
        if not title:
            continue
        key = re.sub(r"\W+", "", title.lower())[:80]
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "source": str(item.get("source") or "新闻源"),
                "title": title[:140],
                "time": str(item.get("time") or "").strip()[:32],
                "summary": _clean_news_title(item.get("summary", ""))[:220],
            }
        )
        if len(out) >= max(1, int(limit)):
            break
    return out


def _freshness(as_of: Optional[date], frequency: str, as_of_now: date) -> Tuple[str, int, int]:
    threshold = FRESHNESS_THRESHOLD_BY_FREQ.get(str(frequency or "D").upper(), 45)
    if as_of is None:
        return "missing", -1, threshold
    stale_days = (as_of_now - as_of).days
    return ("fresh" if stale_days <= threshold else "stale", stale_days, threshold)


def _pick_nearest_value(series: Sequence[Tuple[date, float]], target: date) -> Optional[float]:
    for d, v in reversed(series):
        if d <= target:
            return v
    return series[0][1] if series else None


def _to_monthly_last(series: Sequence[Tuple[date, float]]) -> List[Tuple[date, float]]:
    bucket: Dict[Tuple[int, int], Tuple[date, float]] = {}
    for d, v in series:
        key = (d.year, d.month)
        prev = bucket.get(key)
        if prev is None or d > prev[0]:
            bucket[key] = (d, float(v))
    return [bucket[k] for k in sorted(bucket.keys())]


def _fetch_cn_equity_earnings_yield(engine: Any, start_date: date) -> List[Tuple[date, float]]:
    sql = text(
        """
        SELECT trade_date, pe_ttm
        FROM index_valuation
        WHERE ts_code = :ts_code
          AND trade_date >= :start_date
          AND pe_ttm > 0
        ORDER BY trade_date ASC
        """
    )
    try:
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"ts_code": CN_CORE_INDEX_CODE, "start_date": start_date})
    except Exception:
        return []
    if df.empty:
        return []
    df["trade_date"] = df["trade_date"].apply(_to_date)
    df["pe_ttm"] = pd.to_numeric(df["pe_ttm"], errors="coerce")
    df = df.dropna(subset=["trade_date", "pe_ttm"])
    if df.empty:
        return []
    out: List[Tuple[date, float]] = []
    for _, row in df.iterrows():
        pe = float(row["pe_ttm"])
        if pe <= 0:
            continue
        out.append((row["trade_date"], 100.0 / pe))
    return out


def _fetch_us_equity_earnings_yield(engine: Any, start_date: date) -> Dict[str, Any]:
    # First try DB-resident indicator aliases (if any ingestion exists).
    hist = _fetch_macro_series(engine, ["SP500_PE", "SP500PE", "SPX_PE", "US_PE"], start_date)
    alias_hit = None
    for code in ["SP500_PE", "SP500PE", "SPX_PE", "US_PE"]:
        raw = hist.get(code) or []
        if raw:
            alias_hit = code
            series = [(d, (100.0 / v) if v and v > 0 else None) for d, v in raw if v and v > 0]
            return {"series": series, "source": f"macro_daily:{alias_hit}", "has_db": True}

    # Fallback: scrape monthly PE table from multpl.
    try:
        tables = pd.read_html("https://www.multpl.com/s-p-500-pe-ratio/table/by-month")
    except Exception:
        return {"series": [], "source": "unavailable", "has_db": False}
    if not tables:
        return {"series": [], "source": "unavailable", "has_db": False}
    df = tables[0].copy()
    if df.shape[1] < 2:
        return {"series": [], "source": "unavailable", "has_db": False}
    c_date = df.columns[0]
    c_val = df.columns[1]
    df[c_date] = pd.to_datetime(df[c_date], errors="coerce")
    df[c_val] = pd.to_numeric(df[c_val].astype(str).str.replace(r"[^0-9.\-]", "", regex=True), errors="coerce")
    df = df.dropna(subset=[c_date, c_val])
    if df.empty:
        return {"series": [], "source": "unavailable", "has_db": False}
    out: List[Tuple[date, float]] = []
    for _, row in df.iterrows():
        d = row[c_date].date()
        if d < start_date:
            continue
        pe = float(row[c_val])
        if pe <= 0:
            continue
        out.append((d, 100.0 / pe))
    out.sort(key=lambda x: x[0])
    return {"series": out, "source": "multpl_web", "has_db": False}


def _percentile_rank(values: Sequence[float], current: Optional[float]) -> Optional[float]:
    if current is None:
        return None
    arr = [float(v) for v in values if v is not None]
    if len(arr) < 6:
        return None
    less_or_equal = sum(1 for v in arr if v <= float(current))
    return float(less_or_equal) / float(len(arr)) * 100.0


def _gap_regime_text(gap: Optional[float]) -> str:
    if gap is None:
        return "数据不足"
    if gap < 0:
        return "红灯：债券相对更有性价比"
    if gap < 1.5:
        return "黄灯：股债性价比接近"
    return "绿灯：股票相对更有性价比"


def _align_series(base_dates: Sequence[date], source_series: Sequence[Tuple[date, float]]) -> List[Optional[float]]:
    out: List[Optional[float]] = []
    for d in base_dates:
        out.append(_pick_nearest_value(source_series, d))
    return out


def _compute_yoy_series(level_series: Sequence[Tuple[date, float]]) -> List[Tuple[date, float]]:
    if not level_series:
        return []
    month_map: Dict[Tuple[int, int], float] = {(d.year, d.month): float(v) for d, v in level_series if v is not None}
    out: List[Tuple[date, float]] = []
    for d, v in level_series:
        prev = month_map.get((d.year - 1, d.month))
        if prev in (None, 0):
            continue
        out.append((d, ((float(v) / float(prev)) - 1.0) * 100.0))
    return out


def _compute_mom_delta_series(level_series: Sequence[Tuple[date, float]]) -> List[Tuple[date, float]]:
    if not level_series:
        return []
    out: List[Tuple[date, float]] = []
    prev: Optional[float] = None
    for d, v in level_series:
        cur = float(v)
        if prev is not None:
            out.append((d, cur - prev))
        prev = cur
    return out


def _next_business_day(d: date) -> date:
    cur = d
    while cur.weekday() >= 5:
        cur += timedelta(days=1)
    return cur


def _first_friday(year: int, month: int) -> date:
    d = date(year, month, 1)
    while d.weekday() != 4:
        d += timedelta(days=1)
    return d


def _add_months(d: date, months: int) -> date:
    y = d.year + (d.month - 1 + months) // 12
    m = (d.month - 1 + months) % 12 + 1
    return date(y, m, 1)


def _next_from_calendar(as_of: date, calendar_dates: Sequence[date]) -> Optional[date]:
    for d in sorted(set(calendar_dates)):
        if d > as_of:
            return d
    return None


def _next_nfp_date(as_of: date) -> date:
    official = _next_from_calendar(as_of, BLS_NFP_RELEASE_DATES)
    if official:
        return official
    this_release = _first_friday(as_of.year, as_of.month)
    if this_release > as_of:
        return this_release
    nxt = _add_months(as_of, 1)
    return _first_friday(nxt.year, nxt.month)


def _next_cpi_date(as_of: date) -> date:
    official = _next_from_calendar(as_of, BLS_CPI_RELEASE_DATES)
    if official:
        return official
    this_release = _next_business_day(date(as_of.year, as_of.month, 12))
    if this_release > as_of:
        return this_release
    nxt = _add_months(as_of, 1)
    return _next_business_day(date(nxt.year, nxt.month, 12))


def _next_fomc_date(as_of: date) -> Optional[date]:
    known = [
        date(2026, 1, 28),
        date(2026, 3, 18),
        date(2026, 4, 29),
        date(2026, 6, 17),
        date(2026, 7, 29),
        date(2026, 9, 16),
        date(2026, 10, 28),
        date(2026, 12, 9),
        date(2027, 1, 27),
    ]
    for d in known:
        if d > as_of:
            return d
    return None


def _fetch_macro_series(engine: Any, codes: Sequence[str], start_date: date) -> Dict[str, List[Tuple[date, float]]]:
    if not codes:
        return {}
    out: Dict[str, List[Tuple[date, float]]] = {c: [] for c in codes}
    sql = text(
        """
        SELECT trade_date, close_value
        FROM macro_daily
        WHERE indicator_code = :code
          AND trade_date >= :start_date
        ORDER BY trade_date ASC
        """
    )
    with engine.connect() as conn:
        for code in codes:
            df = pd.read_sql(sql, conn, params={"code": str(code).upper(), "start_date": start_date.strftime("%Y-%m-%d")})
            if df.empty:
                continue
            for _, row in df.iterrows():
                dt = _to_date(row.get("trade_date"))
                val = pd.to_numeric(row.get("close_value"), errors="coerce")
                if dt is None or pd.isna(val):
                    continue
                out[str(code).upper()].append((dt, float(val)))
    return out


def _fetch_indicator_latest_two(engine: Any, codes: Sequence[str]) -> Dict[str, Any]:
    for code in codes:
        sql = text(
            """
            SELECT indicator_code, trade_date, close_value
            FROM macro_daily
            WHERE indicator_code = :code
            ORDER BY trade_date DESC
            LIMIT 2
            """
        )
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"code": code})
        if df.empty:
            continue
        df = df.copy()
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        df["close_value"] = pd.to_numeric(df["close_value"], errors="coerce")
        latest_row = df.iloc[0]
        prev_row = df.iloc[1] if len(df) > 1 else None
        latest_val = None if pd.isna(latest_row["close_value"]) else float(latest_row["close_value"])
        prev_val = None if (prev_row is None or pd.isna(prev_row["close_value"])) else float(prev_row["close_value"])
        return {
            "code": str(latest_row.get("indicator_code") or code),
            "latest_value": latest_val,
            "previous_value": prev_val,
            "change": _calc_change(latest_val, prev_val),
            "as_of_date": _to_date(latest_row.get("trade_date")),
        }
    return {"code": codes[0] if codes else "", "latest_value": None, "previous_value": None, "change": 0.0, "as_of_date": None}


def _collect_yield_context(engine: Any, as_of_now: date, lookback_days: int) -> Dict[str, Any]:
    start_date = as_of_now - timedelta(days=max(lookback_days, 45))
    hist = _fetch_macro_series(engine, ["US2Y", "DGS2", "US10Y", "DGS10", "US30Y", "DGS30"], start_date)
    s2 = hist.get("US2Y") or hist.get("DGS2") or []
    s10 = hist.get("US10Y") or hist.get("DGS10") or []
    s30 = hist.get("US30Y") or hist.get("DGS30") or []
    c2, c10, c30 = (s2[-1][1] if s2 else None), (s10[-1][1] if s10 else None), (s30[-1][1] if s30 else None)
    p2, p10, p30 = _pick_nearest_value(s2, as_of_now - timedelta(days=7)), _pick_nearest_value(s10, as_of_now - timedelta(days=7)), _pick_nearest_value(s30, as_of_now - timedelta(days=7))
    spread = (c10 - c2) if (c10 is not None and c2 is not None) else None
    spread_prev = (p10 - p2) if (p10 is not None and p2 is not None) else None
    return {
        "current": {"2Y": c2, "10Y": c10, "30Y": c30},
        "prev7": {"2Y": p2, "10Y": p10, "30Y": p30},
        "delta1d": {
            "2Y": _calc_change(c2, s2[-2][1] if len(s2) >= 2 else None),
            "10Y": _calc_change(c10, s10[-2][1] if len(s10) >= 2 else None),
            "30Y": _calc_change(c30, s30[-2][1] if len(s30) >= 2 else None),
        },
        "spread_10_2": spread,
        "spread_10_2_prev7": spread_prev,
        "as_of_date": max([s[-1][0] for s in [s2, s10, s30] if s], default=None),
    }


def _collect_gold_silver_context(engine: Any, as_of_now: date, lookback_days: int) -> Dict[str, Any]:
    start_ymd = (as_of_now - timedelta(days=max(lookback_days, 180) + 5)).strftime("%Y%m%d")
    sql = text(
        """
        SELECT trade_date, ts_code, close_price
        FROM futures_price
        WHERE ts_code IN ('au', 'ag')
          AND trade_date >= :start_ymd
        ORDER BY trade_date ASC
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"start_ymd": start_ymd})
    if df.empty:
        return {"latest_value": None, "previous_value": None, "change": 0.0, "as_of_date": None, "series": []}

    df = df.copy()
    df["trade_date"] = df["trade_date"].apply(_to_date)
    df["ts_code"] = df["ts_code"].astype(str).str.lower()
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df = df.dropna(subset=["trade_date", "close_price"])
    if df.empty:
        return {"latest_value": None, "previous_value": None, "change": 0.0, "as_of_date": None, "series": []}

    pv = df.pivot_table(index="trade_date", columns="ts_code", values="close_price", aggfunc="last")
    if "au" not in pv.columns or "ag" not in pv.columns:
        return {"latest_value": None, "previous_value": None, "change": 0.0, "as_of_date": None, "series": []}
    pv = pv.dropna(subset=["au", "ag"]).copy()
    if pv.empty:
        return {"latest_value": None, "previous_value": None, "change": 0.0, "as_of_date": None, "series": []}

    pv["ratio"] = pv["au"] / (pv["ag"] / 1000.0)
    series = [(idx, float(v)) for idx, v in pv["ratio"].items() if pd.notna(v)]
    latest = series[-1][1] if series else None
    prev = series[-2][1] if len(series) >= 2 else None
    return {
        "latest_value": latest,
        "previous_value": prev,
        "change": _calc_change(latest, prev),
        "as_of_date": series[-1][0] if series else None,
        "series": series[-max(2, int(lookback_days)) :],
    }


def _collect_indicator_event_context(engine: Any, code: str, label: str, frequency: str, as_of_now: date, event_window_days: int) -> Dict[str, Any]:
    snap = _fetch_indicator_latest_two(engine, [code])
    as_of_date = snap.get("as_of_date")
    status, stale_days, _ = _freshness(as_of_date, frequency, as_of_now)
    recently_updated = bool(as_of_date and (as_of_now - as_of_date).days <= int(event_window_days))
    return {
        "label": label,
        "code": code,
        "latest_value": snap.get("latest_value"),
        "previous_value": snap.get("previous_value"),
        "change": snap.get("change", 0.0),
        "as_of_date": as_of_date,
        "days_since_update": ((as_of_now - as_of_date).days if as_of_date else -1),
        "recently_updated": recently_updated,
        "freshness_status": status,
        "stale_days": stale_days,
        "frequency": frequency,
    }


def _collect_news_context() -> Dict[str, Any]:
    def _looks_unavailable(raw_text: str, lines: List[str]) -> bool:
        if not lines:
            return True
        marks = ("暂无响应", "未搜索到相关内容", "新闻数据暂不可用", "news_unavailable", "tool_error", "未找到有效新闻内容")
        return any(m in str(raw_text or "") for m in marks)

    def _items_from_lines(lines_in: List[str], source: str) -> List[Dict[str, str]]:
        out: List[Dict[str, str]] = []
        for ln in lines_in:
            clean = _clean_news_title(ln)
            if clean:
                out.append({"source": source, "title": clean, "time": "", "summary": ""})
        return out

    raw = ""
    lines: List[str] = []
    items: List[Dict[str, str]] = []

    try:
        from news_tools import get_financial_news

        raw = _invoke_tool(get_financial_news, {"query": DEFAULT_NEWS_QUERY})
        lines = _compact_lines(raw, limit=14)
        items.extend(_items_from_lines(lines, "新闻工具"))
    except Exception:
        pass

    if _looks_unavailable(raw, lines):
        try:
            from event_ingest_tool import ingest_event_timeline

            payload = ingest_event_timeline(query=DEFAULT_NEWS_QUERY, analysis_horizon="weekly", use_external_news=True)
            timeline = payload.get("timeline") if isinstance(payload, dict) else []
            tmp: List[Dict[str, str]] = []
            for item in timeline if isinstance(timeline, list) else []:
                if not isinstance(item, dict):
                    continue
                src = str(item.get("source") or "抓取源").strip()
                title = str(item.get("title") or "").strip()
                if not title or src == "市场工具":
                    continue
                tmp.append({"source": src, "title": title, "time": str(item.get("timestamp") or ""), "summary": str(item.get("content") or "")[:220]})
                if len(tmp) >= 12:
                    break
            if tmp:
                items = tmp
                lines = [f"[{x['source']}] {x['title']}" for x in tmp]
                raw = "\n".join(lines)
        except Exception:
            pass

    if _looks_unavailable(raw, lines):
        try:
            from search_tools import search_web

            web_raw = _invoke_tool(search_web, {"query": DEFAULT_NEWS_QUERY})
            web_lines = _compact_lines(web_raw, limit=12)
            if web_lines and all(("未配置 ZHIPUAI_API_KEY" not in x and "搜索出错" not in x) for x in web_lines):
                lines = web_lines
                raw = web_raw
                items = _items_from_lines(web_lines, "联网搜索")
        except Exception:
            pass

    if not lines:
        lines = ["⚠️ 所有新闻接口暂不可用，本期先基于指标与市场价格解读。"]
        items = [{"source": "系统", "title": lines[0], "time": "", "summary": ""}]
        raw = lines[0]

    deduped = _dedupe_news_items(items, limit=10)
    out_lines = [f"[{x['source']}] {x['title']}" for x in deduped] if deduped else lines[:10]
    return {"raw_text": raw, "lines": out_lines[:10], "items": deduped[:10]}


def _build_freshness_rows(context: Dict[str, Any], as_of_now: date) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    def add_row(name: str, value: Any, as_of_date: Optional[date], frequency: str, source: str) -> None:
        status, stale_days, _ = _freshness(as_of_date, frequency, as_of_now)
        rows.append({"name": name, "value": value, "as_of_date": as_of_date.strftime("%Y-%m-%d") if as_of_date else "-", "source": source, "status": status, "stale_days": stale_days})

    yld = context.get("yield_curve", {})
    add_row("美国2Y国债", yld.get("current", {}).get("2Y"), yld.get("as_of_date"), "D", "macro_daily")
    add_row("美国10Y国债", yld.get("current", {}).get("10Y"), yld.get("as_of_date"), "D", "macro_daily")
    add_row("美国30Y国债", yld.get("current", {}).get("30Y"), yld.get("as_of_date"), "D", "macro_daily")
    gsr = context.get("gold_silver_ratio", {})
    add_row("金银比", gsr.get("latest_value"), gsr.get("as_of_date"), "D", "futures_price")
    cpi = context.get("cpi", {})
    add_row("美国CPI同比(%)", cpi.get("latest_yoy") if cpi.get("latest_yoy") is not None else cpi.get("latest_value"), cpi.get("as_of_date"), "M", "macro_daily")
    nfp = context.get("nfp", {})
    add_row("美国非农新增(千人)", nfp.get("latest_mom_change") if nfp.get("latest_mom_change") is not None else nfp.get("latest_value"), nfp.get("as_of_date"), "M", "macro_daily")
    fed = context.get("fed", {})
    add_row("FEDFUNDS", fed.get("funds_rate", {}).get("latest_value"), fed.get("funds_rate", {}).get("as_of_date"), "M", "macro_daily")
    add_row("WALCL", fed.get("balance_sheet", {}).get("latest_value"), fed.get("balance_sheet", {}).get("as_of_date"), "W", "macro_daily")
    return rows


def _build_reporter_cards(context: Dict[str, Any], event_window_days: int) -> Dict[str, Any]:
    yld = context.get("yield_curve", {})
    gsr = context.get("gold_silver_ratio", {})
    cpi = context.get("cpi", {})
    nfp = context.get("nfp", {})
    fed = context.get("fed", {})
    news = context.get("news", {}).get("items", [])
    spread = yld.get("spread_10_2")
    spread_prev7 = yld.get("spread_10_2_prev7")
    spread_delta = (spread - spread_prev7) if (spread is not None and spread_prev7 is not None) else 0.0
    first_news = f"[{news[0].get('source')}] {news[0].get('title')}" if news and isinstance(news[0], dict) else "暂无新闻线索"
    cards = [
        {"module": "收益率记者", "facts": [f"10Y-2Y: {(spread if spread is not None else 0):+.2f}%", f"较7天: {spread_delta:+.2f}%"]},
        {"module": "贵金属记者", "facts": [f"金银比: {_safe_float(gsr.get('latest_value'), 0):.2f}", f"日变化: {_safe_float(gsr.get('change'), 0):+.2f}"]},
        {"module": "数据日历记者", "facts": [f"CPI: {'近期更新' if cpi.get('recently_updated') else '无新增'}", f"NFP: {'近期更新' if nfp.get('recently_updated') else '无新增'}", f"窗口: {event_window_days}天"]},
        {"module": "联储记者", "facts": [f"FEDFUNDS: {_safe_float(fed.get('funds_rate', {}).get('latest_value'), 0):.2f}%", f"WALCL: {_safe_float(fed.get('balance_sheet', {}).get('latest_value'), 0):,.0f}"]},
        {"module": "新闻记者", "facts": [first_news]},
    ]
    return {"cards": cards, "digest": " | ".join([f"{c['module']}:{(c['facts'] or [''])[0]}" for c in cards])}


def _collect_equity_bond_value_context(engine: Any, as_of_now: date) -> Dict[str, Any]:
    start_date = as_of_now - timedelta(days=EQUITY_BOND_LOOKBACK_DAYS)
    rank_start_date = as_of_now - timedelta(days=EQUITY_BOND_RANK_LOOKBACK_DAYS)
    # Bonds
    us_bond_hist = _fetch_macro_series(engine, ["US10Y", "DGS10"], rank_start_date)
    cn_bond_hist = _fetch_macro_series(engine, ["CN10Y"], rank_start_date)
    us_bond_series = us_bond_hist.get("US10Y") or us_bond_hist.get("DGS10") or []
    cn_bond_series = cn_bond_hist.get("CN10Y") or []

    # Equities (earnings yield proxy)
    us_eq_payload = _fetch_us_equity_earnings_yield(engine, rank_start_date)
    us_eq_series = us_eq_payload.get("series", [])
    cn_eq_series = _fetch_cn_equity_earnings_yield(engine, rank_start_date)

    # Monthly compression for visual clarity
    us_eq_monthly = _to_monthly_last(us_eq_series)
    cn_eq_monthly = _to_monthly_last(cn_eq_series)

    us_dates_all = [d for d, _ in us_eq_monthly]
    cn_dates_all = [d for d, _ in cn_eq_monthly]
    us_eq_vals_all = [v for _, v in us_eq_monthly]
    cn_eq_vals_all = [v for _, v in cn_eq_monthly]
    us_bond_vals_all = _align_series(us_dates_all, us_bond_series) if us_dates_all else []
    cn_bond_vals_all = _align_series(cn_dates_all, cn_bond_series) if cn_dates_all else []

    def _latest_gap(eq_vals: Sequence[Optional[float]], bond_vals: Sequence[Optional[float]]) -> Optional[float]:
        if not eq_vals or not bond_vals:
            return None
        e = eq_vals[-1]
        b = bond_vals[-1]
        if e is None or b is None:
            return None
        return float(e) - float(b)

    us_gap_series_all = [(d, (e - b)) for d, e, b in zip(us_dates_all, us_eq_vals_all, us_bond_vals_all) if e is not None and b is not None]
    cn_gap_series_all = [(d, (e - b)) for d, e, b in zip(cn_dates_all, cn_eq_vals_all, cn_bond_vals_all) if e is not None and b is not None]
    us_gap = us_gap_series_all[-1][1] if us_gap_series_all else None
    cn_gap = cn_gap_series_all[-1][1] if cn_gap_series_all else None

    us_pct3y = _percentile_rank([v for _, v in us_gap_series_all], us_gap)
    cn_pct3y = _percentile_rank([v for _, v in cn_gap_series_all], cn_gap)
    us_regime = _gap_regime_text(us_gap)
    cn_regime = _gap_regime_text(cn_gap)

    # Chart horizon keeps recent 1y for readability.
    us_dates = [d for d, _ in us_eq_monthly if d >= start_date]
    cn_dates = [d for d, _ in cn_eq_monthly if d >= start_date]
    us_eq_vals = _align_series(us_dates, us_eq_monthly) if us_dates else []
    cn_eq_vals = _align_series(cn_dates, cn_eq_monthly) if cn_dates else []
    us_bond_vals = _align_series(us_dates, us_bond_series) if us_dates else []
    cn_bond_vals = _align_series(cn_dates, cn_bond_series) if cn_dates else []

    if us_gap is not None and cn_gap is not None:
        compare_text = "当前A股相对更有性价比" if cn_gap > us_gap else "当前美股相对更有性价比"
    else:
        compare_text = "双市场数据不完整，本期仅展示可得样本"

    us_source = str(us_eq_payload.get("source") or "unavailable")
    return {
        "us": {
            "dates": us_dates,
            "equity_yield": us_eq_vals,
            "bond_yield": us_bond_vals,
            "latest_gap": us_gap,
            "gap_percentile_3y": us_pct3y,
            "gap_sample_3y": len(us_gap_series_all),
            "regime": us_regime,
            "latest_equity_yield": (us_eq_vals[-1] if us_eq_vals else None),
            "latest_bond_yield": (us_bond_vals[-1] if us_bond_vals else None),
            "equity_label": "美股盈利收益率(E/P)",
            "bond_label": "美债10Y",
            "equity_source": us_source,
            "bond_source": "macro_daily",
            "db_has_equity_data": bool(us_eq_payload.get("has_db", False)),
        },
        "cn": {
            "dates": cn_dates,
            "equity_yield": cn_eq_vals,
            "bond_yield": cn_bond_vals,
            "latest_gap": cn_gap,
            "gap_percentile_3y": cn_pct3y,
            "gap_sample_3y": len(cn_gap_series_all),
            "regime": cn_regime,
            "latest_equity_yield": (cn_eq_vals[-1] if cn_eq_vals else None),
            "latest_bond_yield": (cn_bond_vals[-1] if cn_bond_vals else None),
            "equity_label": "沪深300盈利收益率(E/P)",
            "bond_label": "中国10Y国债",
            "equity_source": "index_valuation",
            "bond_source": "macro_daily",
        },
        "summary": compare_text,
    }


def collect_macro_context(event_window_days: int, chart_lookback_days: int) -> Dict[str, Any]:
    from data_engine import engine

    as_of_now = datetime.now().date()
    yield_curve = _collect_yield_context(engine, as_of_now, chart_lookback_days)
    gold_silver = _collect_gold_silver_context(engine, as_of_now, chart_lookback_days)
    cpi = _collect_indicator_event_context(engine, "CPIAUCSL", "US CPI", "M", as_of_now, event_window_days)
    nfp = _collect_indicator_event_context(engine, "PAYEMS", "US NFP", "M", as_of_now, event_window_days)
    fed = {
        "funds_rate": _collect_indicator_event_context(engine, "FEDFUNDS", "Fed Funds", "M", as_of_now, event_window_days),
        "balance_sheet": _collect_indicator_event_context(engine, "WALCL", "Fed Balance Sheet", "W", as_of_now, event_window_days),
    }

    fed_start = as_of_now - timedelta(days=FED_CHART_LOOKBACK_DAYS + 10)
    fed_hist = _fetch_macro_series(engine, ["FEDFUNDS", "WALCL"], fed_start)
    fed["funds_rate"]["series"] = (fed_hist.get("FEDFUNDS") or [])[-FED_CHART_LOOKBACK_DAYS:]
    fed["balance_sheet"]["series"] = (fed_hist.get("WALCL") or [])[-FED_CHART_LOOKBACK_DAYS:]
    macro_hist = _fetch_macro_series(engine, ["CPIAUCSL", "PAYEMS"], as_of_now - timedelta(days=420))
    cpi_level_series = macro_hist.get("CPIAUCSL") or []
    nfp_level_series = macro_hist.get("PAYEMS") or []
    cpi["series_level"] = cpi_level_series[-CPI_NFP_LOOKBACK_DAYS:]
    nfp["series_level"] = nfp_level_series[-CPI_NFP_LOOKBACK_DAYS:]

    cpi_yoy_series = _compute_yoy_series(cpi_level_series)
    nfp_mom_series = _compute_mom_delta_series(nfp_level_series)
    cpi["series"] = cpi_yoy_series[-CPI_NFP_LOOKBACK_DAYS:]
    nfp["series"] = nfp_mom_series[-CPI_NFP_LOOKBACK_DAYS:]

    if cpi["series"]:
        cpi["latest_yoy"] = cpi["series"][-1][1]
        cpi["previous_yoy"] = cpi["series"][-2][1] if len(cpi["series"]) >= 2 else None
        cpi["yoy_change"] = _calc_change(cpi.get("latest_yoy"), cpi.get("previous_yoy"))
    else:
        cpi["latest_yoy"] = None
        cpi["previous_yoy"] = None
        cpi["yoy_change"] = None

    if nfp["series"]:
        nfp["latest_mom_change"] = nfp["series"][-1][1]
        nfp["previous_mom_change"] = nfp["series"][-2][1] if len(nfp["series"]) >= 2 else None
        nfp["mom_change_delta"] = _calc_change(nfp.get("latest_mom_change"), nfp.get("previous_mom_change"))
    else:
        nfp["latest_mom_change"] = None
        nfp["previous_mom_change"] = None
        nfp["mom_change_delta"] = None

    ctx = {
        "as_of_now": as_of_now,
        "yield_curve": yield_curve,
        "gold_silver_ratio": gold_silver,
        "cpi": cpi,
        "nfp": nfp,
        "fed": fed,
        "equity_bond_value": _collect_equity_bond_value_context(engine, as_of_now),
        "news": _collect_news_context(),
        "calendar": {"next_fomc_date": _next_fomc_date(as_of_now), "next_cpi_date": _next_cpi_date(as_of_now), "next_nfp_date": _next_nfp_date(as_of_now)},
    }
    ctx["reporter_cards"] = _build_reporter_cards(ctx, event_window_days)
    ctx["freshness_rows"] = _build_freshness_rows(ctx, as_of_now)
    return ctx


def _build_yield_curve_chart_json(yield_ctx: Dict[str, Any]) -> str:
    if go is None:
        return json.dumps({"data": [], "layout": {}})
    tenors = ["2Y", "10Y", "30Y"]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=tenors, y=[yield_ctx.get("current", {}).get(k) for k in tenors], mode="lines+markers", name="当前", line=dict(color="#f59e0b", width=3), marker=dict(size=8)))
    fig.add_trace(go.Scatter(x=tenors, y=[yield_ctx.get("prev7", {}).get(k) for k in tenors], mode="lines+markers", name="7天前", line=dict(color="#60a5fa", width=2, dash="dot"), marker=dict(size=7)))
    fig.update_layout(template="plotly_dark", height=340, margin=dict(l=36, r=26, t=20, b=20), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
    fig.update_yaxes(title_text="收益率(%)", zeroline=False)
    return fig.to_json()


def _build_gold_silver_chart_json(gs_ctx: Dict[str, Any], lookback_days: int) -> str:
    if go is None:
        return json.dumps({"data": [], "layout": {}})
    series = gs_ctx.get("series", [])[-max(2, int(lookback_days)) :]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=[d.strftime("%Y-%m-%d") for d, _ in series], y=[v for _, v in series], mode="lines", name="金银比", line=dict(color="#fbbf24", width=2.6)))
    fig.update_layout(title=f"金银比趋势（近{lookback_days}天）", template="plotly_dark", height=340, margin=dict(l=28, r=18, t=64, b=28), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(showgrid=False))
    fig.update_yaxes(title_text="金银比", color="#fbbf24")
    return fig.to_json()


def _build_cpi_nfp_chart_json(cpi_ctx: Dict[str, Any], nfp_ctx: Dict[str, Any]) -> str:
    if go is None or make_subplots is None:
        return json.dumps({"data": [], "layout": {}})
    cpi_series = cpi_ctx.get("series", [])[-CPI_NFP_LOOKBACK_DAYS:]
    nfp_series = nfp_ctx.get("series", [])[-CPI_NFP_LOOKBACK_DAYS:]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(
        go.Scatter(
            x=[d.strftime("%Y-%m-%d") for d, _ in cpi_series],
            y=[v for _, v in cpi_series],
            mode="lines+markers",
            name="CPI同比(%)",
            line=dict(color="#f59e0b", width=2.4),
            marker=dict(size=5),
        ),
        secondary_y=False,
    )
    nfp_vals = [v for _, v in nfp_series]
    nfp_colors = ["rgba(96,165,250,0.65)" if v >= 0 else "rgba(248,113,113,0.65)" for v in nfp_vals]
    fig.add_trace(
        go.Bar(
            x=[d.strftime("%Y-%m-%d") for d, _ in nfp_series],
            y=nfp_vals,
            name="非农新增(千人)",
            marker_color=nfp_colors,
        ),
        secondary_y=True,
    )
    fig.update_layout(template="plotly_dark", height=350, margin=dict(l=36, r=56, t=20, b=24), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(showgrid=False), barmode="relative", showlegend=False)
    fig.update_yaxes(title_text="CPI同比(%)", secondary_y=False, color="#fcd34d")
    fig.update_yaxes(title_text="非农新增(千人)", secondary_y=True, color="#93c5fd", zeroline=True, zerolinecolor="rgba(148,163,184,0.45)")
    return fig.to_json()


def _build_fed_policy_chart_json(fed_ctx: Dict[str, Any], lookback_days: int) -> str:
    if go is None or make_subplots is None:
        return json.dumps({"data": [], "layout": {}})
    fs = fed_ctx.get("funds_rate", {}).get("series", [])[-max(2, int(lookback_days)) :]
    ws = fed_ctx.get("balance_sheet", {}).get("series", [])[-max(2, int(lookback_days)) :]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=[d.strftime("%Y-%m-%d") for d, _ in fs], y=[v for _, v in fs], mode="lines+markers", name="利率", line=dict(color="#60a5fa", width=2.6), marker=dict(size=5)), secondary_y=False)
    fig.add_trace(go.Scatter(x=[d.strftime("%Y-%m-%d") for d, _ in ws], y=[v for _, v in ws], mode="lines", name="资产负债表", line=dict(color="#34d399", width=2.4)), secondary_y=True)
    fig.update_layout(template="plotly_dark", height=350, margin=dict(l=40, r=82, t=20, b=24), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", xaxis=dict(showgrid=False, automargin=True), showlegend=False)
    funds_vals = [v for _, v in fs if v is not None]
    walcl_vals = [v for _, v in ws if v is not None]
    if funds_vals:
        ymin = min(funds_vals)
        ymax = max(funds_vals)
        pad = max((ymax - ymin) * 0.50, 0.20)
        fig.update_yaxes(range=[ymin - pad, ymax + pad], secondary_y=False, title_text="联邦基金利率(%)", color="#93c5fd", title_standoff=12, automargin=True)
    else:
        fig.update_yaxes(title_text="联邦基金利率(%)", secondary_y=False, color="#93c5fd", title_standoff=12, automargin=True)
    if walcl_vals:
        ymin = min(walcl_vals)
        ymax = max(walcl_vals)
        pad = max((ymax - ymin) * 0.16, 8000)
        fig.update_yaxes(range=[ymin - pad, ymax + pad], secondary_y=True, title_text="美联储资产负债表(百万美元)", color="#86efac", title_standoff=18, automargin=True)
    else:
        fig.update_yaxes(title_text="美联储资产负债表(百万美元)", secondary_y=True, color="#86efac", title_standoff=18, automargin=True)
    return fig.to_json()


def _build_equity_bond_value_chart_json(value_ctx: Dict[str, Any]) -> str:
    if go is None or make_subplots is None:
        return json.dumps({"data": [], "layout": {}})

    us = value_ctx.get("us", {})
    cn = value_ctx.get("cn", {})
    us_dates = [d.strftime("%Y-%m") if isinstance(d, date) else str(d) for d in us.get("dates", [])]
    cn_dates = [d.strftime("%Y-%m") if isinstance(d, date) else str(d) for d in cn.get("dates", [])]

    fig = make_subplots(rows=1, cols=2)
    fig.add_trace(
        go.Scatter(x=us_dates, y=us.get("equity_yield", []), mode="lines+markers", name="美股E/P", line=dict(color="#f59e0b", width=2.3)),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=us_dates, y=us.get("bond_yield", []), mode="lines", name="美债10Y", line=dict(color="#60a5fa", width=2.0, dash="dot")),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=cn_dates, y=cn.get("equity_yield", []), mode="lines+markers", name="A股E/P", line=dict(color="#f59e0b", width=2.3)),
        row=1,
        col=2,
    )
    fig.add_trace(
        go.Scatter(x=cn_dates, y=cn.get("bond_yield", []), mode="lines", name="中国10Y", line=dict(color="#60a5fa", width=2.0, dash="dot")),
        row=1,
        col=2,
    )

    fig.update_layout(
        template="plotly_dark",
        height=360,
        margin=dict(l=36, r=28, t=36, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
    )
    fig.update_xaxes(showgrid=False)
    fig.update_yaxes(title_text="收益率(%)", row=1, col=1)
    fig.update_yaxes(title_text="收益率(%)", row=1, col=2)
    return fig.to_json()


def _extract_json_obj(raw_text: str) -> Dict[str, Any]:
    txt = str(raw_text or "").strip()
    if not txt:
        return {}
    if txt.startswith("```"):
        txt = re.sub(r"^```(?:json)?\s*", "", txt)
        txt = re.sub(r"\s*```$", "", txt)
    m = re.search(r"\{[\s\S]*\}", txt)
    if not m:
        return {}
    try:
        obj = json.loads(m.group(0))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _validate_analysis_obj(obj: Dict[str, Any]) -> bool:
    if not isinstance(obj, dict):
        return False
    return all(str(obj.get(k, "")).strip() for k in REQUIRED_ANALYSIS_KEYS)


def _build_rule_based_analysis(context: Dict[str, Any], event_window_days: int) -> Dict[str, str]:
    yld = context.get("yield_curve", {})
    gsr = context.get("gold_silver_ratio", {})
    cpi = context.get("cpi", {})
    nfp = context.get("nfp", {})
    fed = context.get("fed", {})
    news_items = context.get("news", {}).get("items", [])
    news_lines = context.get("news", {}).get("lines", [])

    spread = yld.get("spread_10_2")
    spread_prev7 = yld.get("spread_10_2_prev7")
    spread_delta = ((spread - spread_prev7) if (spread is not None and spread_prev7 is not None) else 0.0)
    spread_tag = "倒挂" if (spread is not None and spread < 0) else "正斜率"
    gsr_change = _safe_float(gsr.get("change"), 0.0)

    cpi_recent = bool(cpi.get("recently_updated"))
    nfp_recent = bool(nfp.get("recently_updated"))
    if cpi_recent or nfp_recent:
        parts = []
        if cpi_recent:
            cpi_yoy = cpi.get("latest_yoy")
            cpi_yoy_prev = cpi.get("previous_yoy")
            if cpi_yoy is not None:
                parts.append(f"CPI刚更新，最新同比 {_safe_float(cpi_yoy, 0):.2f}%（前值 {_safe_float(cpi_yoy_prev, 0):.2f}%）")
            else:
                parts.append(f"CPI刚更新，最新值 {_safe_float(cpi.get('latest_value'), 0):.2f}")
        if nfp_recent:
            nfp_add = nfp.get("latest_mom_change")
            if nfp_add is not None:
                parts.append(f"非农刚更新，本月新增 {_safe_float(nfp_add, 0):+.0f} 千人")
            else:
                parts.append(f"非农刚更新，最新总量 {_safe_float(nfp.get('latest_value'), 0):,.0f} 千人")
        cpi_nfp_comment = "；".join(parts) + "。通常这类数据发布后，市场会在1-2个交易日内重定价降息路径。"
    else:
        cpi_nfp_comment = f"近{event_window_days}天 CPI 和非农都没有新增发布。市场主要在旧框架里交易预期差，盘面容易来回拉扯。"

    overview = (
        f"今天主线还是“利差+流动性”：10Y-2Y {(spread if spread is not None else 0):+.2f}%（{spread_tag}），"
        f"金银比日变化 {gsr_change:+.2f}。翻译成人话：市场还在试探，不是一路顺风。"
    )
    yield_curve_comment = (
        f"收益率曲线现在是{spread_tag}，10Y-2Y {(spread if spread is not None else 0):+.2f}%；较7天前 {spread_delta:+.2f}%。"
        "如果长端继续往上、短端不松，估值敏感资产会先感到压力。"
    )
    gold_silver_comment = (
        f"金银比最新 {_safe_float(gsr.get('latest_value'), 0):.2f}，日变化 {gsr_change:+.2f}。"
        "它像“风险情绪温度计”：上行偏防守，下行偏风险偏好回暖。"
    )
    fed_policy_comment = (
        f"联邦基金利率约 {_safe_float(fed.get('funds_rate', {}).get('latest_value'), 0):.2f}%，"
        f"美联储资产负债表约 {_safe_float(fed.get('balance_sheet', {}).get('latest_value'), 0):,.0f}（百万美元）。"
        "简单说：利率管价格，资产负债表管市场水位。"
    )

    geo_terms = ("war", "conflict", "sanction", "tariff", "middle east", "ukraine", "iran", "israel", "关税", "制裁", "冲突", "中东", "俄乌")
    has_geo = any(any(t in str(v).lower() for t in geo_terms) for it in news_items for v in it.values()) if news_items else False
    top_lines = [f"{x.get('source', '新闻源')}:{x.get('title', '')}" for x in news_items[:3] if isinstance(x, dict) and x.get("title")]
    joined = "；".join(top_lines[:2]) if top_lines else "；".join(news_lines[:2])
    geo_text = "地缘冲突会先推油价和运费，再传导到通胀与利率预期，所以会影响经济资产定价。" if has_geo else "今天地缘变量不是主角，但依旧是会随时插队的风险项。"
    macro_news_comment = (
        f"今天新闻合在一起看，核心是“增长没塌，但政策转向不会太快”：{joined}。"
        "术语小翻译：所谓“风险偏好”，就是大家敢不敢买波动更大的资产，比如成长股。"
        f"{geo_text}"
    )

    # Rule-based allocation: spread + gold/silver ratio + macro event freshness + news risk tone.
    if (spread is not None and spread < 0) or gsr_change > 0:
        alloc = {"gold": 25, "stock_def": 18, "stock_att": 7, "treasury": 28, "corp": 10, "cash": 12, "style": "防守型"}
    elif (spread is not None and spread > 0) and gsr_change < 0:
        alloc = {"gold": 15, "stock_def": 10, "stock_att": 25, "treasury": 22, "corp": 18, "cash": 10, "style": "进攻型"}
    else:
        alloc = {"gold": 20, "stock_def": 14, "stock_att": 14, "treasury": 25, "corp": 15, "cash": 12, "style": "均衡型"}
    allocation_advice_comment = (
        f"建议基于四个信号：10Y-2Y利差、金银比、CPI/非农是否刚更新、新闻风险语气。当前偏{alloc['style']}配置（示例）：\n"
        f"- 黄金 {alloc['gold']}%\n"
        f"- 股票 {alloc['stock_def'] + alloc['stock_att']}%（防守型 {alloc['stock_def']}% / 攻击型 {alloc['stock_att']}%）\n"
        f"- 国债 {alloc['treasury']}%\n"
        f"- 公司债 {alloc['corp']}%\n"
        f"- 现金 {alloc['cash']}%\n"
        "以上为模型化战术参考，不构成个性化投资建议。"
    )

    return {
        "overview": overview,
        "yield_curve_comment": yield_curve_comment,
        "gold_silver_comment": gold_silver_comment,
        "cpi_nfp_comment": cpi_nfp_comment,
        "fed_policy_comment": fed_policy_comment,
        "macro_news_comment": macro_news_comment,
        "allocation_advice_comment": allocation_advice_comment,
    }


def _build_llm_analysis(context: Dict[str, Any], event_window_days: int, use_llm: bool) -> Tuple[Dict[str, str], str]:
    fallback = _build_rule_based_analysis(context, event_window_days=event_window_days)
    if not use_llm:
        return fallback, "disabled_by_flag"
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        return fallback, "missing_api_key"
    try:
        from langchain_core.messages import HumanMessage
        from llm_compat import ChatTongyiCompat as ChatTongyi

        llm = ChatTongyi(model=os.getenv("MACRO_RADAR_LLM_MODEL", "qwen-plus"), api_key=api_key, temperature=0.35, max_retries=1)
        llm_input = {
            "yield_curve": context.get("yield_curve", {}),
            "gold_silver_ratio": context.get("gold_silver_ratio", {}),
            "cpi": context.get("cpi", {}),
            "nfp": context.get("nfp", {}),
            "fed": context.get("fed", {}),
            "calendar": context.get("calendar", {}),
            "news_items": context.get("news", {}).get("items", [])[:8],
        }
        prompt = f"""
你是“交易汇AI宏观主编”。用轻松、通俗、易懂的语气写分析，不要学术腔。
只输出 JSON，必须包含：
overview
yield_curve_comment
gold_silver_comment
cpi_nfp_comment
fed_policy_comment
macro_news_comment
allocation_advice_comment

要求：
1) 每段 2-4 句，先结论后解释。
2) 术语要解释并举例（如“风险偏好”）。
3) 新闻段写综合观点，不逐条抄新闻。
4) 可加入地缘冲突对油价/通胀/利率预期的传导影响。
5) 投资建议需给黄金、股票(攻击/防守)、国债、公司债、现金的百分比并说明依据。
6) 若CPI/NFP近{event_window_days}天更新，需点名影响；否则写“本期无新增发布”。

输入：
{json.dumps(llm_input, ensure_ascii=False, default=str)}
""".strip()
        rsp = llm.invoke([HumanMessage(content=prompt)])
        parsed = _extract_json_obj(str(getattr(rsp, "content", rsp)))
        if _validate_analysis_obj(parsed):
            return {k: str(parsed.get(k, "")).strip() for k in REQUIRED_ANALYSIS_KEYS}, "llm"
        return fallback, "llm_parse_failed"
    except Exception:
        return fallback, "llm_exception"


def _text_to_html(text: str) -> str:
    escaped = html.escape(str(text or "").strip())
    escaped = escaped.replace("\r\n", "\n").replace("\r", "\n").replace("\n", "<br>")
    return escaped or "N/A"


def _render_freshness_table(rows: Sequence[Dict[str, Any]]) -> str:
    tr = []
    for row in rows:
        value = row.get("value")
        value_txt = f"{value:,.2f}" if isinstance(value, (int, float)) else ("-" if value in (None, "") else str(value))
        tr.append("<tr>" f"<td>{html.escape(str(row.get('name', '-')))}</td>" f"<td>{html.escape(value_txt)}</td>" f"<td>{html.escape(str(row.get('as_of_date', '-')))}</td>" f"<td>{html.escape(str(row.get('source', '-')))}</td>" f"<td>{html.escape(str(row.get('status', '-')))}</td>" f"<td>{html.escape(str(row.get('stale_days', '-')))}</td>" "</tr>")
    return "".join(tr)


def _fmt_date(value: Any) -> str:
    d = _to_date(value)
    return d.strftime("%Y-%m-%d") if d else "-"


def _fmt_num(value: Any, digits: int = 2) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):,.{digits}f}"
    except Exception:
        return str(value)


def _fmt_signed(value: Any, digits: int = 2, suffix: str = "") -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):+,.{digits}f}{suffix}"
    except Exception:
        return str(value)


def _allocation_method_text(context: Dict[str, Any], event_window_days: int) -> str:
    yld = context.get("yield_curve", {})
    gsr = context.get("gold_silver_ratio", {})
    cpi = context.get("cpi", {})
    nfp = context.get("nfp", {})
    spread = yld.get("spread_10_2")
    gsr_change = _safe_float(gsr.get("change"), 0.0)
    cpi_recent = bool(cpi.get("recently_updated"))
    nfp_recent = bool(nfp.get("recently_updated"))

    risk_tag = "偏防守" if ((spread is not None and spread < 0) or gsr_change > 0) else ("偏进攻" if ((spread is not None and spread > 0) and gsr_change < 0) else "均衡")
    return (
        f"建议是按“四因子框架”生成：1) 10Y-2Y利差（衡量经济预期），2) 金银比变化（衡量避险温度），"
        f"3) CPI/非农在最近{event_window_days}天是否更新（衡量事件冲击），4) 新闻风险语气（衡量外部扰动）。"
        f"当前信号组合判定为「{risk_tag}」。"
    )


def render_macro_radar_html(
    generated_at: datetime,
    context: Dict[str, Any],
    analysis: Dict[str, str],
    analysis_source: str,
    event_window_days: int,
    chart_lookback_days: int,
) -> str:
    yield_ctx = context.get("yield_curve", {})
    gsr_ctx = context.get("gold_silver_ratio", {})
    cpi_ctx = context.get("cpi", {})
    nfp_ctx = context.get("nfp", {})
    fed_ctx = context.get("fed", {})
    value_ctx = context.get("equity_bond_value", {})
    calendar = context.get("calendar", {})
    freshness_rows = context.get("freshness_rows", [])
    news_items = context.get("news", {}).get("items", [])

    spread = yield_ctx.get("spread_10_2")
    walcl_change = fed_ctx.get("balance_sheet", {}).get("change")
    cpi_flag = "近期更新" if cpi_ctx.get("recently_updated") else "本期无新增发布"
    nfp_flag = "近期更新" if nfp_ctx.get("recently_updated") else "本期无新增发布"
    cpi_latest_yoy = cpi_ctx.get("latest_yoy") if cpi_ctx.get("latest_yoy") is not None else cpi_ctx.get("latest_value")
    nfp_latest_add = nfp_ctx.get("latest_mom_change") if nfp_ctx.get("latest_mom_change") is not None else nfp_ctx.get("latest_value")

    published_text = generated_at.strftime("%Y-%m-%d %H:%M:%S")
    explain_source = "交易汇AI宏观主编" if analysis_source in {"llm", "llm_parse_failed", "llm_exception"} else "交易汇AI宏观主编（规则回退）"
    news_count = len([x for x in news_items if isinstance(x, dict) and x.get("title")])

    curve_json = _build_yield_curve_chart_json(yield_ctx)
    gsr_json = _build_gold_silver_chart_json(gsr_ctx, chart_lookback_days)
    cpi_nfp_json = _build_cpi_nfp_chart_json(cpi_ctx, nfp_ctx)
    fed_json = _build_fed_policy_chart_json(fed_ctx, FED_CHART_LOOKBACK_DAYS)
    value_json = _build_equity_bond_value_chart_json(value_ctx)

    allocation_basis = _allocation_method_text(context, event_window_days=event_window_days)
    overview_html = _text_to_html(analysis.get("overview", ""))
    yc_html = _text_to_html(analysis.get("yield_curve_comment", ""))
    gs_html = _text_to_html(analysis.get("gold_silver_comment", ""))
    cpi_nfp_html = _text_to_html(analysis.get("cpi_nfp_comment", ""))
    fed_html = _text_to_html(analysis.get("fed_policy_comment", ""))
    news_html = _text_to_html(analysis.get("macro_news_comment", ""))
    alloc_html = _text_to_html(analysis.get("allocation_advice_comment", ""))
    freshness_html = _render_freshness_table(freshness_rows)

    return f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{html.escape(CHANNEL_NAME)}</title>
  <style>
    body {{ margin:0; background:#0b1220; color:#dbe7ff; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif; }}
    .container {{ max-width:980px; margin:0 auto; padding:24px 16px 48px; }}
    .hero {{ background:linear-gradient(135deg,#132746,#1d3662); border:1px solid rgba(148,163,184,.25); border-radius:14px; padding:18px 20px; }}
    .hero h1 {{ margin:0 0 10px 0; font-size:28px; color:#f8fbff; }}
    .meta {{ color:#9fb3d9; font-size:13px; }}
    .tag {{ display:inline-block; margin-top:10px; background:rgba(96,165,250,.18); border:1px solid rgba(96,165,250,.45); color:#dbeafe; border-radius:999px; padding:5px 10px; font-size:12px; }}
    .card {{ background:rgba(15,23,42,.76); border:1px solid rgba(148,163,184,.22); border-radius:12px; padding:14px; margin-top:12px; }}
    .card h2 {{ margin:0 0 8px 0; font-size:19px; color:#d9e8ff; }}
    .card p {{ margin:8px 0; line-height:1.72; color:#d0ddf7; }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(230px,1fr)); gap:8px; margin-top:8px; }}
    .kpi {{ background:rgba(30,41,59,.65); border:1px solid rgba(148,163,184,.2); border-radius:10px; padding:10px; }}
    .kpi .k {{ color:#9fb3d9; font-size:12px; }}
    .kpi .v {{ color:#f8fbff; font-size:18px; font-weight:700; margin-top:4px; }}
    .chart {{ width:100%; height:360px; }}
    .fallback {{ margin-top:8px; color:#a7bddf; font-size:13px; line-height:1.6; }}
    table {{ width:100%; border-collapse:collapse; margin-top:8px; }}
    th, td {{ border-bottom:1px solid rgba(148,163,184,.2); text-align:left; padding:8px 6px; font-size:13px; vertical-align:top; }}
    th {{ color:#9fb3d9; font-weight:600; }}
    .muted {{ color:#8ea5ca; font-size:12px; line-height:1.6; }}
    .hint {{ margin-top:8px; padding:8px 10px; border-radius:8px; border:1px dashed rgba(148,163,184,.35); background:rgba(30,41,59,.35); color:#b9c9e6; font-size:12px; }}
  </style>
</head>
<body>
  <div class="container">
    <div class="hero">
      <h1>🌍 宏观周报</h1>
      <div class="meta">发布时间：{published_text} (Asia/Shanghai)</div>
      <div class="tag">解释来源：{html.escape(explain_source)}</div>
    </div>

    <div class="card">
      <h2>1) 今日宏观结论</h2>
      <p>{overview_html}</p>
      <div class="grid">
        <div class="kpi"><div class="k">10Y-2Y 利差</div><div class="v">{_fmt_signed(spread, 2, "%")}</div></div>
        <div class="kpi"><div class="k">金银比日变化</div><div class="v">{_fmt_signed(gsr_ctx.get("change"), 2)}</div></div>
        <div class="kpi"><div class="k">下次 CPI</div><div class="v">{_fmt_date(calendar.get("next_cpi_date"))}</div></div>
      </div>
      <div class="hint">{html.escape(allocation_basis)}</div>
    </div>

    <div class="card">
      <h2>2) 美国国债收益率曲线</h2>
      <div class="muted">图例：橙线=当前收益率，蓝虚线=7天前。</div>
      <div id="yield-curve-chart" class="chart"></div>
      <p>{yc_html}</p>
      <div class="fallback">降级摘要：2Y {_fmt_num(yield_ctx.get("current", {}).get("2Y"))}% / 10Y {_fmt_num(yield_ctx.get("current", {}).get("10Y"))}% / 30Y {_fmt_num(yield_ctx.get("current", {}).get("30Y"))}%</div>
    </div>

    <div class="card">
      <h2>3) 金银比（近 {int(chart_lookback_days)} 天）</h2>
      <div id="gold-silver-chart" class="chart"></div>
      <p>{gs_html}</p>
      <div class="fallback">降级摘要：金银比 {_fmt_num(gsr_ctx.get("latest_value"))}（数据日期 {_fmt_date(gsr_ctx.get("as_of_date"))}）</div>
    </div>

    <div class="card">
      <h2>4) CPI 与非农（最近 {int(event_window_days)} 天事件窗口 + 半年走势）</h2>
      <div class="muted">图例：橙线=CPI同比（%），柱体=非农新增（千人，蓝正红负）。</div>
      <div class="grid">
        <div class="kpi"><div class="k">CPI 状态</div><div class="v">{html.escape(cpi_flag)}</div></div>
        <div class="kpi"><div class="k">非农状态</div><div class="v">{html.escape(nfp_flag)}</div></div>
      </div>
      <div class="grid">
        <div class="kpi"><div class="k">最近一次 CPI 同比</div><div class="v">{_fmt_num(cpi_latest_yoy)}%（{_fmt_date(cpi_ctx.get("as_of_date"))}）</div></div>
        <div class="kpi"><div class="k">最近一次非农新增</div><div class="v">{_fmt_signed(nfp_latest_add, 0)} 千人（{_fmt_date(nfp_ctx.get("as_of_date"))}）</div></div>
      </div>
      <div class="grid">
        <div class="kpi"><div class="k">下次 CPI 日期</div><div class="v">{_fmt_date(calendar.get("next_cpi_date"))}</div></div>
        <div class="kpi"><div class="k">下次非农日期</div><div class="v">{_fmt_date(calendar.get("next_nfp_date"))}</div></div>
      </div>
      <div id="cpi-nfp-chart" class="chart"></div>
      <div class="fallback">降级摘要：CPI同比 {_fmt_num(cpi_latest_yoy)}%（{_fmt_date(cpi_ctx.get("as_of_date"))}） / 非农新增 {_fmt_signed(nfp_latest_add, 0)} 千人（{_fmt_date(nfp_ctx.get("as_of_date"))}）</div>
      <p>{cpi_nfp_html}</p>
    </div>

    <div class="card">
      <h2>5) 美联储利率与资产负债表（近 {FED_CHART_LOOKBACK_DAYS} 天）</h2>
      <div class="muted">下次美联储议息会议：{_fmt_date(calendar.get("next_fomc_date"))}</div>
      <div class="muted">图例：蓝线=联邦基金利率，绿线=资产负债表规模。</div>
      <div id="fed-policy-chart" class="chart"></div>
      <p>{fed_html}</p>
      <div class="muted">联邦基金利率: {_fmt_num(fed_ctx.get("funds_rate", {}).get("latest_value"))}% | 资产负债表: {_fmt_num(fed_ctx.get("balance_sheet", {}).get("latest_value"), 0)}（百万美元）</div>
      <div class="muted">联储总资产一周变化 = 本周资产负债表减上一周；为正通常代表流动性边际增加，为负通常代表流动性边际回收。当前：{_fmt_signed(walcl_change, 0)}（百万美元）。</div>
    </div>

    <div class="card">
      <h2>6) 双市场股债性价比（股票E/P vs 10Y国债）</h2>
      <div class="muted">判断逻辑：股票盈利收益率(E/P) 高于国债收益率越多，股票相对“性价比”通常越高。</div>
      <div class="muted">左图=美股(E/P vs 美债10Y)，右图=A股(E/P vs 中国10Y)。</div>
      <div id="equity-bond-value-chart" class="chart"></div>
      <div class="grid">
        <div class="kpi"><div class="k">美股相对性价比(E/P-10Y)</div><div class="v">{_fmt_signed(value_ctx.get("us", {}).get("latest_gap"), 2, "%")}</div></div>
        <div class="kpi"><div class="k">A股相对性价比(E/P-10Y)</div><div class="v">{_fmt_signed(value_ctx.get("cn", {}).get("latest_gap"), 2, "%")}</div></div>
        <div class="kpi"><div class="k">综合结论</div><div class="v">{html.escape(str(value_ctx.get("summary") or "-"))}</div></div>
      </div>
      <div class="grid">
        <div class="kpi"><div class="k">美股股债比分位(3年)</div><div class="v">{_fmt_num(value_ctx.get("us", {}).get("gap_percentile_3y"))}%</div></div>
        <div class="kpi"><div class="k">A股股债比分位(3年)</div><div class="v">{_fmt_num(value_ctx.get("cn", {}).get("gap_percentile_3y"))}%</div></div>
      </div>
      <div class="muted">分位样本：美股 {int(value_ctx.get("us", {}).get("gap_sample_3y") or 0)} 期，A股 {int(value_ctx.get("cn", {}).get("gap_sample_3y") or 0)} 期。</div>
      <div class="fallback">分位口径：基于近3年月频样本，对“股债比 = E/P-10Y”进行历史百分位计算。美股估值优先读数据库别名(SP500_PE/SP500PE/SPX_PE/US_PE)，缺失时回退 multpl 网页抓取。</div>
    </div>

    <div class="card">
      <h2>7) 当日宏观新闻热点</h2>
      <p>{news_html}</p>
      <div class="muted">本段基于多源新闻综合归纳（本期有效线索：{news_count} 条），不逐条罗列。</div>
    </div>

    <div class="card">
      <h2>8) 投资建议（示例配置）</h2>
      <p>{alloc_html}</p>
      <div class="hint">建议依据：{html.escape(allocation_basis)}</div>
    </div>

    <div class="card">
      <h2>9) 数据新鲜度</h2>
      <table>
        <thead><tr><th>指标</th><th>最新值</th><th>as_of_date</th><th>source</th><th>freshness</th><th>stale_days</th></tr></thead>
        <tbody>{freshness_html}</tbody>
      </table>
    </div>
  </div>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <script>
    (function () {{
      var curveFigure = {curve_json};
      var goldSilverFigure = {gsr_json};
      var cpiNfpFigure = {cpi_nfp_json};
      var fedFigure = {fed_json};

      if (window.Plotly && curveFigure && curveFigure.data) {{
        Plotly.newPlot('yield-curve-chart', curveFigure.data, curveFigure.layout, {{ displayModeBar: false, responsive: true }});
      }}
      if (window.Plotly && goldSilverFigure && goldSilverFigure.data) {{
        Plotly.newPlot('gold-silver-chart', goldSilverFigure.data, goldSilverFigure.layout, {{ displayModeBar: false, responsive: true }});
      }}
      if (window.Plotly && cpiNfpFigure && cpiNfpFigure.data) {{
        Plotly.newPlot('cpi-nfp-chart', cpiNfpFigure.data, cpiNfpFigure.layout, {{ displayModeBar: false, responsive: true }});
      }}
      if (window.Plotly && fedFigure && fedFigure.data) {{
        Plotly.newPlot('fed-policy-chart', fedFigure.data, fedFigure.layout, {{ displayModeBar: false, responsive: true }});
      }}
      var valueFigure = {value_json};
      if (window.Plotly && valueFigure && valueFigure.data) {{
        Plotly.newPlot('equity-bond-value-chart', valueFigure.data, valueFigure.layout, {{ displayModeBar: false, responsive: true }});
      }}
    }})();
  </script>
</body>
</html>
"""


def extract_summary_from_html(html_content: str) -> str:
    txt = re.sub(r"(?is)<script.*?>.*?</script>", " ", str(html_content or ""))
    txt = re.sub(r"(?is)<style.*?>.*?</style>", " ", txt)
    txt = re.sub(r"(?is)<[^>]+>", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt[:200] if txt else "宏观周报"


def publish_report(html_content: str, generated_at: datetime) -> Tuple[bool, Any]:
    import subscription_service as sub_svc

    weekday = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"][generated_at.weekday()]
    title = f"{generated_at.strftime('%m月%d日')} {weekday} 宏观周报"
    summary = extract_summary_from_html(html_content)
    return sub_svc.publish_content(
        channel_code=CHANNEL_CODE,
        title=title,
        content=html_content,
        summary=summary,
    )


def run(
    dry_run: bool = False,
    preview_path: Optional[str] = DEFAULT_PREVIEW_PATH,
    event_window_days: int = DEFAULT_EVENT_WINDOW_DAYS,
    chart_lookback_days: int = DEFAULT_CHART_LOOKBACK_DAYS,
    use_llm: bool = True,
    skip_refresh_geo: bool = False,
    recent_limit: Optional[int] = None,
) -> Tuple[bool, Dict[str, Any]]:
    if skip_refresh_geo:
        print("[macro_radar] --skip-refresh-geo 已废弃：当前版本不再包含地缘风险链路。")
    if recent_limit is not None:
        print("[macro_radar] --recent-limit 已废弃：当前版本新闻条数由内部策略控制。")

    generated_at = datetime.now()
    print("[macro_radar] collecting macro context ...")
    context = collect_macro_context(event_window_days=event_window_days, chart_lookback_days=chart_lookback_days)
    print("[macro_radar] building analysis ...")
    analysis, analysis_source = _build_llm_analysis(context, event_window_days=event_window_days, use_llm=use_llm)
    print(f"[macro_radar] analysis source: {analysis_source}")

    html_content = render_macro_radar_html(
        generated_at=generated_at,
        context=context,
        analysis=analysis,
        analysis_source=analysis_source,
        event_window_days=event_window_days,
        chart_lookback_days=chart_lookback_days,
    )

    if preview_path:
        Path(preview_path).write_text(html_content, encoding="utf-8")
        print(f"[macro_radar] preview written: {preview_path}")

    if dry_run:
        print("[macro_radar] dry-run mode: skip publish.")
        return True, {"dry_run": True, "analysis_source": analysis_source, "html_len": len(html_content)}

    print("[macro_radar] publishing to subscription center ...")
    ok, result = publish_report(html_content, generated_at=generated_at)
    if ok:
        print(f"[macro_radar] publish success, content_id={result}")
        return True, {"content_id": result, "analysis_source": analysis_source, "html_len": len(html_content)}

    print(f"[macro_radar] publish failed: {result}")
    return False, {"error": str(result), "analysis_source": analysis_source}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate and publish Macro Risk Radar report.")
    parser.add_argument("--dry-run", action="store_true", help="Generate preview only; do not publish.")
    parser.add_argument("--preview-path", default=DEFAULT_PREVIEW_PATH, help="Preview HTML output path.")
    parser.add_argument("--event-window-days", type=int, default=DEFAULT_EVENT_WINDOW_DAYS, help="Event window days for CPI/NFP update detection.")
    parser.add_argument("--chart-lookback-days", type=int, default=DEFAULT_CHART_LOOKBACK_DAYS, help="Lookback days for market trend charts.")
    parser.add_argument("--no-llm", action="store_true", help="Disable LLM and use rule-based commentary only.")
    parser.add_argument("--skip-refresh-geo", action="store_true", help="Deprecated no-op option kept for backward compatibility.")
    parser.add_argument("--recent-limit", type=int, default=None, help="Deprecated no-op option kept for backward compatibility.")
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    ok, payload = run(
        dry_run=bool(args.dry_run),
        preview_path=args.preview_path,
        event_window_days=max(1, int(args.event_window_days)),
        chart_lookback_days=max(30, int(args.chart_lookback_days)),
        use_llm=not bool(args.no_llm),
        skip_refresh_geo=bool(args.skip_refresh_geo),
        recent_limit=args.recent_limit,
    )
    if not ok:
        raise SystemExit(1)
    print(f"[macro_radar] done: {payload}")


if __name__ == "__main__":
    main()

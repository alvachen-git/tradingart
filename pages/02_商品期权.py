import streamlit as st
import pandas as pd
import sys
import os
import re
import time
import logging
import math
import data_engine as de
from sqlalchemy import text
import datetime as dt
from html import escape
from ui_components import (
    inject_option_page_header_style,
    inject_sidebar_toggle_style,
    render_option_page_title,
    render_option_sidebar_footer,
)
from option_kline_chart import lightweight_chart_loader_html, render_option_kline_chart
from symbol_match import strict_futures_prefix_pattern

try:
    from futures_fund_flow_tools import get_futures_fund_flow_data, get_single_futures_trend
except Exception:
    get_futures_fund_flow_data = None
    get_single_futures_trend = None
# 1. 基础配置
st.set_page_config(
    page_title="爱波塔-商品期权技术分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

PAGE_NAME = "商品期权"
_PAGE_T0 = time.perf_counter()
_PERF_LOGGER = logging.getLogger(__name__)
USE_GLOBAL_MONITOR_SNAPSHOT = False
CONTRACT_LOOKBACK_DAYS = 420
MAX_CONTRACT_POOL_ROWS = 1200
MAX_CONTRACT_OPTIONS = 120
MAX_CHART_ROWS = 520

COMMODITY_MAP = {
    "IH": "上证50", "IF": "沪深300", "IM": "中证1000",
    "au": "黄金", "ag": "白银", "cu": "铜", "al": "铝", "zn": "锌", "ni": "镍", "sn": "锡",
    "lc": "碳酸锂", "si": "工业硅", "ps": "多晶硅", "pt": "铂金", "pd": "钯金",
    "rb": "螺纹钢", "i": "铁矿石", "hc": "热卷", "jm": "焦煤", "ad": "铝合金", "fg": "玻璃",
    "sa": "纯碱", "ao": "氧化铝", "sh": "烧碱", "sp": "纸浆", "lg": "原木",
    "SM": "锰硅", "SF": "硅铁",
    "M": "豆粕", "a": "豆一", "RM": "菜粕", "y": "豆油", "oi": "菜油", "p": "棕榈油", "pk": "花生",
    "sc": "原油", "ta": "PTA", "px": "对二甲苯", "PR": "瓶片", "ma": "甲醇", "v": "PVC",
    "eb": "苯乙烯", "bz": "纯苯", "eg": "乙二醇", "pp": "聚丙烯", "l": "塑料", "bu": "沥青",
    "fu": "燃料油", "br": "BR橡胶", "ur": "尿素",
    "ru": "橡胶", "c": "玉米", "jd": "鸡蛋", "CF": "棉花", "SR": "白糖", "ap": "苹果", "CJ": "红枣", "lh": "生猪",
}


def _perf_page_log(
    *,
    page: str,
    render_ms: float = 0.0,
    db_ms: float = 0.0,
    api_ms: float = 0.0,
    cache_hit: int = -1,
    rows: int = -1,
    stage: str = "main",
) -> None:
    msg = (
        f"PERF_PAGE page={page} stage={stage} "
        f"render_ms={render_ms:.1f} db_ms={db_ms:.1f} api_ms={api_ms:.1f} "
        f"cache_hit={cache_hit} rows={rows}"
    )
    print(msg)
    _PERF_LOGGER.info(msg)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_recent_contract_pool(cutoff_yyyymmdd: str, variety_code: str) -> list[str]:
    if de.engine is None:
        return []
    clean_code = re.sub(r"[^A-Z0-9]", "", str(variety_code).upper())
    prefix_sql = "ts_code LIKE :prefix_like"
    params = {
        "cutoff": cutoff_yyyymmdd,
        "prefix_like": f"{clean_code}%",
    }
    if len(clean_code) == 1:
        prefix_sql += " AND ts_code REGEXP :prefix_regex"
        params["prefix_regex"] = strict_futures_prefix_pattern(clean_code)
    sql = text(
        f"""
        SELECT DISTINCT ts_code
        FROM commodity_iv_history
        WHERE trade_date >= :cutoff
          AND {prefix_sql}
        ORDER BY ts_code DESC
        LIMIT {MAX_CONTRACT_POOL_ROWS}
        """
    )
    with de.engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
    return [str(r[0]) for r in rows if r and r[0]]


@st.cache_data(ttl=300, show_spinner=False)
def _cached_comprehensive_market_data() -> pd.DataFrame:
    # Reuse the same precomputed dataset as ranking page to keep IV Rank consistent.
    return de.get_comprehensive_market_data()


@st.cache_data(ttl=300, show_spinner=False)
def _cached_futures_fund_flow_overview(days: int = 20) -> pd.DataFrame:
    if get_futures_fund_flow_data is None:
        return pd.DataFrame()
    return get_futures_fund_flow_data(days=days)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_single_futures_fund_trend(symbol: str, days: int = 60) -> pd.DataFrame:
    if get_single_futures_trend is None or not symbol:
        return pd.DataFrame()
    return get_single_futures_trend(symbol=symbol, days=days)


@st.cache_data(ttl=300, show_spinner=False)
def _cached_latest_broker_extremes(product_code: str) -> dict:
    if de.engine is None:
        return {}

    clean_code = re.sub(r"[^A-Za-z0-9]", "", str(product_code or ""))
    if not clean_code:
        return {}
    clean_upper = clean_code.upper()

    prefix_sql = "(ts_code LIKE :prefix_like_lower OR ts_code LIKE :prefix_like_upper)"
    params = {
        "prefix_like_lower": f"{clean_code.lower()}%",
        "prefix_like_upper": f"{clean_upper}%",
    }
    if len(clean_upper) == 1:
        prefix_sql = "ts_code REGEXP :prefix_regex"
        params = {"prefix_regex": strict_futures_prefix_pattern(clean_upper)}

    latest_sql = text(
        f"""
        SELECT trade_date
        FROM futures_holding
        WHERE {prefix_sql}
          AND ts_code NOT LIKE '%TAS%'
        ORDER BY trade_date DESC
        LIMIT 1
        """
    )
    sql = text(
        f"""
        SELECT
            broker,
            SUM(long_vol) AS long_vol,
            SUM(short_vol) AS short_vol,
            MAX(trade_date) AS trade_date
        FROM futures_holding
        WHERE trade_date = :trade_date
          AND {prefix_sql}
          AND ts_code NOT LIKE '%TAS%'
        GROUP BY broker
        HAVING SUM(long_vol) > 0 OR SUM(short_vol) > 0
        """
    )

    try:
        latest_df = pd.read_sql(latest_sql, de.engine, params=params)
        if latest_df.empty:
            return {}
        query_params = {**params, "trade_date": str(latest_df.iloc[0].get("trade_date") or "")}
        df = pd.read_sql(sql, de.engine, params=query_params)
    except Exception:
        return {}
    if df.empty:
        return {}

    df["long_vol"] = pd.to_numeric(df["long_vol"], errors="coerce").fillna(0.0)
    df["short_vol"] = pd.to_numeric(df["short_vol"], errors="coerce").fillna(0.0)
    long_row = df.sort_values("long_vol", ascending=False).iloc[0]
    short_row = df.sort_values("short_vol", ascending=False).iloc[0]
    return {
        "long_broker": str(long_row.get("broker") or "--"),
        "long_vol": float(long_row.get("long_vol") or 0.0),
        "short_broker": str(short_row.get("broker") or "--"),
        "short_vol": float(short_row.get("short_vol") or 0.0),
        "trade_date": str(long_row.get("trade_date") or short_row.get("trade_date") or ""),
    }


def _extract_contract_code(contract_label: str) -> str:
    if not isinstance(contract_label, str):
        return ""
    match = re.match(r"([A-Za-z]+\d{3,4})", contract_label.strip())
    return match.group(1).upper() if match else ""


def _pick_market_row_by_product(df_market: pd.DataFrame, product_code: str, used_contract: str = ""):
    if df_market is None or df_market.empty:
        return None

    product_code = str(product_code or "").upper()
    used_contract = str(used_contract or "").upper()
    work = df_market.copy()
    work["__contract_code"] = work["合约"].apply(_extract_contract_code)
    candidates = work[work["__contract_code"].str.startswith(product_code, na=False)]
    if candidates.empty:
        return None

    if used_contract:
        exact = candidates[candidates["__contract_code"] == used_contract]
        if not exact.empty:
            return exact.iloc[0]

    # Default to the first row (same display order as ranking dataset).
    return candidates.iloc[0]


def _normalize_futures_symbol(value: str) -> str:
    return re.sub(r"[^A-Z]", "", str(value or "").upper())


def _safe_float(value, default: float | None = None) -> float | None:
    try:
        if value is None:
            return default
        parsed = pd.to_numeric(value, errors="coerce")
        if pd.isna(parsed):
            return default
        return float(parsed)
    except Exception:
        return default


def _format_signed_amount_wan(value) -> str:
    amount = _safe_float(value, 0.0) or 0.0
    sign = "+" if amount > 0 else ""
    if abs(amount) >= 10000:
        return f"{sign}{amount / 10000:.2f}亿"
    return f"{sign}{amount:,.0f}万"


def _format_amount_wan(value) -> str:
    amount = _safe_float(value, 0.0) or 0.0
    if abs(amount) >= 10000:
        return f"{amount / 10000:.2f}亿"
    return f"{amount:,.0f}万"


def _format_compact_number(value) -> str:
    amount = _safe_float(value, 0.0) or 0.0
    if abs(amount) >= 10000:
        return f"{amount / 10000:.1f}万"
    return f"{amount:,.0f}"


def _format_lots(value) -> str:
    amount = _safe_float(value, 0.0) or 0.0
    if abs(amount) >= 10000:
        return f"{amount / 10000:.1f}万手"
    return f"{amount:,.0f}手"


def _format_percent(value, digits: int = 1) -> str:
    amount = _safe_float(value)
    if amount is None:
        return "--"
    return f"{amount:.{digits}f}%"


def _format_rank_score(value) -> str:
    amount = _safe_float(value)
    if amount is None:
        return "--"
    amount = max(0.0, min(100.0, amount))
    return f"{amount:.0f}/100"


def _format_signed_pp(value, digits: int = 1) -> str:
    amount = _safe_float(value)
    if amount is None:
        return "--"
    sign = "+" if amount > 0 else ""
    return f"{sign}{amount:.{digits}f}pp"


def _format_date_short(value) -> str:
    if value is None or value == "":
        return "--"
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return str(value)
    return parsed.strftime("%m/%d")


def _tone_for_signed(value) -> str:
    amount = _safe_float(value, 0.0) or 0.0
    if amount > 0:
        return "positive"
    if amount < 0:
        return "negative"
    return "neutral"


def _tone_color(tone: str) -> str:
    if tone == "positive":
        return "#dc2626"
    if tone == "negative":
        return "#059669"
    return "#111827"


def _positive_log(value):
    amount = _safe_float(value)
    if amount is None or amount <= 0:
        return None
    return math.log(amount)


def _compute_hv_from_chart(chart_k: pd.DataFrame | None, window: int = 20) -> pd.DataFrame:
    if chart_k is None or chart_k.empty or "close" not in chart_k.columns:
        return pd.DataFrame(columns=["date_obj", "hv"])

    work = chart_k.copy()
    date_col = "date" if "date" in work.columns else "trade_date"
    if date_col not in work.columns:
        return pd.DataFrame(columns=["date_obj", "hv"])

    work["date_obj"] = pd.to_datetime(work[date_col], errors="coerce")
    work["close"] = pd.to_numeric(work["close"], errors="coerce")
    work = work.dropna(subset=["date_obj", "close"]).sort_values("date_obj")
    work = work[work["close"] > 0].copy()
    if len(work) <= window:
        return pd.DataFrame(columns=["date_obj", "hv"])

    work["log_close"] = work["close"].map(_positive_log)
    work["hv"] = work["log_close"].diff().rolling(window).std() * math.sqrt(252) * 100
    return work[["date_obj", "hv"]].dropna()


def _build_hv_spread_stats(df_iv: pd.DataFrame | None, chart_k: pd.DataFrame | None) -> dict:
    stats = {
        "hv": None,
        "hv_percentile": None,
        "hv_date": "",
        "iv_hv": None,
        "iv_hv_percentile": None,
    }
    if df_iv is None or df_iv.empty:
        return stats

    iv_work = df_iv.copy()
    if "trade_date" not in iv_work.columns or "iv" not in iv_work.columns:
        return stats

    iv_work["date_obj"] = pd.to_datetime(iv_work["trade_date"], errors="coerce")
    iv_work["iv"] = pd.to_numeric(iv_work["iv"], errors="coerce")
    iv_work = iv_work.dropna(subset=["date_obj", "iv"])
    if iv_work.empty:
        return stats

    if "hv" in iv_work.columns:
        iv_work["hv"] = pd.to_numeric(iv_work["hv"], errors="coerce")
        hv_work = iv_work[["date_obj", "iv", "hv"]].copy()
    else:
        hv_work = pd.DataFrame()

    has_positive_hv = (
        not hv_work.empty
        and (pd.to_numeric(hv_work["hv"], errors="coerce").dropna() > 0).any()
    )
    if not has_positive_hv:
        computed_hv = _compute_hv_from_chart(chart_k)
        if computed_hv.empty:
            return stats
        hv_work = iv_work[["date_obj", "iv"]].merge(computed_hv, on="date_obj", how="left")

    valid = hv_work.dropna(subset=["iv", "hv"]).copy()
    valid = valid[(valid["iv"] > 0) & (valid["hv"] > 0)].sort_values("date_obj").tail(252)
    if valid.empty:
        return stats

    latest = valid.iloc[-1]
    latest_iv = _safe_float(latest.get("iv"))
    latest_hv = _safe_float(latest.get("hv"))
    if latest_iv is None or latest_hv is None:
        return stats

    spread = latest_iv - latest_hv
    hv_series = pd.to_numeric(valid["hv"], errors="coerce").dropna()
    hv_percentile = None
    if not hv_series.empty:
        hv_percentile = float((hv_series <= latest_hv).mean() * 100)
    spread_series = pd.to_numeric(valid["iv"], errors="coerce") - pd.to_numeric(valid["hv"], errors="coerce")
    spread_series = spread_series.dropna()
    spread_percentile = None
    if not spread_series.empty:
        spread_percentile = float((spread_series <= spread).mean() * 100)

    stats.update(
        {
            "hv": latest_hv,
            "hv_percentile": hv_percentile,
            "hv_date": _format_date_short(latest.get("date_obj")),
            "iv_hv": spread,
            "iv_hv_percentile": spread_percentile,
        }
    )
    return stats


def _pick_fund_flow_row(df_flow: pd.DataFrame, product_code: str, target_contract: str = ""):
    if df_flow is None or df_flow.empty:
        return None

    product = _normalize_futures_symbol(product_code)
    target = str(target_contract or "").split(".")[0].upper()
    work = df_flow.copy()
    if "symbol" not in work.columns:
        work["symbol"] = ""
    if "ts_code" not in work.columns:
        work["ts_code"] = ""
    work["__symbol"] = work["symbol"].map(_normalize_futures_symbol)
    work["__ts_code"] = work["ts_code"].astype(str).str.split(".").str[0].str.upper()

    if target and any(ch.isdigit() for ch in target):
        exact = work[work["__ts_code"] == target]
        if not exact.empty:
            return exact.iloc[0]

    candidates = work[work["__symbol"] == product]
    if not candidates.empty:
        return candidates.iloc[0]
    return None


def _compute_iv_stats(
    df_iv: pd.DataFrame | None,
    df_rank_base: pd.DataFrame,
    market_row,
    chart_k: pd.DataFrame | None = None,
) -> dict:
    stats = {
        "curr_iv": None,
        "iv_rank": None,
        "max_iv": None,
        "min_iv": None,
        "hv": None,
        "hv_percentile": None,
        "hv_date": "",
        "iv_hv": None,
        "iv_hv_percentile": None,
        "latest_date": "",
        "status": "数据不足",
        "tone": "neutral",
    }
    if df_iv is None or df_iv.empty:
        return stats

    latest_date = df_iv.iloc[-1].get("trade_date")
    stats["latest_date"] = _format_date_short(latest_date)

    if market_row is not None:
        curr_iv_val = _safe_float(market_row.get("当前IV"))
        iv_rank_val = _safe_float(market_row.get("IV Rank"))
        if curr_iv_val is not None and iv_rank_val is not None:
            stats["curr_iv"] = curr_iv_val
            stats["iv_rank"] = iv_rank_val

    if stats["curr_iv"] is None or stats["iv_rank"] is None:
        if df_rank_base is not None and not df_rank_base.empty:
            curr_iv = _safe_float(df_rank_base.iloc[-1].get("iv"), 0.0) or 0.0
            max_iv = _safe_float(df_rank_base["iv"].max(), curr_iv) or curr_iv
            min_iv = _safe_float(df_rank_base["iv"].min(), curr_iv) or curr_iv
            iv_rank = (curr_iv - min_iv) / (max_iv - min_iv) * 100 if max_iv > min_iv else 0.0
            stats.update({"curr_iv": curr_iv, "iv_rank": iv_rank, "max_iv": max_iv, "min_iv": min_iv})
        else:
            curr_iv = _safe_float(df_iv.iloc[-1].get("iv"), 0.0) or 0.0
            stats.update({"curr_iv": curr_iv, "iv_rank": 0.0, "max_iv": curr_iv, "min_iv": curr_iv})
    else:
        if df_rank_base is not None and not df_rank_base.empty:
            stats["max_iv"] = _safe_float(df_rank_base["iv"].max(), stats["curr_iv"])
            stats["min_iv"] = _safe_float(df_rank_base["iv"].min(), stats["curr_iv"])
        else:
            stats["max_iv"] = stats["curr_iv"]
            stats["min_iv"] = stats["curr_iv"]

    iv_rank = _safe_float(stats["iv_rank"], 0.0) or 0.0
    if iv_rank < 20:
        stats.update({"status": "偏低", "tone": "positive"})
    elif iv_rank < 60:
        stats.update({"status": "正常", "tone": "neutral"})
    elif iv_rank < 85:
        stats.update({"status": "偏高", "tone": "warning"})
    else:
        stats.update({"status": "极高", "tone": "negative"})
    stats.update(_build_hv_spread_stats(df_iv, chart_k))
    return stats


def _build_fund_flow_snapshot(flow_row, trend_df: pd.DataFrame, variety_code: str) -> dict:
    latest_trend = trend_df.iloc[-1] if trend_df is not None and not trend_df.empty else None
    latest_date = ""
    if latest_trend is not None:
        latest_date = _format_date_short(latest_trend.get("trade_date"))

    today_flow = _safe_float(flow_row.get("today_flow") if flow_row is not None else None, 0.0) or 0.0
    net_flow = _safe_float(flow_row.get("net_flow") if flow_row is not None else None, 0.0) or 0.0
    total_margin = _safe_float(flow_row.get("total_margin") if flow_row is not None else None, 0.0) or 0.0
    liquid_fund = _safe_float(flow_row.get("liquid_fund") if flow_row is not None else None, 0.0) or 0.0
    oi_change = _safe_float(latest_trend.get("oi_change") if latest_trend is not None else None, 0.0) or 0.0
    recent_5d = net_flow
    if trend_df is not None and not trend_df.empty and "fund_flow" in trend_df.columns:
        recent_5d = pd.to_numeric(trend_df["fund_flow"], errors="coerce").tail(5).fillna(0).sum()

    ts_code = ""
    if flow_row is not None:
        ts_code = str(flow_row.get("ts_code") or "")
    if not ts_code and latest_trend is not None:
        ts_code = str(latest_trend.get("ts_code") or "")
    if not ts_code:
        ts_code = variety_code.upper()

    return {
        "today_flow": today_flow,
        "recent_5d": recent_5d,
        "net_flow": net_flow,
        "total_margin": total_margin,
        "liquid_fund": liquid_fund,
        "oi_change": oi_change,
        "ts_code": ts_code,
        "latest_date": latest_date,
        "available": flow_row is not None or (trend_df is not None and not trend_df.empty),
    }


def _prepare_commodity_lwc_frames(
    chart_k: pd.DataFrame,
    chart_iv: pd.DataFrame,
    period: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    k_df = chart_k.copy()
    if "volume" not in k_df.columns:
        k_df["volume"] = 0.0
    k_df = k_df[["date", "open", "high", "low", "close", "volume"]].copy()
    k_df["date_obj"] = pd.to_datetime(k_df["date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume"):
        k_df[column] = pd.to_numeric(k_df[column], errors="coerce")
    k_df["volume"] = k_df["volume"].fillna(0.0)
    k_df = k_df.dropna(subset=["date_obj", "open", "high", "low", "close"]).sort_values("date_obj")

    if chart_iv is None or chart_iv.empty or not {"date", "iv"}.issubset(chart_iv.columns):
        iv_df = pd.DataFrame(columns=["date_obj", "iv"])
    else:
        iv_df = chart_iv[["date", "iv"]].copy()
        iv_df["date_obj"] = pd.to_datetime(iv_df["date"], errors="coerce")
        iv_df["iv"] = pd.to_numeric(iv_df["iv"], errors="coerce")
        iv_df = iv_df.dropna(subset=["date_obj", "iv"])
        iv_df = iv_df[iv_df["iv"] > 0].sort_values("date_obj")

    if not k_df.empty and not iv_df.empty:
        iv_df = iv_df[
            (iv_df["date_obj"] >= k_df["date_obj"].min())
            & (iv_df["date_obj"] <= k_df["date_obj"].max())
        ]

    if period == "weekly":
        k_df = (
            k_df.set_index("date_obj")
            .resample("W-FRI")
            .agg({"open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"})
            .dropna(subset=["open", "high", "low", "close"])
            .reset_index()
        )
        if not iv_df.empty:
            iv_df = (
                iv_df.set_index("date_obj")[["iv"]]
                .resample("W-FRI")
                .last()
                .dropna()
                .reset_index()
            )

    for window in (5, 20, 60):
        k_df[f"ma{window}"] = k_df["close"].rolling(window).mean()
    k_df["date"] = k_df["date_obj"].dt.strftime("%Y-%m-%d")
    if not iv_df.empty:
        iv_df["date"] = iv_df["date_obj"].dt.strftime("%Y-%m-%d")
    else:
        iv_df["date"] = pd.Series(dtype="object")

    return (
        k_df[["date", "open", "high", "low", "close", "volume", "ma5", "ma20", "ma60"]],
        iv_df[["date", "iv"]],
    )


def _line_records(frame: pd.DataFrame, value_column: str) -> list[dict[str, float | str]]:
    if frame is None or frame.empty or value_column not in frame.columns:
        return []
    rows: list[dict[str, float | str]] = []
    for _, row in frame[["date", value_column]].dropna().iterrows():
        rows.append({"time": str(row["date"]), "value": float(row[value_column])})
    return rows


def _build_commodity_lwc_dataset(k_df: pd.DataFrame, iv_df: pd.DataFrame) -> dict[str, object]:
    candles: list[dict[str, float | str]] = []
    volumes: list[dict[str, float | str]] = []
    for _, row in k_df.iterrows():
        open_price = float(row["open"])
        close_price = float(row["close"])
        time_value = str(row["date"])
        candles.append(
            {
                "time": time_value,
                "open": open_price,
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": close_price,
            }
        )
        volumes.append(
            {
                "time": time_value,
                "value": float(row.get("volume", 0.0) or 0.0),
                "color": "rgba(239, 68, 68, 0.34)"
                if close_price >= open_price
                else "rgba(16, 185, 129, 0.34)",
            }
        )

    latest: dict[str, float | str] = {}
    if candles:
        latest = {"time": candles[-1]["time"], "close": float(candles[-1]["close"])}
        if len(candles) >= 2 and float(candles[-2]["close"]):
            previous_close = float(candles[-2]["close"])
            change = float(candles[-1]["close"]) - previous_close
            latest["change"] = change
            latest["change_pct"] = change / previous_close * 100

    return {
        "candles": candles,
        "volumes": volumes,
        "ma5": _line_records(k_df, "ma5"),
        "ma20": _line_records(k_df, "ma20"),
        "ma60": _line_records(k_df, "ma60"),
        "iv": _line_records(iv_df, "iv"),
        "latest": latest,
    }


def _recent_price_bounds(chart_k: pd.DataFrame | None) -> tuple[float | None, float | None]:
    if chart_k is None or chart_k.empty or "close" not in chart_k.columns:
        return None, None
    close_series = pd.to_numeric(chart_k["close"], errors="coerce").dropna()
    if close_series.empty:
        return None, None
    recent = close_series.tail(60)
    return float(recent.quantile(0.2)), float(recent.quantile(0.8))


def _build_commodity_lwc_payload(chart_k: pd.DataFrame, chart_iv: pd.DataFrame, symbol: str) -> dict[str, object]:
    daily_k, daily_iv = _prepare_commodity_lwc_frames(chart_k, chart_iv, "daily")
    weekly_k, weekly_iv = _prepare_commodity_lwc_frames(chart_k, chart_iv, "weekly")
    daily = _build_commodity_lwc_dataset(daily_k, daily_iv)
    weekly = _build_commodity_lwc_dataset(weekly_k, weekly_iv)
    support, pressure = _recent_price_bounds(chart_k)
    reference_lines = []
    if pressure is not None:
        reference_lines.append({"price": pressure, "color": "#dc2626", "title": "压力", "lineWidth": 1})
    if support is not None:
        reference_lines.append({"price": support, "color": "#16a34a", "title": "支撑", "lineWidth": 1})

    return {
        "symbol": symbol,
        **daily,
        "datasets": {"daily": daily, "weekly": weekly},
        "referenceLines": reference_lines,
        "config": {
            "showTitle": False,
            "showLatest": False,
            "enablePeriodSwitch": True,
            "activePeriod": "daily",
            "priceDigits": 2,
            "useTimeVisibleRange": True,
            "storageNamespace": "commodity-options-chart-drawings",
            "titleContext": "商品期货日线 · 本地数据库",
            "ivLabel": "隐含IV",
        },
    }


def _render_commodity_price_iv_chart(chart_k: pd.DataFrame, chart_iv: pd.DataFrame, symbol: str) -> None:
    payload = _build_commodity_lwc_payload(chart_k, chart_iv, symbol)
    render_option_kline_chart(
        payload,
        chart_loader_html=lightweight_chart_loader_html(),
        height=650,
    )


def _tooltip_icon_html(text: str) -> str:
    if not text:
        return ""
    safe_text = escape(text, quote=True)
    return (
        f'<span class="commodity-lab-info" tabindex="0" '
        f'aria-label="{safe_text}" data-tooltip="{safe_text}">i</span>'
    )


def _metric_tile_html(
    label: str,
    value: str,
    detail: str = "",
    tone: str = "neutral",
    extra: str = "",
    tooltip: str = "",
    variant: str = "",
) -> str:
    variant_class = f" is-{escape(variant)}" if variant else ""
    return (
        f'<div class="commodity-lab-metric is-{escape(tone)}{variant_class}">'
        f'<div class="commodity-lab-metric-label"><span>{escape(label)}</span>{_tooltip_icon_html(tooltip)}</div>'
        f'<div class="commodity-lab-metric-value">{escape(value)}</div>'
        f'<div class="commodity-lab-metric-detail">{escape(detail)}</div>'
        f"{extra}</div>"
    )


def _render_overview_groups(iv_stats: dict, flow: dict, broker_extremes: dict | None = None) -> None:
    broker_extremes = broker_extremes or {}
    curr_iv = iv_stats.get("curr_iv")
    iv_rank = iv_stats.get("iv_rank")
    hv = iv_stats.get("hv")
    hv_percentile = iv_stats.get("hv_percentile")
    iv_hv = iv_stats.get("iv_hv")
    iv_hv_percentile = iv_stats.get("iv_hv_percentile")
    iv_tone = str(iv_stats.get("tone") or "neutral")
    flow_tone = _tone_for_signed(flow.get("today_flow"))
    recent_tone = _tone_for_signed(flow.get("recent_5d"))
    oi_tone = _tone_for_signed(flow.get("oi_change"))
    broker_date = _format_date_short(broker_extremes.get("trade_date"))
    tooltips = {
        "curr_iv": "当前期权市场预期的波动。价格涨且IV也升，通常说明追涨意愿强；价格跌且IV升，多半是避险和空头压力在增加。",
        "iv_rank": "把当前IV放到过去一年里看位置。分位低说明期权不算贵，行情启动时更有弹性；分位高说明情绪较满，要防冲高回落或急跌反弹。",
        "hv": "过去20天价格实际动得有多大。上涨时HV抬升，偏多头加速；下跌时HV抬升，偏空头加速；低分位则说明行情还在蓄力。",
        "iv_hv": "看市场预期波动比实际波动贵不贵。高分位说明预期偏热，容易透支；低分位说明预期不贵，突破行情更容易放大。",
        "today_flow": "用持仓变化、价格和保证金估算今天资金方向。流入配合上涨偏多，流出配合下跌偏空；背离时说明多空有分歧。",
        "recent_flow": "看最近5天资金有没有连续同向。持续流入对价格偏支撑，持续流出会让反弹压力变大。",
        "margin": "估算留在该品种里的保证金规模。资金沉淀越高，说明关注度越高，价格到关键位时更容易放大波动。",
        "oi_change": "今天总持仓比上一交易日的变化。上涨增仓偏多头主动，下跌增仓偏空头主动；减仓多是原方向资金离场。",
        "long_broker": "最新日多单最多的席位。龙头席位持续加多，对价格偏支撑；如果价格不涨，说明上方卖压也不轻。",
        "short_broker": "最新日空单最多的席位。空头集中增加，对价格偏压力；如果价格不跌，说明下方买盘承接较强。",
    }
    option_cards = [
        _metric_tile_html(
            "当前 IV",
            f"{curr_iv:.2f}%" if curr_iv is not None else "--",
            f"{iv_stats.get('status') or '数据不足'} · 更新 {iv_stats.get('latest_date') or '--'}",
            iv_tone,
            tooltip=tooltips["curr_iv"],
        ),
        _metric_tile_html(
            "IV Rank",
            _format_rank_score(iv_rank),
            "近252个交易日",
            iv_tone,
            tooltip=tooltips["iv_rank"],
        ),
        _metric_tile_html(
            "20D HV分位",
            _format_rank_score(hv_percentile),
            f"实际 {_format_percent(hv, 2)} · 更新 {iv_stats.get('hv_date') or '--'}",
            tooltip=tooltips["hv"],
        ),
        _metric_tile_html(
            "IV-HV分位",
            _format_rank_score(iv_hv_percentile),
            f"实际 {_format_signed_pp(iv_hv, 1)}",
            tooltip=tooltips["iv_hv"],
        ),
    ]
    flow_cards = [
        _metric_tile_html(
            "今日资金流",
            _format_signed_amount_wan(flow.get("today_flow")),
            f"主力 {flow.get('ts_code') or '--'}",
            flow_tone,
            tooltip=tooltips["today_flow"],
        ),
        _metric_tile_html(
            "近5日净流",
            _format_signed_amount_wan(flow.get("recent_5d")),
            f"更新 {flow.get('latest_date') or '--'}",
            recent_tone,
            tooltip=tooltips["recent_flow"],
        ),
        _metric_tile_html(
            "资金沉淀",
            _format_amount_wan(flow.get("total_margin")),
            "持仓保证金估算",
            tooltip=tooltips["margin"],
        ),
        _metric_tile_html(
            "持仓变化",
            _format_compact_number(flow.get("oi_change")),
            "较前一交易日",
            oi_tone,
            tooltip=tooltips["oi_change"],
        ),
    ]
    broker_cards = [
        _metric_tile_html(
            "最大多头席位",
            str(broker_extremes.get("long_broker") or "--"),
            f"多单 {_format_lots(broker_extremes.get('long_vol'))} · {broker_date}",
            "positive",
            tooltip=tooltips["long_broker"],
            variant="broker",
        ),
        _metric_tile_html(
            "最大空头席位",
            str(broker_extremes.get("short_broker") or "--"),
            f"空单 {_format_lots(broker_extremes.get('short_vol'))} · {broker_date}",
            "negative",
            tooltip=tooltips["short_broker"],
            variant="broker",
        ),
    ]

    st.markdown(
        '<div class="commodity-lab-kpi-strip">'
        + "".join(option_cards + flow_cards + broker_cards)
        + "</div>",
        unsafe_allow_html=True,
    )


def _render_volatility_rail(iv_stats: dict, chart_k: pd.DataFrame, target_contract: str) -> None:
    iv_rank = _safe_float(iv_stats.get("iv_rank"), 0.0) or 0.0
    iv_rank = max(0.0, min(100.0, iv_rank))
    tone = str(iv_stats.get("tone") or "neutral")
    color = _tone_color("negative" if tone == "warning" else tone)
    if tone == "warning":
        color = "#d97706"

    latest_close = None
    support = None
    pressure = None
    if chart_k is not None and not chart_k.empty:
        close_series = pd.to_numeric(chart_k["close"], errors="coerce").dropna()
        if not close_series.empty:
            latest_close = float(close_series.iloc[-1])
            recent = close_series.tail(60)
            support = float(recent.quantile(0.2))
            pressure = float(recent.quantile(0.8))
    latest_close_text = f"{latest_close:.2f}" if latest_close is not None else "--"
    pressure_text = f"{pressure:.2f}" if pressure is not None else "--"
    support_text = f"{support:.2f}" if support is not None else "--"

    st.markdown(
        f"""
        <div class="commodity-lab-rail">
            <div class="commodity-lab-rail-head">
                <strong>波动率与区间速览</strong>
                <span>更新 {escape(str(iv_stats.get("latest_date") or "--"))}</span>
            </div>
            <div class="commodity-lab-iv-block">
                <div class="commodity-lab-iv-top"><span>IV等级</span><em style="color:{color}">{escape(str(iv_stats.get("status") or "数据不足"))}</em></div>
                <div class="commodity-lab-iv-value" style="color:{color}">{iv_rank:.0f}<small>/100</small></div>
                <div class="commodity-lab-meter"><span style="width:{iv_rank:.1f}%;background:{color}"></span></div>
                <div class="commodity-lab-meter-scale"><span>0</span><span>25</span><span>50</span><span>75</span><span>100</span></div>
            </div>
            <div class="commodity-lab-rail-row">
                <div><div class="commodity-lab-rail-label">最新收盘</div><div class="commodity-lab-rail-value">{escape(latest_close_text)}</div></div>
                <div class="commodity-lab-rail-detail">{escape(str(target_contract))}</div>
            </div>
            <div class="commodity-lab-rail-row">
                <div><div class="commodity-lab-rail-label">近期压力</div><div class="commodity-lab-rail-value danger">{escape(pressure_text)}</div></div>
                <div class="commodity-lab-rail-detail">60日分位</div>
            </div>
            <div class="commodity-lab-rail-row">
                <div><div class="commodity-lab-rail-label">近期支撑</div><div class="commodity-lab-rail-value success">{escape(support_text)}</div></div>
                <div class="commodity-lab-rail-detail">60日分位</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_flow_judgment(flow: dict) -> None:
    if not flow.get("available"):
        st.markdown(
            """
            <div class="commodity-lab-rail compact">
                <div class="commodity-lab-rail-head"><strong>资金流判断</strong><span>等待数据</span></div>
                <div class="commodity-lab-empty">当前品种暂未匹配到资金流样本。</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    today = _safe_float(flow.get("today_flow"), 0.0) or 0.0
    recent = _safe_float(flow.get("recent_5d"), 0.0) or 0.0
    margin = abs(_safe_float(flow.get("total_margin"), 0.0) or 0.0)
    base = margin if margin > 0 else max(abs(today), abs(recent), 1.0)
    strength = max(-100.0, min(100.0, recent / base * 100))
    meter_width = min(100.0, abs(strength) * 2.0)
    tone = _tone_for_signed(recent)
    if tone == "positive":
        label = "资金偏流入"
        color = "#dc2626"
    elif tone == "negative":
        label = "资金偏流出"
        color = "#059669"
    else:
        label = "资金中性"
        color = "#64748b"

    st.markdown(
        f"""
        <div class="commodity-lab-rail compact">
            <div class="commodity-lab-rail-head">
                <strong>资金流判断</strong>
                <span>近5日</span>
            </div>
            <div class="commodity-lab-flow-verdict" style="color:{color}">{escape(label)}</div>
            <div class="commodity-lab-flow-copy">近5日净流 {escape(_format_signed_amount_wan(recent))}，今日 {escape(_format_signed_amount_wan(today))}。</div>
            <div class="commodity-lab-meter"><span style="width:{meter_width:.1f}%;background:{color}"></span></div>
            <div class="commodity-lab-meter-scale"><span>弱</span><span>中</span><span>强</span></div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _inject_commodity_lab_style() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stMain"],
        [data-testid="stMainBlockContainer"] {
            background: #f6f8fb !important;
        }

        [data-testid="stMainBlockContainer"] {
            max-width: 100% !important;
            padding-bottom: 52px !important;
        }

        .option-page-title-wrap {
            margin-bottom: 6px !important;
        }

        .commodity-lab-header-marker,
        .commodity-lab-page-nav {
            display: none;
        }

        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-header-marker),
        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-page-nav) {
            align-items: center !important;
            gap: 16px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-header-marker) {
            margin-bottom: 4px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-page-nav) {
            margin: 0 0 14px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-header-marker) div[data-testid="stSelectbox"],
        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-page-nav) div[data-testid="stSelectbox"] {
            margin-bottom: 0 !important;
        }

        .commodity-lab-kpi-strip {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 8px;
            margin: 8px 0 14px;
        }

        .commodity-lab-rail,
        div[data-testid="stColumn"]:has(.commodity-lab-chart-head) {
            border: 1px solid #d8e0ea;
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .03);
        }

        .commodity-lab-metric {
            min-width: 0;
            min-height: 82px;
            padding: 13px 14px 11px;
            border: 1px solid #e2e8f0;
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(15, 23, 42, .03);
            box-sizing: border-box;
        }

        .commodity-lab-metric-label {
            position: relative;
            display: flex;
            align-items: center;
            gap: 5px;
            color: #64748b !important;
            font-size: 12px;
            font-weight: 650;
            line-height: 1.25;
            white-space: nowrap;
        }

        .commodity-lab-info {
            position: relative;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 14px;
            height: 14px;
            border: 1px solid #cbd5e1;
            border-radius: 999px;
            color: #64748b !important;
            background: #f8fafc;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 9px;
            font-weight: 750;
            line-height: 1;
            cursor: help;
        }

        .commodity-lab-info::after {
            content: attr(data-tooltip);
            position: absolute;
            top: 20px;
            left: 50%;
            z-index: 50;
            width: 248px;
            max-width: min(248px, calc(100vw - 40px));
            padding: 9px 10px;
            border: 1px solid #d8e0ea;
            border-radius: 8px;
            background: #ffffff;
            box-shadow: 0 12px 30px rgba(15, 23, 42, .16);
            color: #334155 !important;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            font-size: 12px;
            font-weight: 520;
            line-height: 1.45;
            text-align: left;
            white-space: normal;
            opacity: 0;
            pointer-events: none;
            transform: translateX(-50%) translateY(-3px);
            transition: opacity .15s ease, transform .15s ease, visibility .15s ease;
            visibility: hidden;
        }

        .commodity-lab-info:hover::after,
        .commodity-lab-info:focus::after {
            opacity: 1;
            transform: translateX(-50%) translateY(0);
            visibility: visible;
        }

        .commodity-lab-metric-value {
            margin-top: 7px;
            color: #111827 !important;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 21px;
            font-weight: 760;
            letter-spacing: 0;
            line-height: 1.1;
            overflow-wrap: anywhere;
            white-space: normal;
        }

        .commodity-lab-metric.is-broker .commodity-lab-metric-value {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
            font-size: 16px;
            font-weight: 760;
            line-height: 1.18;
        }

        .commodity-lab-metric.is-broker .commodity-lab-metric-detail {
            font-size: 10.5px;
        }

        .commodity-lab-metric.is-positive .commodity-lab-metric-value {
            color: #dc2626 !important;
        }

        .commodity-lab-metric.is-negative .commodity-lab-metric-value {
            color: #059669 !important;
        }

        .commodity-lab-metric.is-warning .commodity-lab-metric-value {
            color: #d97706 !important;
        }

        .commodity-lab-metric-detail {
            margin-top: 7px;
            color: #64748b !important;
            font-size: 11px;
            line-height: 1.25;
            white-space: normal;
        }

        .commodity-lab-chart-head,
        .commodity-lab-rail-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 12px;
        }

        .commodity-lab-chart-head strong,
        .commodity-lab-rail-head strong {
            color: #111827 !important;
            font-size: 18px;
            font-weight: 720;
            line-height: 1.25;
        }

        .commodity-lab-chart-head span,
        .commodity-lab-rail-head span {
            color: #64748b !important;
            font-size: 12px;
            white-space: nowrap;
        }

        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-rail) {
            align-items: stretch !important;
            gap: 16px !important;
            margin-bottom: 16px !important;
        }

        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-rail) > div[data-testid="stColumn"],
        div[data-testid="stHorizontalBlock"]:has(.commodity-lab-chart-head) > div[data-testid="stColumn"] {
            min-width: 0 !important;
        }

        div[data-testid="stColumn"]:has(.commodity-lab-chart-head) {
            padding: 16px 16px 10px !important;
            overflow: hidden;
        }

        .commodity-lab-chart-head {
            margin-bottom: 8px;
        }

        .commodity-lab-rail {
            min-height: 312px;
            padding: 16px 18px;
            box-sizing: border-box;
        }

        .commodity-lab-rail + .commodity-lab-rail {
            margin-top: 10px;
        }

        .commodity-lab-rail.compact {
            min-height: 184px;
        }

        .commodity-lab-rail-head {
            padding-bottom: 14px;
            border-bottom: 1px solid #d8e0ea;
        }

        .commodity-lab-iv-block {
            padding: 20px 0 18px;
            border-bottom: 1px solid #d8e0ea;
        }

        .commodity-lab-iv-top,
        .commodity-lab-rail-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 14px;
        }

        .commodity-lab-iv-top span,
        .commodity-lab-rail-label {
            color: #64748b !important;
            font-size: 13px;
            font-weight: 650;
        }

        .commodity-lab-iv-top em {
            font-style: normal;
            font-size: 14px;
            font-weight: 680;
        }

        .commodity-lab-iv-value {
            margin: 6px 0 14px;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 30px;
            font-weight: 780;
            line-height: 1.15;
        }

        .commodity-lab-iv-value small {
            color: #64748b !important;
            font-size: 14px;
            font-weight: 650;
        }

        .commodity-lab-meter {
            height: 5px;
            overflow: hidden;
            border-radius: 999px;
            background: #e2e8f0;
        }

        .commodity-lab-meter > span {
            display: block;
            height: 100%;
            border-radius: inherit;
        }

        .commodity-lab-meter-scale {
            display: flex;
            justify-content: space-between;
            margin-top: 7px;
            color: #94a3b8 !important;
            font-size: 10px;
        }

        .commodity-lab-rail-row {
            min-height: 70px;
            padding: 13px 0;
            border-bottom: 1px solid #d8e0ea;
        }

        .commodity-lab-rail-row:last-child {
            border-bottom: 0;
        }

        .commodity-lab-rail-value {
            margin-top: 5px;
            color: #111827 !important;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 21px;
            font-weight: 740;
            white-space: nowrap;
        }

        .commodity-lab-rail-value.danger {
            color: #dc2626 !important;
        }

        .commodity-lab-rail-value.success {
            color: #059669 !important;
        }

        .commodity-lab-rail-detail {
            color: #64748b !important;
            font-family: "SFMono-Regular", Menlo, Monaco, Consolas, monospace;
            font-size: 12px;
            font-weight: 650;
            text-align: right;
            white-space: nowrap;
        }

        .commodity-lab-flow-verdict {
            margin: 18px 0 6px;
            font-size: 24px;
            font-weight: 780;
            line-height: 1.15;
        }

        .commodity-lab-flow-copy,
        .commodity-lab-empty {
            margin: 0 0 14px;
            color: #64748b !important;
            font-size: 12px;
            line-height: 1.5;
        }

        @media (max-width: 1180px) {
            .commodity-lab-kpi-strip {
                grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            }
        }

        @media (max-width: 768px) {
            .commodity-lab-kpi-strip {
                grid-template-columns: 1fr;
            }

            .commodity-lab-metric-value {
                font-size: 20px;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()
    render_option_sidebar_footer("commodity_option")

with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# 🔥【手机端专属补丁】修复文字看不清的问题
st.markdown("""
<style>
    @media (max-width: 768px) {
        /* ===========================
           1. 标题与文字颜色修复 (新增)
           =========================== */
        /* 强制所有标题 (h1-h4) 变成深黑色 */
        h1, h2, h3, h4, h5, h6 {
            color: #1f2937 !important; /* 深炭灰色，对比度极高 */
        }

        /* 修复普通文本 (p) 的颜色，防止正文也看不清 */
        [data-testid="stMarkdownContainer"] p {
            color: #374151 !important;
        }

        /* ===========================
           2. 指标卡片 (st.metric) 修复
           =========================== */
        [data-testid="stMetric"] {
            background-color: #ffffff !important; /* 强制白底 */
            border: 1px solid #e5e7eb !important; /* 浅灰边框 */
            border-radius: 8px !important;
            padding: 12px 16px !important;
            box-shadow: 0 1px 2px rgba(0,0,0,0.05) !important;
            margin-bottom: 8px !important;
        }

        /* 标签 (如 "当前 IV") */
        [data-testid="stMetricLabel"] {
            color: #6b7280 !important; /* 灰色 */
            font-size: 14px !important;
        }

        /* 数值 (如 "15.90%") */
        [data-testid="stMetricValue"] {
            color: #111827 !important; /* 纯黑 */
        }

        /* ===========================
           3. 其他组件适配
           =========================== */
        /* 状态提示框 (st.info/warning) */
        [data-testid="stAlert"] {
            background-color: #f3f4f6 !important;
            color: #1f2937 !important;
            border: 1px solid #e5e7eb !important;
        }
        [data-testid="stAlert"] p {
            color: #1f2937 !important;
        }

        /* 下拉框文字 */
        div[data-baseweb="select"] span {
            color: #1f2937 !important;
        }

        /* 手机端顶部容器文字 */
        .mobile-top-container {
            color: #1f2937 !important;
        }
    }
</style>
""", unsafe_allow_html=True)
st.markdown("<style>.stSelectbox {margin-bottom: 20px;}</style>", unsafe_allow_html=True)

# 🔥【PC端样式修复】解决Edge浏览器白底看不到文字的问题
st.markdown("""
<style>
    /* ===========================
       PC端样式修复 (Edge浏览器兼容)
       =========================== */
    /* PC端全局样式 */
    body, p, span, div {
        color: #1f2937 !important;
    }

    /* 1. 全局文字颜色强制设定 */
    body {
        color: #1f2937 !important;
    }

    p, span, div, label {
        color: #374151 !important;
    }

    /* 2. 所有标题颜色 */
    h1, h2, h3, h4, h5, h6 {
        color: #111827 !important;
        font-weight: 600 !important;
    }

    /* 3. Streamlit Metric 组件修复 */
    [data-testid="stMetric"] {
        background-color: #ffffff !important;
        border: 1px solid #e5e7eb !important;
        border-radius: 8px !important;
        padding: 12px 16px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1) !important;
    }

    [data-testid="stMetricLabel"] {
        color: #6b7280 !important;
        font-size: 14px !important;
        font-weight: 500 !important;
    }

    [data-testid="stMetricValue"] {
        color: #111827 !important;
        font-size: 28px !important;
        font-weight: 600 !important;
    }

    [data-testid="stMetricDelta"] {
        font-size: 14px !important;
        font-weight: 500 !important;
    }

    /* 4. Markdown容器文字 */
    [data-testid="stMarkdownContainer"] {
        color: #1f2937 !important;
    }

    [data-testid="stMarkdownContainer"] p {
        color: #374151 !important;
    }

    [data-testid="stMarkdownContainer"] strong {
        color: #111827 !important;
        font-weight: 600 !important;
    }

    [data-testid="stMarkdownContainer"] li {
        color: #374151 !important;
    }

    /* 5. 下拉框和选择器 */
    div[data-baseweb="select"] {
        background-color: #ffffff !important;
    }

    div[data-baseweb="select"] span {
        color: #1f2937 !important;
    }

    div[data-baseweb="select"] input {
        color: #1f2937 !important;
    }
    

    /* 7. 按钮文字 */
    button {
        color: #ffffff !important;
        font-weight: 500 !important;
    }

    button[kind="secondary"] {
        color: #1f2937 !important;
        background-color: #ffffff !important;
        border: 1px solid #d1d5db !important;
    }

    /* 8. Info/Warning/Success/Error 提示框 */
    [data-testid="stAlert"] {
        background-color: #f3f4f6 !important;
        border-left: 4px solid #3b82f6 !important;
        color: #1f2937 !important;
    }

    [data-testid="stAlert"] p {
        color: #1f2937 !important;
    }

    [data-testid="stAlert"] div {
        color: #1f2937 !important;
    }

    /* 9. 表格样式 */
    [data-testid="stTable"] {
        color: #1f2937 !important;
    }

    [data-testid="stDataFrame"] {
        color: #1f2937 !important;
    }

    table {
        color: #1f2937 !important;
    }

    th {
        background-color: #f3f4f6 !important;
        color: #111827 !important;
        font-weight: 600 !important;
    }

    td {
        color: #374151 !important;
    }

    /* 10. 文本输入框 */
    input, textarea, select {
        color: #1f2937 !important;
        background-color: #ffffff !important;
        border: 1px solid #d1d5db !important;
    }

    input::placeholder {
        color: #9ca3af !important;
    }

    /* 11. 标签和标题 */
    label {
        color: #374151 !important;
        font-weight: 500 !important;
    }

    /* 12. 链接 */
    a {
        color: #2563eb !important;
    }

    a:hover {
        color: #1d4ed8 !important;
    }

    /* 13. 代码块 */
    code {
        color: #111827 !important;
        background-color: #f3f4f6 !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
    }

    pre {
        background-color: #f3f4f6 !important;
        color: #111827 !important;
    }

    /* 14. Expander组件 */
    [data-testid="stExpander"] {
        background-color: #ffffff !important;
        border: 1px solid #e5e7eb !important;
    }

    [data-testid="stExpander"] summary {
        color: #111827 !important;
        font-weight: 500 !important;
    }

    /* 15. 确保主容器背景色 */
    .main {
        background-color: #ffffff !important;
    }

    [data-testid="stAppViewContainer"] {
        background-color: #ffffff !important;
    }

    /* 16. 修复可能的透明背景问题 */
    .element-container {
        background-color: transparent !important;
    }

    /* 17. 顶部状态栏 */
    header[data-testid="stHeader"] {
        background-color: #ffffff !important;
    }

    /* 18. Tabs组件 */
    [data-testid="stTabs"] button {
        color: #6b7280 !important;
    }

    [data-testid="stTabs"] button[aria-selected="true"] {
        color: #111827 !important;
        font-weight: 600 !important;
    }

    /* 19. 确保所有子元素继承颜色 */
    * {
        color: inherit;
    }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
    /* ========================================
       深色侧边栏样式 (与Home页保持一致)
       ======================================== */
       
    /* 主内容区背景色修复 */
    .stApp {
        background-color: #f8f9fa !important;
    }
    
    [data-testid="stAppViewContainer"] {
        background-color: #f8f9fa !important;
    }
    
    .main {
        background-color: #f8f9fa !important;
    }
    
    [data-testid="stMainBlockContainer"] {
        background-color: #f8f9fa !important;
    }
    
    /* 确保卡片保持白色对比 */
    [data-testid="stMetric"] {
        background-color: #ffffff !important;
    }
    
    [data-testid="stPlotlyChart"] {
        background-color: #ffffff !important;
        padding: 16px !important;
        border-radius: 8px !important;
    }

    /* 1. 侧边栏整体深色背景 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important; /* 深蓝黑色，与Home页一致 */
    }

    /* 2. 侧边栏内所有文字变亮色 */
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] label {
        color: #cbd5e1 !important; /* 亮灰蓝色文字 */
    }

    /* 3. 侧边栏标题（h1-h6）样式 */
    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3,
    [data-testid="stSidebar"] h4 {
        color: #f1f5f9 !important; /* 更亮的白色 */
        font-weight: 600 !important;
    }

    /* 4. 下拉选择框样式 */
    [data-testid="stSidebar"] div[data-baseweb="select"] {
        background-color: #1e293b !important; /* 稍亮的深色背景 */
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] > div {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] [role="combobox"] {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] input {
        color: #e2e8f0 !important;
        -webkit-text-fill-color: #e2e8f0 !important;
        caret-color: #e2e8f0 !important;
        background-color: transparent !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] input::selection {
        background-color: #334155 !important;
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] span {
        color: #e2e8f0 !important; /* 亮色文字 */
    }

    [data-testid="stSidebar"] div[data-baseweb="select"] svg {
        fill: #cbd5e1 !important; /* 下拉箭头亮色 */
    }

    /* 5. 输入框样式 */
    [data-testid="stSidebar"] input {
        background-color: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] input::placeholder {
        color: #64748b !important;
    }

    /* 6. 按钮样式 */
    [data-testid="stSidebar"] button {
        background-color: #1e293b !important;
        color: #e2e8f0 !important;
        border: 1px solid #334155 !important;
        transition: all 0.2s ease-in-out !important;
    }

    [data-testid="stSidebar"] button:hover {
        background-color: #334155 !important;
        border-color: #475569 !important;
        color: #ffffff !important;
    }

    /* 7. 单选按钮和复选框 */
    [data-testid="stSidebar"] [data-baseweb="radio"] label,
    [data-testid="stSidebar"] [data-baseweb="checkbox"] label {
        color: #cbd5e1 !important;
    }

    /* 8. 滑块（Slider）样式 */
    [data-testid="stSidebar"] [data-baseweb="slider"] {
        color: #cbd5e1 !important;
    }

    /* 9. Expander（展开器）样式 */
    [data-testid="stSidebar"] [data-testid="stExpander"] {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] [data-testid="stExpander"] summary {
        color: #e2e8f0 !important;
    }

    /* 10. 分隔线样式 */
    [data-testid="stSidebar"] hr {
        border-color: #334155 !important;
    }

    /* 12. 侧边栏中的Info/Warning/Success框 */
    [data-testid="stSidebar"] [data-testid="stAlert"] {
        background-color: #1e293b !important;
        border-left: 4px solid #3b82f6 !important;
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] [data-testid="stAlert"] p {
        color: #cbd5e1 !important;
    }

    /* 13. 侧边栏中的Markdown容器 */
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        color: #cbd5e1 !important;
    }

    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] strong {
        color: #f1f5f9 !important;
    }

    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] a {
        color: #60a5fa !important;
    }

    /* 14. 侧边栏中的代码块 */
    [data-testid="stSidebar"] code {
        background-color: #1e3a5f !important;
        color: #ffd700 !important;
        border: 1px solid #4a90e2 !important;
        padding: 2px 6px !important;
        border-radius: 4px !important;
    }

    /* 15. 联系卡片样式（如果有的话）*/
    [data-testid="stSidebar"] .contact-card {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        padding: 16px !important;
    }

    [data-testid="stSidebar"] .contact-title {
        color: #f1f5f9 !important;
    }

    [data-testid="stSidebar"] .contact-item {
        color: #94a3b8 !important;
    }

    [data-testid="stSidebar"] .wechat-highlight {
        color: #00e676 !important; /* 微信绿 */
    }

    /* 16. 确保侧边栏顶部区域也是深色 */
    [data-testid="stSidebarNav"] {
        background-color: #0f172a !important;
    }

    /* 17. 侧边栏中的选择框下拉菜单 */
    [data-testid="stSidebar"] ul[role="listbox"] {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
    }

    [data-testid="stSidebar"] ul[role="listbox"] li {
        color: #e2e8f0 !important;
    }

    [data-testid="stSidebar"] ul[role="listbox"] li:hover {
        background-color: #334155 !important;
    }

    /* 18. 状态指示器 */
    [data-testid="stSidebar"] .stMetricValue {
        color: #f1f5f9 !important;
    }

    [data-testid="stSidebar"] .stMetricLabel {
        color: #94a3b8 !important;
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")
inject_option_page_header_style()
_inject_commodity_lab_style()


# 获取合约列表函数 (已修复 % 报错问题)
@st.cache_data(ttl=300, show_spinner=False)
def get_contracts(v: str, cutoff_yyyymmdd: str, current_yymm: int):
    if de.engine is None:
        return []
    try:
        # 直接按品种前缀在 SQL 过滤，避免全表合约池扫描
        raw_codes = _cached_recent_contract_pool(cutoff_yyyymmdd, v)
        valid_subs = []

        for code in raw_codes:
            # 正则提取：字母部分 + 数字部分
            match = re.match(r"([a-zA-Z]+)(\d+)", code)
            if not match:
                continue

            prefix = match.group(1)
            num_part = match.group(2)

            # SQL端已做严格前缀过滤，这里仅做最终保险
            if prefix.upper() != v.upper():
                continue

            # --- 修复 2: 过滤过期合约 ---
            # 处理年份：郑商所 3位 (501 -> 2501)，其他 4位 (2501)
            if len(num_part) == 3:
                # 假设是 2020 年代，补全为 2501 这种格式
                compare_val = int('2' + num_part)
            elif len(num_part) == 4:
                compare_val = int(num_part)
            else:
                continue

            # 过滤逻辑：只显示 未过期 或 最近1个月内过期 的合约
            # 比如现在是 2512，那么 2511 还会显示，2510 就不显示了
            if compare_val >= (current_yymm - 1):
                valid_subs.append(code)

        # 去重并限制展示数量，降低 selectbox 与前端渲染负载
        valid_subs = sorted(list(dict.fromkeys(valid_subs)), reverse=True)[:MAX_CONTRACT_OPTIONS]

        # 把 "主力连续" 放在第一个
        options = [f"{v.upper()} (主力连续)"] + valid_subs
        return options

    except Exception as e:
        st.error(f"合约加载失败: {e}")
        return []


title_col, variety_col, contract_col = st.columns([0.50, 0.20, 0.30], gap="small", vertical_alignment="center")
with title_col:
    st.markdown('<div class="commodity-lab-header-marker"></div>', unsafe_allow_html=True)
    render_option_page_title("商品期权")
with variety_col:
    variety = st.selectbox(
        "品种",
        list(COMMODITY_MAP.keys()),
        format_func=lambda x: f"{x} ({COMMODITY_MAP[x]})",
        label_visibility="collapsed",
    )

now = dt.datetime.now()
cutoff_yyyymmdd = (now - dt.timedelta(days=CONTRACT_LOOKBACK_DAYS)).strftime("%Y%m%d")
current_yymm = int(now.strftime("%y%m"))
_db_t0 = time.perf_counter()
options = get_contracts(variety, cutoff_yyyymmdd, current_yymm)
_perf_page_log(
    page=PAGE_NAME,
    db_ms=(time.perf_counter() - _db_t0) * 1000,
    rows=len(options),
    stage="get_contracts",
)

with contract_col:
    if options:
        selected_opt = st.selectbox("合约", options, label_visibility="collapsed")
    else:
        st.selectbox("合约", ["暂无可用合约"], disabled=True, label_visibility="collapsed")
        selected_opt = None

if selected_opt and "主力连续" in selected_opt:
    target_contract = variety.upper()
    is_continuous = True
else:
    target_contract = selected_opt
    is_continuous = False

view_options = ["总览", "资金流", "波动率结构", "持仓信号"]
if st.session_state.get("commodity_option_active_view") not in view_options:
    st.session_state["commodity_option_active_view"] = "总览"

nav_col, _ = st.columns([0.58, 0.42], gap="small", vertical_alignment="center")
with nav_col:
    st.markdown('<div class="commodity-lab-page-nav"></div>', unsafe_allow_html=True)
    active_view = st.segmented_control(
        "页面",
        options=view_options,
        label_visibility="collapsed",
        key="commodity_option_active_view",
    ) or "总览"

st.markdown('<div class="option-page-header-divider"></div>', unsafe_allow_html=True)

if not options:
    st.warning(f"未找到 {variety} 的相关合约数据")

# 3. 数据获取函数
@st.cache_data(ttl=90, show_spinner=False)
def get_chart_data(code: str, is_continuous_flag: bool):
    if not code: return None, None
    try:
        # A. 获取 IV (直接查 commodity_iv_history)
        sql_iv = text(
            f"""
            SELECT trade_date, iv, hv, used_contract
            FROM (
                SELECT trade_date, iv, hv, used_contract
                FROM commodity_iv_history
                WHERE ts_code=:c
                ORDER BY trade_date DESC
                LIMIT {MAX_CHART_ROWS}
            ) t
            ORDER BY trade_date
            """
        )
        try:
            df_iv = pd.read_sql(sql_iv, de.engine, params={"c": code})
        except Exception:
            sql_iv = text(
                f"""
                SELECT trade_date, iv, used_contract
                FROM (
                    SELECT trade_date, iv, used_contract
                    FROM commodity_iv_history
                    WHERE ts_code=:c
                    ORDER BY trade_date DESC
                    LIMIT {MAX_CHART_ROWS}
                ) t
                ORDER BY trade_date
                """
            )
            df_iv = pd.read_sql(sql_iv, de.engine, params={"c": code})

        # B. 获取 K线 (期货价格)
        sql_k = text(
            f"""
            SELECT
                trade_date,
                open_price AS open,
                high_price AS high,
                low_price AS low,
                close_price AS close,
                vol AS volume
            FROM (
                SELECT trade_date, open_price, high_price, low_price, close_price, vol
                FROM futures_price
                WHERE ts_code=:c
                ORDER BY trade_date DESC
                LIMIT {MAX_CHART_ROWS}
            ) t
            ORDER BY trade_date
            """
        )
        df_k = pd.read_sql(sql_k, de.engine, params={"c": code})

        # 容错：如果查 IF (主连) 没查到价格，尝试查 IF0 (常见的连续代码)
        if df_k.empty and is_continuous_flag:
            alternatives = [f"{code}0", f"{code}888", f"{code.lower()}0"]
            for alt in alternatives:
                df_k = pd.read_sql(sql_k, de.engine, params={"c": alt})
                if not df_k.empty: break

        return df_k, df_iv
    except Exception as e:
        return None, None


@st.cache_data(ttl=90, show_spinner=False)
def _prepare_chart_frames(df_kline: pd.DataFrame, df_iv: pd.DataFrame | None):
    if "volume" not in df_kline.columns:
        df_kline = df_kline.copy()
        df_kline["volume"] = 0.0
    chart_k = df_kline[["trade_date", "open", "high", "low", "close", "volume"]].copy()
    chart_k.columns = ["date", "open", "high", "low", "close", "volume"]
    chart_k["date"] = pd.to_datetime(chart_k["date"]).dt.strftime("%Y-%m-%d")

    chart_iv = pd.DataFrame()
    rank_base = pd.DataFrame()
    if df_iv is not None and not df_iv.empty:
        chart_iv = df_iv[["trade_date", "iv"]].rename(columns={"trade_date": "date"})
        chart_iv["date"] = pd.to_datetime(chart_iv["date"]).dt.strftime("%Y-%m-%d")
        chart_iv["iv"] = pd.to_numeric(chart_iv["iv"], errors="coerce")
        chart_iv = chart_iv[chart_iv["iv"] > 0]

        rank_base = df_iv.copy()
        rank_base["iv"] = pd.to_numeric(rank_base["iv"], errors="coerce")
        rank_base = rank_base[rank_base["iv"] > 0.0001].tail(252)

    return chart_k, chart_iv, rank_base


# 4. 绘图逻辑
if target_contract:
    _db_t0 = time.perf_counter()
    df_kline, df_iv = get_chart_data(target_contract, is_continuous)
    chart_rows = (len(df_kline) if df_kline is not None else 0) + (len(df_iv) if df_iv is not None else 0)
    _perf_page_log(
        page=PAGE_NAME,
        db_ms=(time.perf_counter() - _db_t0) * 1000,
        rows=chart_rows,
        stage="get_chart_data",
    )

    if df_kline is not None and not df_kline.empty:
        chart_k, chart_iv, df_rank_base = _prepare_chart_frames(df_kline, df_iv)
        latest_used_contract = ""
        if df_iv is not None and not df_iv.empty:
            latest_used_contract = str(df_iv.iloc[-1].get("used_contract") or "").split(".")[0].upper()

        market_row = None
        if is_continuous and df_iv is not None and not df_iv.empty and USE_GLOBAL_MONITOR_SNAPSHOT:
            _db_t1 = time.perf_counter()
            df_market = _cached_comprehensive_market_data()
            _perf_page_log(
                page=PAGE_NAME,
                db_ms=(time.perf_counter() - _db_t1) * 1000,
                rows=len(df_market),
                stage="get_comprehensive_market_data",
            )
            market_row = _pick_market_row_by_product(df_market, variety, latest_used_contract)

        flow_symbol = variety.upper() if is_continuous else (_extract_contract_code(str(target_contract)) or str(target_contract).upper())
        _db_t2 = time.perf_counter()
        df_flow_overview = _cached_futures_fund_flow_overview(20)
        flow_row = _pick_fund_flow_row(df_flow_overview, variety, flow_symbol)
        _perf_page_log(
            page=PAGE_NAME,
            db_ms=(time.perf_counter() - _db_t2) * 1000,
            rows=len(df_flow_overview),
            stage="get_futures_fund_flow_overview",
        )

        _db_t3 = time.perf_counter()
        df_flow_trend = _cached_single_futures_fund_trend(flow_symbol, 60)
        _perf_page_log(
            page=PAGE_NAME,
            db_ms=(time.perf_counter() - _db_t3) * 1000,
            rows=len(df_flow_trend),
            stage="get_single_futures_fund_trend",
        )

        _db_t4 = time.perf_counter()
        broker_extremes = _cached_latest_broker_extremes(variety)
        _perf_page_log(
            page=PAGE_NAME,
            db_ms=(time.perf_counter() - _db_t4) * 1000,
            rows=1 if broker_extremes else 0,
            stage="get_latest_broker_extremes",
        )

        iv_stats = _compute_iv_stats(df_iv, df_rank_base, market_row, chart_k)
        flow_snapshot = _build_fund_flow_snapshot(flow_row, df_flow_trend, variety)

        _render_overview_groups(iv_stats, flow_snapshot, broker_extremes)

        main_col, rail_col = st.columns([2.45, 1.05], gap="small")
        with main_col:
            _render_commodity_price_iv_chart(chart_k, chart_iv, str(target_contract))

            if is_continuous and df_iv is not None and not df_iv.empty:
                used = df_iv.iloc[-1].get("used_contract")
                if used:
                    st.caption(f"当前主力合约参考: {used}，IV 计算基于此合约。")

        with rail_col:
            _render_volatility_rail(iv_stats, chart_k, str(target_contract))
            _render_flow_judgment(flow_snapshot)

    else:
        st.warning(f"暂无 {target_contract} 的 K 线数据。")
        if is_continuous:
            st.caption("提示：可能是数据库中 futures_price 表缺少主连代码（如 IF 或 IF0）。")

_perf_page_log(
    page=PAGE_NAME,
    render_ms=(time.perf_counter() - _PAGE_T0) * 1000,
    cache_hit=-1,
    stage="page_done",
)





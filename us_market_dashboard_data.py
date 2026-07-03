from __future__ import annotations

import datetime as dt
import json
import math
import re
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text

from us_options_polygon import (
    compact_date,
    DEFAULT_UNDERLYINGS as DEFAULT_US_OPTION_UNDERLYINGS,
    dte_for_trade_date,
    get_db_engine,
    get_us_option_chain_daily,
    get_us_underlying_iv_rank,
    normalize_iv_value,
    table_names,
)


DASHBOARD_UNDERLYING_PRIORITY = ("SPY", "QQQ", "DIA", "IWM")
DEFAULT_DASHBOARD_UNDERLYINGS = DASHBOARD_UNDERLYING_PRIORITY + tuple(
    sorted(symbol for symbol in DEFAULT_US_OPTION_UNDERLYINGS if symbol not in DASHBOARD_UNDERLYING_PRIORITY)
)
UNDERLYING_DISPLAY_NAMES = {
    "SPY": "标普500ETF",
    "QQQ": "纳指100ETF",
    "IWM": "罗素2000ETF",
    "GLD": "黄金ETF",
    "TLT": "20年美债ETF",
    "SLV": "白银ETF",
    "XLF": "金融板块ETF",
    "XLE": "能源板块ETF",
    "DIA": "道指ETF",
    "HYG": "高收益债ETF",
    "TSLA": "特斯拉",
    "NVDA": "英伟达",
    "AMD": "超威半导体",
    "AAPL": "苹果",
    "AMZN": "亚马逊",
    "AVGO": "博通",
    "COIN": "Coinbase",
    "GOOGL": "谷歌",
    "HOOD": "Robinhood",
    "INTC": "英特尔",
    "META": "Meta",
    "MSFT": "微软",
    "MSTR": "MicroStrategy",
    "NFLX": "奈飞",
    "PLTR": "帕兰提尔",
    "SMCI": "超微电脑",
    "TSM": "台积电",
}

STOCK_DAILY_COLUMNS = ["date", "symbol", "open", "high", "low", "close", "volume", "adjClose"]
OPTION_CHAIN_COLUMNS = [
    "trade_date",
    "option_ticker",
    "underlying",
    "call_put",
    "strike",
    "expiration_date",
    "expiration_type",
    "settlement_type",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vwap",
    "transactions",
    "open_interest",
    "provider_iv",
    "computed_iv",
    "iv_source",
    "underlying_price",
    "dte",
    "cycle_label",
    "iv",
    "iv_pct",
    "moneyness_pct",
]
VOLATILITY_CONE_TARGETS = (7, 14, 21, 30, 45, 60, 90)
VOLATILITY_CONE_COLUMNS = ["dte_target", "p10", "p25", "p50", "p75", "p90", "sample_count"]
VOLATILITY_CONE_LINE_COLUMNS = ["dte_target", "dte", "expiration_date", "iv_pct", "sample_count"]
VOLATILITY_CONE_DAILY_CACHE_TABLE = "us_option_volatility_cone_daily"
VOLATILITY_CONE_DAILY_CACHE_COLUMNS = [
    "trade_date",
    "underlying",
    "dte_target",
    "dte",
    "expiration_date",
    "iv_pct",
    "sample_count",
]
OTM_VOLATILITY_CURVE_COLUMNS = ["moneyness_pct", "iv_pct", "call_put", "expiration_date", "dte"]
OI_DEFENSE_COLUMNS = [
    "trade_date",
    "date",
    "underlying",
    "underlying_close",
    "call_strike",
    "call_oi",
    "call_distance_pct",
    "call_expiration",
    "put_strike",
    "put_oi",
    "put_distance_pct",
    "put_expiration",
    "total_call_oi",
    "total_put_oi",
    "put_call_oi",
]
OI_DEFENSE_CACHE_TABLE = "us_option_oi_defense_daily"
MARKET_METRICS_COLUMNS = [
    "trade_date",
    "underlying",
    "atm_iv_pct",
    "iv_change_1d",
    "rv20_pct",
    "rv60_pct",
    "iv_rv20_spread",
    "iv_30d",
    "iv_60d",
    "term_slope_30_60",
    "term_state",
    "skew_expiration",
    "put_skew_5pct",
    "call_skew_5pct",
    "put_call_oi",
    "put_call_volume",
    "zero_dte_volume_share_pct",
    "top_oi_strike",
    "top_oi",
    "top5_oi_share_pct",
    "total_open_interest",
    "total_volume",
    "monthly_contract_count",
    "short_cycle_contract_count",
    "provider_iv_rows",
    "computed_iv_rows",
    "open_interest_rows",
    "source",
    "updated_at",
]


def oi_defense_y_axis_range(
    defense_df: pd.DataFrame | None,
    *,
    padding_ratio: float = 0.12,
    min_padding: float = 1.0,
) -> list[float] | None:
    if defense_df is None or defense_df.empty:
        return None

    value_series = []
    for col in ("underlying_close", "call_strike", "put_strike"):
        if col in defense_df.columns:
            values = pd.to_numeric(defense_df[col], errors="coerce").dropna()
            if not values.empty:
                value_series.append(values)
    if not value_series:
        return None

    all_values = pd.concat(value_series, ignore_index=True)
    low = float(all_values.min())
    high = float(all_values.max())
    if not math.isfinite(low) or not math.isfinite(high):
        return None

    span = high - low
    if span <= 0:
        padding = max(float(min_padding), abs(high or 1.0) * float(padding_ratio))
    else:
        padding = max(float(min_padding), span * float(padding_ratio))
    return [low - padding, high + padding]
MARKET_METRIC_NUMERIC_COLUMNS = [
    "atm_iv_pct",
    "iv_change_1d",
    "rv20_pct",
    "rv60_pct",
    "iv_rv20_spread",
    "iv_30d",
    "iv_60d",
    "term_slope_30_60",
    "put_skew_5pct",
    "call_skew_5pct",
    "put_call_oi",
    "put_call_volume",
    "zero_dte_volume_share_pct",
    "top_oi_strike",
    "top_oi",
    "top5_oi_share_pct",
    "total_open_interest",
    "total_volume",
    "monthly_contract_count",
    "short_cycle_contract_count",
    "provider_iv_rows",
    "computed_iv_rows",
    "open_interest_rows",
]
MARKET_CLIMATE_COLUMNS = [
    "indicator_code",
    "as_of_date",
    "value",
    "secondary_value",
    "unit",
    "source",
    "payload_json",
    "updated_at",
]
MARKET_CLIMATE_CARD_ORDER = [
    "VIX期限",
    "利率曲线",
    "实际利率",
    "政策预期",
    "AAII情绪",
    "VIX净仓",
    "供应链压力",
    "信用利差",
]
MARKET_CLIMATE_CACHE_CODES = [
    "VIX_TERM",
    "FEDWATCH",
    "AAII_BULL_BEAR",
    "CFTC_VIX_LEV_NET",
    "GSCPI",
]
MARKET_CLIMATE_MACRO_CODES = [
    "DGS10",
    "T10Y3M",
    "DFII10",
    "BAMLH0A0HYM2",
    "SOFR",
    "FEDFUNDS",
]
MARKET_CLIMATE_FRESHNESS_DAYS = {
    "VIX_TERM": 7,
    "FEDWATCH": 7,
    "AAII_BULL_BEAR": 14,
    "CFTC_VIX_LEV_NET": 14,
    "GSCPI": 60,
    "DGS10": 7,
    "T10Y3M": 7,
    "DFII10": 7,
    "BAMLH0A0HYM2": 7,
    "SOFR": 7,
    "FEDFUNDS": 90,
}
MARKET_CLIMATE_HINTS = {
    "VIX期限": "读法：VIX9D减VIX3M。>0代表短期恐慌高于中期，事件压力高，偏看空或防守；-5到0较常见；<-5说明短期压力低，偏利好风险偏好。",
    "利率曲线": "读法：10年美债减3个月利率。<0是倒挂，越深越担心经济放慢，偏压股市；0到1%算修复区；>1.5%可能是长端利率太高，也会压估值。",
    "实际利率": "读法：扣掉通胀后的10年真实利率。>2%资金成本偏高，压股票估值；1到2%中性偏紧；<1%较宽松。上行偏空，下行偏多。",
    "政策预期": "读法：市场认为美联储下次FOMC最可能的动作。维持或降息概率高，通常说明政策压力没有加大，偏利好；加息概率升，或高利率更久，偏压股市。",
    "AAII情绪": "读法：散户看多比例减看空比例。>+20pp很乐观，容易拥挤，要防追高；<-20pp很悲观，反而常有反弹土壤；-10到+10大致中性。",
    "VIX净仓": "读法：杠杆基金VIX净仓占未平仓比例。>+10%说明防波动的人多，市场紧张；<-10%说明押平静的人多，短线利好风险偏好，但坏消息来时波动会放大。",
    "供应链压力": "读法：0附近算正常。>1说明供应链紧、成本和通胀压力高，偏压估值；<-1说明供应链很顺，偏利多。看趋势：上行偏空，下行偏多。",
    "信用利差": "读法：高收益债比国债多给的利差。<3%风险偏好好，偏利多；3到5%是警戒区；>5%信用压力大，偏看空；>8%通常是明显风险事件。",
}
HISTORICAL_PERCENTILE_FIELDS = {
    "iv_change_1d": "iv_change_1d_percentile",
    "iv_rv20_spread": "iv_rv20_percentile",
    "term_slope_30_60": "term_slope_percentile",
    "put_skew_5pct": "put_skew_5pct_percentile",
    "call_skew_5pct": "call_skew_5pct_percentile",
    "put_call_skew_5pct": "put_call_skew_5pct_percentile",
    "put_call_oi": "put_call_oi_percentile",
    "put_call_volume": "put_call_volume_percentile",
    "zero_dte_volume_share_pct": "zero_dte_volume_share_percentile",
    "top5_oi_share_pct": "top5_oi_share_percentile",
    "total_open_interest": "total_open_interest_percentile",
    "total_volume": "total_volume_percentile",
}


def dashboard_engine():
    return get_db_engine()


def normalize_underlying(underlying: str) -> str:
    return str(underlying or "").strip().upper()


def normalize_trade_date(value: str | dt.date | dt.datetime | None) -> str:
    return compact_date(value)


def safe_table_name(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name or ""):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def _mysql_force_index(engine, index_name: str) -> str:
    dialect_name = getattr(getattr(engine, "dialect", None), "name", "")
    if dialect_name != "mysql":
        return ""
    safe_name = safe_table_name(index_name)
    return f" FORCE INDEX ({safe_name})"


def _named_in_clause(prefix: str, values: list[str]) -> tuple[str, dict[str, str]]:
    params = {f"{prefix}_{idx}": value for idx, value in enumerate(values)}
    placeholders = ", ".join(f":{key}" for key in params)
    return placeholders, params


def option_table_names(use_test_tables: bool = False) -> dict[str, str]:
    return table_names(use_test_tables)


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


_TABLE_EXISTS_CACHE: dict[tuple[int, str], bool] = {}
_TABLE_COLUMNS_CACHE: dict[tuple[int, str], set[str]] = {}


def _schema_cache_enabled(engine) -> bool:
    return getattr(getattr(engine, "dialect", None), "name", "") != "sqlite"


def table_exists(engine, table_name: str) -> bool:
    if engine is None:
        return False
    safe_name = safe_table_name(table_name)
    cache_key = (id(engine), safe_name)
    if _schema_cache_enabled(engine) and cache_key in _TABLE_EXISTS_CACHE:
        return _TABLE_EXISTS_CACHE[cache_key]
    try:
        exists = bool(inspect(engine).has_table(safe_name))
    except Exception:
        return False
    if _schema_cache_enabled(engine):
        _TABLE_EXISTS_CACHE[cache_key] = exists
    return exists


def table_columns(engine, table_name: str) -> set[str]:
    if engine is None:
        return set()
    safe_name = safe_table_name(table_name)
    cache_key = (id(engine), safe_name)
    if _schema_cache_enabled(engine) and cache_key in _TABLE_COLUMNS_CACHE:
        return set(_TABLE_COLUMNS_CACHE[cache_key])
    try:
        columns = {str(col["name"]) for col in inspect(engine).get_columns(safe_name)}
    except Exception:
        return set()
    if _schema_cache_enabled(engine):
        _TABLE_EXISTS_CACHE[cache_key] = True
        _TABLE_COLUMNS_CACHE[cache_key] = set(columns)
    return columns


def _select_expr(columns: set[str], column: str, alias: str | None = None) -> str:
    alias = alias or column
    if column in columns:
        return f"{safe_table_name(column)} AS {safe_table_name(alias)}"
    return f"NULL AS {safe_table_name(alias)}"


def _scalar(engine, sql, params: dict[str, Any] | None = None) -> Any:
    try:
        with engine.connect() as conn:
            return conn.execute(sql, params or {}).scalar()
    except Exception:
        return None


def _clean_number(value: Any) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _as_date(value: Any) -> dt.date | None:
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()


def _date_detail(value: Any) -> str:
    as_of = _as_date(value)
    if as_of is None:
        return ""
    return as_of.strftime("%m/%d")


def _format_plain_number(value: Any, digits: int = 1) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    return f"{number:,.{digits}f}"


def _format_pct_card(value: Any, digits: int = 1, *, signed: bool = False) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.{digits}f}%"


def _format_pp_card(value: Any, digits: int = 1, *, signed: bool = True) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    prefix = "+" if signed and number > 0 else ""
    return f"{prefix}{number:.{digits}f}pp"


def _format_signed_value(value: Any, digits: int = 1, suffix: str = "") -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    prefix = "+" if number > 0 else ""
    return f"{prefix}{number:.{digits}f}{suffix}"


def _format_bp_change(value: Any, *, signed: bool = True) -> str:
    number = _clean_number(value)
    if number is None:
        return "--"
    bps = number * 100
    prefix = "+" if signed and bps > 0 else ""
    return f"{prefix}{bps:.0f}bp"


def _payload_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(value))
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _freshness_status(as_of: Any, code: str, today: dt.date | None = None) -> tuple[str, int | None]:
    as_of_date = _as_date(as_of)
    if as_of_date is None:
        return "missing", None
    today = today or dt.datetime.now().date()
    age = (today - as_of_date).days
    max_age = MARKET_CLIMATE_FRESHNESS_DAYS.get(code, 14)
    return ("stale" if age > max_age else "fresh"), age


def _detail_with_date(detail: str, as_of: Any, code: str, today: dt.date | None) -> str:
    parts = [part for part in [detail, _date_detail(as_of)] if part]
    status, age = _freshness_status(as_of, code, today)
    if status == "stale" and age is not None:
        parts.append(f"旧{age}天")
    return " · ".join(parts) if parts else "暂无缓存"


def _market_climate_card(
    label: str,
    value: str = "--",
    detail: str = "暂无缓存",
    color: str = "#94a3b8",
    *,
    as_of: Any = None,
    code: str = "",
    today: dt.date | None = None,
) -> dict[str, Any]:
    status, age = _freshness_status(as_of, code, today) if code else ("missing", None)
    return {
        "label": label,
        "value": value or "--",
        "detail": detail or "暂无缓存",
        "color": color,
        "as_of": _as_date(as_of).isoformat() if _as_date(as_of) else None,
        "freshness": status,
        "age_days": age,
        "hint": MARKET_CLIMATE_HINTS.get(label, ""),
    }


def _empty_market_climate_card(label: str) -> dict[str, Any]:
    return _market_climate_card(label)


def _load_latest_market_climate_rows(engine, codes: list[str]) -> dict[str, dict[str, Any]]:
    if engine is None:
        return {}
    columns = table_columns(engine, "market_climate_daily")
    if not {"indicator_code", "as_of_date", "value"}.issubset(columns):
        return {}

    selected = [_select_expr(columns, col) for col in MARKET_CLIMATE_COLUMNS]
    codes = [str(code).strip() for code in codes if str(code or "").strip()]
    if not codes:
        return {}
    placeholders, params = _named_in_clause("code", codes)
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM market_climate_daily
        WHERE indicator_code IN ({placeholders})
        ORDER BY indicator_code, as_of_date DESC
        """
    )
    out: dict[str, dict[str, Any]] = {}
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return {}
    if df.empty:
        return {}
    df = df.drop_duplicates(subset=["indicator_code"], keep="first")
    for row_dict in df.to_dict(orient="records"):
        code = str(row_dict.get("indicator_code") or "")
        if not code:
            continue
        row = dict(row_dict)
        row["value"] = _clean_number(row.get("value"))
        row["secondary_value"] = _clean_number(row.get("secondary_value"))
        row["payload"] = _payload_dict(row.get("payload_json"))
        out[code] = row
    return out


def _load_macro_history_rows(engine, codes: list[str], limit: int = 90) -> dict[str, pd.DataFrame]:
    if engine is None:
        return {}
    columns = table_columns(engine, "macro_daily")
    if not {"trade_date", "indicator_code", "close_value"}.issubset(columns):
        return {}

    selected = [
        _select_expr(columns, "trade_date"),
        _select_expr(columns, "indicator_code"),
        _select_expr(columns, "indicator_name"),
        _select_expr(columns, "close_value"),
        _select_expr(columns, "change_value"),
        _select_expr(columns, "change_pct"),
    ]
    limit = min(max(int(limit or 90), 2), 500)
    codes = [str(code).strip() for code in codes if str(code or "").strip()]
    if not codes:
        return {}
    placeholders, params = _named_in_clause("code", codes)
    params["limit"] = limit
    sql = text(
        f"""
        SELECT trade_date, indicator_code, indicator_name, close_value, change_value, change_pct
        FROM (
            SELECT {", ".join(selected)},
                   ROW_NUMBER() OVER (PARTITION BY indicator_code ORDER BY trade_date DESC) AS rn
            FROM macro_daily
            WHERE indicator_code IN ({placeholders})
        ) scoped
        WHERE rn <= :limit
        ORDER BY indicator_code, trade_date DESC
        """
    )
    try:
        all_rows = pd.read_sql(sql, engine, params=params)
    except Exception:
        fallback_sql = text(
            f"""
            SELECT {", ".join(selected)}
            FROM macro_daily
            WHERE indicator_code IN ({placeholders})
            ORDER BY indicator_code, trade_date DESC
            """
        )
        try:
            all_rows = pd.read_sql(fallback_sql, engine, params={key: value for key, value in params.items() if key != "limit"})
        except Exception:
            return {}
    if all_rows.empty:
        return {}

    out: dict[str, pd.DataFrame] = {}
    for code, df in all_rows.groupby("indicator_code", dropna=False):
        code_text = str(code or "")
        if not code_text:
            continue
        df = df.head(limit).copy()
        if df.empty:
            continue
        df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
        for col in ("close_value", "change_value", "change_pct"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["trade_date", "close_value"]).sort_values("trade_date").reset_index(drop=True)
        if not df.empty:
            out[code_text] = df
    return out


def _latest_macro_row(macro_rows: dict[str, pd.DataFrame], code: str) -> pd.Series | None:
    df = macro_rows.get(code)
    if df is None or df.empty:
        return None
    return df.iloc[-1]


def _macro_change_since(macro_rows: dict[str, pd.DataFrame], code: str, days: int) -> float | None:
    df = macro_rows.get(code)
    if df is None or len(df) < 2:
        return None
    latest = df.iloc[-1]
    latest_date = _as_date(latest.get("trade_date"))
    latest_value = _clean_number(latest.get("close_value"))
    if latest_date is None or latest_value is None:
        return None
    target_date = latest_date - dt.timedelta(days=max(int(days or 1), 1))
    eligible = df[df["trade_date"].dt.date <= target_date]
    if eligible.empty:
        eligible = df.iloc[:-1]
    if eligible.empty:
        return None
    base = _clean_number(eligible.iloc[-1].get("close_value"))
    if base is None:
        return None
    return latest_value - base


def _rate_curve_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    ten_year = _latest_macro_row(macro_rows, "DGS10")
    curve = _latest_macro_row(macro_rows, "T10Y3M")
    if ten_year is None:
        return _empty_market_climate_card("利率曲线")
    ten_value = _clean_number(ten_year.get("close_value"))
    curve_value = _clean_number(curve.get("close_value")) if curve is not None else None
    color = "#dc2626" if curve_value is not None and curve_value < 0 else "#2563eb"
    detail = "10Y-3M " + (_format_pp_card(curve_value, 2) if curve_value is not None else "--")
    return _market_climate_card(
        "利率曲线",
        _format_pct_card(ten_value, 2),
        _detail_with_date(detail, ten_year.get("trade_date"), "DGS10", today),
        color,
        as_of=ten_year.get("trade_date"),
        code="DGS10",
        today=today,
    )


def _real_yield_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    row = _latest_macro_row(macro_rows, "DFII10")
    if row is None:
        return _empty_market_climate_card("实际利率")
    change = _macro_change_since(macro_rows, "DFII10", 5)
    value = _clean_number(row.get("close_value"))
    color = "#dc2626" if value is not None and value >= 2.0 else "#2563eb"
    detail = "5日 " + (_format_bp_change(change) if change is not None else "--")
    return _market_climate_card(
        "实际利率",
        _format_pct_card(value, 2),
        _detail_with_date(detail, row.get("trade_date"), "DFII10", today),
        color,
        as_of=row.get("trade_date"),
        code="DFII10",
        today=today,
    )


def _credit_spread_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    row = _latest_macro_row(macro_rows, "BAMLH0A0HYM2")
    if row is None:
        return _empty_market_climate_card("信用利差")
    change = _macro_change_since(macro_rows, "BAMLH0A0HYM2", 30)
    value = _clean_number(row.get("close_value"))
    color = "#dc2626" if change is not None and change > 0 else "#059669"
    detail = "1M " + (_format_bp_change(change) if change is not None else "--")
    return _market_climate_card(
        "信用利差",
        _format_pct_card(value, 2),
        _detail_with_date(detail, row.get("trade_date"), "BAMLH0A0HYM2", today),
        color,
        as_of=row.get("trade_date"),
        code="BAMLH0A0HYM2",
        today=today,
    )


def _vix_term_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("VIX_TERM")
    if not row:
        return _empty_market_climate_card("VIX期限")
    spread = _clean_number(row.get("value"))
    payload = row.get("payload") or {}
    state = "近端倒挂" if spread is not None and spread > 0 else "远端更高"
    detail = state
    vix = _clean_number(payload.get("vix"))
    if vix is not None:
        detail = f"VIX {_format_plain_number(vix, 1)}"
    color = "#dc2626" if spread is not None and spread > 0 else "#059669"
    return _market_climate_card(
        "VIX期限",
        _format_signed_value(spread, 1, "点"),
        _detail_with_date(detail, row.get("as_of_date"), "VIX_TERM", today),
        color,
        as_of=row.get("as_of_date"),
        code="VIX_TERM",
        today=today,
    )


def _policy_rate_fallback_card(macro_rows: dict[str, pd.DataFrame], today: dt.date | None) -> dict[str, Any]:
    sofr = _latest_macro_row(macro_rows, "SOFR")
    fedfunds = _latest_macro_row(macro_rows, "FEDFUNDS")
    row = sofr if sofr is not None else fedfunds
    if row is None:
        return _empty_market_climate_card("政策预期")
    value = _clean_number(row.get("close_value"))
    source_label = "SOFR" if sofr is not None else "Fed Funds"
    detail = f"{source_label}替代"
    if sofr is not None and fedfunds is not None:
        sofr_value = _clean_number(sofr.get("close_value"))
        fedfunds_value = _clean_number(fedfunds.get("close_value"))
        spread = sofr_value - fedfunds_value if sofr_value is not None and fedfunds_value is not None else None
        detail = "SOFR-Fed " + (_format_bp_change(spread) if spread is not None else "--")
    return _market_climate_card(
        "政策预期",
        _format_pct_card(value, 2),
        _detail_with_date(detail, row.get("trade_date"), str(row.get("indicator_code") or "SOFR"), today),
        "#7c3aed",
        as_of=row.get("trade_date"),
        code=str(row.get("indicator_code") or "SOFR"),
        today=today,
    )


def _fedwatch_card(
    climate_rows: dict[str, dict[str, Any]],
    macro_rows: dict[str, pd.DataFrame],
    today: dt.date | None,
) -> dict[str, Any]:
    row = climate_rows.get("FEDWATCH")
    if not row:
        return _policy_rate_fallback_card(macro_rows, today)
    payload = row.get("payload") or {}
    probability = _clean_number(row.get("value"))
    action_label = str(payload.get("action_label") or payload.get("action") or "最高概率")
    meeting_date = payload.get("meeting_date") or payload.get("event_date")
    detail = f"会议 {_date_detail(meeting_date)}" if meeting_date else "下次会议"
    return _market_climate_card(
        "政策预期",
        f"{action_label} {_format_pct_card(probability, 0)}",
        _detail_with_date(detail, row.get("as_of_date"), "FEDWATCH", today),
        "#7c3aed",
        as_of=row.get("as_of_date"),
        code="FEDWATCH",
        today=today,
    )


def _aaii_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("AAII_BULL_BEAR")
    if not row:
        return _empty_market_climate_card("AAII情绪")
    payload = row.get("payload") or {}
    spread = _clean_number(row.get("value"))
    bullish = _clean_number(payload.get("bullish_pct"))
    bearish = _clean_number(payload.get("bearish_pct"))
    detail = "多空差"
    if bullish is not None and bearish is not None:
        detail = f"牛{bullish:.0f} 熊{bearish:.0f}"
    color = "#dc2626" if spread is not None and spread > 15 else "#2563eb"
    return _market_climate_card(
        "AAII情绪",
        _format_pp_card(spread, 1),
        _detail_with_date(detail, row.get("as_of_date"), "AAII_BULL_BEAR", today),
        color,
        as_of=row.get("as_of_date"),
        code="AAII_BULL_BEAR",
        today=today,
    )


def _cftc_vix_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("CFTC_VIX_LEV_NET")
    if not row:
        return _empty_market_climate_card("VIX净仓")
    ratio = _clean_number(row.get("value"))
    net_contracts = _clean_number(row.get("secondary_value"))
    detail = "杠杆基金/OI"
    if net_contracts is not None:
        detail = f"净{net_contracts:,.0f}张"
    color = "#dc2626" if ratio is not None and ratio > 0 else "#2563eb"
    return _market_climate_card(
        "VIX净仓",
        _format_pct_card(ratio, 1, signed=True),
        _detail_with_date(detail, row.get("as_of_date"), "CFTC_VIX_LEV_NET", today),
        color,
        as_of=row.get("as_of_date"),
        code="CFTC_VIX_LEV_NET",
        today=today,
    )


def _gscpi_card(climate_rows: dict[str, dict[str, Any]], today: dt.date | None) -> dict[str, Any]:
    row = climate_rows.get("GSCPI")
    if not row:
        return _empty_market_climate_card("供应链压力")
    value = _clean_number(row.get("value"))
    change_3m = _clean_number(row.get("secondary_value"))
    color = "#dc2626" if value is not None and value > 1 else "#059669"
    detail = "3M " + (_format_signed_value(change_3m, 2) if change_3m is not None else "--")
    return _market_climate_card(
        "供应链压力",
        _format_plain_number(value, 2),
        _detail_with_date(detail, row.get("as_of_date"), "GSCPI", today),
        color,
        as_of=row.get("as_of_date"),
        code="GSCPI",
        today=today,
    )


def load_market_climate_strip(engine=None, today: dt.date | None = None) -> list[dict[str, Any]]:
    """Return eight cached market-climate cards for the US options dashboard.

    This function deliberately performs only local database reads. External
    market data is refreshed by update_market_climate_daily.py and cached in
    market_climate_daily so the Streamlit first paint stays fast.
    """
    engine = engine or dashboard_engine()
    climate_rows = _load_latest_market_climate_rows(engine, MARKET_CLIMATE_CACHE_CODES)
    macro_rows = _load_macro_history_rows(engine, MARKET_CLIMATE_MACRO_CODES)
    cards = [
        _vix_term_card(climate_rows, today),
        _rate_curve_card(macro_rows, today),
        _real_yield_card(macro_rows, today),
        _fedwatch_card(climate_rows, macro_rows, today),
        _aaii_card(climate_rows, today),
        _cftc_vix_card(climate_rows, today),
        _gscpi_card(climate_rows, today),
        _credit_spread_card(macro_rows, today),
    ]
    by_label = {card["label"]: card for card in cards}
    return [by_label.get(label, _empty_market_climate_card(label)) for label in MARKET_CLIMATE_CARD_ORDER]


def load_stock_daily(symbol: str, limit: int = 420, engine=None) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(STOCK_DAILY_COLUMNS)

    columns = table_columns(engine, "stock_prices")
    if "symbol" not in columns or "date" not in columns:
        return _empty_df(STOCK_DAILY_COLUMNS)

    limit = min(max(int(limit or 1), 1), 5000)
    selected = [
        _select_expr(columns, "date"),
        _select_expr(columns, "symbol"),
        _select_expr(columns, "open"),
        _select_expr(columns, "high"),
        _select_expr(columns, "low"),
        _select_expr(columns, "close"),
        _select_expr(columns, "volume"),
        _select_expr(columns, "adjClose"),
    ]
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM stock_prices
        WHERE symbol = :symbol
        ORDER BY date DESC
        LIMIT {limit}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"symbol": normalize_underlying(symbol)})
    except Exception:
        return _empty_df(STOCK_DAILY_COLUMNS)

    if df.empty:
        return _empty_df(STOCK_DAILY_COLUMNS)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in ("open", "high", "low", "close", "volume", "adjClose"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[STOCK_DAILY_COLUMNS]


def load_latest_option_trade_date(
    underlying: str,
    *,
    use_test_tables: bool = True,
    engine=None,
) -> str | None:
    engine = engine or dashboard_engine()
    if engine is None:
        return None

    names = option_table_names(use_test_tables)
    underlying = normalize_underlying(underlying)
    candidates: list[str] = []
    for logical_name in ("daily", "iv"):
        table_name = safe_table_name(names[logical_name])
        if not table_exists(engine, table_name):
            continue
        columns = table_columns(engine, table_name)
        if not {"trade_date", "underlying"}.issubset(columns):
            continue
        value = _scalar(
            engine,
            text(
                f"""
                SELECT MAX(trade_date)
                FROM {table_name}{_mysql_force_index(engine, "idx_underlying_date")}
                WHERE underlying = :underlying
                """
            ),
            {"underlying": underlying},
        )
        if value:
            candidates.append(str(value))
    if not candidates:
        return None
    return normalize_trade_date(max(candidates))


def load_available_option_trade_dates(
    underlying: str,
    *,
    use_test_tables: bool = True,
    limit: int = 260,
    engine=None,
) -> list[str]:
    engine = engine or dashboard_engine()
    if engine is None:
        return []

    names = option_table_names(use_test_tables)
    table_name = safe_table_name(names["daily"])
    if not table_exists(engine, table_name):
        return []
    columns = table_columns(engine, table_name)
    if not {"trade_date", "underlying"}.issubset(columns):
        return []

    limit = min(max(int(limit or 1), 1), 5000)
    sql = text(
        f"""
        SELECT trade_date
        FROM {table_name}{_mysql_force_index(engine, "idx_underlying_date")}
        WHERE underlying = :underlying
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {limit}
        """
    )
    try:
        rows = pd.read_sql(sql, engine, params={"underlying": normalize_underlying(underlying)})
    except Exception:
        return []
    if rows.empty or "trade_date" not in rows.columns:
        return []
    dates = [normalize_trade_date(value) for value in rows["trade_date"].tolist()]
    return [value for value in dates if len(value) == 8]


def _underlying_price_by_trade_date(
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, float]:
    prices: dict[str, float] = {}
    stock_df = load_stock_daily(symbol, limit=5000, engine=engine)
    if not stock_df.empty:
        scoped = stock_df.copy()
        scoped["trade_date"] = scoped["date"].apply(normalize_trade_date)
        scoped["close"] = pd.to_numeric(scoped["close"], errors="coerce")
        scoped = scoped[(scoped["trade_date"] >= start_date) & (scoped["trade_date"] <= end_date)]
        prices.update(
            {
                str(row.trade_date): float(row.close)
                for row in scoped.itertuples(index=False)
                if pd.notna(row.close)
            }
        )

    names = option_table_names(use_test_tables)
    iv_table = safe_table_name(names["iv"])
    if engine is None or not table_exists(engine, iv_table):
        return prices
    columns = table_columns(engine, iv_table)
    if not {"trade_date", "underlying", "underlying_price"}.issubset(columns):
        return prices
    sql = text(
        f"""
        SELECT trade_date, AVG(underlying_price) AS underlying_price
        FROM {iv_table}
        WHERE underlying = :underlying
          AND trade_date >= :start_date
          AND trade_date <= :end_date
          AND underlying_price IS NOT NULL
        GROUP BY trade_date
        """
    )
    try:
        iv_prices = pd.read_sql(
            sql,
            engine,
            params={
                "underlying": normalize_underlying(symbol),
                "start_date": start_date,
                "end_date": end_date,
            },
        )
    except Exception:
        return prices
    for row in iv_prices.itertuples(index=False):
        trade_date = normalize_trade_date(row.trade_date)
        if trade_date and trade_date not in prices and pd.notna(row.underlying_price):
            prices[trade_date] = float(row.underlying_price)
    return prices


def _load_cached_oi_defense_history(
    underlying: str,
    end_date: str,
    *,
    window: int,
    engine=None,
) -> pd.DataFrame:
    if engine is None:
        return _empty_df(OI_DEFENSE_COLUMNS)
    columns = table_columns(engine, OI_DEFENSE_CACHE_TABLE)
    required = {"trade_date", "underlying", "call_strike", "put_strike"}
    if not required.issubset(columns):
        return _empty_df(OI_DEFENSE_COLUMNS)

    selected = [_select_expr(columns, col) for col in OI_DEFENSE_COLUMNS]
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM {OI_DEFENSE_CACHE_TABLE}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
        ORDER BY trade_date DESC
        LIMIT {window}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": underlying, "end_date": end_date})
    except Exception:
        return _empty_df(OI_DEFENSE_COLUMNS)
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    else:
        df["date"] = pd.to_datetime(df["trade_date"], format="%Y%m%d", errors="coerce")
    for col in [
        "underlying_close",
        "call_strike",
        "call_oi",
        "call_distance_pct",
        "put_strike",
        "put_oi",
        "put_distance_pct",
        "total_call_oi",
        "total_put_oi",
        "put_call_oi",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[OI_DEFENSE_COLUMNS].sort_values("trade_date").reset_index(drop=True)


def load_oi_defense_history(
    underlying: str,
    end_date: str | dt.date | dt.datetime,
    *,
    window: int = 20,
    use_test_tables: bool = True,
    prefer_cache: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OI_DEFENSE_COLUMNS)

    underlying = normalize_underlying(underlying)
    end_text = normalize_trade_date(end_date)
    if not end_text:
        return _empty_df(OI_DEFENSE_COLUMNS)

    window = min(max(int(window or 20), 1), 260)
    if prefer_cache:
        cached = _load_cached_oi_defense_history(underlying, end_text, window=window, engine=engine)
        if not cached.empty:
            latest_cached_date = (
                cached["trade_date"]
                .dropna()
                .astype(str)
                .loc[lambda series: series.str.len() > 0]
                .max()
            )
            if latest_cached_date >= end_text:
                return cached

    names = option_table_names(use_test_tables)
    daily_table = safe_table_name(names["daily"])
    contracts_table = safe_table_name(names["contracts"])
    if not table_exists(engine, daily_table) or not table_exists(engine, contracts_table):
        return _empty_df(OI_DEFENSE_COLUMNS)

    daily_columns = table_columns(engine, daily_table)
    contract_columns = table_columns(engine, contracts_table)
    required_daily = {"trade_date", "underlying", "option_ticker", "open_interest"}
    required_contracts = {"option_ticker", "call_put", "strike", "expiration_date"}
    if not required_daily.issubset(daily_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(OI_DEFENSE_COLUMNS)

    date_limit = min(max(window * 5, window), 1300)
    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {daily_table}{_mysql_force_index(engine, "idx_underlying_date")}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {date_limit}
        """
    )
    try:
        date_df = pd.read_sql(dates_sql, engine, params={"underlying": underlying, "end_date": end_text})
    except Exception:
        return _empty_df(OI_DEFENSE_COLUMNS)
    if date_df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    selected_dates = (
        date_df["trade_date"]
        .apply(normalize_trade_date)
        .dropna()
        .astype(str)
        .loc[lambda series: series.str.len() > 0]
        .drop_duplicates()
        .head(window)
        .sort_values()
        .tolist()
    )
    if not selected_dates:
        return _empty_df(OI_DEFENSE_COLUMNS)

    start_text = selected_dates[0]
    price_map = _underlying_price_by_trade_date(
        underlying,
        start_text,
        end_text,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    rows_sql = text(
        f"""
        SELECT
            d.trade_date AS trade_date,
            d.option_ticker AS option_ticker,
            d.underlying AS underlying,
            d.open_interest AS open_interest,
            c.call_put AS call_put,
            c.strike AS strike,
            c.expiration_date AS expiration_date
        FROM {daily_table} d{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON d.option_ticker = c.option_ticker
        WHERE d.underlying = :underlying
          AND d.trade_date >= :start_date
          AND d.trade_date <= :end_date
        """
    )
    try:
        df = pd.read_sql(
            rows_sql,
            engine,
            params={"underlying": underlying, "start_date": start_text, "end_date": end_text},
        )
    except Exception:
        return _empty_df(OI_DEFENSE_COLUMNS)
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    selected_set = set(selected_dates)
    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    df = df[df["trade_date"].isin(selected_set)].copy()
    df["call_put"] = df["call_put"].astype(str).str.upper().str.slice(0, 1)
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce")
    df["strike"] = pd.to_numeric(df["strike"], errors="coerce")
    df = df[df["call_put"].isin(["C", "P"])]
    df = df[(df["open_interest"] > 0) & df["strike"].notna()]
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    df["dte"] = df.apply(lambda row: dte_for_trade_date(row["expiration_date"], row["trade_date"]), axis=1)
    df["dte"] = pd.to_numeric(df["dte"], errors="coerce")
    df = df[df["dte"].between(0, 90)]
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    df["underlying_close"] = df["trade_date"].map(price_map)
    has_price = pd.to_numeric(df["underlying_close"], errors="coerce") > 0
    df["distance_pct"] = None
    df.loc[has_price, "distance_pct"] = (
        (df.loc[has_price, "strike"] - df.loc[has_price, "underlying_close"])
        / df.loc[has_price, "underlying_close"]
        * 100
    )
    df = df[df["distance_pct"].isna() | (pd.to_numeric(df["distance_pct"], errors="coerce").abs() <= 25)]
    if df.empty:
        return _empty_df(OI_DEFENSE_COLUMNS)

    output_rows: list[dict[str, Any]] = []
    for trade_date in selected_dates:
        day = df[df["trade_date"] == trade_date].copy()
        if day.empty:
            continue
        row: dict[str, Any] = {
            "trade_date": trade_date,
            "date": pd.to_datetime(trade_date, format="%Y%m%d", errors="coerce"),
            "underlying": underlying,
            "underlying_close": price_map.get(trade_date),
            "call_strike": None,
            "call_oi": None,
            "call_distance_pct": None,
            "call_expiration": None,
            "put_strike": None,
            "put_oi": None,
            "put_distance_pct": None,
            "put_expiration": None,
        }
        total_call_oi = float(day.loc[day["call_put"] == "C", "open_interest"].sum())
        total_put_oi = float(day.loc[day["call_put"] == "P", "open_interest"].sum())
        row["total_call_oi"] = total_call_oi if total_call_oi > 0 else None
        row["total_put_oi"] = total_put_oi if total_put_oi > 0 else None
        row["put_call_oi"] = total_put_oi / total_call_oi if total_call_oi > 0 else None

        for side, prefix in (("C", "call"), ("P", "put")):
            side_df = day[day["call_put"] == side]
            if side_df.empty:
                continue
            by_strike = side_df.groupby("strike", dropna=True)["open_interest"].sum().sort_values(ascending=False)
            if by_strike.empty:
                continue
            top_strike = float(by_strike.index[0])
            top_oi = float(by_strike.iloc[0])
            top_rows = side_df[side_df["strike"] == top_strike]
            by_expiration = (
                top_rows.groupby("expiration_date", dropna=False)["open_interest"].sum().sort_values(ascending=False)
            )
            expiration = str(by_expiration.index[0]) if not by_expiration.empty else None
            close_price = row.get("underlying_close")
            distance_pct = (top_strike - float(close_price)) / float(close_price) * 100 if close_price else None
            row[f"{prefix}_strike"] = top_strike
            row[f"{prefix}_oi"] = top_oi
            row[f"{prefix}_distance_pct"] = distance_pct
            row[f"{prefix}_expiration"] = expiration

        if row.get("call_strike") is not None or row.get("put_strike") is not None:
            output_rows.append(row)

    if not output_rows:
        return _empty_df(OI_DEFENSE_COLUMNS)
    out = pd.DataFrame(output_rows).sort_values("trade_date").tail(window).reset_index(drop=True)
    for col in [
        "underlying_close",
        "call_strike",
        "call_oi",
        "call_distance_pct",
        "put_strike",
        "put_oi",
        "put_distance_pct",
        "total_call_oi",
        "total_put_oi",
        "put_call_oi",
    ]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    return out[OI_DEFENSE_COLUMNS]


def selected_underlying_price(stock_daily: pd.DataFrame, trade_date: str) -> float | None:
    if stock_daily is None or stock_daily.empty or "date" not in stock_daily.columns:
        return None
    target = normalize_trade_date(trade_date)
    df = stock_daily.copy()
    df["trade_date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y%m%d")
    exact = df[df["trade_date"] == target]
    row = exact.iloc[-1] if not exact.empty else df.iloc[-1]
    try:
        close = float(row.get("close"))
        return close if close > 0 else None
    except Exception:
        return None


def _cycle_label(expiration_type: Any, dte: Any) -> str:
    try:
        dte_int = int(dte)
    except Exception:
        dte_int = 999999
    if dte_int <= 0:
        return "0DTE"
    if dte_int == 1:
        return "1DTE"
    exp_type = str(expiration_type or "").strip()
    return exp_type or "unknown"


def load_option_chain_daily(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    df = get_us_option_chain_daily(
        normalize_underlying(underlying),
        normalize_trade_date(trade_date),
        include_short_cycle=include_short_cycle,
        use_test_tables=use_test_tables,
        engine=engine,
    )
    if df is None or df.empty:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    return _finalize_option_chain_frame(df, trade_date, underlying_price=underlying_price)


def _finalize_option_chain_frame(
    df: pd.DataFrame,
    trade_date: str | dt.date | dt.datetime,
    *,
    underlying_price: float | None = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    df = df.copy()
    for col in OPTION_CHAIN_COLUMNS:
        if col not in df.columns:
            df[col] = None

    numeric_cols = [
        "strike",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transactions",
        "open_interest",
        "provider_iv",
        "computed_iv",
        "underlying_price",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    if underlying_price is not None:
        df["underlying_price"] = df["underlying_price"].fillna(float(underlying_price))

    trade_date_text = normalize_trade_date(trade_date)
    df["dte"] = df["expiration_date"].apply(
        lambda value: dte_for_trade_date(value, trade_date_text) if str(value or "").strip() else None
    )
    df["cycle_label"] = df.apply(lambda row: _cycle_label(row.get("expiration_type"), row.get("dte")), axis=1)
    df["iv"] = df.apply(
        lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")),
        axis=1,
    )
    df["iv_pct"] = df["iv"].apply(lambda value: value * 100 if value is not None and pd.notna(value) else None)

    price = pd.to_numeric(df["underlying_price"], errors="coerce")
    strike = pd.to_numeric(df["strike"], errors="coerce")
    df["moneyness_pct"] = ((strike - price) / price * 100).where(price > 0)

    sort_cols = ["expiration_date", "strike", "call_put", "option_ticker"]
    return df[OPTION_CHAIN_COLUMNS].sort_values(sort_cols).reset_index(drop=True)


def load_option_surface_snapshot(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    moneyness_range: float = 10.0,
    max_dte: int = 135,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OPTION_CHAIN_COLUMNS)

    names = option_table_names(use_test_tables)
    contracts = safe_table_name(names["contracts"])
    daily = safe_table_name(names["daily"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, contracts) or not table_exists(engine, daily) or not table_exists(engine, iv):
        return _empty_df(OPTION_CHAIN_COLUMNS)

    trade_date_text = normalize_trade_date(trade_date)
    trade_dt = pd.to_datetime(trade_date_text, format="%Y%m%d", errors="coerce")
    if pd.isna(trade_dt):
        return _empty_df(OPTION_CHAIN_COLUMNS)

    price_param = float(underlying_price) if underlying_price is not None and pd.notna(underlying_price) else None
    price_expr = "COALESCE(h.underlying_price, :underlying_price)"
    where_cycle = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    sql = text(
        f"""
        SELECT d.trade_date, d.option_ticker, d.underlying, c.call_put, c.strike,
               c.expiration_date, c.expiration_type, c.settlement_type,
               d.open, d.high, d.low, d.close, d.volume, d.vwap, d.transactions,
               d.open_interest, h.provider_iv, h.computed_iv, h.iv_source,
               {price_expr} AS underlying_price
        FROM {daily} d{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts} c ON d.option_ticker = c.option_ticker
        LEFT JOIN {iv} h ON d.trade_date = h.trade_date AND d.option_ticker = h.option_ticker
        WHERE d.underlying = :underlying
          AND d.trade_date = :trade_date
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND {price_expr} > 0
          AND ABS(c.strike - {price_expr}) / {price_expr} <= :moneyness_limit
          {where_cycle}
        ORDER BY c.expiration_date ASC, c.strike ASC, c.call_put ASC
        """
    )
    params = {
        "underlying": normalize_underlying(underlying),
        "trade_date": trade_date_text,
        "underlying_price": price_param,
        "expiration_start": trade_dt.strftime("%Y-%m-%d"),
        "expiration_end": (trade_dt + pd.Timedelta(days=max(int(max_dte or 135), 1))).strftime("%Y-%m-%d"),
        "moneyness_limit": max(float(moneyness_range or 10.0), 0.1) / 100.0,
    }
    try:
        df = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(OPTION_CHAIN_COLUMNS)
    return _finalize_option_chain_frame(df, trade_date_text, underlying_price=underlying_price)


def _valid_dte_targets(dte_targets: tuple[int, ...] | list[int] | None = None) -> list[int]:
    values = dte_targets or VOLATILITY_CONE_TARGETS
    out: list[int] = []
    for value in values:
        try:
            target = int(value)
        except Exception:
            continue
        if target > 0 and target not in out:
            out.append(target)
    return sorted(out)


def build_volatility_cone_line(
    chain: pd.DataFrame,
    *,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    moneyness_band: float = 2.5,
) -> pd.DataFrame:
    targets = _valid_dte_targets(dte_targets)
    if chain is None or chain.empty or not targets:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    df = chain.copy()
    for col in ("iv_pct", "open_interest", "moneyness_pct", "dte"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    if "expiration_date" not in df.columns:
        df["expiration_date"] = ""

    band = max(float(moneyness_band or 2.5), 0.1)
    df = df.dropna(subset=["iv_pct", "moneyness_pct", "dte"])
    df = df[(df["dte"] > 0) & (df["moneyness_pct"].abs() <= band)]
    if df.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for (expiration, dte), group in df.groupby(["expiration_date", "dte"], dropna=False):
        iv_pct = _weighted_average(group["iv_pct"], group.get("open_interest"))
        if iv_pct is None:
            continue
        rows.append(
            {
                "expiration_date": str(expiration or ""),
                "dte": float(dte),
                "iv_pct": iv_pct,
                "sample_count": int(len(group)),
            }
        )
    expiry_iv = pd.DataFrame(rows)
    if expiry_iv.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    line_rows: list[dict[str, Any]] = []
    for target in targets:
        scoped = expiry_iv.assign(dte_distance=(expiry_iv["dte"] - target).abs())
        selected = scoped.sort_values(["dte_distance", "dte", "expiration_date"]).iloc[0]
        line_rows.append(
            {
                "dte_target": int(target),
                "dte": float(selected["dte"]),
                "expiration_date": str(selected["expiration_date"]),
                "iv_pct": float(selected["iv_pct"]),
                "sample_count": int(selected["sample_count"]),
            }
        )
    return pd.DataFrame(line_rows, columns=VOLATILITY_CONE_LINE_COLUMNS)


def build_otm_volatility_curve(
    chain: pd.DataFrame,
    *,
    target_dte: int = 30,
    dte_min: int = 20,
    dte_max: int = 45,
    moneyness_range: float = 10.0,
    min_abs_moneyness: float = 0.5,
) -> pd.DataFrame:
    if chain is None or chain.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    df = chain.copy()
    for col in ("iv_pct", "open_interest", "moneyness_pct", "dte"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ("call_put", "expiration_date"):
        if col not in df.columns:
            df[col] = ""

    span = max(float(moneyness_range or 10.0), 0.1)
    df["call_put"] = df["call_put"].astype(str).str.upper()
    df = df.dropna(subset=["iv_pct", "moneyness_pct", "dte"])
    df = df[df["moneyness_pct"].between(-span, span)]
    df = df[df["dte"].between(int(dte_min), int(dte_max))]
    if df.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    expiry = (
        df.assign(dte_distance=(df["dte"] - int(target_dte)).abs())
        .sort_values(["dte_distance", "dte", "expiration_date"])["expiration_date"]
        .iloc[0]
    )
    curve = df[df["expiration_date"].astype(str) == str(expiry)].copy()
    curve = curve[
        ((curve["call_put"] == "P") & (curve["moneyness_pct"] < 0))
        | ((curve["call_put"] == "C") & (curve["moneyness_pct"] > 0))
    ]
    min_abs = max(float(min_abs_moneyness or 0.0), 0.0)
    if min_abs > 0:
        curve = curve[curve["moneyness_pct"].abs() >= min_abs]
    if curve.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for (moneyness, call_put), group in curve.groupby(["moneyness_pct", "call_put"], dropna=False):
        iv_pct = _weighted_average(group["iv_pct"], group.get("open_interest"))
        if iv_pct is None:
            continue
        rows.append(
            {
                "moneyness_pct": float(moneyness),
                "iv_pct": iv_pct,
                "call_put": str(call_put or ""),
                "expiration_date": str(expiry),
                "dte": float(pd.to_numeric(group["dte"], errors="coerce").median()),
            }
        )
    if not rows:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)
    return pd.DataFrame(rows, columns=OTM_VOLATILITY_CURVE_COLUMNS).sort_values("moneyness_pct").reset_index(drop=True)


def load_volatility_cone_line_snapshot(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    moneyness_band: float = 2.5,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    targets = _valid_dte_targets(dte_targets)
    if engine is None or not targets:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    names = option_table_names(use_test_tables)
    contracts_table = safe_table_name(names["contracts"])
    iv_table = safe_table_name(names["iv"])
    if not table_exists(engine, contracts_table) or not table_exists(engine, iv_table):
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    iv_columns = table_columns(engine, iv_table)
    contract_columns = table_columns(engine, contracts_table)
    required_iv = {"trade_date", "option_ticker", "underlying", "provider_iv", "computed_iv", "underlying_price"}
    required_contracts = {"option_ticker", "strike", "expiration_date"}
    if not required_iv.issubset(iv_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    underlying = normalize_underlying(underlying)
    trade_date_text = normalize_trade_date(trade_date)
    trade_dt = pd.to_datetime(trade_date_text, format="%Y%m%d", errors="coerce")
    if not underlying or pd.isna(trade_dt):
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    price_param = float(underlying_price) if underlying_price is not None and pd.notna(underlying_price) else None
    price_expr = "COALESCE(h.underlying_price, :underlying_price)"
    iv_value_expr = (
        "CASE WHEN COALESCE(h.provider_iv, h.computed_iv) > 3 "
        "THEN COALESCE(h.provider_iv, h.computed_iv) / 100.0 "
        "ELSE COALESCE(h.provider_iv, h.computed_iv) END"
    )
    weight_expr = (
        "CASE WHEN h.open_interest IS NOT NULL AND h.open_interest > 0 THEN h.open_interest ELSE 1 END"
        if "open_interest" in iv_columns
        else "1"
    )
    where_cycle = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    sql = text(
        f"""
        SELECT c.expiration_date,
               SUM(({iv_value_expr}) * ({weight_expr})) / NULLIF(SUM({weight_expr}), 0) * 100.0 AS iv_pct,
               COUNT(*) AS sample_count
        FROM {iv_table} h{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND h.trade_date = :trade_date
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND {price_expr} > 0
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND ABS(c.strike - {price_expr}) / {price_expr} <= :moneyness_limit
          {where_cycle}
        GROUP BY c.expiration_date
        ORDER BY c.expiration_date
        """
    )
    params = {
        "underlying": underlying,
        "trade_date": trade_date_text,
        "underlying_price": price_param,
        "expiration_start": trade_dt.strftime("%Y-%m-%d"),
        "expiration_end": (trade_dt + pd.Timedelta(days=max(targets) + 45)).strftime("%Y-%m-%d"),
        "moneyness_limit": max(float(moneyness_band or 2.5), 0.1) / 100.0,
    }
    try:
        raw = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)
    if raw.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    raw["iv_pct"] = pd.to_numeric(raw.get("iv_pct"), errors="coerce")
    raw["sample_count"] = pd.to_numeric(raw.get("sample_count"), errors="coerce").fillna(0)
    raw["dte"] = raw["expiration_date"].apply(lambda value: dte_for_trade_date(value, trade_date_text))
    raw = raw.dropna(subset=["iv_pct", "dte"])
    if raw.empty:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for target in targets:
        scoped = raw.assign(dte_distance=(pd.to_numeric(raw["dte"], errors="coerce") - target).abs())
        scoped = scoped.dropna(subset=["dte_distance", "iv_pct"])
        if scoped.empty:
            continue
        selected = scoped.sort_values(["dte_distance", "dte", "expiration_date"]).iloc[0]
        rows.append(
            {
                "dte_target": int(target),
                "dte": float(selected["dte"]),
                "expiration_date": str(selected["expiration_date"] or ""),
                "iv_pct": float(selected["iv_pct"]),
                "sample_count": int(selected["sample_count"]),
            }
        )
    if not rows:
        return _empty_df(VOLATILITY_CONE_LINE_COLUMNS)
    return pd.DataFrame(rows, columns=VOLATILITY_CONE_LINE_COLUMNS)


def load_otm_volatility_curve_snapshot(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    use_test_tables: bool = True,
    underlying_price: float | None = None,
    target_dte: int = 30,
    dte_min: int = 20,
    dte_max: int = 45,
    moneyness_range: float = 10.0,
    min_abs_moneyness: float = 0.5,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    names = option_table_names(use_test_tables)
    contracts_table = safe_table_name(names["contracts"])
    iv_table = safe_table_name(names["iv"])
    if not table_exists(engine, contracts_table) or not table_exists(engine, iv_table):
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    iv_columns = table_columns(engine, iv_table)
    contract_columns = table_columns(engine, contracts_table)
    required_iv = {"trade_date", "option_ticker", "underlying", "provider_iv", "computed_iv", "underlying_price"}
    required_contracts = {"option_ticker", "call_put", "strike", "expiration_date"}
    if not required_iv.issubset(iv_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    underlying = normalize_underlying(underlying)
    trade_date_text = normalize_trade_date(trade_date)
    trade_dt = pd.to_datetime(trade_date_text, format="%Y%m%d", errors="coerce")
    if not underlying or pd.isna(trade_dt):
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    price_param = float(underlying_price) if underlying_price is not None and pd.notna(underlying_price) else None
    price_expr = "COALESCE(h.underlying_price, :underlying_price)"
    iv_value_expr = (
        "CASE WHEN COALESCE(h.provider_iv, h.computed_iv) > 3 "
        "THEN COALESCE(h.provider_iv, h.computed_iv) / 100.0 "
        "ELSE COALESCE(h.provider_iv, h.computed_iv) END"
    )
    weight_expr = (
        "CASE WHEN h.open_interest IS NOT NULL AND h.open_interest > 0 THEN h.open_interest ELSE 1 END"
        if "open_interest" in iv_columns
        else "1"
    )
    where_cycle = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    dte_min_value = max(int(dte_min), 1)
    dte_max_value = max(int(dte_max), dte_min_value)
    span = max(float(moneyness_range or 10.0), 0.1)
    sql = text(
        f"""
        SELECT c.call_put,
               c.strike,
               c.expiration_date,
               {price_expr} AS underlying_price,
               SUM(({iv_value_expr}) * ({weight_expr})) / NULLIF(SUM({weight_expr}), 0) * 100.0 AS iv_pct,
               COUNT(*) AS sample_count
        FROM {iv_table} h{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND h.trade_date = :trade_date
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND {price_expr} > 0
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND ABS(c.strike - {price_expr}) / {price_expr} <= :moneyness_limit
          {where_cycle}
        GROUP BY c.expiration_date, c.call_put, c.strike, {price_expr}
        ORDER BY c.expiration_date, c.strike, c.call_put
        """
    )
    params = {
        "underlying": underlying,
        "trade_date": trade_date_text,
        "underlying_price": price_param,
        "expiration_start": (trade_dt + pd.Timedelta(days=dte_min_value)).strftime("%Y-%m-%d"),
        "expiration_end": (trade_dt + pd.Timedelta(days=dte_max_value)).strftime("%Y-%m-%d"),
        "moneyness_limit": span / 100.0,
    }
    try:
        raw = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)
    if raw.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    raw["call_put"] = raw.get("call_put", "").astype(str).str.upper()
    for col in ("strike", "underlying_price", "iv_pct"):
        raw[col] = pd.to_numeric(raw.get(col), errors="coerce")
    raw["dte"] = raw["expiration_date"].apply(lambda value: dte_for_trade_date(value, trade_date_text))
    raw["moneyness_pct"] = ((raw["strike"] - raw["underlying_price"]) / raw["underlying_price"] * 100).where(
        raw["underlying_price"] > 0
    )
    raw = raw.dropna(subset=["iv_pct", "moneyness_pct", "dte"])
    raw = raw[raw["dte"].between(dte_min_value, dte_max_value)]
    if raw.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    expiry = (
        raw.assign(dte_distance=(raw["dte"] - int(target_dte)).abs())
        .sort_values(["dte_distance", "dte", "expiration_date"])["expiration_date"]
        .iloc[0]
    )
    curve = raw[raw["expiration_date"].astype(str) == str(expiry)].copy()
    curve = curve[
        ((curve["call_put"] == "P") & (curve["moneyness_pct"] < 0))
        | ((curve["call_put"] == "C") & (curve["moneyness_pct"] > 0))
    ]
    min_abs = max(float(min_abs_moneyness or 0.0), 0.0)
    if min_abs > 0:
        curve = curve[curve["moneyness_pct"].abs() >= min_abs]
    if curve.empty:
        return _empty_df(OTM_VOLATILITY_CURVE_COLUMNS)

    out = curve[["moneyness_pct", "iv_pct", "call_put", "expiration_date", "dte"]].copy()
    return out.sort_values("moneyness_pct").reset_index(drop=True)


def _normalize_cone_source_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw is None or raw.empty:
        return _empty_df(OPTION_CHAIN_COLUMNS)
    df = raw.copy()
    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    for col in ("provider_iv", "computed_iv", "open_interest", "underlying_price", "strike"):
        if col not in df.columns:
            df[col] = None
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["iv"] = df.apply(
        lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")),
        axis=1,
    )
    df["iv_pct"] = df["iv"].apply(lambda value: value * 100 if value is not None and pd.notna(value) else None)
    df["dte"] = df.apply(lambda row: dte_for_trade_date(row.get("expiration_date"), row.get("trade_date")), axis=1)
    price = pd.to_numeric(df["underlying_price"], errors="coerce")
    strike = pd.to_numeric(df["strike"], errors="coerce")
    df["moneyness_pct"] = ((strike - price) / price * 100).where(price > 0)
    return df


def _percentile_cone_from_daily_rows(history: pd.DataFrame) -> pd.DataFrame:
    if history is None or history.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    source = history.copy()
    source["dte_target"] = pd.to_numeric(source.get("dte_target"), errors="coerce")
    source["iv_pct"] = pd.to_numeric(source.get("iv_pct"), errors="coerce")
    source = source.dropna(subset=["dte_target", "iv_pct"])
    if source.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    rows: list[dict[str, Any]] = []
    for target in sorted(source["dte_target"].dropna().astype(int).unique().tolist()):
        values = pd.to_numeric(source.loc[source["dte_target"] == target, "iv_pct"], errors="coerce").dropna()
        if values.empty:
            continue
        rows.append(
            {
                "dte_target": int(target),
                "p10": float(values.quantile(0.10)),
                "p25": float(values.quantile(0.25)),
                "p50": float(values.quantile(0.50)),
                "p75": float(values.quantile(0.75)),
                "p90": float(values.quantile(0.90)),
                "sample_count": int(len(values)),
            }
        )
    if not rows:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    return pd.DataFrame(rows, columns=VOLATILITY_CONE_COLUMNS).sort_values("dte_target").reset_index(drop=True)


def _load_cached_volatility_cone_history(
    underlying: str,
    end_date: str,
    *,
    window: int,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    engine=None,
) -> pd.DataFrame:
    if engine is None or not table_exists(engine, VOLATILITY_CONE_DAILY_CACHE_TABLE):
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    columns = table_columns(engine, VOLATILITY_CONE_DAILY_CACHE_TABLE)
    required = {"trade_date", "underlying", "dte_target", "iv_pct"}
    if not required.issubset(columns):
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    targets = _valid_dte_targets(dte_targets)
    date_limit = min(max(int(window or 252), 2), 500)
    target_clause = ""
    params: dict[str, Any] = {"underlying": underlying, "end_date": end_date}
    if targets:
        placeholders, target_params = _named_in_clause("target", targets)
        params.update(target_params)
        target_clause = f"AND dte_target IN ({placeholders})"

    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {VOLATILITY_CONE_DAILY_CACHE_TABLE}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
          {target_clause}
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {date_limit}
        """
    )
    try:
        dates_df = pd.read_sql(dates_sql, engine, params=params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    if dates_df.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_values = [normalize_trade_date(value) for value in dates_df["trade_date"].tolist()]
    date_values = [value for value in date_values if value]
    if not date_values or max(date_values) < end_date:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_placeholders, date_params = _named_in_clause("cache_date", date_values)
    read_params = {"underlying": underlying, **date_params}
    if targets:
        target_placeholders, target_params = _named_in_clause("cache_target", targets)
        read_params.update(target_params)
        read_target_clause = f"AND dte_target IN ({target_placeholders})"
    else:
        read_target_clause = ""
    sql = text(
        f"""
        SELECT trade_date, dte_target, iv_pct
        FROM {VOLATILITY_CONE_DAILY_CACHE_TABLE}
        WHERE underlying = :underlying
          AND trade_date IN ({date_placeholders})
          {read_target_clause}
        ORDER BY trade_date, dte_target
        """
    )
    try:
        history = pd.read_sql(sql, engine, params=read_params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    return _percentile_cone_from_daily_rows(history)


def load_volatility_cone_history(
    underlying: str,
    end_date: str | dt.date | dt.datetime,
    *,
    window: int = 252,
    dte_targets: tuple[int, ...] | list[int] | None = None,
    moneyness_band: float = 2.5,
    use_test_tables: bool = True,
    prefer_cache: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    targets = _valid_dte_targets(dte_targets)
    if engine is None or not targets:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    underlying = normalize_underlying(underlying)
    end_text = normalize_trade_date(end_date)
    if not underlying or not end_text:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_limit = min(max(int(window or 252), 2), 500)
    if prefer_cache:
        cached = _load_cached_volatility_cone_history(
            underlying,
            end_text,
            window=date_limit,
            dte_targets=targets,
            engine=engine,
        )
        if not cached.empty:
            return cached

    names = option_table_names(use_test_tables)
    iv_table = safe_table_name(names["iv"])
    contracts_table = safe_table_name(names["contracts"])
    if not table_exists(engine, iv_table) or not table_exists(engine, contracts_table):
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    iv_columns = table_columns(engine, iv_table)
    contract_columns = table_columns(engine, contracts_table)
    required_iv = {"trade_date", "option_ticker", "underlying", "provider_iv", "computed_iv", "underlying_price"}
    required_contracts = {"option_ticker", "strike", "expiration_date"}
    if not required_iv.issubset(iv_columns) or not required_contracts.issubset(contract_columns):
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {iv_table}{_mysql_force_index(engine, "idx_underlying_date")}
        WHERE underlying = :underlying
          AND trade_date <= :end_date
        GROUP BY trade_date
        ORDER BY trade_date DESC
        LIMIT {date_limit}
        """
    )
    try:
        dates_df = pd.read_sql(dates_sql, engine, params={"underlying": underlying, "end_date": end_text})
    except Exception:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    if dates_df.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    date_values = [normalize_trade_date(value) for value in dates_df["trade_date"].tolist()]
    date_values = [value for value in date_values if value]
    if not date_values:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    start_date = min(date_values)
    end_dt = pd.to_datetime(end_text, format="%Y%m%d", errors="coerce")
    start_dt = pd.to_datetime(start_date, format="%Y%m%d", errors="coerce")
    if pd.isna(end_dt) or pd.isna(start_dt):
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    expiration_start = start_dt.strftime("%Y-%m-%d")
    expiration_end = (end_dt + pd.Timedelta(days=max(targets) + 45)).strftime("%Y-%m-%d")
    placeholders, params = _named_in_clause("date", date_values)
    params.update(
        {
            "underlying": underlying,
            "expiration_start": expiration_start,
            "expiration_end": expiration_end,
            "moneyness_limit": max(float(moneyness_band or 2.5), 0.1) / 100.0,
        }
    )
    iv_value_expr = (
        "CASE WHEN COALESCE(h.provider_iv, h.computed_iv) > 3 "
        "THEN COALESCE(h.provider_iv, h.computed_iv) / 100.0 "
        "ELSE COALESCE(h.provider_iv, h.computed_iv) END"
    )
    weight_expr = (
        "CASE WHEN h.open_interest IS NOT NULL AND h.open_interest > 0 THEN h.open_interest ELSE 1 END"
        if "open_interest" in iv_columns
        else "1"
    )
    sql = text(
        f"""
        SELECT h.trade_date,
               c.expiration_date,
               SUM(({iv_value_expr}) * ({weight_expr})) / NULLIF(SUM({weight_expr}), 0) * 100.0 AS iv_pct,
               COUNT(*) AS sample_count
        FROM {iv_table} h{_mysql_force_index(engine, "idx_underlying_date")}
        JOIN {contracts_table} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND h.trade_date IN ({placeholders})
          AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
          AND h.underlying_price > 0
          AND c.expiration_date >= :expiration_start
          AND c.expiration_date <= :expiration_end
          AND ABS(c.strike - h.underlying_price) / h.underlying_price <= :moneyness_limit
        GROUP BY h.trade_date, c.expiration_date
        ORDER BY h.trade_date, c.expiration_date
        """
    )
    try:
        raw = pd.read_sql(sql, engine, params=params)
    except Exception:
        return _empty_df(VOLATILITY_CONE_COLUMNS)
    if raw.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    source = raw.copy()
    source["trade_date"] = source["trade_date"].apply(normalize_trade_date)
    source["iv_pct"] = pd.to_numeric(source.get("iv_pct"), errors="coerce")
    source["sample_count"] = pd.to_numeric(source.get("sample_count"), errors="coerce").fillna(0)
    source["dte"] = source.apply(lambda row: dte_for_trade_date(row.get("expiration_date"), row.get("trade_date")), axis=1)
    source = source.dropna(subset=["trade_date", "iv_pct", "dte"])
    if source.empty:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    line_rows: list[pd.DataFrame] = []
    for trade_date, day in source.groupby("trade_date", dropna=False):
        day = day.copy()
        for target in targets:
            scoped = day.assign(dte_distance=(pd.to_numeric(day["dte"], errors="coerce") - target).abs())
            scoped = scoped.dropna(subset=["dte_distance", "iv_pct"])
            if scoped.empty:
                continue
            selected = scoped.sort_values(["dte_distance", "dte", "expiration_date"]).iloc[0]
            line_rows.append(
                pd.DataFrame(
                    [
                        {
                            "trade_date": str(trade_date),
                            "dte_target": int(target),
                            "iv_pct": float(selected["iv_pct"]),
                        }
                    ]
                )
            )
    if not line_rows:
        return _empty_df(VOLATILITY_CONE_COLUMNS)

    return _percentile_cone_from_daily_rows(pd.concat(line_rows, ignore_index=True))


def summarize_option_chain(chain: pd.DataFrame) -> dict[str, Any]:
    if chain is None or chain.empty:
        return {
            "rows": 0,
            "monthly": 0,
            "short_cycle": 0,
            "zero_dte": 0,
            "one_dte": 0,
            "expirations": 0,
            "provider_iv_rows": 0,
            "computed_iv_rows": 0,
            "open_interest_rows": 0,
        }
    expiration_type = chain.get("expiration_type", pd.Series(dtype=object)).astype(str)
    dte = pd.to_numeric(chain.get("dte", pd.Series(dtype=float)), errors="coerce")
    return {
        "rows": int(len(chain)),
        "monthly": int((expiration_type == "monthly").sum()),
        "short_cycle": int((expiration_type != "monthly").sum()),
        "zero_dte": int((dte <= 0).sum()),
        "one_dte": int((dte == 1).sum()),
        "expirations": int(chain.get("expiration_date", pd.Series(dtype=object)).nunique()),
        "provider_iv_rows": int(chain.get("provider_iv", pd.Series(dtype=float)).notna().sum()),
        "computed_iv_rows": int(chain.get("computed_iv", pd.Series(dtype=float)).notna().sum()),
        "open_interest_rows": int(chain.get("open_interest", pd.Series(dtype=float)).notna().sum()),
    }


def option_chain_empty_summary() -> dict[str, int]:
    return {
        "rows": 0,
        "monthly": 0,
        "short_cycle": 0,
        "zero_dte": 0,
        "one_dte": 0,
        "expirations": 0,
        "provider_iv_rows": 0,
        "computed_iv_rows": 0,
        "open_interest_rows": 0,
    }


def load_option_chain_summary(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    include_short_cycle: bool = True,
    include_iv_counts: bool = True,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, int]:
    engine = engine or dashboard_engine()
    if engine is None:
        return option_chain_empty_summary()

    names = option_table_names(use_test_tables)
    daily = safe_table_name(names["daily"])
    contracts = safe_table_name(names["contracts"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, daily) or not table_exists(engine, contracts):
        return option_chain_empty_summary()

    daily_columns = table_columns(engine, daily)
    contract_columns = table_columns(engine, contracts)
    if not {"trade_date", "underlying", "option_ticker", "open_interest"}.issubset(daily_columns):
        return option_chain_empty_summary()
    if not {"option_ticker", "expiration_date", "expiration_type"}.issubset(contract_columns):
        return option_chain_empty_summary()

    trade_date_text = normalize_trade_date(trade_date)
    underlying = normalize_underlying(underlying)
    short_cycle_clause = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    iv_join = ""
    provider_expr = "0"
    computed_expr = "0"
    if include_iv_counts and table_exists(engine, iv):
        iv_columns = table_columns(engine, iv)
        if {"trade_date", "option_ticker", "provider_iv", "computed_iv"}.issubset(iv_columns):
            iv_join = (
                f"LEFT JOIN {iv} h "
                "ON h.trade_date = d.trade_date AND h.option_ticker = d.option_ticker"
            )
            provider_expr = "CASE WHEN h.provider_iv IS NOT NULL THEN 1 ELSE 0 END"
            computed_expr = "CASE WHEN h.computed_iv IS NOT NULL THEN 1 ELSE 0 END"

    if getattr(getattr(engine, "dialect", None), "name", "") in {"mysql", "mariadb"}:
        dte_expr = "DATEDIFF(STR_TO_DATE(c.expiration_date, '%Y-%m-%d'), STR_TO_DATE(d.trade_date, '%Y%m%d'))"
        sql = text(
            f"""
            SELECT
                COUNT(*) AS rows_count,
                SUM(CASE WHEN c.expiration_type = 'monthly' THEN 1 ELSE 0 END) AS monthly_count,
                SUM(CASE WHEN c.expiration_type = 'monthly' THEN 0 ELSE 1 END) AS short_cycle_count,
                SUM(CASE WHEN {dte_expr} <= 0 THEN 1 ELSE 0 END) AS zero_dte_count,
                SUM(CASE WHEN {dte_expr} = 1 THEN 1 ELSE 0 END) AS one_dte_count,
                COUNT(DISTINCT c.expiration_date) AS expiration_count,
                SUM({provider_expr}) AS provider_iv_count,
                SUM({computed_expr}) AS computed_iv_count,
                SUM(CASE WHEN d.open_interest IS NOT NULL THEN 1 ELSE 0 END) AS open_interest_count
            FROM {daily} d
            JOIN {contracts} c ON d.option_ticker = c.option_ticker
            {iv_join}
            WHERE d.underlying = :underlying
              AND d.trade_date = :trade_date
              {short_cycle_clause}
            """
        )
        try:
            row = pd.read_sql(sql, engine, params={"underlying": underlying, "trade_date": trade_date_text})
            if not row.empty:
                data = row.iloc[0].to_dict()
                return {
                    "rows": int(data.get("rows_count") or 0),
                    "monthly": int(data.get("monthly_count") or 0),
                    "short_cycle": int(data.get("short_cycle_count") or 0),
                    "zero_dte": int(data.get("zero_dte_count") or 0),
                    "one_dte": int(data.get("one_dte_count") or 0),
                    "expirations": int(data.get("expiration_count") or 0),
                    "provider_iv_rows": int(data.get("provider_iv_count") or 0),
                    "computed_iv_rows": int(data.get("computed_iv_count") or 0),
                    "open_interest_rows": int(data.get("open_interest_count") or 0),
                }
        except Exception:
            pass

    selected_cols = [
        "d.trade_date AS trade_date",
        "d.open_interest AS open_interest",
        "c.expiration_date AS expiration_date",
        "c.expiration_type AS expiration_type",
    ]
    if iv_join:
        selected_cols.extend(["h.provider_iv AS provider_iv", "h.computed_iv AS computed_iv"])
    else:
        selected_cols.extend(["NULL AS provider_iv", "NULL AS computed_iv"])
    sql = text(
        f"""
        SELECT {", ".join(selected_cols)}
        FROM {daily} d
        JOIN {contracts} c ON d.option_ticker = c.option_ticker
        {iv_join}
        WHERE d.underlying = :underlying
          AND d.trade_date = :trade_date
          {short_cycle_clause}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": underlying, "trade_date": trade_date_text})
    except Exception:
        return option_chain_empty_summary()
    if df.empty:
        return option_chain_empty_summary()

    expiration_type = df.get("expiration_type", pd.Series(dtype=object)).astype(str)
    dte = df["expiration_date"].apply(lambda value: dte_for_trade_date(value, trade_date_text))
    dte = pd.to_numeric(dte, errors="coerce")
    return {
        "rows": int(len(df)),
        "monthly": int((expiration_type == "monthly").sum()),
        "short_cycle": int((expiration_type != "monthly").sum()),
        "zero_dte": int((dte <= 0).sum()),
        "one_dte": int((dte == 1).sum()),
        "expirations": int(df.get("expiration_date", pd.Series(dtype=object)).nunique()),
        "provider_iv_rows": int(df.get("provider_iv", pd.Series(dtype=float)).notna().sum()),
        "computed_iv_rows": int(df.get("computed_iv", pd.Series(dtype=float)).notna().sum()),
        "open_interest_rows": int(df.get("open_interest", pd.Series(dtype=float)).notna().sum()),
    }


def load_underlying_iv_rank(
    underlying: str,
    *,
    window: int = 252,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, Any] | None:
    engine = engine or dashboard_engine()
    if engine is None:
        return None
    return get_us_underlying_iv_rank(
        normalize_underlying(underlying),
        window=window,
        use_test_tables=use_test_tables,
        engine=engine,
    )


def load_iv_history(
    underlying: str,
    *,
    window: int = 252,
    use_test_tables: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    names = option_table_names(use_test_tables)
    contracts = safe_table_name(names["contracts"])
    iv = safe_table_name(names["iv"])
    if not table_exists(engine, contracts) or not table_exists(engine, iv):
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    # Keep the day window aligned with get_us_underlying_iv_rank. Do not limit
    # raw option rows before filtering, because one trading day can contain
    # thousands of monthly contracts and an early row cap hides older dates.
    day_limit = max(min(int(window or 252), 1500), 1)
    row_limit = max(day_limit * 5000, 1000)
    if getattr(getattr(engine, "dialect", None), "name", "") in {"mysql", "mariadb"}:
        sql = text(
            f"""
            WITH filtered AS (
                SELECT h.trade_date, h.provider_iv, h.computed_iv, h.open_interest,
                       CASE
                           WHEN COALESCE(h.provider_iv, h.computed_iv) > 3
                               THEN COALESCE(h.provider_iv, h.computed_iv) / 100
                           ELSE COALESCE(h.provider_iv, h.computed_iv)
                       END AS iv_value
                FROM {iv} h
                JOIN {contracts} c ON h.option_ticker = c.option_ticker
                WHERE h.underlying = :underlying
                  AND c.expiration_type = 'monthly'
                  AND COALESCE(h.provider_iv, h.computed_iv) IS NOT NULL
                  AND h.underlying_price > 0
                  AND DATEDIFF(STR_TO_DATE(c.expiration_date, '%Y-%m-%d'), STR_TO_DATE(h.trade_date, '%Y%m%d')) BETWEEN 20 AND 90
                  AND ABS(c.strike - h.underlying_price) / h.underlying_price <= 0.10
                  AND (h.open_interest IS NULL OR h.open_interest > 0)
            )
            SELECT trade_date,
                   CASE
                       WHEN SUM(COALESCE(open_interest, 0)) > 0
                           THEN SUM(iv_value * COALESCE(open_interest, 0)) / SUM(COALESCE(open_interest, 0))
                       ELSE AVG(iv_value)
                   END AS iv,
                   COUNT(*) AS source_rows,
                   SUM(CASE WHEN provider_iv IS NOT NULL THEN 1 ELSE 0 END) AS provider_rows,
                   SUM(CASE WHEN computed_iv IS NOT NULL THEN 1 ELSE 0 END) AS computed_rows
            FROM filtered
            GROUP BY trade_date
            ORDER BY trade_date DESC
            LIMIT :day_limit
            """
        )
        try:
            out = pd.read_sql(
                sql,
                engine,
                params={"underlying": normalize_underlying(underlying), "day_limit": day_limit},
            )
            if not out.empty:
                for col in ("iv", "source_rows", "provider_rows", "computed_rows"):
                    out[col] = pd.to_numeric(out[col], errors="coerce")
                out = out.dropna(subset=["iv"]).sort_values("trade_date").tail(window).reset_index(drop=True)
                out["iv_pct"] = out["iv"] * 100
                return out[["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"]]
        except Exception:
            pass

    sql = text(
        f"""
        SELECT h.trade_date, h.provider_iv, h.computed_iv, h.iv_source,
               h.open_interest, h.underlying_price, c.strike, c.expiration_date,
               c.call_put, c.expiration_type
        FROM {iv} h
        JOIN {contracts} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND c.expiration_type = 'monthly'
        ORDER BY h.trade_date DESC
        LIMIT {row_limit}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": normalize_underlying(underlying)})
    except Exception:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])
    if df.empty:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    df["iv"] = df.apply(
        lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")),
        axis=1,
    )
    df = df.dropna(subset=["iv"])
    if df.empty:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    for col in ("open_interest", "underlying_price", "strike"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["dte"] = df.apply(lambda row: dte_for_trade_date(row["expiration_date"], row["trade_date"]), axis=1)
    df = df[(df["dte"] >= 20) & (df["dte"] <= 90)]
    df = df[df["underlying_price"] > 0]
    df = df[(df["strike"] - df["underlying_price"]).abs() / df["underlying_price"] <= 0.10]
    if "open_interest" in df.columns:
        df = df[df["open_interest"].isna() | (df["open_interest"] > 0)]
    if df.empty:
        return _empty_df(["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"])

    def aggregate(day: pd.DataFrame) -> pd.Series:
        weights = day["open_interest"].fillna(0).astype(float)
        if weights.sum() > 0:
            iv_value = float((day["iv"].astype(float) * weights).sum() / weights.sum())
        else:
            iv_value = float(day["iv"].astype(float).mean())
        return pd.Series(
            {
                "iv": iv_value,
                "source_rows": int(len(day)),
                "provider_rows": int(day["provider_iv"].notna().sum()),
                "computed_rows": int(day["computed_iv"].notna().sum()),
            }
        )

    out = pd.DataFrame(
        [
            {"trade_date": trade_date, **aggregate(day).to_dict()}
            for trade_date, day in df.groupby("trade_date")
        ]
    )
    out = out.sort_values("trade_date").tail(window).reset_index(drop=True)
    out["iv_pct"] = out["iv"] * 100
    return out[["trade_date", "iv", "iv_pct", "source_rows", "provider_rows", "computed_rows"]]


def load_market_metrics_history(
    underlying: str,
    *,
    window: int = 252,
    use_test_tables: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(MARKET_METRICS_COLUMNS)

    names = option_table_names(use_test_tables)
    table_name = safe_table_name(names.get("metrics", ""))
    if not table_name or not table_exists(engine, table_name):
        return _empty_df(MARKET_METRICS_COLUMNS)
    columns = table_columns(engine, table_name)
    if not {"trade_date", "underlying"}.issubset(columns):
        return _empty_df(MARKET_METRICS_COLUMNS)

    limit = min(max(int(window or 252) * 4, 300), 5000)
    selected = [_select_expr(columns, col) for col in MARKET_METRICS_COLUMNS]
    sql = text(
        f"""
        SELECT {", ".join(selected)}
        FROM {table_name}
        WHERE underlying = :underlying
        ORDER BY trade_date DESC
        LIMIT {limit}
        """
    )
    try:
        df = pd.read_sql(sql, engine, params={"underlying": normalize_underlying(underlying)})
    except Exception:
        return _empty_df(MARKET_METRICS_COLUMNS)
    if df.empty:
        return _empty_df(MARKET_METRICS_COLUMNS)

    df["trade_date"] = df["trade_date"].apply(normalize_trade_date)
    for col in MARKET_METRIC_NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df[MARKET_METRICS_COLUMNS].sort_values("trade_date").reset_index(drop=True)


def _weighted_average(values: pd.Series, weights: pd.Series | None = None) -> float | None:
    clean_values = pd.to_numeric(values, errors="coerce")
    if weights is not None:
        clean_weights = pd.to_numeric(weights, errors="coerce").fillna(0)
        valid = clean_values.notna() & (clean_weights > 0)
        if valid.any() and float(clean_weights[valid].sum()) > 0:
            return float((clean_values[valid] * clean_weights[valid]).sum() / clean_weights[valid].sum())
    values_only = clean_values.dropna()
    if values_only.empty:
        return None
    return float(values_only.mean())


def _filter_stock_to_trade_date(stock_df: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if stock_df is None or stock_df.empty or not trade_date or "date" not in stock_df.columns:
        return stock_df if stock_df is not None else pd.DataFrame()
    cutoff = pd.to_datetime(normalize_trade_date(trade_date), format="%Y%m%d", errors="coerce")
    if pd.isna(cutoff):
        return stock_df
    out = stock_df.copy()
    out["_date_for_filter"] = pd.to_datetime(out["date"], errors="coerce")
    out = out[out["_date_for_filter"] <= cutoff].drop(columns=["_date_for_filter"])
    return out


def calculate_realized_volatility(
    stock_df: pd.DataFrame,
    *,
    window: int = 20,
    trade_date: str | dt.date | dt.datetime | None = None,
) -> float | None:
    if stock_df is None or stock_df.empty or "close" not in stock_df.columns:
        return None
    scoped = _filter_stock_to_trade_date(stock_df, normalize_trade_date(trade_date) if trade_date else None)
    close = pd.to_numeric(scoped.get("close"), errors="coerce").dropna()
    if len(close) < 3:
        return None
    window = max(int(window or 1), 1)
    returns = close.pct_change().dropna().tail(window)
    if len(returns) < 2:
        return None
    return float(returns.std() * math.sqrt(252) * 100)


def _iv_history_until(iv_history: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if iv_history is None or iv_history.empty:
        return pd.DataFrame(columns=["trade_date", "iv_pct"])
    out = iv_history.copy()
    out["trade_date"] = out["trade_date"].apply(lambda value: normalize_trade_date(value) if value is not None else "")
    out["iv_pct"] = pd.to_numeric(out.get("iv_pct"), errors="coerce")
    out = out.dropna(subset=["iv_pct"]).sort_values("trade_date")
    if trade_date:
        out = out[out["trade_date"] <= normalize_trade_date(trade_date)]
    return out.reset_index(drop=True)


def _percentile_rank(values: pd.Series, current_value: float | None) -> float | None:
    if current_value is None:
        return None
    series = pd.to_numeric(values, errors="coerce").dropna()
    if series.empty:
        return None
    return float((series <= float(current_value)).sum() / len(series) * 100)


def _iv_rank_from_history(
    iv_history: pd.DataFrame,
    *,
    current_iv_pct: float | None,
    trade_date: str | None,
    fallback: dict[str, Any] | None = None,
) -> dict[str, float | int | None]:
    history = _iv_history_until(iv_history, trade_date)
    history["iv_change_1d"] = pd.to_numeric(history.get("iv_pct"), errors="coerce").diff()
    series = pd.to_numeric(history.get("iv_pct"), errors="coerce").dropna()
    current_value = None
    if not history.empty and trade_date:
        exact = history[history["trade_date"] == normalize_trade_date(trade_date)]
        if not exact.empty:
            current_value = float(exact["iv_pct"].iloc[-1])
    if current_value is None and not series.empty:
        current_value = float(series.iloc[-1])
    if current_value is None:
        current = pd.to_numeric(pd.Series([current_iv_pct]), errors="coerce").dropna()
        current_value = float(current.iloc[0]) if not current.empty else None
    if series.empty or current_value is None:
        return {
            "iv_rank": (fallback or {}).get("iv_rank"),
            "iv_percentile": (fallback or {}).get("iv_percentile"),
            "current_monthly_iv_pct": current_value,
            "iv_change_1d": None,
            "iv_change_1d_percentile": None,
            "iv_change_5d": None,
            "iv_change_20d": None,
            "iv_history_days": int((fallback or {}).get("days") or 0),
        }
    min_iv = float(series.min())
    max_iv = float(series.max())
    iv_rank = None if math.isclose(max_iv, min_iv) else (current_value - min_iv) / (max_iv - min_iv) * 100
    iv_percentile = float((series <= current_value).sum() / len(series) * 100)
    fallback_date = str((fallback or {}).get("date") or "")
    if fallback_date and normalize_trade_date(fallback_date) == normalize_trade_date(trade_date):
        iv_rank = (fallback or {}).get("iv_rank", iv_rank)
        iv_percentile = (fallback or {}).get("iv_percentile", iv_percentile)
    current_change_1d = None
    if not history.empty:
        current_change_1d = pd.to_numeric(pd.Series([history["iv_change_1d"].iloc[-1]]), errors="coerce").dropna()
        current_change_1d = float(current_change_1d.iloc[0]) if not current_change_1d.empty else None
    return {
        "iv_rank": iv_rank,
        "iv_percentile": iv_percentile,
        "current_monthly_iv_pct": current_value,
        "iv_change_1d": current_change_1d,
        "iv_change_1d_percentile": _percentile_rank(history["iv_change_1d"], current_change_1d),
        "iv_change_5d": current_value - float(series.iloc[-6]) if len(series) >= 6 else None,
        "iv_change_20d": current_value - float(series.iloc[-21]) if len(series) >= 21 else None,
        "iv_history_days": int(len(series)),
    }


def _prepared_option_chain(chain_df: pd.DataFrame) -> pd.DataFrame:
    if chain_df is None or chain_df.empty:
        return pd.DataFrame()
    df = chain_df.copy()
    for col in ("strike", "open_interest", "volume", "iv_pct", "moneyness_pct", "dte", "underlying_price"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        else:
            df[col] = None
    df["call_put"] = df.get("call_put", pd.Series(dtype=object)).astype(str).str.upper()
    df["expiration_type"] = df.get("expiration_type", pd.Series(dtype=object)).astype(str)
    df["expiration_date"] = df.get("expiration_date", pd.Series(dtype=object)).astype(str)
    df["open_interest"] = df["open_interest"].fillna(0)
    df["volume"] = df["volume"].fillna(0)
    return df


def _monthly_atm_frame(chain_df: pd.DataFrame, *, moneyness_band: float = 2.0) -> pd.DataFrame:
    df = _prepared_option_chain(chain_df)
    if df.empty:
        return df
    monthly = df[
        (df["expiration_type"] == "monthly")
        & df["iv_pct"].notna()
        & df["dte"].notna()
        & (df["dte"] > 0)
        & df["moneyness_pct"].notna()
    ].copy()
    if monthly.empty:
        return monthly
    return monthly[monthly["moneyness_pct"].abs() <= float(moneyness_band)].copy()


def calculate_atm_iv_pct(
    chain_df: pd.DataFrame,
    *,
    underlying_price: float | None = None,
    dte_min: int = 20,
    dte_max: int = 90,
    moneyness_band: float = 10.0,
) -> float | None:
    monthly_atm = _monthly_atm_frame(chain_df, moneyness_band=moneyness_band)
    if monthly_atm.empty:
        return None
    monthly_atm = monthly_atm[
        monthly_atm["dte"].between(float(dte_min), float(dte_max))
        & monthly_atm["iv_pct"].notna()
    ].copy()
    if monthly_atm.empty:
        return None
    if underlying_price is not None and "strike" in monthly_atm.columns:
        monthly_atm["_distance"] = (pd.to_numeric(monthly_atm["strike"], errors="coerce") - float(underlying_price)).abs()
        nearest = monthly_atm.sort_values(["_distance", "dte"]).head(24)
        if not nearest.empty:
            monthly_atm = nearest
    return _weighted_average(monthly_atm["iv_pct"], monthly_atm["open_interest"])


def _term_iv_metrics(chain_df: pd.DataFrame) -> dict[str, Any]:
    monthly_atm = _monthly_atm_frame(chain_df, moneyness_band=2.0)
    if monthly_atm.empty:
        return {
            "iv_30d": None,
            "iv_60d": None,
            "term_slope_30_60": None,
            "term_slope_percentile": None,
            "term_state": "样本不足",
        }

    rows = []
    for expiration, group in monthly_atm.groupby("expiration_date", dropna=False):
        rows.append(
            {
                "expiration_date": str(expiration),
                "dte": float(group["dte"].median()),
                "iv_pct": _weighted_average(group["iv_pct"], group["open_interest"]),
            }
        )
    term = pd.DataFrame(rows).dropna(subset=["iv_pct", "dte"])
    if term.empty:
        return {
            "iv_30d": None,
            "iv_60d": None,
            "term_slope_30_60": None,
            "term_slope_percentile": None,
            "term_state": "样本不足",
        }

    def nearest(target: float) -> float | None:
        near = term.assign(distance=(term["dte"] - target).abs()).sort_values("distance").head(1)
        return float(near["iv_pct"].iloc[0]) if not near.empty else None

    iv_30d = nearest(30)
    iv_60d = nearest(60)
    slope = iv_60d - iv_30d if iv_30d is not None and iv_60d is not None else None
    if slope is None:
        state = "样本不足"
    elif slope <= -1.0:
        state = "Backwardation"
    elif slope >= 1.0:
        state = "Contango"
    else:
        state = "Flat"

    slope_samples = []
    ordered = term.sort_values("dte").reset_index(drop=True)
    for left_idx in range(len(ordered)):
        for right_idx in range(left_idx + 1, len(ordered)):
            left = ordered.iloc[left_idx]
            right = ordered.iloc[right_idx]
            if float(right["dte"]) <= float(left["dte"]):
                continue
            slope_samples.append(float(right["iv_pct"]) - float(left["iv_pct"]))

    return {
        "iv_30d": iv_30d,
        "iv_60d": iv_60d,
        "term_slope_30_60": slope,
        "term_slope_percentile": _percentile_rank(pd.Series(slope_samples, dtype=float), slope),
        "term_state": state,
    }


def _fixed_moneyness_iv(group: pd.DataFrame, *, call_put: str | None, center: float, band: float = 1.0) -> float | None:
    side = group
    if call_put:
        side = side[side["call_put"] == call_put]
    side = side[side["moneyness_pct"].between(center - band, center + band)]
    if side.empty:
        return None
    return _weighted_average(side["iv_pct"], side["open_interest"])


def _skew_metrics(chain_df: pd.DataFrame) -> dict[str, Any]:
    df = _prepared_option_chain(chain_df)
    if df.empty:
        return {
            "skew_expiration": None,
            "put_skew_5pct": None,
            "call_skew_5pct": None,
            "put_call_skew_5pct": None,
            "put_skew_5pct_percentile": None,
            "call_skew_5pct_percentile": None,
            "put_call_skew_5pct_percentile": None,
        }
    monthly = df[
        (df["expiration_type"] == "monthly")
        & df["iv_pct"].notna()
        & df["moneyness_pct"].notna()
        & df["dte"].notna()
        & (df["dte"] > 0)
    ].copy()
    if monthly.empty:
        return {
            "skew_expiration": None,
            "put_skew_5pct": None,
            "call_skew_5pct": None,
            "put_call_skew_5pct": None,
            "put_skew_5pct_percentile": None,
            "call_skew_5pct_percentile": None,
            "put_call_skew_5pct_percentile": None,
        }

    skew_rows = []
    for expiration_date, group in monthly.groupby("expiration_date", dropna=False):
        atm_iv = _fixed_moneyness_iv(group, call_put=None, center=0, band=1.0)
        put_iv = _fixed_moneyness_iv(group, call_put="P", center=-5, band=1.0)
        call_iv = _fixed_moneyness_iv(group, call_put="C", center=5, band=1.0)
        skew_rows.append(
            {
                "expiration_date": str(expiration_date),
                "dte": float(pd.to_numeric(group["dte"], errors="coerce").median()),
                "put_skew_5pct": put_iv - atm_iv if put_iv is not None and atm_iv is not None else None,
                "call_skew_5pct": call_iv - atm_iv if call_iv is not None and atm_iv is not None else None,
            }
        )
    skew_table = pd.DataFrame(skew_rows).dropna(subset=["dte"])
    if not skew_table.empty and {"put_skew_5pct", "call_skew_5pct"}.issubset(skew_table.columns):
        skew_table["put_call_skew_5pct"] = pd.to_numeric(
            skew_table["put_skew_5pct"], errors="coerce"
        ) - pd.to_numeric(skew_table["call_skew_5pct"], errors="coerce")

    candidates = monthly[monthly["dte"].between(20, 45)]
    if candidates.empty:
        candidates = monthly
    expiration = (
        candidates.assign(dte_distance=(candidates["dte"] - 30).abs())
        .sort_values(["dte_distance", "expiration_date"])
        ["expiration_date"]
        .iloc[0]
    )
    slice_df = monthly[monthly["expiration_date"] == expiration]
    atm_iv = _fixed_moneyness_iv(slice_df, call_put=None, center=0, band=1.0)
    put_iv = _fixed_moneyness_iv(slice_df, call_put="P", center=-5, band=1.0)
    call_iv = _fixed_moneyness_iv(slice_df, call_put="C", center=5, band=1.0)
    put_skew = put_iv - atm_iv if put_iv is not None and atm_iv is not None else None
    call_skew = call_iv - atm_iv if call_iv is not None and atm_iv is not None else None
    put_call_skew = put_skew - call_skew if put_skew is not None and call_skew is not None else None
    return {
        "skew_expiration": str(expiration),
        "put_skew_5pct": put_skew,
        "call_skew_5pct": call_skew,
        "put_call_skew_5pct": put_call_skew,
        "put_skew_5pct_percentile": _percentile_rank(skew_table["put_skew_5pct"], put_skew)
        if "put_skew_5pct" in skew_table
        else None,
        "call_skew_5pct_percentile": _percentile_rank(skew_table["call_skew_5pct"], call_skew)
        if "call_skew_5pct" in skew_table
        else None,
        "put_call_skew_5pct_percentile": _percentile_rank(skew_table["put_call_skew_5pct"], put_call_skew)
        if "put_call_skew_5pct" in skew_table
        else None,
    }


def _positioning_metrics(chain_df: pd.DataFrame) -> dict[str, Any]:
    df = _prepared_option_chain(chain_df)
    if df.empty:
        return {
            "put_call_oi": None,
            "put_call_volume": None,
            "zero_dte_volume_share_pct": None,
            "top_oi_strike": None,
            "top_oi": None,
            "top5_oi_share_pct": None,
            "total_open_interest": None,
            "total_volume": None,
            "put_call_oi_percentile": None,
            "put_call_volume_percentile": None,
        }
    call_oi = float(df.loc[df["call_put"] == "C", "open_interest"].sum())
    put_oi = float(df.loc[df["call_put"] == "P", "open_interest"].sum())
    call_volume = float(df.loc[df["call_put"] == "C", "volume"].sum())
    put_volume = float(df.loc[df["call_put"] == "P", "volume"].sum())
    all_oi = float(df["open_interest"].sum())
    total_volume = call_volume + put_volume
    zero_dte_volume = float(df.loc[df["dte"] <= 0, "volume"].sum())

    oi_candidates = df[df["open_interest"] > 0].copy()
    near_term = oi_candidates[oi_candidates["dte"].between(0, 90)]
    if not near_term.empty:
        oi_candidates = near_term
    near_price = oi_candidates[oi_candidates["moneyness_pct"].abs() <= 25] if "moneyness_pct" in oi_candidates else oi_candidates
    if not near_price.empty:
        oi_candidates = near_price

    if oi_candidates.empty:
        top_oi_strike = None
        top_oi = None
        top5_share = None
    else:
        by_strike = oi_candidates.groupby("strike", dropna=True)["open_interest"].sum().sort_values(ascending=False)
        top_oi_strike = float(by_strike.index[0]) if not by_strike.empty else None
        top_oi = float(by_strike.iloc[0]) if not by_strike.empty else None
        total_oi = float(by_strike.sum())
        top5_share = float(by_strike.head(5).sum() / total_oi * 100) if total_oi > 0 else None

    expiry_rows = []
    for _, group in df.groupby("expiration_date", dropna=False):
        expiry_call_oi = float(group.loc[group["call_put"] == "C", "open_interest"].sum())
        expiry_put_oi = float(group.loc[group["call_put"] == "P", "open_interest"].sum())
        expiry_call_volume = float(group.loc[group["call_put"] == "C", "volume"].sum())
        expiry_put_volume = float(group.loc[group["call_put"] == "P", "volume"].sum())
        expiry_rows.append(
            {
                "put_call_oi": expiry_put_oi / expiry_call_oi if expiry_call_oi > 0 else None,
                "put_call_volume": expiry_put_volume / expiry_call_volume if expiry_call_volume > 0 else None,
            }
        )
    expiry_metrics = pd.DataFrame(expiry_rows)
    put_call_oi = put_oi / call_oi if call_oi > 0 else None
    put_call_volume = put_volume / call_volume if call_volume > 0 else None

    return {
        "put_call_oi": put_call_oi,
        "put_call_volume": put_call_volume,
        "zero_dte_volume_share_pct": zero_dte_volume / total_volume * 100 if total_volume > 0 else None,
        "top_oi_strike": top_oi_strike,
        "top_oi": top_oi,
        "top5_oi_share_pct": top5_share,
        "total_open_interest": all_oi if all_oi > 0 else None,
        "total_volume": total_volume if total_volume > 0 else None,
        "put_call_oi_percentile": _percentile_rank(expiry_metrics.get("put_call_oi", pd.Series(dtype=float)), put_call_oi),
        "put_call_volume_percentile": _percentile_rank(
            expiry_metrics.get("put_call_volume", pd.Series(dtype=float)), put_call_volume
        ),
    }


def _iv_rv_spread_metrics(
    stock_df: pd.DataFrame,
    iv_history: pd.DataFrame,
    *,
    trade_date: str,
    current_iv_pct: float | None,
    rv20_pct: float | None,
) -> dict[str, float | None]:
    current_spread = current_iv_pct - rv20_pct if current_iv_pct is not None and rv20_pct is not None else None
    history = _iv_history_until(iv_history, trade_date)
    if history.empty:
        return {"iv_rv20_spread": current_spread, "iv_rv20_percentile": None}

    rows = []
    for _, row in history.iterrows():
        day = str(row.get("trade_date") or "")
        day_iv = pd.to_numeric(pd.Series([row.get("iv_pct")]), errors="coerce").dropna()
        day_rv = calculate_realized_volatility(stock_df, window=20, trade_date=day)
        if day_iv.empty or day_rv is None:
            continue
        rows.append(float(day_iv.iloc[0]) - float(day_rv))
    series = pd.Series(rows, dtype=float)
    return {
        "iv_rv20_spread": current_spread,
        "iv_rv20_percentile": _percentile_rank(series, current_spread),
    }


def _metric_history_until(metrics_history: pd.DataFrame, trade_date: str | None) -> pd.DataFrame:
    if metrics_history is None or metrics_history.empty:
        return pd.DataFrame(columns=MARKET_METRICS_COLUMNS)
    out = metrics_history.copy()
    out["trade_date"] = out["trade_date"].apply(lambda value: normalize_trade_date(value) if value is not None else "")
    out = out.sort_values("trade_date")
    if trade_date:
        out = out[out["trade_date"] <= normalize_trade_date(trade_date)]
    for col in MARKET_METRIC_NUMERIC_COLUMNS:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out.reset_index(drop=True)


def _clean_metric_value(value: Any) -> float | None:
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _derive_put_call_skew(put_skew: Any, call_skew: Any) -> float | None:
    put_value = _clean_metric_value(put_skew)
    call_value = _clean_metric_value(call_skew)
    if put_value is None or call_value is None:
        return None
    return put_value - call_value


def apply_historical_percentiles(
    metrics: dict[str, Any],
    metrics_history: pd.DataFrame,
    *,
    trade_date: str | dt.date | dt.datetime,
    window: int = 252,
    min_samples: int = 60,
) -> dict[str, Any]:
    out = dict(metrics or {})
    history = _metric_history_until(metrics_history, normalize_trade_date(trade_date))
    out["historical_percentile_window"] = int(window)
    out["historical_percentile_min_samples"] = int(min_samples)

    for percentile_key in HISTORICAL_PERCENTILE_FIELDS.values():
        out[percentile_key] = None

    if history.empty:
        out["put_call_skew_5pct"] = _derive_put_call_skew(out.get("put_skew_5pct"), out.get("call_skew_5pct"))
        for field in HISTORICAL_PERCENTILE_FIELDS:
            out[f"{field}_history_count"] = 0
            out[f"{field}_insufficient_history"] = True
        return out

    exact = history[history["trade_date"] == normalize_trade_date(trade_date)]
    if not exact.empty:
        exact_row = exact.iloc[-1]
        for col in MARKET_METRICS_COLUMNS:
            if col in {"trade_date", "underlying", "source", "updated_at"}:
                continue
            value = exact_row.get(col)
            if pd.notna(value):
                out[col] = value

    out["put_call_skew_5pct"] = _derive_put_call_skew(out.get("put_skew_5pct"), out.get("call_skew_5pct"))
    if {"put_skew_5pct", "call_skew_5pct"}.issubset(history.columns):
        history["put_call_skew_5pct"] = pd.to_numeric(history["put_skew_5pct"], errors="coerce") - pd.to_numeric(
            history["call_skew_5pct"], errors="coerce"
        )

    history_window = max(int(window or 252), 1)
    min_count = max(int(min_samples or 1), 1)
    for field, percentile_key in HISTORICAL_PERCENTILE_FIELDS.items():
        series = pd.to_numeric(history.get(field, pd.Series(dtype=float)), errors="coerce").dropna().tail(history_window)
        current_value = _clean_metric_value(out.get(field))
        sample_count = int(len(series))
        out[f"{field}_history_count"] = sample_count
        out[f"{field}_insufficient_history"] = sample_count < min_count
        if current_value is None or sample_count < min_count:
            out[percentile_key] = None
        else:
            out[percentile_key] = _percentile_rank(series, current_value)
    return out


def calculate_overview_metrics_from_market_history(
    *,
    stock_df: pd.DataFrame,
    market_metrics_history: pd.DataFrame,
    trade_date: str | dt.date | dt.datetime,
) -> dict[str, Any]:
    trade_date_text = normalize_trade_date(trade_date)
    history = _metric_history_until(market_metrics_history, trade_date_text)
    if history.empty:
        return calculate_volatility_positioning_metrics(
            stock_df=stock_df,
            chain_df=pd.DataFrame(),
            iv_history=pd.DataFrame(columns=["trade_date", "iv_pct"]),
            trade_date=trade_date_text,
            current_iv_pct=None,
            iv_rank=None,
            market_metrics_history=market_metrics_history,
        )

    exact = history[history["trade_date"] == trade_date_text]
    current_row = exact.iloc[-1] if not exact.empty else history.iloc[-1]
    rv20 = calculate_realized_volatility(stock_df, window=20, trade_date=trade_date_text)
    rv60 = calculate_realized_volatility(stock_df, window=60, trade_date=trade_date_text)

    metrics: dict[str, Any] = {
        "rv20_pct": rv20,
        "rv60_pct": rv60,
    }
    for col in MARKET_METRICS_COLUMNS:
        if col in {"trade_date", "underlying", "source", "updated_at"}:
            continue
        value = current_row.get(col)
        if pd.notna(value):
            metrics[col] = value

    current_iv = _clean_metric_value(metrics.get("atm_iv_pct"))
    series = pd.to_numeric(history.get("atm_iv_pct", pd.Series(dtype=float)), errors="coerce").dropna().tail(252)
    if current_iv is not None and not series.empty:
        min_iv = float(series.min())
        max_iv = float(series.max())
        metrics["iv_rank"] = None if math.isclose(max_iv, min_iv) else (current_iv - min_iv) / (max_iv - min_iv) * 100
        metrics["iv_percentile"] = float((series <= current_iv).sum() / len(series) * 100)
        metrics["current_monthly_iv_pct"] = current_iv
        metrics["iv_history_days"] = int(len(series))

        if _clean_metric_value(metrics.get("iv_change_1d")) is None:
            metrics["iv_change_1d"] = current_iv - float(series.iloc[-2]) if len(series) >= 2 else None
        metrics["iv_change_5d"] = current_iv - float(series.iloc[-6]) if len(series) >= 6 else None
        metrics["iv_change_20d"] = current_iv - float(series.iloc[-21]) if len(series) >= 21 else None
    else:
        metrics.setdefault("iv_rank", None)
        metrics.setdefault("iv_percentile", None)
        metrics.setdefault("current_monthly_iv_pct", current_iv)
        metrics.setdefault("iv_history_days", 0)
        metrics.setdefault("iv_change_5d", None)
        metrics.setdefault("iv_change_20d", None)

    if _clean_metric_value(metrics.get("iv_rv20_spread")) is None and current_iv is not None and rv20 is not None:
        metrics["iv_rv20_spread"] = current_iv - rv20

    return apply_historical_percentiles(
        metrics,
        history,
        trade_date=trade_date_text,
    )


def calculate_volatility_positioning_metrics(
    *,
    stock_df: pd.DataFrame,
    chain_df: pd.DataFrame,
    iv_history: pd.DataFrame,
    trade_date: str | dt.date | dt.datetime,
    current_iv_pct: float | None,
    iv_rank: dict[str, Any] | None = None,
    market_metrics_history: pd.DataFrame | None = None,
) -> dict[str, Any]:
    trade_date_text = normalize_trade_date(trade_date)
    rank_metrics = _iv_rank_from_history(
        iv_history,
        current_iv_pct=current_iv_pct,
        trade_date=trade_date_text,
        fallback=iv_rank,
    )
    rv20 = calculate_realized_volatility(stock_df, window=20, trade_date=trade_date_text)
    rv60 = calculate_realized_volatility(stock_df, window=60, trade_date=trade_date_text)
    spread_metrics = _iv_rv_spread_metrics(
        stock_df,
        iv_history,
        trade_date=trade_date_text,
        current_iv_pct=current_iv_pct,
        rv20_pct=rv20,
    )
    metrics: dict[str, Any] = {
        "atm_iv_pct": current_iv_pct,
        "rv20_pct": rv20,
        "rv60_pct": rv60,
        **rank_metrics,
        **spread_metrics,
        **_term_iv_metrics(chain_df),
        **_skew_metrics(chain_df),
        **_positioning_metrics(chain_df),
    }
    if market_metrics_history is not None:
        metrics = apply_historical_percentiles(
            metrics,
            market_metrics_history,
            trade_date=trade_date_text,
        )
    return metrics


def _duplicate_count(engine, table_name: str, key_columns: list[str]) -> int | None:
    table_name = safe_table_name(table_name)
    columns = table_columns(engine, table_name)
    if not all(col in columns for col in key_columns):
        return None
    group_cols = ", ".join(safe_table_name(col) for col in key_columns)
    sql = text(
        f"""
        SELECT COALESCE(SUM(extra_count), 0)
        FROM (
            SELECT COUNT(*) - 1 AS extra_count
            FROM {table_name}
            GROUP BY {group_cols}
            HAVING COUNT(*) > 1
        ) dupes
        """
    )
    value = _scalar(engine, sql)
    return int(value or 0)


def collect_option_table_diagnostics(
    underlying: str,
    trade_date: str | dt.date | dt.datetime,
    *,
    use_test_tables: bool = True,
    engine=None,
) -> dict[str, Any]:
    engine = engine or dashboard_engine()
    names = option_table_names(use_test_tables)
    underlying = normalize_underlying(underlying)
    trade_date = normalize_trade_date(trade_date)
    table_rows: list[dict[str, Any]] = []
    missing_rows: list[dict[str, Any]] = []

    if engine is None:
        return {"tables": table_rows, "missing": missing_rows}

    duplicate_keys = {
        "contracts": ["option_ticker"],
        "daily": ["trade_date", "option_ticker"],
        "iv": ["trade_date", "option_ticker"],
        "metrics": ["trade_date", "underlying"],
    }
    latest_col = {"daily": "trade_date", "iv": "trade_date", "metrics": "trade_date"}

    for logical_name, table_name in names.items():
        table_name = safe_table_name(table_name)
        exists = table_exists(engine, table_name)
        columns = table_columns(engine, table_name) if exists else set()
        row_count = _scalar(engine, text(f"SELECT COUNT(*) FROM {table_name}")) if exists else None
        underlying_count = None
        if exists and "underlying" in columns:
            underlying_count = _scalar(
                engine,
                text(f"SELECT COUNT(*) FROM {table_name} WHERE underlying = :underlying"),
                {"underlying": underlying},
            )
        latest_trade_date = None
        if exists and latest_col.get(logical_name) in columns:
            latest_trade_date = _scalar(
                engine,
                text(
                    f"""
                    SELECT MAX(trade_date)
                    FROM {table_name}
                    WHERE underlying = :underlying
                    """
                ),
                {"underlying": underlying},
            )
        duplicate_count = _duplicate_count(engine, table_name, duplicate_keys[logical_name]) if exists else None
        table_rows.append(
            {
                "table": table_name,
                "exists": exists,
                "rows": int(row_count or 0) if exists else 0,
                "underlying_rows": int(underlying_count or 0) if underlying_count is not None else None,
                "latest_trade_date": latest_trade_date,
                "duplicate_keys": duplicate_count,
            }
        )

        if not exists:
            continue
        checks: list[tuple[str, str]] = []
        if logical_name == "contracts":
            checks = [
                ("expiration_type", "expiration_type IS NULL OR expiration_type = '' OR expiration_type = 'unknown'"),
                ("settlement_type", "settlement_type IS NULL OR settlement_type = '' OR settlement_type = 'unknown'"),
            ]
        elif logical_name == "daily":
            checks = [
                ("close", "close IS NULL"),
                ("volume", "volume IS NULL"),
                ("open_interest", "open_interest IS NULL"),
            ]
        elif logical_name == "iv":
            checks = [
                ("iv", "provider_iv IS NULL AND computed_iv IS NULL"),
                ("underlying_price", "underlying_price IS NULL"),
            ]
        elif logical_name == "metrics":
            checks = [
                ("atm_iv_pct", "atm_iv_pct IS NULL"),
                ("iv_rv20_spread", "iv_rv20_spread IS NULL"),
                ("term_slope_30_60", "term_slope_30_60 IS NULL"),
                ("put_call_oi", "put_call_oi IS NULL"),
            ]
        for field, condition in checks:
            if field != "iv" and field not in columns:
                continue
            sql = text(
                f"""
                SELECT COUNT(*)
                FROM {table_name}
                WHERE underlying = :underlying
                  AND ({condition})
                """
            )
            missing_rows.append(
                {
                    "table": table_name,
                    "field": field,
                    "missing_or_unknown": int(_scalar(engine, sql, {"underlying": underlying}) or 0),
                }
            )

    return {"tables": table_rows, "missing": missing_rows}


def validate_short_cycle_band(chain: pd.DataFrame, band_pct: float = 5.0) -> dict[str, Any]:
    if chain is None or chain.empty:
        return {"status": "no_rows", "short_cycle_rows": 0, "checked_rows": 0, "out_of_band_rows": 0}

    dte = pd.to_numeric(chain.get("dte", pd.Series(dtype=float)), errors="coerce")
    exp_type = chain.get("expiration_type", pd.Series(dtype=object)).astype(str)
    short_cycle = chain[(exp_type != "monthly") | (dte <= 1)].copy()
    if short_cycle.empty:
        return {"status": "ok", "short_cycle_rows": 0, "checked_rows": 0, "out_of_band_rows": 0}

    price = pd.to_numeric(short_cycle.get("underlying_price"), errors="coerce")
    strike = pd.to_numeric(short_cycle.get("strike"), errors="coerce")
    valid = short_cycle[(price > 0) & strike.notna()].copy()
    if valid.empty:
        return {
            "status": "unknown",
            "short_cycle_rows": int(len(short_cycle)),
            "checked_rows": 0,
            "out_of_band_rows": None,
        }

    moneyness = (strike.loc[valid.index] - price.loc[valid.index]).abs() / price.loc[valid.index] * 100
    out_of_band = valid[moneyness > float(band_pct)]
    return {
        "status": "ok" if out_of_band.empty else "fail",
        "short_cycle_rows": int(len(short_cycle)),
        "checked_rows": int(len(valid)),
        "out_of_band_rows": int(len(out_of_band)),
    }

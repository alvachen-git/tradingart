from __future__ import annotations

import datetime as dt
import math
import re
from typing import Any

import pandas as pd
from sqlalchemy import inspect, text

from us_options_polygon import (
    compact_date,
    dte_for_trade_date,
    get_db_engine,
    get_us_option_chain_daily,
    get_us_underlying_iv_rank,
    normalize_iv_value,
    table_names,
)


DEFAULT_DASHBOARD_UNDERLYINGS = ("SPY", "QQQ", "IWM", "SPX", "NDX", "RUT", "VIX")

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
HISTORICAL_PERCENTILE_FIELDS = {
    "iv_change_1d": "iv_change_1d_percentile",
    "iv_rv20_spread": "iv_rv20_percentile",
    "term_slope_30_60": "term_slope_percentile",
    "put_skew_5pct": "put_skew_5pct_percentile",
    "call_skew_5pct": "call_skew_5pct_percentile",
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


def option_table_names(use_test_tables: bool = False) -> dict[str, str]:
    return table_names(use_test_tables)


def _empty_df(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def table_exists(engine, table_name: str) -> bool:
    if engine is None:
        return False
    try:
        return bool(inspect(engine).has_table(safe_table_name(table_name)))
    except Exception:
        return False


def table_columns(engine, table_name: str) -> set[str]:
    if engine is None or not table_exists(engine, table_name):
        return set()
    try:
        return {str(col["name"]) for col in inspect(engine).get_columns(safe_table_name(table_name))}
    except Exception:
        return set()


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


def load_stock_daily(symbol: str, limit: int = 420, engine=None) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None or not table_exists(engine, "stock_prices"):
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
        WHERE UPPER(symbol) = :symbol
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
                FROM {table_name}
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
        FROM {table_name}
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


def load_oi_defense_history(
    underlying: str,
    end_date: str | dt.date | dt.datetime,
    *,
    window: int = 20,
    use_test_tables: bool = True,
    engine=None,
) -> pd.DataFrame:
    engine = engine or dashboard_engine()
    if engine is None:
        return _empty_df(OI_DEFENSE_COLUMNS)

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

    underlying = normalize_underlying(underlying)
    end_text = normalize_trade_date(end_date)
    if not end_text:
        return _empty_df(OI_DEFENSE_COLUMNS)

    window = min(max(int(window or 20), 1), 260)
    date_limit = min(max(window * 5, window), 1300)
    dates_sql = text(
        f"""
        SELECT trade_date
        FROM {daily_table}
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
        FROM {daily_table} d
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

    # Keep the row window aligned with get_us_underlying_iv_rank so the dashboard
    # can derive rank and history from one query without changing the visible IV Rank口径.
    limit = max(min(int(window or 252), 1500) * 300, 1000)
    if getattr(getattr(engine, "dialect", None), "name", "") in {"mysql", "mariadb"}:
        sql = text(
            f"""
            WITH candidate AS (
                SELECT h.trade_date, h.provider_iv, h.computed_iv, h.open_interest,
                       h.underlying_price, c.strike, c.expiration_date
                FROM {iv} h
                JOIN {contracts} c ON h.option_ticker = c.option_ticker
                WHERE h.underlying = :underlying
                  AND c.expiration_type = 'monthly'
                ORDER BY h.trade_date DESC
                LIMIT :row_limit
            ),
            filtered AS (
                SELECT trade_date, provider_iv, computed_iv, open_interest,
                       CASE
                           WHEN COALESCE(provider_iv, computed_iv) > 3
                               THEN COALESCE(provider_iv, computed_iv) / 100
                           ELSE COALESCE(provider_iv, computed_iv)
                       END AS iv_value
                FROM candidate
                WHERE COALESCE(provider_iv, computed_iv) IS NOT NULL
                  AND underlying_price > 0
                  AND DATEDIFF(STR_TO_DATE(expiration_date, '%Y-%m-%d'), STR_TO_DATE(trade_date, '%Y%m%d')) BETWEEN 20 AND 90
                  AND ABS(strike - underlying_price) / underlying_price <= 0.10
                  AND (open_interest IS NULL OR open_interest > 0)
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
            ORDER BY trade_date ASC
            """
        )
        try:
            out = pd.read_sql(
                sql,
                engine,
                params={"underlying": normalize_underlying(underlying), "row_limit": limit},
            )
            if not out.empty:
                for col in ("iv", "source_rows", "provider_rows", "computed_rows"):
                    out[col] = pd.to_numeric(out[col], errors="coerce")
                out = out.dropna(subset=["iv"]).tail(window).reset_index(drop=True)
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
        LIMIT {limit}
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
            "put_skew_5pct_percentile": None,
            "call_skew_5pct_percentile": None,
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
            "put_skew_5pct_percentile": None,
            "call_skew_5pct_percentile": None,
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
    return {
        "skew_expiration": str(expiration),
        "put_skew_5pct": put_skew,
        "call_skew_5pct": call_skew,
        "put_skew_5pct_percentile": _percentile_rank(skew_table["put_skew_5pct"], put_skew)
        if "put_skew_5pct" in skew_table
        else None,
        "call_skew_5pct_percentile": _percentile_rank(skew_table["call_skew_5pct"], call_skew)
        if "call_skew_5pct" in skew_table
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

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import time
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Sequence

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import URL, create_engine, text

from cn_market_climate_data import (
    CARD_ORDER,
    CHINEXT_CSI1000_RS,
    IM_BASIS,
    MARKET_AMOUNT,
    SZ50_CSI1000_RS,
    STAR50_CHINEXT_RS,
    ClimateMetricRow,
    MarginDailyRecord,
    OfficialMarginClient,
    build_cn10y_metric_row,
    build_im_basis_metric_row,
    build_margin_metric_rows,
    build_market_amount_metric_row,
    build_relative_strength_metric_rows,
    ensure_cn_market_climate_tables,
    mark_stale_metrics,
    store_climate_rows,
    store_margin_records,
)
from term_structure_service import build_index_basis_longterm_payload


INDEX_CODES = ("000016.SH", "000852.SH", "000688.SH", "399006.SZ")
CORE_CURRENT_DATE_METRICS = (
    MARKET_AMOUNT,
    SZ50_CSI1000_RS,
    STAR50_CHINEXT_RS,
    CHINEXT_CSI1000_RS,
    IM_BASIS,
)
JIN10_SSE_MARGIN_HISTORY_URL = "https://cdn.jin10.com/data_center/reports/fs_1.json"
JIN10_SZSE_MARGIN_HISTORY_URL = "https://cdn.jin10.com/data_center/reports/fs_2.json"


def compact_date(value: Any) -> str:
    raw = str(value or "").strip().replace("-", "").replace("/", "")
    if len(raw) != 8 or not raw.isdigit():
        raise ValueError(f"非法日期 {value!r}，期望 YYYYMMDD")
    pd.to_datetime(raw, format="%Y%m%d", errors="raise")
    return raw


def create_engine_from_env() -> Any:
    load_dotenv(Path(__file__).resolve().parent / ".env", override=True)
    required = {name: os.getenv(name) for name in ("DB_USER", "DB_PASSWORD", "DB_HOST", "DB_NAME")}
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise RuntimeError(f"数据库配置缺失: {', '.join(missing)}")
    url = URL.create(
        "mysql+pymysql",
        username=required["DB_USER"],
        password=required["DB_PASSWORD"],
        host=required["DB_HOST"],
        port=int(os.getenv("DB_PORT", "3306")),
        database=required["DB_NAME"],
    )
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


def _read_sql_or_empty(engine: Any, sql: str, params: dict[str, Any]) -> pd.DataFrame:
    try:
        return pd.read_sql(text(sql), engine, params=params)
    except Exception:
        return pd.DataFrame()


def load_trading_dates(engine: Any, start_date: str, end_date: str) -> list[str]:
    frame = _read_sql_or_empty(
        engine,
        """
        SELECT DISTINCT REPLACE(REPLACE(trade_date, '-', ''), '/', '') AS trade_date
        FROM index_price
        WHERE ts_code = '000852.SH'
          AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') BETWEEN :start_date AND :end_date
        ORDER BY trade_date
        """,
        {"start_date": start_date, "end_date": end_date},
    )
    if frame.empty:
        return []
    return [str(day) for day in frame["trade_date"].dropna().tolist()]


def create_tushare_client() -> Any:
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        raise RuntimeError("ETF市场环境更新需要 TUSHARE_TOKEN")
    import tushare as ts

    ts.set_token(token)
    return ts.pro_api()


def resolve_expected_trade_date(pro: Any, end_date: str) -> str:
    """Resolve the latest open SSE session at or before the requested date."""
    end = compact_date(end_date)
    start = (pd.to_datetime(end, format="%Y%m%d") - pd.Timedelta(days=14)).strftime("%Y%m%d")
    calendar = pro.trade_cal(
        exchange="SSE",
        start_date=start,
        end_date=end,
        is_open=1,
        fields="cal_date,is_open",
    )
    if calendar is None or calendar.empty or "cal_date" not in calendar.columns:
        raise RuntimeError(f"交易日历未返回 {start}~{end} 的开放日")
    dates = (
        calendar["cal_date"]
        .astype(str)
        .str.replace("-", "", regex=False)
        .loc[lambda values: values.str.fullmatch(r"\d{8}")]
    )
    if dates.empty:
        raise RuntimeError(f"交易日历没有 {end} 或之前的开放日")
    return str(dates.max())


def _normalize_index_daily(raw: pd.DataFrame, ts_code: str, trade_date: str) -> pd.DataFrame:
    columns = [
        "trade_date",
        "ts_code",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "pct_chg",
        "vol",
        "amount",
    ]
    if raw is None or raw.empty:
        return pd.DataFrame(columns=columns)
    frame = raw.copy().rename(
        columns={
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
        }
    )
    if "trade_date" not in frame.columns:
        return pd.DataFrame(columns=columns)
    frame["trade_date"] = frame["trade_date"].astype(str).str.replace("-", "", regex=False)
    frame = frame[frame["trade_date"] == compact_date(trade_date)].copy()
    if frame.empty:
        return pd.DataFrame(columns=columns)
    frame["ts_code"] = ts_code
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    return frame[columns].drop_duplicates(subset=["trade_date", "ts_code"], keep="last")


def refresh_required_index_date(
    engine: Any,
    pro: Any,
    trade_date: str,
    *,
    write: bool,
) -> tuple[pd.DataFrame, list[str]]:
    """Fetch only missing climate indices for one date and optionally upsert them."""
    day = compact_date(trade_date)
    placeholders = ",".join(f":code_{index}" for index in range(len(INDEX_CODES)))
    params = {f"code_{index}": code for index, code in enumerate(INDEX_CODES)}
    params["trade_date"] = day
    existing = _read_sql_or_empty(
        engine,
        f"""
        SELECT REPLACE(REPLACE(trade_date, '-', ''), '/', '') AS trade_date, ts_code,
               open_price, high_price, low_price, close_price, pct_chg, vol, amount
        FROM index_price
        WHERE ts_code IN ({placeholders})
          AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') = :trade_date
        """,
        params,
    )
    existing_codes = set(existing.get("ts_code", pd.Series(dtype="object")).astype(str))
    fetched_frames: list[pd.DataFrame] = []
    for code in INDEX_CODES:
        if code in existing_codes:
            continue
        last_error: Exception | None = None
        raw = pd.DataFrame()
        for attempt in range(3):
            try:
                raw = pro.index_daily(ts_code=code, start_date=day, end_date=day)
                break
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        if last_error is not None and (raw is None or raw.empty):
            print(f"[warn] {code} 指数日线拉取失败: {last_error}")
        normalized = _normalize_index_daily(raw, code, day)
        if not normalized.empty:
            fetched_frames.append(normalized)

    fetched = (
        pd.concat(fetched_frames, ignore_index=True)
        if fetched_frames
        else pd.DataFrame(columns=existing.columns)
    )
    if write and not fetched.empty:
        insert_sql = text(
            """
            INSERT INTO index_price
            (trade_date, ts_code, open_price, high_price, low_price, close_price, pct_chg, vol, amount)
            VALUES
            (:trade_date, :ts_code, :open_price, :high_price, :low_price, :close_price,
             :pct_chg, :vol, :amount)
            """
        )
        delete_sql = text(
            "DELETE FROM index_price WHERE trade_date=:trade_date AND ts_code=:ts_code"
        )
        with engine.begin() as conn:
            for row in fetched.to_dict(orient="records"):
                conn.execute(delete_sql, row)
                conn.execute(insert_sql, row)

    available_codes = existing_codes | set(
        fetched.get("ts_code", pd.Series(dtype="object")).astype(str)
    )
    missing = [code for code in INDEX_CODES if code not in available_codes]
    combined = pd.concat([existing, fetched], ignore_index=True) if not fetched.empty else existing
    return combined, missing


def fetch_margin_records(
    client: OfficialMarginClient,
    *,
    trading_dates: Sequence[str],
    end_date: str,
    backfill: bool,
    existing_margin: pd.DataFrame | None = None,
    history_start: str | None = None,
) -> tuple[list[MarginDailyRecord], list[str]]:
    records: list[MarginDailyRecord] = []
    warnings: list[str] = []
    if not backfill:
        official_error = "最近7个自然日未找到沪深共同发布日"
        try:
            result = client.fetch_common_day(end_date, lookback_calendar_days=7)
        except Exception as exc:
            result = None
            official_error = f"融资官方接口拉取失败: {exc}"
        if result is not None:
            return result[1], []

        existing = existing_margin if existing_margin is not None else pd.DataFrame()
        mirror_start = (
            str(existing["trade_date"].min())
            if not existing.empty and "trade_date" in existing.columns
            else (pd.to_datetime(end_date) - pd.DateOffset(years=3)).strftime("%Y%m%d")
        )
        try:
            sse_mirror = fetch_sse_margin_mirror(mirror_start, end_date)
            szse_mirror = fetch_szse_margin_mirror(mirror_start, end_date)
            validate_margin_mirror_against_official(sse_mirror, existing, exchange_id="SSE")
            validate_margin_mirror_against_official(szse_mirror, existing, exchange_id="SZSE")
            sse_by_date = {record.trade_date: record for record in sse_mirror}
            szse_by_date = {record.trade_date: record for record in szse_mirror}
            for row in existing.itertuples(index=False):
                source_name = str(getattr(row, "source_name", ""))
                exchange_id = str(getattr(row, "exchange_id", ""))
                if "official" not in source_name.lower() or exchange_id not in {"SSE", "SZSE"}:
                    continue
                financing_buy = getattr(row, "financing_buy_yuan", None)
                official_record = MarginDailyRecord(
                    trade_date=str(getattr(row, "trade_date", "")),
                    exchange_id=exchange_id,
                    financing_balance_yuan=float(getattr(row, "financing_balance_yuan")),
                    financing_buy_yuan=(
                        None if pd.isna(financing_buy) else float(financing_buy)
                    ),
                    source_name=source_name,
                    quality_status=str(getattr(row, "quality_status", "ok") or "ok"),
                )
                if exchange_id == "SSE":
                    sse_by_date[official_record.trade_date] = official_record
                else:
                    szse_by_date[official_record.trade_date] = official_record
            common_dates = sorted(set(sse_by_date) & set(szse_by_date))
            if not common_dates:
                raise RuntimeError("备选源没有沪深共同日期")
            day = common_dates[-1]
            fallback_records = [
                replace(
                    sse_by_date[day],
                    source_name="jin10_sse_daily_fallback_validated",
                    quality_status="fallback_validated",
                ),
                replace(
                    szse_by_date[day],
                    source_name="jin10_szse_daily_fallback_validated",
                    quality_status="fallback_validated",
                ),
            ]
            if not existing.empty:
                for index, fallback in enumerate(fallback_records):
                    official = existing[
                        (existing["trade_date"].astype(str) == day)
                        & (existing["exchange_id"].astype(str) == fallback.exchange_id)
                        & existing["source_name"].astype(str).str.contains("official", case=False)
                    ]
                    if official.empty:
                        continue
                    row = official.iloc[-1]
                    financing_buy = row.get("financing_buy_yuan")
                    fallback_records[index] = MarginDailyRecord(
                        trade_date=day,
                        exchange_id=fallback.exchange_id,
                        financing_balance_yuan=float(row["financing_balance_yuan"]),
                        financing_buy_yuan=(
                            None if pd.isna(financing_buy) else float(financing_buy)
                        ),
                        source_name=str(row["source_name"]),
                        quality_status=str(row.get("quality_status") or "ok"),
                    )
            return fallback_records, [f"{official_error}；已回退到校验镜像共同日{day}"]
        except Exception as exc:
            return [], [f"{official_error}；备选源同样不可用: {exc}"]

    start_date = compact_date(history_start or min(trading_dates))
    existing = existing_margin if existing_margin is not None else pd.DataFrame()
    existing_keys = {
        (str(row.trade_date), str(row.exchange_id))
        for row in existing.itertuples(index=False)
        if hasattr(row, "trade_date") and hasattr(row, "exchange_id")
    }
    try:
        sse_records = client.fetch_sse_range(start_date, end_date)
    except Exception as exc:
        return [], [f"上交所区间批量拉取失败: {exc}"]
    try:
        sse_mirror = fetch_sse_margin_mirror(start_date, end_date)
        sse_official_frame = pd.concat(
            [
                existing,
                pd.DataFrame([asdict(record) for record in sse_records]),
            ],
            ignore_index=True,
        )
        validate_margin_mirror_against_official(
            sse_mirror,
            sse_official_frame,
            exchange_id="SSE",
        )
        official_sse_dates = {record.trade_date for record in sse_records}
        sse_supplement = [
            record for record in sse_mirror if record.trade_date not in official_sse_dates
        ]
        if sse_supplement:
            warnings.append(
                f"上交所官方区间接口未覆盖全部目标日期，"
                f"经官方重叠校验后由历史镜像补充{len(sse_supplement)}日"
            )
        sse_records = [*sse_records, *sse_supplement]
    except Exception as exc:
        return [], [f"上交所历史镜像校验失败: {exc}"]
    try:
        szse_records = fetch_szse_margin_mirror(start_date, end_date)
        validate_szse_mirror_against_official(szse_records, existing)
    except Exception as exc:
        return [], [f"深交所历史镜像校验失败: {exc}"]

    candidates = [*sse_records, *szse_records]
    records = [
        record
        for record in candidates
        if (record.trade_date, record.exchange_id) not in existing_keys
    ]
    available = existing_keys | {(record.trade_date, record.exchange_id) for record in records}
    sse_dates = {day for day, exchange in available if exchange == "SSE" and start_date <= day <= end_date}
    szse_dates = {day for day, exchange in available if exchange == "SZSE" and start_date <= day <= end_date}
    for day in sorted(sse_dates - szse_dates):
        warnings.append(f"{day} 缺少深市融资历史，未拼接错日数据")
    return records, warnings


def _fetch_margin_mirror(
    exchange_id: str,
    url: str,
    start_date: str,
    end_date: str,
    *,
    session: requests.Session | Any | None = None,
) -> list[MarginDailyRecord]:
    """Fetch a one-shot history mirror; daily/current updates still prefer exchanges."""
    http = session or requests.Session()
    if hasattr(http, "trust_env"):
        http.trust_env = False
    last_error: Exception | None = None
    payload: Any = None
    for attempt in range(3):
        try:
            response = http.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            break
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    if not isinstance(payload, dict):
        raise RuntimeError(f"{exchange_id} history mirror request failed: {last_error}")
    values = payload.get("values", {})
    if not isinstance(values, dict):
        raise RuntimeError(f"{exchange_id} history mirror missing values")

    start = compact_date(start_date)
    end = compact_date(end_date)
    records: list[MarginDailyRecord] = []
    for raw_date, raw_values in values.items():
        try:
            day = pd.to_datetime(raw_date).strftime("%Y%m%d")
        except (TypeError, ValueError):
            continue
        if not start <= day <= end or not isinstance(raw_values, (list, tuple)) or len(raw_values) < 2:
            continue
        buy = pd.to_numeric(raw_values[0], errors="coerce")
        balance = pd.to_numeric(raw_values[1], errors="coerce")
        if pd.isna(balance):
            continue
        records.append(
            MarginDailyRecord(
                trade_date=day,
                exchange_id=exchange_id,
                financing_balance_yuan=float(balance),
                financing_buy_yuan=None if pd.isna(buy) else float(buy),
                source_name=f"jin10_{exchange_id.lower()}_history_validated",
                quality_status="mirror_validated",
            )
        )
    return sorted(records, key=lambda record: record.trade_date)


def fetch_sse_margin_mirror(
    start_date: str,
    end_date: str,
    *,
    session: requests.Session | Any | None = None,
) -> list[MarginDailyRecord]:
    return _fetch_margin_mirror(
        "SSE", JIN10_SSE_MARGIN_HISTORY_URL, start_date, end_date, session=session
    )


def fetch_szse_margin_mirror(
    start_date: str,
    end_date: str,
    *,
    session: requests.Session | Any | None = None,
) -> list[MarginDailyRecord]:
    return _fetch_margin_mirror(
        "SZSE", JIN10_SZSE_MARGIN_HISTORY_URL, start_date, end_date, session=session
    )


def validate_margin_mirror_against_official(
    mirror_records: Sequence[MarginDailyRecord],
    official_df: pd.DataFrame,
    *,
    exchange_id: str,
    min_overlap: int = 20,
    max_relative_error: float = 2e-6,
) -> None:
    required = {"trade_date", "exchange_id", "financing_balance_yuan", "source_name"}
    if official_df is None or official_df.empty or not required.issubset(official_df.columns):
        raise RuntimeError(f"缺少{exchange_id}官方重合样本，禁止使用历史镜像")
    official = official_df[
        (official_df["exchange_id"] == exchange_id)
        & official_df["source_name"].astype(str).str.startswith(f"{exchange_id.lower()}_official")
    ].copy()
    mirror_map = {record.trade_date: record.financing_balance_yuan for record in mirror_records}
    official["mirror_balance"] = official["trade_date"].astype(str).map(mirror_map)
    official["financing_balance_yuan"] = pd.to_numeric(
        official["financing_balance_yuan"], errors="coerce"
    )
    overlap = official.dropna(subset=["mirror_balance", "financing_balance_yuan"])
    if len(overlap) < max(int(min_overlap), 1):
        raise RuntimeError(f"{exchange_id}官方/镜像重合样本不足: {len(overlap)}")
    relative_error = (
        (overlap["mirror_balance"] - overlap["financing_balance_yuan"]).abs()
        / overlap["financing_balance_yuan"].abs()
    )
    observed = float(relative_error.max())
    if not math.isfinite(observed) or observed > max_relative_error:
        raise RuntimeError(
            f"{exchange_id}历史镜像与官方数据偏差超限: {observed:.8g} > {max_relative_error:.8g}"
        )


def validate_szse_mirror_against_official(
    mirror_records: Sequence[MarginDailyRecord],
    official_df: pd.DataFrame,
    *,
    min_overlap: int = 20,
    max_relative_error: float = 2e-6,
) -> None:
    validate_margin_mirror_against_official(
        mirror_records,
        official_df,
        exchange_id="SZSE",
        min_overlap=min_overlap,
        max_relative_error=max_relative_error,
    )


def normalize_market_amount_frames(sse: pd.DataFrame, szse: pd.DataFrame) -> pd.DataFrame:
    """Combine same-day exchange amount and float-market-value statistics."""
    frames: list[pd.DataFrame] = []
    for raw, exchange in ((sse, "SSE"), (szse, "SZSE")):
        if raw is None or raw.empty or not {"trade_date", "amount"}.issubset(raw.columns):
            continue
        selected = ["trade_date", "amount"]
        if "float_mv" in raw.columns:
            selected.append("float_mv")
        frame = raw[selected].copy()
        frame["trade_date"] = frame["trade_date"].astype(str).str.replace("-", "", regex=False)
        frame["amount"] = pd.to_numeric(frame["amount"], errors="coerce")
        frame["float_mv"] = pd.to_numeric(frame.get("float_mv"), errors="coerce")
        frame = frame.dropna(subset=["amount"])
        frame["exchange_id"] = exchange
        frames.append(frame)
    if len(frames) != 2:
        return pd.DataFrame(columns=["trade_date", "amount_yuan", "float_mv_yuan"])
    combined = pd.concat(frames, ignore_index=True)
    pivot = combined.pivot_table(
        index="trade_date", columns="exchange_id", values=["amount", "float_mv"], aggfunc="last"
    )
    amount = pivot.get("amount", pd.DataFrame())
    if amount.empty or not {"SSE", "SZSE"}.issubset(amount.columns):
        return pd.DataFrame(columns=["trade_date", "amount_yuan", "float_mv_yuan"])
    common = amount[["SSE", "SZSE"]].dropna().sum(axis=1).rename("amount_yi").to_frame()
    float_mv = pivot.get("float_mv", pd.DataFrame())
    if not float_mv.empty and {"SSE", "SZSE"}.issubset(float_mv.columns):
        common["float_mv_yi"] = float_mv[["SSE", "SZSE"]].dropna().sum(axis=1)
    else:
        common["float_mv_yi"] = float("nan")
    common = common.reset_index()
    common["amount_yuan"] = common["amount_yi"] * 100_000_000
    common["float_mv_yuan"] = common["float_mv_yi"] * 100_000_000
    return common[["trade_date", "amount_yuan", "float_mv_yuan"]].sort_values("trade_date")


def _daily_info_with_retry(pro: Any, *, market_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            result = pro.daily_info(
                ts_code=market_code,
                start_date=start_date,
                end_date=end_date,
                fields="trade_date,ts_code,amount,float_mv",
            )
            return result if result is not None else pd.DataFrame()
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"{market_code} 成交汇总拉取失败: {last_error}") from last_error


def fetch_market_amount_history(start_date: str, end_date: str, *, pro: Any | None = None) -> pd.DataFrame:
    pro = pro or create_tushare_client()
    sse = _daily_info_with_retry(pro, market_code="SH_MARKET", start_date=start_date, end_date=end_date)
    szse = _daily_info_with_retry(pro, market_code="SZ_MARKET", start_date=start_date, end_date=end_date)
    return normalize_market_amount_frames(sse, szse)


def load_source_frames(engine: Any, *, start_date: str, end_date: str) -> dict[str, Any]:
    margin = _read_sql_or_empty(
        engine,
        """
        SELECT trade_date, exchange_id, financing_balance_yuan
        FROM cn_margin_daily
        WHERE trade_date BETWEEN :start_date AND :end_date
        ORDER BY trade_date, exchange_id
        """,
        {"start_date": start_date, "end_date": end_date},
    )
    placeholders = ",".join(f":code_{index}" for index in range(len(INDEX_CODES)))
    index_params = {f"code_{index}": code for index, code in enumerate(INDEX_CODES)}
    index_params.update({"start_date": start_date, "end_date": end_date})
    index_prices = _read_sql_or_empty(
        engine,
        f"""
        SELECT REPLACE(REPLACE(trade_date, '-', ''), '/', '') AS trade_date,
               ts_code, close_price
        FROM index_price
        WHERE ts_code IN ({placeholders})
          AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') BETWEEN :start_date AND :end_date
        ORDER BY trade_date, ts_code
        """,
        index_params,
    )
    cn10y = _read_sql_or_empty(
        engine,
        """
        SELECT REPLACE(REPLACE(trade_date, '-', ''), '/', '') AS trade_date, close_value
        FROM macro_daily
        WHERE indicator_code = 'CN10Y'
          AND REPLACE(REPLACE(trade_date, '-', ''), '/', '') BETWEEN :start_date AND :end_date
        ORDER BY trade_date
        """,
        {"start_date": start_date, "end_date": end_date},
    )
    return {
        "margin": margin,
        "float_mv": pd.DataFrame(columns=["trade_date", "float_mv_yuan"]),
        "index": index_prices,
        "cn10y": cn10y,
    }


def basis_frame_from_payload(payload: dict[str, Any]) -> pd.DataFrame:
    points = payload.get("points", []) if isinstance(payload, dict) else []
    if not isinstance(points, list):
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "trade_date": point.get("trade_date"),
                "contract": point.get("contract"),
                "futures_close": point.get("futures_close"),
                "spot_close": point.get("spot_close"),
            }
            for point in points
            if isinstance(point, dict)
        ]
    )


def build_snapshot_rows(
    sources: dict[str, Any],
    *,
    as_of_date: str,
    trading_dates: Sequence[str],
) -> list[ClimateMetricRow]:
    rows: list[ClimateMetricRow] = []
    rows.extend(build_margin_metric_rows(sources["margin"], sources["float_mv"], as_of_date=as_of_date))
    amount = build_market_amount_metric_row(sources["amount"], as_of_date=as_of_date)
    if amount is not None:
        rows.append(amount)
    rows.extend(build_relative_strength_metric_rows(sources["index"], as_of_date=as_of_date))
    rate = build_cn10y_metric_row(sources["cn10y"], as_of_date=as_of_date)
    if rate is not None:
        rows.append(rate)
    basis = build_im_basis_metric_row(sources["basis"], as_of_date=as_of_date)
    if basis is not None:
        rows.append(basis)
    return mark_stale_metrics(rows, as_of_date=as_of_date, trading_dates=trading_dates)


def trading_day_lag(actual_date: str, expected_date: str, trading_dates: Sequence[str]) -> int | None:
    actual = str(actual_date or "")
    expected = str(expected_date or "")
    if len(actual) != 8 or len(expected) != 8:
        return None
    calendar = sorted({str(day) for day in trading_dates if str(day) <= expected})
    return sum(actual < day <= expected for day in calendar)


def build_metric_diagnostics(
    rows: Sequence[ClimateMetricRow],
    *,
    expected_date: str,
    trading_dates: Sequence[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    latest_by_code: dict[str, ClimateMetricRow] = {}
    for row in sorted(rows, key=lambda item: item.trade_date):
        latest_by_code[row.metric_code] = row

    diagnostics: list[dict[str, Any]] = []
    strict_failures: list[str] = []
    for code in CARD_ORDER:
        row = latest_by_code.get(code)
        actual_date = row.trade_date if row is not None else ""
        required_current = code in CORE_CURRENT_DATE_METRICS
        ready = actual_date == expected_date
        if required_current and not ready:
            strict_failures.append(code)
        source_dates = row.source_dates if row is not None else {}
        diagnostics.append(
            {
                "code": code,
                "expected_date": expected_date,
                "actual_date": actual_date,
                "date": actual_date,
                "lag_trading_days": trading_day_lag(actual_date, expected_date, trading_dates),
                "source": ",".join(sorted(source_dates)) if source_dates else "",
                "source_dates": source_dates,
                "value": row.metric_value if row is not None else None,
                "percentile": row.percentile if row is not None else None,
                "samples": row.sample_count if row is not None else 0,
                "quality": row.quality_status if row is not None else "missing",
                "required_current": required_current,
                "current_date_ready": ready,
            }
        )
    return diagnostics, strict_failures


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="更新ETF期权页八项A股市场环境指标")
    parser.add_argument("--date", help="更新截止日，YYYYMMDD；默认今天")
    parser.add_argument("--backfill-start", help="回补起始日，YYYYMMDD；建议动态三年前")
    parser.add_argument(
        "--margin-only",
        action="store_true",
        help="只回补融资历史；必须与 --backfill-start 一起使用",
    )
    parser.add_argument("--dry-run", action="store_true", help="只拉取和计算，不建表、不写数据库")
    parser.add_argument(
        "--require-core-date",
        action="store_true",
        help="要求成交额、三组指数强弱和IM贴水更新到预期交易日；不限制融资与中国10Y",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    end_date = compact_date(args.date or dt.date.today().strftime("%Y%m%d"))
    backfill_start = compact_date(args.backfill_start) if args.backfill_start else None
    if backfill_start and backfill_start > end_date:
        raise ValueError("--backfill-start 不能晚于 --date")
    if args.margin_only and not backfill_start:
        raise ValueError("--margin-only 必须与 --backfill-start 一起使用")

    engine = create_engine_from_env()
    if not args.dry_run:
        ensure_cn_market_climate_tables(engine)

    if args.margin_only:
        existing_margin = _read_sql_or_empty(
            engine,
            """
            SELECT trade_date, exchange_id, financing_balance_yuan, financing_buy_yuan,
                   source_name, quality_status
            FROM cn_margin_daily
            WHERE trade_date BETWEEN :start_date AND :end_date
            """,
            {"start_date": backfill_start, "end_date": end_date},
        )
        fetched_margin, warnings = fetch_margin_records(
            OfficialMarginClient(),
            trading_dates=(),
            end_date=end_date,
            backfill=True,
            existing_margin=existing_margin,
            history_start=backfill_start,
        )
        if fetched_margin and not args.dry_run:
            store_margin_records(engine, fetched_margin)
        summary = {
            "dry_run": bool(args.dry_run),
            "margin_only": True,
            "start_date": backfill_start,
            "end_date": end_date,
            "margin_rows_fetched": len(fetched_margin),
            "warnings": warnings[:20],
            "warning_count": len(warnings),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    pro = create_tushare_client()
    preflight_warnings: list[str] = []
    try:
        expected_date = resolve_expected_trade_date(pro, end_date)
    except Exception as exc:
        if args.require_core_date:
            summary = {
                "dry_run": bool(args.dry_run),
                "end_date": end_date,
                "expected_date": "",
                "require_core_date": True,
                "strict_ready": False,
                "strict_failures": list(CORE_CURRENT_DATE_METRICS),
                "fatal_error": f"交易日历解析失败: {exc}",
            }
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 2
        expected_date = end_date
        preflight_warnings.append(f"交易日历解析失败，非严格模式暂按请求日处理: {exc}")

    refreshed_index, missing_index_codes = refresh_required_index_date(
        engine,
        pro,
        expected_date,
        write=not args.dry_run,
    )
    if missing_index_codes:
        preflight_warnings.append(
            f"{expected_date} 必需指数仍缺失: {','.join(missing_index_codes)}"
        )

    three_year_start = (
        pd.to_datetime(backfill_start or expected_date) - pd.DateOffset(years=3)
    ).strftime("%Y%m%d")
    calendar_start = backfill_start or three_year_start
    trading_dates = load_trading_dates(engine, calendar_start, expected_date)
    if args.dry_run and not refreshed_index.empty:
        dry_run_dates = refreshed_index[
            refreshed_index["ts_code"].astype(str) == "000852.SH"
        ]["trade_date"].astype(str).tolist()
        trading_dates = sorted(set(trading_dates) | set(dry_run_dates))
    if not trading_dates:
        raise RuntimeError("index_price 未找到中证1000交易日，无法建立同日与陈旧度门禁")

    client = OfficialMarginClient()
    margin_dates = trading_dates if backfill_start else [trading_dates[-1]]
    existing_margin = _read_sql_or_empty(
        engine,
        """
        SELECT trade_date, exchange_id, financing_balance_yuan, financing_buy_yuan,
               source_name, quality_status
        FROM cn_margin_daily
        WHERE trade_date BETWEEN :start_date AND :end_date
        """,
        {"start_date": calendar_start, "end_date": expected_date},
    )
    fetched_margin, warnings = fetch_margin_records(
        client,
        trading_dates=margin_dates,
        end_date=expected_date,
        backfill=bool(backfill_start),
        existing_margin=existing_margin,
        history_start=backfill_start,
    )
    if fetched_margin and not args.dry_run:
        store_margin_records(engine, fetched_margin)

    sources = load_source_frames(engine, start_date=three_year_start, end_date=expected_date)
    if args.dry_run and not refreshed_index.empty:
        sources["index"] = pd.concat(
            [sources["index"], refreshed_index[["trade_date", "ts_code", "close_price"]]],
            ignore_index=True,
        ).drop_duplicates(subset=["trade_date", "ts_code"], keep="last")
    if args.dry_run and fetched_margin:
        fetched_frame = pd.DataFrame([asdict(record) for record in fetched_margin])
        sources["margin"] = pd.concat([sources["margin"], fetched_frame], ignore_index=True)
    market_stats = fetch_market_amount_history(three_year_start, expected_date, pro=pro)
    sources["amount"] = market_stats[["trade_date", "amount_yuan"]].dropna(
        subset=["amount_yuan"]
    )
    sources["float_mv"] = market_stats[["trade_date", "float_mv_yuan"]].dropna(
        subset=["float_mv_yuan"]
    )

    basis_years = max(
        1,
        math.ceil(
            (pd.to_datetime(expected_date) - pd.to_datetime(calendar_start)).days / 365.25
        )
        + 1,
    )
    sources["basis"] = basis_frame_from_payload(
        build_index_basis_longterm_payload(engine, "IM", lookback_years=basis_years)
    )

    snapshot_dates = trading_dates if backfill_start else [trading_dates[-1]]
    by_key: dict[tuple[str, str], ClimateMetricRow] = {}
    for snapshot_date in snapshot_dates:
        for row in build_snapshot_rows(sources, as_of_date=snapshot_date, trading_dates=trading_dates):
            by_key[(row.trade_date, row.metric_code)] = row
    rows = list(by_key.values())
    if not args.dry_run:
        store_climate_rows(engine, rows)

    metric_diagnostics, strict_failures = build_metric_diagnostics(
        rows,
        expected_date=expected_date,
        trading_dates=trading_dates,
    )
    all_warnings = [*preflight_warnings, *warnings]
    summary = {
        "dry_run": bool(args.dry_run),
        "end_date": end_date,
        "expected_date": expected_date,
        "backfill_start": backfill_start,
        "require_core_date": bool(args.require_core_date),
        "strict_ready": not strict_failures,
        "strict_failures": strict_failures,
        "margin_rows_fetched": len(fetched_margin),
        "climate_rows_computed": len(rows),
        "metrics": metric_diagnostics,
        "warnings": all_warnings[:20],
        "warning_count": len(all_warnings),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if args.require_core_date and strict_failures:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

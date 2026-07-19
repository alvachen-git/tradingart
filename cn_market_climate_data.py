from __future__ import annotations

import datetime as dt
import json
import math
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable, Sequence

import pandas as pd
import requests
from sqlalchemy import text


SSE_MARGIN_URL = "https://query.sse.com.cn/marketdata/tradedata/queryMargin.do"
SZSE_MARGIN_URL = "https://www.szse.cn/api/report/ShowReport/data"

MARGIN_TABLE = "cn_margin_daily"
CLIMATE_TABLE = "cn_market_climate_daily"

MARGIN_LEVERAGE = "MARGIN_LEVERAGE"
MARGIN_MOMENTUM_5D = "MARGIN_MOMENTUM_5D"
MARKET_AMOUNT = "MARKET_AMOUNT"
SZ50_CSI1000_RS = "SZ50_CSI1000_RS"
STAR50_CHINEXT_RS = "STAR50_CHINEXT_RS"
CHINEXT_CSI1000_RS = "CHINEXT_CSI1000_RS"
CN10Y_RATE = "CN10Y_RATE"
IM_BASIS = "IM_BASIS"

CARD_ORDER = (
    MARKET_AMOUNT,
    MARGIN_MOMENTUM_5D,
    MARGIN_LEVERAGE,
    SZ50_CSI1000_RS,
    STAR50_CHINEXT_RS,
    CHINEXT_CSI1000_RS,
    CN10Y_RATE,
    IM_BASIS,
)

INDEX_PAIR_CONFIG = (
    (SZ50_CSI1000_RS, "000016.SH", "000852.SH", "50", "1000"),
    (STAR50_CHINEXT_RS, "000688.SH", "399006.SZ", "科创", "创业"),
    (CHINEXT_CSI1000_RS, "399006.SZ", "000852.SH", "创业", "1000"),
)


@dataclass(frozen=True)
class MarginDailyRecord:
    trade_date: str
    exchange_id: str
    financing_balance_yuan: float
    financing_buy_yuan: float | None
    source_name: str
    quality_status: str = "ok"


@dataclass(frozen=True)
class ClimateMetricRow:
    trade_date: str
    metric_code: str
    metric_value: float | None
    percentile: float | None
    secondary_value: float | None
    sample_count: int
    payload: dict[str, Any]
    source_dates: dict[str, str]
    quality_status: str = "ok"


def _compact_date(value: Any) -> str:
    if value is None:
        return ""
    raw = str(value).strip().replace("-", "").replace("/", "")
    return raw[:8] if len(raw) >= 8 and raw[:8].isdigit() else ""


def _number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", "").strip()
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


class OfficialMarginClient:
    """Read daily margin summaries directly from SSE and SZSE public endpoints."""

    def __init__(
        self,
        *,
        session: requests.Session | Any | None = None,
        timeout_seconds: float = 10.0,
        attempts: int = 3,
        retry_sleep_seconds: float = 0.5,
    ) -> None:
        self.session = session or requests.Session()
        if hasattr(self.session, "trust_env"):
            self.session.trust_env = False
        self.timeout_seconds = max(float(timeout_seconds), 0.1)
        self.attempts = max(int(attempts), 1)
        self.retry_sleep_seconds = max(float(retry_sleep_seconds), 0.0)

    def _get_json(self, url: str, *, params: dict[str, Any], headers: dict[str, str]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                if attempt + 1 < self.attempts and self.retry_sleep_seconds:
                    time.sleep(self.retry_sleep_seconds * (attempt + 1))
        raise RuntimeError(f"official margin request failed: {last_error}") from last_error

    def fetch_sse(self, trade_date: str) -> MarginDailyRecord | None:
        day = _compact_date(trade_date)
        if not day:
            raise ValueError(f"invalid trade_date: {trade_date}")
        payload = self._get_json(
            SSE_MARGIN_URL,
            params={
                "isPagination": "true",
                "beginDate": day,
                "endDate": day,
                "tabType": "",
                "stockCode": "",
                "pageHelp.pageSize": "20",
                "pageHelp.pageNo": "1",
                "pageHelp.beginPage": "1",
                "pageHelp.cacheSize": "1",
                "pageHelp.endPage": "1",
            },
            headers={"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
        )
        rows = payload.get("result", []) if isinstance(payload, dict) else []
        if not rows:
            return None
        return self._parse_sse_row(rows[0], fallback_date=day)

    @staticmethod
    def _parse_sse_row(row: Any, *, fallback_date: str = "") -> MarginDailyRecord | None:
        row = row if isinstance(row, dict) else {}
        balance = _number(row.get("rzye"))
        if balance is None:
            return None
        return MarginDailyRecord(
            trade_date=_compact_date(row.get("opDate")) or fallback_date,
            exchange_id="SSE",
            financing_balance_yuan=balance,
            financing_buy_yuan=_number(row.get("rzmre")),
            source_name="sse_official",
        )

    def fetch_sse_range(self, start_date: str, end_date: str) -> list[MarginDailyRecord]:
        start = _compact_date(start_date)
        end = _compact_date(end_date)
        if not start or not end or start > end:
            raise ValueError(f"invalid SSE date range: {start_date} -> {end_date}")
        payload = self._get_json(
            SSE_MARGIN_URL,
            params={
                "isPagination": "true",
                "beginDate": start,
                "endDate": end,
                "tabType": "",
                "stockCode": "",
                "pageHelp.pageSize": "5000",
                "pageHelp.pageNo": "1",
                "pageHelp.beginPage": "1",
                "pageHelp.cacheSize": "1",
                "pageHelp.endPage": "5",
            },
            headers={"Referer": "https://www.sse.com.cn/", "User-Agent": "Mozilla/5.0"},
        )
        rows = payload.get("result", []) if isinstance(payload, dict) else []
        records = [self._parse_sse_row(row) for row in rows]
        return sorted(
            [record for record in records if record is not None and start <= record.trade_date <= end],
            key=lambda record: record.trade_date,
        )

    def fetch_szse(self, trade_date: str) -> MarginDailyRecord | None:
        day = _compact_date(trade_date)
        if not day:
            raise ValueError(f"invalid trade_date: {trade_date}")
        formatted = f"{day[:4]}-{day[4:6]}-{day[6:]}"
        payload = self._get_json(
            SZSE_MARGIN_URL,
            params={
                "SHOWTYPE": "JSON",
                "CATALOGID": "1837_xxpl",
                "txtDate": formatted,
                "tab1PAGENO": "1",
            },
            headers={
                "Referer": "https://www.szse.cn/disclosure/margin/margin/index.html",
                "User-Agent": "Mozilla/5.0",
            },
        )
        sections = payload if isinstance(payload, list) else []
        summary = next(
            (
                section
                for section in sections
                if isinstance(section, dict)
                and str(section.get("metadata", {}).get("tabkey")) == "tab1"
            ),
            None,
        )
        rows = summary.get("data", []) if isinstance(summary, dict) else []
        if not rows:
            return None
        row = rows[0] if isinstance(rows[0], dict) else {}
        balance_yi = _number(row.get("jrrzye"))
        if balance_yi is None:
            return None
        buy_yi = _number(row.get("jrrzmr"))
        return MarginDailyRecord(
            trade_date=day,
            exchange_id="SZSE",
            financing_balance_yuan=balance_yi * 100_000_000,
            financing_buy_yuan=buy_yi * 100_000_000 if buy_yi is not None else None,
            source_name="szse_official",
        )

    def fetch_common_day(
        self,
        end_date: str | dt.date,
        *,
        lookback_calendar_days: int = 7,
    ) -> tuple[str, list[MarginDailyRecord]] | None:
        end = pd.to_datetime(end_date, errors="coerce")
        if pd.isna(end):
            raise ValueError(f"invalid end_date: {end_date}")
        for offset in range(max(int(lookback_calendar_days), 0) + 1):
            day = (end - pd.Timedelta(days=offset)).strftime("%Y%m%d")
            sse = self.fetch_sse(day)
            szse = self.fetch_szse(day)
            if sse is not None and szse is not None and sse.trade_date == szse.trade_date == day:
                return day, [sse, szse]
        return None


def ensure_cn_market_climate_tables(engine: Any) -> None:
    margin_ddl = f"""
        CREATE TABLE IF NOT EXISTS {MARGIN_TABLE} (
            trade_date VARCHAR(8) NOT NULL,
            exchange_id VARCHAR(8) NOT NULL,
            financing_balance_yuan NUMERIC(22, 2) NOT NULL,
            financing_buy_yuan NUMERIC(22, 2),
            source_name VARCHAR(64) NOT NULL,
            quality_status VARCHAR(24) NOT NULL DEFAULT 'ok',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, exchange_id)
        )
    """
    climate_ddl = f"""
        CREATE TABLE IF NOT EXISTS {CLIMATE_TABLE} (
            trade_date VARCHAR(8) NOT NULL,
            metric_code VARCHAR(48) NOT NULL,
            metric_value DOUBLE,
            percentile DOUBLE,
            secondary_value DOUBLE,
            sample_count INTEGER NOT NULL DEFAULT 0,
            payload_json TEXT,
            source_dates_json TEXT,
            quality_status VARCHAR(24) NOT NULL DEFAULT 'ok',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, metric_code)
        )
    """
    with engine.begin() as conn:
        conn.execute(text(margin_ddl))
        conn.execute(text(climate_ddl))


def store_margin_records(engine: Any, records: Sequence[MarginDailyRecord]) -> int:
    rows = [asdict(record) for record in records]
    if not rows:
        return 0
    with engine.begin() as conn:
        for row in rows:
            conn.execute(
                text(
                    f"DELETE FROM {MARGIN_TABLE} "
                    "WHERE trade_date=:trade_date AND exchange_id=:exchange_id"
                ),
                row,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {MARGIN_TABLE}
                    (trade_date, exchange_id, financing_balance_yuan, financing_buy_yuan,
                     source_name, quality_status)
                    VALUES
                    (:trade_date, :exchange_id, :financing_balance_yuan, :financing_buy_yuan,
                     :source_name, :quality_status)
                    """
                ),
                row,
            )
    return len(rows)


def store_climate_rows(engine: Any, records: Sequence[ClimateMetricRow]) -> int:
    if not records:
        return 0
    with engine.begin() as conn:
        for record in records:
            row = {
                "trade_date": record.trade_date,
                "metric_code": record.metric_code,
                "metric_value": record.metric_value,
                "percentile": record.percentile,
                "secondary_value": record.secondary_value,
                "sample_count": int(record.sample_count),
                "payload_json": serialize_json(record.payload),
                "source_dates_json": serialize_json(record.source_dates),
                "quality_status": record.quality_status,
            }
            conn.execute(
                text(
                    f"DELETE FROM {CLIMATE_TABLE} "
                    "WHERE trade_date=:trade_date AND metric_code=:metric_code"
                ),
                row,
            )
            conn.execute(
                text(
                    f"""
                    INSERT INTO {CLIMATE_TABLE}
                    (trade_date, metric_code, metric_value, percentile, secondary_value,
                     sample_count, payload_json, source_dates_json, quality_status)
                    VALUES
                    (:trade_date, :metric_code, :metric_value, :percentile, :secondary_value,
                     :sample_count, :payload_json, :source_dates_json, :quality_status)
                    """
                ),
                row,
            )
    return len(records)


def empirical_percentile(
    values: Iterable[Any],
    current: Any,
    *,
    min_samples: int = 1,
) -> tuple[float | None, int]:
    series = pd.to_numeric(pd.Series(list(values), dtype="object"), errors="coerce").dropna()
    current_value = _number(current)
    sample_count = int(len(series))
    if current_value is None or sample_count < max(int(min_samples), 1):
        return None, sample_count
    percentile = int((series <= current_value).sum()) / sample_count * 100
    return float(max(0.0, min(100.0, percentile))), sample_count


def _dated_frame(df: pd.DataFrame, *, date_col: str = "trade_date") -> pd.DataFrame:
    if df is None or df.empty or date_col not in df.columns:
        return pd.DataFrame()
    out = df.copy()
    out[date_col] = out[date_col].map(_compact_date)
    out = out[out[date_col].str.len() == 8]
    return out.sort_values(date_col).drop_duplicates()


def _scope_as_of(df: pd.DataFrame, as_of_date: str | None) -> pd.DataFrame:
    if df.empty or not as_of_date:
        return df
    day = _compact_date(as_of_date)
    return df[df["trade_date"] <= day]


def _calendar_window(df: pd.DataFrame, latest_date: str, years: int) -> pd.DataFrame:
    latest = pd.to_datetime(latest_date, format="%Y%m%d", errors="coerce")
    if pd.isna(latest):
        return df.iloc[0:0]
    start = (latest - pd.DateOffset(years=max(int(years), 1))).strftime("%Y%m%d")
    return df[df["trade_date"] >= start]


def build_margin_metric_rows(
    margin_df: pd.DataFrame,
    float_mv_df: pd.DataFrame,
    *,
    as_of_date: str | None = None,
    min_samples: int = 252,
) -> list[ClimateMetricRow]:
    margin = _dated_frame(margin_df)
    float_mv = _dated_frame(float_mv_df)
    required_margin = {"trade_date", "exchange_id", "financing_balance_yuan"}
    if not required_margin.issubset(margin.columns) or "float_mv_yuan" not in float_mv.columns:
        return []
    margin["financing_balance_yuan"] = pd.to_numeric(
        margin["financing_balance_yuan"], errors="coerce"
    )
    pivot = margin.pivot_table(
        index="trade_date",
        columns="exchange_id",
        values="financing_balance_yuan",
        aggfunc="last",
    )
    if not {"SSE", "SZSE"}.issubset(pivot.columns):
        return []
    combined = pivot[["SSE", "SZSE"]].dropna().sum(axis=1).rename("margin_balance_yuan")
    float_mv["float_mv_yuan"] = pd.to_numeric(float_mv["float_mv_yuan"], errors="coerce")
    market = (
        float_mv.dropna(subset=["float_mv_yuan"])
        .groupby("trade_date", as_index=True)["float_mv_yuan"]
        .last()
    )
    history = pd.concat([combined, market], axis=1, join="inner").dropna().reset_index()
    history = _scope_as_of(history, as_of_date)
    history = history[(history["margin_balance_yuan"] > 0) & (history["float_mv_yuan"] > 0)]
    if history.empty:
        return []
    history["leverage_pct"] = history["margin_balance_yuan"] / history["float_mv_yuan"] * 100
    history["momentum_5d_pct"] = history["margin_balance_yuan"].pct_change(5) * 100
    history["momentum_5d_yuan"] = history["margin_balance_yuan"].diff(5)
    latest = history.iloc[-1]
    window = _calendar_window(history, str(latest["trade_date"]), 3)
    leverage_pct, leverage_count = empirical_percentile(
        window["leverage_pct"], latest["leverage_pct"], min_samples=min_samples
    )
    momentum_window = window.dropna(subset=["momentum_5d_pct"])
    momentum_pct, momentum_count = empirical_percentile(
        momentum_window["momentum_5d_pct"],
        latest["momentum_5d_pct"],
        min_samples=min_samples,
    )
    day = str(latest["trade_date"])
    sources = {"margin": day, "float_mv": day}
    return [
        ClimateMetricRow(
            trade_date=day,
            metric_code=MARGIN_LEVERAGE,
            metric_value=float(latest["leverage_pct"]),
            percentile=leverage_pct,
            secondary_value=float(latest["margin_balance_yuan"]),
            sample_count=leverage_count,
            payload={
                "balance_yi": float(latest["margin_balance_yuan"]) / 100_000_000,
                "float_mv_trillion": float(latest["float_mv_yuan"]) / 1_000_000_000_000,
                "ratio_pct": float(latest["leverage_pct"]),
            },
            source_dates=sources,
            quality_status="ok" if leverage_pct is not None else "insufficient",
        ),
        ClimateMetricRow(
            trade_date=day,
            metric_code=MARGIN_MOMENTUM_5D,
            metric_value=_number(latest["momentum_5d_pct"]),
            percentile=momentum_pct,
            secondary_value=_number(latest["momentum_5d_yuan"]),
            sample_count=momentum_count,
            payload={
                "change_pct": _number(latest["momentum_5d_pct"]),
                "change_yi": (
                    _number(latest["momentum_5d_yuan"]) / 100_000_000
                    if _number(latest["momentum_5d_yuan"]) is not None
                    else None
                ),
            },
            source_dates={"margin": day},
            quality_status="ok" if momentum_pct is not None else "insufficient",
        ),
    ]


def build_market_amount_metric_row(
    market_amount_df: pd.DataFrame,
    *,
    as_of_date: str | None = None,
    min_samples: int = 252,
) -> ClimateMetricRow | None:
    history = _scope_as_of(_dated_frame(market_amount_df), as_of_date)
    if history.empty or "amount_yuan" not in history.columns:
        return None
    history["amount_yuan"] = pd.to_numeric(history["amount_yuan"], errors="coerce")
    history = history.dropna(subset=["amount_yuan"])
    if history.empty:
        return None
    history["ma20"] = history["amount_yuan"].rolling(20, min_periods=10).mean()
    latest = history.iloc[-1]
    window = _calendar_window(history, str(latest["trade_date"]), 3)
    percentile, count = empirical_percentile(
        window["amount_yuan"], latest["amount_yuan"], min_samples=min_samples
    )
    ma20 = _number(latest["ma20"])
    ma20_ratio = float(latest["amount_yuan"]) / ma20 if ma20 and ma20 > 0 else None
    day = str(latest["trade_date"])
    return ClimateMetricRow(
        trade_date=day,
        metric_code=MARKET_AMOUNT,
        metric_value=float(latest["amount_yuan"]),
        percentile=percentile,
        secondary_value=ma20_ratio,
        sample_count=count,
        payload={
            "amount_trillion": float(latest["amount_yuan"]) / 1_000_000_000_000,
            "ma20_ratio": ma20_ratio,
        },
        source_dates={"market_amount": day},
        quality_status="ok" if percentile is not None else "insufficient",
    )


def build_relative_strength_metric_rows(
    index_df: pd.DataFrame,
    *,
    as_of_date: str | None = None,
    lookback_sessions: int = 126,
    min_samples: int = 60,
) -> list[ClimateMetricRow]:
    prices = _scope_as_of(_dated_frame(index_df), as_of_date)
    required = {"trade_date", "ts_code", "close_price"}
    if prices.empty or not required.issubset(prices.columns):
        return []
    prices["close_price"] = pd.to_numeric(prices["close_price"], errors="coerce")
    pivot = prices.pivot_table(
        index="trade_date", columns="ts_code", values="close_price", aggfunc="last"
    )
    rows: list[ClimateMetricRow] = []
    for metric_code, numerator, denominator, numerator_label, denominator_label in INDEX_PAIR_CONFIG:
        if numerator not in pivot.columns or denominator not in pivot.columns:
            continue
        pair = pivot[[numerator, denominator]].dropna()
        pair = pair[(pair[numerator] > 0) & (pair[denominator] > 0)].tail(lookback_sessions)
        if pair.empty:
            continue
        ratio = pair[numerator] / pair[denominator]
        latest_ratio = float(ratio.iloc[-1])
        percentile, count = empirical_percentile(
            ratio, latest_ratio, min_samples=min_samples
        )
        numerator_return = (float(pair[numerator].iloc[-1]) / float(pair[numerator].iloc[0]) - 1) * 100
        denominator_return = (
            float(pair[denominator].iloc[-1]) / float(pair[denominator].iloc[0]) - 1
        ) * 100
        day = str(pair.index[-1])
        rows.append(
            ClimateMetricRow(
                trade_date=day,
                metric_code=metric_code,
                metric_value=latest_ratio,
                percentile=percentile,
                secondary_value=numerator_return - denominator_return,
                sample_count=count,
                payload={
                    "numerator_label": numerator_label,
                    "denominator_label": denominator_label,
                    "numerator_return_pct": numerator_return,
                    "denominator_return_pct": denominator_return,
                    "spread_pp": numerator_return - denominator_return,
                    "lookback_sessions": int(len(pair)),
                },
                source_dates={numerator: day, denominator: day},
                quality_status="ok" if percentile is not None else "insufficient",
            )
        )
    return rows


def build_cn10y_metric_row(
    macro_df: pd.DataFrame,
    *,
    as_of_date: str | None = None,
    min_samples: int = 252,
) -> ClimateMetricRow | None:
    history = _scope_as_of(_dated_frame(macro_df), as_of_date)
    if history.empty or "close_value" not in history.columns:
        return None
    history["close_value"] = pd.to_numeric(history["close_value"], errors="coerce")
    history = history.dropna(subset=["close_value"])
    if history.empty:
        return None
    latest = history.iloc[-1]
    window = _calendar_window(history, str(latest["trade_date"]), 3)
    percentile, count = empirical_percentile(
        window["close_value"], latest["close_value"], min_samples=min_samples
    )
    delta_5d_bp = None
    if len(history) >= 6:
        delta_5d_bp = (float(history.iloc[-1]["close_value"]) - float(history.iloc[-6]["close_value"])) * 100
    day = str(latest["trade_date"])
    return ClimateMetricRow(
        trade_date=day,
        metric_code=CN10Y_RATE,
        metric_value=float(latest["close_value"]),
        percentile=percentile,
        secondary_value=delta_5d_bp,
        sample_count=count,
        payload={"delta_5d_bp": delta_5d_bp},
        source_dates={"CN10Y": day},
        quality_status="ok" if percentile is not None else "insufficient",
    )


def build_im_basis_metric_row(
    basis_df: pd.DataFrame,
    *,
    as_of_date: str | None = None,
    min_samples: int = 120,
) -> ClimateMetricRow | None:
    history = _scope_as_of(_dated_frame(basis_df), as_of_date)
    required = {"trade_date", "contract", "futures_close", "spot_close"}
    if history.empty or not required.issubset(history.columns):
        return None
    for col in ("futures_close", "spot_close"):
        history[col] = pd.to_numeric(history[col], errors="coerce")
    history = history.dropna(subset=["futures_close", "spot_close"])
    history = history[(history["futures_close"] > 0) & (history["spot_close"] > 0)]
    if history.empty:
        return None
    history["basis_pct"] = (history["futures_close"] / history["spot_close"] - 1) * 100
    latest = history.iloc[-1]
    window = _calendar_window(history, str(latest["trade_date"]), 1)
    percentile, count = empirical_percentile(
        window["basis_pct"], latest["basis_pct"], min_samples=min_samples
    )
    day = str(latest["trade_date"])
    return ClimateMetricRow(
        trade_date=day,
        metric_code=IM_BASIS,
        metric_value=float(latest["basis_pct"]),
        percentile=percentile,
        secondary_value=None,
        sample_count=count,
        payload={
            "contract": (
                f"IM{str(latest['contract'])}"
                if str(latest["contract"]).isdigit() and len(str(latest["contract"])) == 4
                else str(latest["contract"])
            ),
            "basis_pct": float(latest["basis_pct"]),
        },
        source_dates={"IM": day, "000852.SH": day},
        quality_status="ok" if percentile is not None else "insufficient",
    )


def mark_stale_metrics(
    records: Sequence[ClimateMetricRow],
    *,
    as_of_date: str,
    trading_dates: Sequence[Any],
    max_lag_sessions: int = 2,
) -> list[ClimateMetricRow]:
    """Mark a cached metric stale when its own source date lags the reference market day."""
    reference = _compact_date(as_of_date)
    calendar = sorted(
        {
            day
            for raw in trading_dates
            if (day := _compact_date(raw)) and day <= reference
        }
    )
    if not reference or not calendar:
        return list(records)

    marked: list[ClimateMetricRow] = []
    for record in records:
        lag = sum(record.trade_date < day <= reference for day in calendar)
        if lag <= max(int(max_lag_sessions), 0):
            marked.append(record)
            continue
        payload = {**record.payload, "stale_trading_days": lag}
        marked.append(
            ClimateMetricRow(
                **{
                    **asdict(record),
                    "payload": payload,
                    "quality_status": "stale",
                }
            )
        )
    return marked


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _fmt_signed(value: Any, digits: int = 1, suffix: str = "%") -> str:
    number = _number(value)
    if number is None:
        return "--"
    return f"{number:+.{digits}f}{suffix}"


def _fmt_percentile(value: Any) -> str:
    number = _number(value)
    return f"{number:.0f}/100" if number is not None else "--"


def _date_label(value: Any) -> str:
    day = _compact_date(value)
    return f"{day[4:6]}/{day[6:]}" if day else "--"


def _card_color(percentile: Any, *, neutral: str = "#0f172a") -> str:
    value = _number(percentile)
    if value is None:
        return "#64748b"
    if value >= 80:
        return "#dc2626"
    if value <= 20:
        return "#2563eb"
    return neutral


def _percentile_judgement(
    percentile: Any,
    *,
    high: str,
    middle: str,
    low: str,
) -> str:
    value = _number(percentile)
    if value is None:
        return "等待更新"
    if value >= 80:
        return high
    if value <= 20:
        return low
    return middle


def _empty_card(metric_code: str) -> dict[str, Any]:
    labels = {
        MARGIN_LEVERAGE: "沪深融资杠杆",
        MARGIN_MOMENTUM_5D: "融资5日动能",
        MARKET_AMOUNT: "沪深成交额",
        SZ50_CSI1000_RS: "50/1000强弱",
        STAR50_CHINEXT_RS: "科创/创业强弱",
        CHINEXT_CSI1000_RS: "创业/1000强弱",
        CN10Y_RATE: "中国10Y利率",
        IM_BASIS: "IM期指基差",
    }
    return {
        "metric_code": metric_code,
        "label": labels[metric_code],
        "value": "--",
        "detail": "暂未更新",
        "color": "#64748b",
        "hint": "数据暂未更新，请稍后再看。",
        "as_of": "",
        "quality_status": "missing",
    }


def format_climate_card(row: dict[str, Any]) -> dict[str, Any]:
    code = str(row.get("metric_code") or "")
    card = _empty_card(code)
    payload = _json_object(row.get("payload_json"))
    percentile = _number(row.get("percentile"))
    value = _number(row.get("metric_value"))
    day = _compact_date(row.get("trade_date"))
    date_text = _date_label(day)
    quality = str(row.get("quality_status") or "ok")
    if code == MARGIN_LEVERAGE:
        judgement = _percentile_judgement(
            percentile,
            high="杠杆偏高",
            middle="杠杆适中",
            low="杠杆偏低",
        )
        card.update(
            value=_fmt_percentile(percentile),
            detail=f"融资占流通市值 {_number(payload.get('ratio_pct')) or 0:.2f}% · {judgement} · {date_text}",
            hint=(
                "观察市场使用融资资金的程度。百分位越高，说明杠杆和风险偏好越高，"
                "市场波动也可能更大；偏低则说明资金更谨慎。"
            ),
        )
    elif code == MARGIN_MOMENTUM_5D:
        change_yi = _number(payload.get("change_yi"))
        if change_yi is None:
            change_text = "净变化--"
        elif change_yi >= 0:
            change_text = f"净增{abs(change_yi):.0f}亿"
        else:
            change_text = f"净减{abs(change_yi):.0f}亿"
        if value is None:
            judgement = "等待更新"
        elif value > 0:
            judgement = "融资升温"
        elif value < 0:
            judgement = "融资降温"
        else:
            judgement = "融资持平"
        card.update(
            value=_fmt_signed(value, 2),
            detail=f"{change_text} · {judgement} · 分位 {_fmt_percentile(percentile)} · {date_text}",
            hint=(
                "看最近5个交易日融资资金是在流入还是流出。净增通常代表风险偏好升温，"
                "净减则代表资金趋于谨慎。"
            ),
        )
    elif code == MARKET_AMOUNT:
        amount = _number(payload.get("amount_trillion"))
        ma20_ratio = _number(payload.get("ma20_ratio"))
        if ma20_ratio is None:
            judgement = "等待更新"
        elif ma20_ratio >= 1.1:
            judgement = "放量升温"
        elif ma20_ratio <= 0.9:
            judgement = "缩量降温"
        else:
            judgement = "交投平稳"
        card.update(
            value=_fmt_percentile(percentile),
            detail=f"{amount:.2f}万亿 · 较20日均值{ma20_ratio:.2f}倍 · {judgement} · {date_text}" if amount is not None and ma20_ratio is not None else f"等待更新 · {date_text}",
            hint=(
                "观察沪深股市当天的交易热度。百分位越高，说明成交越活跃；"
                "高于近期均值属于放量，低于近期均值属于缩量。"
            ),
        )
    elif code in {SZ50_CSI1000_RS, STAR50_CHINEXT_RS, CHINEXT_CSI1000_RS}:
        left = str(payload.get("numerator_label") or "A")
        right = str(payload.get("denominator_label") or "B")
        left_ret = _fmt_signed(payload.get("numerator_return_pct"), 1)
        right_ret = _fmt_signed(payload.get("denominator_return_pct"), 1)
        spread_value = _number(payload.get("spread_pp"))
        if spread_value is None:
            judgement = "等待更新"
        elif spread_value > 0:
            judgement = f"{left}更强"
        elif spread_value < 0:
            judgement = f"{right}更强"
        else:
            judgement = "表现接近"
        card.update(
            value=_fmt_percentile(percentile),
            detail=f"{left} {left_ret} · {right} {right_ret} · {judgement} · {date_text}",
            hint=(
                f"比较近半年{left}和{right}谁表现更强。百分位越高，{left}越占优；"
                f"越低则{right}越占优。两边可能一起上涨或下跌，要结合卡片里的涨跌幅判断。"
            ),
        )
    elif code == CN10Y_RATE:
        delta_5d_bp = _number(payload.get("delta_5d_bp"))
        if delta_5d_bp is None:
            judgement = "等待更新"
        elif delta_5d_bp >= 0.5:
            judgement = "利率上行"
        elif delta_5d_bp <= -0.5:
            judgement = "利率回落"
        else:
            judgement = "利率持平"
        card.update(
            value=f"{value:.2f}%" if value is not None else "--",
            detail=f"5日 {_fmt_signed(delta_5d_bp, 0, 'bp')} · {judgement} · 分位 {_fmt_percentile(percentile)} · {date_text}",
            hint=(
                "观察国内长期利率的方向。利率上行通常会增加高估值资产的压力，"
                "利率回落则相对有利；百分位越高，说明利率处在较高位置。"
            ),
        )
    elif code == IM_BASIS:
        contract = str(payload.get("contract") or "IM")
        if value is None:
            basis_text = "等待更新"
            judgement = "情绪待观察"
        elif value < 0:
            basis_text = f"贴水{abs(value):.2f}%"
            judgement = "期货偏谨慎"
        elif value > 0:
            basis_text = f"升水{value:.2f}%"
            judgement = "期货偏强"
        else:
            basis_text = "平水"
            judgement = "情绪中性"
        card.update(
            value=_fmt_percentile(percentile),
            detail=f"{contract} · {basis_text} · {judgement} · {date_text}",
            hint=(
                "比较IM期货与中证1000现货的强弱。百分位越高，说明期货相对越强；"
                "贴水通常代表期货更谨慎，升水则代表期货更强。正负值按原方向比较，不取绝对值。"
            ),
        )
    card.update(
        color=_card_color(percentile),
        as_of=day,
        quality_status=quality,
    )
    if quality != "ok":
        card["color"] = "#64748b"
        if quality == "insufficient":
            card["detail"] = f"{card['detail']} · 历史数据不足"
        elif quality == "stale":
            lag = int(_number(payload.get("stale_trading_days")) or 0)
            card["detail"] = f"{card['detail']} · 更新滞后{lag}个交易日"
    return card


def load_cn_market_climate_strip(engine: Any) -> list[dict[str, Any]]:
    if engine is None:
        return [_empty_card(code) for code in CARD_ORDER]
    sql = text(
        f"""
        SELECT c.trade_date, c.metric_code, c.metric_value, c.percentile,
               c.secondary_value, c.sample_count, c.payload_json,
               c.source_dates_json, c.quality_status
        FROM {CLIMATE_TABLE} c
        JOIN (
            SELECT metric_code, MAX(trade_date) AS trade_date
            FROM {CLIMATE_TABLE}
            GROUP BY metric_code
        ) latest
          ON latest.metric_code = c.metric_code
         AND latest.trade_date = c.trade_date
        """
    )
    try:
        with engine.connect() as conn:
            records = [dict(row) for row in conn.execute(sql).mappings().all()]
    except Exception:
        return [_empty_card(code) for code in CARD_ORDER]
    by_code = {
        str(row.get("metric_code")): format_climate_card(row)
        for row in records
        if str(row.get("metric_code")) in CARD_ORDER
    }
    return [by_code.get(code, _empty_card(code)) for code in CARD_ORDER]


def serialize_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)

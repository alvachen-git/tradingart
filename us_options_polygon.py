from __future__ import annotations

import argparse
import csv
import datetime as dt
import gzip
import importlib.util
import io
import json
import math
import os
import re
import subprocess
import sys
import time
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence
from urllib.parse import urlencode

try:
    import pandas as pd
except Exception:  # pragma: no cover - lets pure logic tests run in tiny envs
    pd = None

try:
    import requests
except Exception:  # pragma: no cover - API calls will fail clearly at runtime
    requests = None

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv(*_args, **_kwargs):
        return False

try:
    from sqlalchemy import create_engine, text
except Exception:  # pragma: no cover - DB calls will fail clearly at runtime
    create_engine = None

    def text(sql):
        return sql

try:
    from py_vollib_vectorized import vectorized_implied_volatility
except Exception:  # pragma: no cover - optional in tiny test envs
    vectorized_implied_volatility = None

def _manual_load_env(path: Path, override: bool = True) -> None:
    if not path.exists():
        return
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", key):
            continue
        if not override and key in os.environ:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ[key] = value


def _load_env(path: Path | None = None, override: bool = True) -> None:
    loaded = False
    try:
        if path is not None:
            loaded = bool(load_dotenv(dotenv_path=path, override=override))
        else:
            loaded = bool(load_dotenv(override=override))
    except Exception:
        loaded = False
    if path is not None and not loaded:
        _manual_load_env(path, override=override)


_CURRENT_DIR = Path(__file__).resolve().parent
_PARENT_ENV = _CURRENT_DIR.parent / ".env"
_LOCAL_ENV = _CURRENT_DIR / ".env"
_load_env(_PARENT_ENV, override=True)
_load_env(_LOCAL_ENV, override=True)

DEFAULT_UNDERLYINGS = (
    "SPY",
    "QQQ",
    "DIA",
    "IWM",
    "AAPL",
    "ADBE",
    "AMD",
    "AMZN",
    "APP",
    "ARM",
    "ASML",
    "AVGO",
    "BA",
    "BABA",
    "BAC",
    "C",
    "CAT",
    "COIN",
    "CRM",
    "CRWD",
    "CVNA",
    "DELL",
    "DIS",
    "DRAM",
    "EEM",
    "F",
    "FXI",
    "GME",
    "GLD",
    "GOOGL",
    "HOOD",
    "HYG",
    "IBM",
    "INTC",
    "JPM",
    "KRE",
    "LLY",
    "MARA",
    "META",
    "MRVL",
    "MSFT",
    "MSTR",
    "MU",
    "NFLX",
    "NKE",
    "NVDA",
    "ORCL",
    "PANW",
    "PDD",
    "PFE",
    "PLTR",
    "PYPL",
    "QCOM",
    "RIVN",
    "RKLB",
    "SHOP",
    "SLV",
    "SMCI",
    "SMH",
    "SNOW",
    "SOFI",
    "SPCX",
    "TLT",
    "TSM",
    "TSLA",
    "UBER",
    "UNH",
    "USO",
    "VRT",
    "WFC",
    "WMT",
    "XBI",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLV",
    "XLY",
)
OPTION_TICKER_RE = re.compile(r"^O:([A-Z0-9]+?)(\d{6})([CP])(\d{8})$")
MASSIVE_BASE_URL = os.getenv("MASSIVE_API_BASE_URL", "https://api.massive.com").rstrip("/")
POLYGON_BASE_URL = os.getenv("POLYGON_API_BASE_URL", "https://api.polygon.io").rstrip("/")
SOURCE_NAME = "massive"

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME")


@dataclass
class ParsedOptionTicker:
    option_ticker: str
    root: str
    expiration_date: str
    call_put: str
    strike: float


@dataclass
class OptionContract:
    option_ticker: str
    underlying: str
    call_put: str
    strike: float
    expiration_date: str
    contract_root: str
    expiration_type: str
    settlement_type: str
    exercise_style: str = ""
    shares_per_contract: float | None = None
    source: str = SOURCE_NAME
    updated_at: str = ""


@dataclass
class OptionDaily:
    trade_date: str
    option_ticker: str
    underlying: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None
    vwap: float | None = None
    transactions: float | None = None
    open_interest: float | None = None
    source: str = SOURCE_NAME
    updated_at: str = ""


@dataclass
class OptionIV:
    trade_date: str
    option_ticker: str
    underlying: str
    provider_iv: float | None = None
    computed_iv: float | None = None
    iv_source: str = ""
    open_interest: float | None = None
    underlying_price: float | None = None
    source: str = SOURCE_NAME
    updated_at: str = ""


@dataclass
class OptionMarketMetricDaily:
    trade_date: str
    underlying: str
    atm_iv_pct: float | None = None
    iv_change_1d: float | None = None
    rv20_pct: float | None = None
    rv60_pct: float | None = None
    iv_rv20_spread: float | None = None
    iv_30d: float | None = None
    iv_60d: float | None = None
    term_slope_30_60: float | None = None
    term_state: str | None = None
    skew_expiration: str | None = None
    put_skew_5pct: float | None = None
    call_skew_5pct: float | None = None
    put_call_oi: float | None = None
    put_call_volume: float | None = None
    zero_dte_volume_share_pct: float | None = None
    top_oi_strike: float | None = None
    top_oi: float | None = None
    top5_oi_share_pct: float | None = None
    total_open_interest: float | None = None
    total_volume: float | None = None
    monthly_contract_count: int | None = None
    short_cycle_contract_count: int | None = None
    provider_iv_rows: int | None = None
    computed_iv_rows: int | None = None
    open_interest_rows: int | None = None
    source: str = "local_metrics"
    updated_at: str = ""


class MassiveAPIError(RuntimeError):
    pass


class MassiveRateLimitError(MassiveAPIError):
    pass


class MassiveFlatFileMissingError(MassiveAPIError):
    pass


def get_api_key() -> str:
    return (os.getenv("MASSIVE_API_KEY") or os.getenv("POLYGON_API_KEY") or "").strip()


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    if create_engine is None:
        raise RuntimeError("sqlalchemy is required for database access")
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


def table_names(use_test_tables: bool = False) -> dict[str, str]:
    suffix = "_test" if use_test_tables else ""
    return {
        "contracts": f"us_option_contracts{suffix}",
        "daily": f"us_option_daily{suffix}",
        "iv": f"us_option_iv_history{suffix}",
        "metrics": f"us_option_market_metrics_daily{suffix}",
    }


def _validate_table_name(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name or ""):
        raise ValueError(f"Invalid table name: {name!r}")
    return name


def ensure_us_option_tables(engine, use_test_tables: bool = False) -> None:
    names = table_names(use_test_tables)
    contracts = _validate_table_name(names["contracts"])
    daily = _validate_table_name(names["daily"])
    iv = _validate_table_name(names["iv"])
    metrics = _validate_table_name(names["metrics"])
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {contracts} (
                option_ticker VARCHAR(40) NOT NULL,
                underlying VARCHAR(20) NOT NULL,
                call_put VARCHAR(1) NOT NULL,
                strike DOUBLE NOT NULL,
                expiration_date VARCHAR(10) NOT NULL,
                contract_root VARCHAR(20) NOT NULL,
                expiration_type VARCHAR(20) NOT NULL,
                settlement_type VARCHAR(20) NOT NULL,
                exercise_style VARCHAR(20),
                shares_per_contract DOUBLE,
                source VARCHAR(30) NOT NULL,
                updated_at VARCHAR(32),
                PRIMARY KEY (option_ticker),
                INDEX idx_underlying_exp (underlying, expiration_date),
                INDEX idx_expiration_type (expiration_type)
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {daily} (
                trade_date VARCHAR(8) NOT NULL,
                option_ticker VARCHAR(40) NOT NULL,
                underlying VARCHAR(20) NOT NULL,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                vwap DOUBLE,
                transactions DOUBLE,
                open_interest DOUBLE,
                source VARCHAR(30) NOT NULL,
                updated_at VARCHAR(32),
                PRIMARY KEY (trade_date, option_ticker),
                INDEX idx_underlying_date (underlying, trade_date),
                INDEX idx_trade_date (trade_date)
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {iv} (
                trade_date VARCHAR(8) NOT NULL,
                option_ticker VARCHAR(40) NOT NULL,
                underlying VARCHAR(20) NOT NULL,
                provider_iv DOUBLE,
                computed_iv DOUBLE,
                iv_source VARCHAR(30),
                open_interest DOUBLE,
                underlying_price DOUBLE,
                source VARCHAR(30) NOT NULL,
                updated_at VARCHAR(32),
                PRIMARY KEY (trade_date, option_ticker),
                INDEX idx_underlying_date (underlying, trade_date)
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {metrics} (
                trade_date VARCHAR(8) NOT NULL,
                underlying VARCHAR(20) NOT NULL,
                atm_iv_pct DOUBLE,
                iv_change_1d DOUBLE,
                rv20_pct DOUBLE,
                rv60_pct DOUBLE,
                iv_rv20_spread DOUBLE,
                iv_30d DOUBLE,
                iv_60d DOUBLE,
                term_slope_30_60 DOUBLE,
                term_state VARCHAR(20),
                skew_expiration VARCHAR(10),
                put_skew_5pct DOUBLE,
                call_skew_5pct DOUBLE,
                put_call_oi DOUBLE,
                put_call_volume DOUBLE,
                zero_dte_volume_share_pct DOUBLE,
                top_oi_strike DOUBLE,
                top_oi DOUBLE,
                top5_oi_share_pct DOUBLE,
                total_open_interest DOUBLE,
                total_volume DOUBLE,
                monthly_contract_count INT,
                short_cycle_contract_count INT,
                provider_iv_rows INT,
                computed_iv_rows INT,
                open_interest_rows INT,
                source VARCHAR(30) NOT NULL,
                updated_at VARCHAR(32),
                PRIMARY KEY (trade_date, underlying),
                INDEX idx_underlying_date (underlying, trade_date),
                INDEX idx_trade_date (trade_date)
            )
        """))


def parse_option_ticker(option_ticker: str) -> ParsedOptionTicker | None:
    ticker = str(option_ticker or "").strip().upper()
    match = OPTION_TICKER_RE.match(ticker)
    if not match:
        return None
    root, yymmdd, call_put, strike_raw = match.groups()
    year = 2000 + int(yymmdd[:2])
    expiration = f"{year:04d}-{yymmdd[2:4]}-{yymmdd[4:6]}"
    strike = int(strike_raw) / 1000.0
    return ParsedOptionTicker(
        option_ticker=ticker,
        root=root,
        expiration_date=expiration,
        call_put=call_put,
        strike=strike,
    )


def compact_date(value: str | dt.date | dt.datetime | None) -> str:
    if value is None:
        return ""
    if isinstance(value, dt.datetime):
        return value.date().strftime("%Y%m%d")
    if isinstance(value, dt.date):
        return value.strftime("%Y%m%d")
    raw = str(value).strip()
    if not raw:
        return ""
    return raw.replace("-", "").replace("/", "")[:8]


def dashed_date(value: str | dt.date | dt.datetime | None) -> str:
    compact = compact_date(value)
    if len(compact) != 8:
        return ""
    return f"{compact[:4]}-{compact[4:6]}-{compact[6:8]}"


def parse_date(value: str | dt.date | dt.datetime) -> dt.date:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    dashed = dashed_date(value)
    if not dashed:
        raise ValueError(f"Invalid date: {value!r}")
    return dt.date.fromisoformat(dashed)


def is_third_friday(value: str | dt.date | dt.datetime) -> bool:
    date = parse_date(value)
    if date.weekday() != 4:
        return False
    return 15 <= date.day <= 21


def first_third_friday_between(start: dt.date, end: dt.date) -> dt.date | None:
    current = start
    while current <= end:
        if is_third_friday(current):
            return current
        current += dt.timedelta(days=1)
    return None


def classify_contract(
    option_ticker: str,
    underlying: str,
    expiration_date: str | dt.date,
) -> tuple[str, str, str]:
    parsed = parse_option_ticker(option_ticker)
    root = parsed.root if parsed else str(underlying or "").upper()
    underlying_norm = str(underlying or "").upper()
    exp_type = "unknown"
    settlement_type = "unknown"

    weekly_roots = {f"{underlying_norm}W", f"{underlying_norm}P"}
    if root in weekly_roots or root.endswith("W") or root.endswith("P"):
        exp_type = "short_cycle"
        settlement_type = "PM"
    elif underlying_norm in {"SPX", "NDX", "RUT"}:
        if root == underlying_norm and is_third_friday(expiration_date):
            exp_type = "monthly"
            settlement_type = "AM"
        else:
            exp_type = "short_cycle"
            settlement_type = "PM"
    elif underlying_norm == "VIX":
        if root == "VIX":
            exp_type = "monthly"
            settlement_type = "AM"
        elif root == "VIXW":
            exp_type = "short_cycle"
            settlement_type = "AM"
        else:
            exp_type = "unknown"
            settlement_type = "unknown"
    else:
        if is_third_friday(expiration_date):
            exp_type = "monthly"
        else:
            exp_type = "short_cycle"
        settlement_type = "physical"

    return exp_type, settlement_type, root


def dte_for_trade_date(expiration_date: str | dt.date, trade_date: str | dt.date) -> int:
    return (parse_date(expiration_date) - parse_date(trade_date)).days


def is_short_cycle_for_storage(contract: OptionContract, trade_date: str | dt.date) -> bool:
    if contract.expiration_type != "monthly":
        return True
    try:
        return dte_for_trade_date(contract.expiration_date, trade_date) <= 1
    except Exception:
        return True


def strike_within_band(strike: Any, underlying_price: Any, band_pct: float) -> bool:
    try:
        strike_val = float(strike)
        price_val = float(underlying_price)
    except Exception:
        return False
    if price_val <= 0 or strike_val <= 0:
        return False
    return abs(strike_val - price_val) / price_val <= float(band_pct) / 100.0


def should_keep_contract_for_storage(
    contract: OptionContract,
    trade_date: str | dt.date,
    underlying_price: float | None,
    short_strike_band_pct: float = 5.0,
) -> bool:
    if not is_short_cycle_for_storage(contract, trade_date):
        return True
    return strike_within_band(contract.strike, underlying_price, short_strike_band_pct)


def _clean_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _now_text() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def normalize_contract(raw: dict[str, Any], underlying: str) -> OptionContract | None:
    ticker = str(raw.get("ticker") or raw.get("option_ticker") or "").strip().upper()
    parsed = parse_option_ticker(ticker)
    if not ticker or not parsed:
        return None
    expiration_date = str(raw.get("expiration_date") or parsed.expiration_date)
    call_put = str(raw.get("contract_type") or parsed.call_put).strip().lower()
    call_put = "C" if call_put.startswith("c") else "P" if call_put.startswith("p") else parsed.call_put
    strike = _clean_float(raw.get("strike_price"))
    if strike is None:
        strike = parsed.strike
    exp_type, settlement, root = classify_contract(ticker, underlying, expiration_date)
    return OptionContract(
        option_ticker=ticker,
        underlying=str(raw.get("underlying_ticker") or underlying).upper(),
        call_put=call_put,
        strike=float(strike),
        expiration_date=dashed_date(expiration_date),
        contract_root=root,
        expiration_type=exp_type,
        settlement_type=settlement,
        exercise_style=str(raw.get("exercise_style") or "").lower(),
        shares_per_contract=_clean_float(raw.get("shares_per_contract")),
        updated_at=_now_text(),
    )


def contract_from_snapshot(raw: dict[str, Any], underlying: str) -> OptionContract | None:
    details = raw.get("details") if isinstance(raw.get("details"), dict) else {}
    return normalize_contract(details, underlying)


def daily_from_snapshot(raw: dict[str, Any], contract: OptionContract, trade_date: str) -> OptionDaily:
    day = raw.get("day") if isinstance(raw.get("day"), dict) else {}
    return OptionDaily(
        trade_date=compact_date(trade_date),
        option_ticker=contract.option_ticker,
        underlying=contract.underlying,
        open=_clean_float(day.get("open")),
        high=_clean_float(day.get("high")),
        low=_clean_float(day.get("low")),
        close=_clean_float(day.get("close")),
        volume=_clean_float(day.get("volume")),
        vwap=_clean_float(day.get("vwap")),
        transactions=_clean_float(day.get("transactions")),
        open_interest=_clean_float(raw.get("open_interest")),
        updated_at=_now_text(),
    )


def daily_from_aggregate(raw: dict[str, Any], contract: OptionContract, trade_date: str) -> OptionDaily:
    return OptionDaily(
        trade_date=compact_date(trade_date),
        option_ticker=contract.option_ticker,
        underlying=contract.underlying,
        open=_clean_float(raw.get("o")),
        high=_clean_float(raw.get("h")),
        low=_clean_float(raw.get("l")),
        close=_clean_float(raw.get("c")),
        volume=_clean_float(raw.get("v")),
        vwap=_clean_float(raw.get("vw")),
        transactions=_clean_float(raw.get("n")),
        updated_at=_now_text(),
    )


def iv_from_snapshot(
    raw: dict[str, Any],
    contract: OptionContract,
    trade_date: str,
    underlying_price: float | None,
) -> OptionIV:
    provider_iv = normalize_iv_value(raw.get("implied_volatility"))
    return OptionIV(
        trade_date=compact_date(trade_date),
        option_ticker=contract.option_ticker,
        underlying=contract.underlying,
        provider_iv=provider_iv,
        computed_iv=None,
        iv_source="provider_snapshot" if provider_iv is not None else "",
        open_interest=_clean_float(raw.get("open_interest")),
        underlying_price=_clean_float(underlying_price),
        updated_at=_now_text(),
    )


def normalize_iv_value(value: Any) -> float | None:
    val = _clean_float(value)
    if val is None or val <= 0:
        return None
    if val > 3:
        return val / 100.0
    return val


class MassiveOptionsClient:
    def __init__(
        self,
        api_key: str | None = None,
        base_url: str = MASSIVE_BASE_URL,
        timeout: int = 30,
        max_retries: int = 3,
        sleep_seconds: float = 0.25,
    ):
        self.api_key = (api_key or get_api_key()).strip()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep_seconds = sleep_seconds
        self.session = requests.Session() if requests is not None and hasattr(requests, "Session") else None

    def require_key(self) -> None:
        if not self.api_key:
            raise MassiveAPIError("Missing MASSIVE_API_KEY or POLYGON_API_KEY")

    def _request_json_node(self, path_or_url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        self.require_key()
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        req_params = dict(params or {})
        req_params.setdefault("apiKey", self.api_key)
        separator = "&" if "?" in url else "?"
        full_url = f"{url}{separator}{urlencode(req_params)}" if req_params else url
        node_bin = os.getenv(
            "MASSIVE_NODE_BIN",
            r"C:\Users\alvachen\.cache\codex-runtimes\codex-primary-runtime\dependencies\node\bin\node.exe",
        )
        script = (
            "const url = process.env.MASSIVE_REQUEST_URL;"
            "fetch(url).then(async r => {"
            " const text = await r.text();"
            " if (!r.ok) { console.error(text.slice(0, 1000)); process.exit(r.status || 1); }"
            " console.log(text);"
            "}).catch(e => { console.error(e && e.stack || e); process.exit(2); });"
        )
        env = os.environ.copy()
        env["MASSIVE_REQUEST_URL"] = full_url
        if str(os.getenv("MASSIVE_NODE_INSECURE_TLS", "")).strip().lower() in {"1", "true", "yes", "on"}:
            env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0"
        else:
            env["NODE_OPTIONS"] = " ".join(
                part for part in [env.get("NODE_OPTIONS", ""), "--use-system-ca"] if part
            ).strip()
        try:
            proc = subprocess.run(
                [node_bin, "-e", script],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                env=env,
                check=False,
            )
        except Exception as exc:
            raise MassiveAPIError(f"Node transport failed: {exc}") from exc
        if proc.returncode != 0:
            err = (proc.stderr or proc.stdout or "").strip()
            raise MassiveAPIError(f"Node transport HTTP/error rc={proc.returncode}: {err[:500]}")
        try:
            return json.loads(proc.stdout)
        except Exception as exc:
            raise MassiveAPIError(f"Node transport returned invalid JSON: {proc.stdout[:500]}") from exc

    def request_json(self, path_or_url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if str(os.getenv("MASSIVE_HTTP_TRANSPORT", "")).strip().lower() == "node":
            return self._request_json_node(path_or_url, params)
        self.require_key()
        url = path_or_url if path_or_url.startswith("http") else f"{self.base_url}{path_or_url}"
        req_params = dict(params or {})
        req_params.setdefault("apiKey", self.api_key)
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                if self.session is None:
                    raise MassiveAPIError("requests is required for Massive API calls")
                resp = self.session.get(url, params=req_params, timeout=self.timeout)
                if resp.status_code == 429:
                    raise MassiveRateLimitError(resp.text[:300])
                if resp.status_code >= 500:
                    raise MassiveAPIError(f"HTTP {resp.status_code}: {resp.text[:300]}")
                if resp.status_code >= 400:
                    raise MassiveAPIError(f"HTTP {resp.status_code}: {resp.text[:300]}")
                return resp.json()
            except MassiveRateLimitError as exc:
                last_error = exc
                time.sleep(max(1.0, self.sleep_seconds) * (attempt + 1))
            except ((requests.RequestException if requests is not None and hasattr(requests, "RequestException") else Exception), MassiveAPIError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    break
                time.sleep(self.sleep_seconds * (attempt + 1))
        raise MassiveAPIError(str(last_error or "Massive request failed"))

    def iter_paginated(self, path: str, params: dict[str, Any] | None = None) -> Iterator[dict[str, Any]]:
        url: str | None = path
        req_params = dict(params or {})
        while url:
            data = self.request_json(url, req_params)
            for item in data.get("results") or []:
                if isinstance(item, dict):
                    yield item
            next_url = data.get("next_url")
            url = str(next_url) if next_url else None
            req_params = {}

    def list_contracts(
        self,
        underlying: str,
        as_of: str | None = None,
        expired: bool = True,
        limit: int = 1000,
    ) -> list[OptionContract]:
        params = {
            "underlying_ticker": underlying.upper(),
            "expired": str(bool(expired)).lower(),
            "limit": limit,
            "sort": "expiration_date",
            "order": "asc",
        }
        if as_of:
            params["as_of"] = dashed_date(as_of)
        contracts = []
        for raw in self.iter_paginated("/v3/reference/options/contracts", params):
            contract = normalize_contract(raw, underlying)
            if contract:
                contracts.append(contract)
        return contracts

    def contracts_page(
        self,
        underlying: str,
        as_of: str | None = None,
        expiration_gte: str | None = None,
        expiration_lte: str | None = None,
        strike_gte: float | None = None,
        strike_lte: float | None = None,
        limit: int = 25,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {
            "underlying_ticker": underlying.upper(),
            "expired": "true",
            "limit": limit,
            "sort": "expiration_date",
            "order": "asc",
        }
        if as_of:
            params["as_of"] = dashed_date(as_of)
        if expiration_gte:
            params["expiration_date.gte"] = dashed_date(expiration_gte)
        if expiration_lte:
            params["expiration_date.lte"] = dashed_date(expiration_lte)
        if strike_gte is not None:
            params["strike_price.gte"] = float(strike_gte)
        if strike_lte is not None:
            params["strike_price.lte"] = float(strike_lte)
        data = self.request_json("/v3/reference/options/contracts", params)
        return [item for item in (data.get("results") or []) if isinstance(item, dict)]

    def option_chain_snapshot(self, underlying: str, limit: int = 250) -> list[dict[str, Any]]:
        params = {"limit": limit}
        return list(self.iter_paginated(f"/v3/snapshot/options/{underlying.upper()}", params))

    def aggregate_daily(self, option_ticker: str, start: str, end: str) -> list[dict[str, Any]]:
        path = f"/v2/aggs/ticker/{option_ticker}/range/1/day/{dashed_date(start)}/{dashed_date(end)}"
        data = self.request_json(path, {"adjusted": "true", "sort": "asc", "limit": 5000})
        return data.get("results") or []

    def underlying_daily_close(self, underlying: str, trade_date: str) -> float | None:
        path = f"/v2/aggs/ticker/{underlying.upper()}/range/1/day/{dashed_date(trade_date)}/{dashed_date(trade_date)}"
        data = self.request_json(path, {"adjusted": "true", "sort": "asc", "limit": 1})
        results = data.get("results") or []
        if not results:
            return None
        return _clean_float(results[0].get("c"))


def _engine_is_mysql(engine) -> bool:
    try:
        return engine.dialect.name.lower().startswith("mysql")
    except Exception:
        return False


def _records_from_dataclasses(items: Iterable[Any]) -> list[dict[str, Any]]:
    records = []
    for item in items:
        records.append(asdict(item))
    return records


def upsert_records(engine, table: str, records: Sequence[dict[str, Any]], pk_cols: Sequence[str]) -> int:
    if not records:
        return 0
    table = _validate_table_name(table)
    cols = sorted({key for rec in records for key in rec.keys()})
    if not cols:
        return 0
    if _engine_is_mysql(engine):
        col_sql = ", ".join(cols)
        val_sql = ", ".join(f":{col}" for col in cols)
        update_cols = [col for col in cols if col not in set(pk_cols)]
        update_sql = ", ".join(f"{col}=VALUES({col})" for col in update_cols)
        sql = f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql})"
        if update_sql:
            sql += f" ON DUPLICATE KEY UPDATE {update_sql}"
        with engine.begin() as conn:
            conn.execute(text(sql), records)
        return len(records)

    # Generic fallback for tests/dev SQLite: delete then insert by primary key.
    with engine.begin() as conn:
        for rec in records:
            where = " AND ".join(f"{col}=:{col}" for col in pk_cols)
            conn.execute(text(f"DELETE FROM {table} WHERE {where}"), {col: rec.get(col) for col in pk_cols})
        col_sql = ", ".join(cols)
        val_sql = ", ".join(f":{col}" for col in cols)
        conn.execute(text(f"INSERT INTO {table} ({col_sql}) VALUES ({val_sql})"), records)
    return len(records)


def save_contracts(engine, contracts: Sequence[OptionContract], use_test_tables: bool = False) -> int:
    return upsert_records(
        engine,
        table_names(use_test_tables)["contracts"],
        _records_from_dataclasses(contracts),
        ("option_ticker",),
    )


def save_daily(engine, rows: Sequence[OptionDaily], use_test_tables: bool = False) -> int:
    return upsert_records(
        engine,
        table_names(use_test_tables)["daily"],
        _records_from_dataclasses(rows),
        ("trade_date", "option_ticker"),
    )


def save_iv(engine, rows: Sequence[OptionIV], use_test_tables: bool = False) -> int:
    return upsert_records(
        engine,
        table_names(use_test_tables)["iv"],
        _records_from_dataclasses(rows),
        ("trade_date", "option_ticker"),
    )


def save_market_metrics(
    engine,
    rows: Sequence[OptionMarketMetricDaily],
    use_test_tables: bool = False,
) -> int:
    return upsert_records(
        engine,
        table_names(use_test_tables)["metrics"],
        _records_from_dataclasses(rows),
        ("trade_date", "underlying"),
    )


def fetch_underlying_close_from_db(engine, underlying: str, trade_date: str) -> float | None:
    if engine is None:
        return None
    if pd is None:
        return None
    underlying_norm = underlying.upper()
    trade_date_compact = compact_date(trade_date)
    trade_date_dashed = dashed_date(trade_date)
    index_candidates = [underlying.upper(), f"{underlying.upper()}.US", f"I:{underlying.upper()}"]
    try:
        stock_sql = text("""
            SELECT close_price FROM stock_price
            WHERE ts_code = :code AND trade_date = :trade_date
            ORDER BY trade_date DESC LIMIT 1
        """)
        df = pd.read_sql(stock_sql, engine, params={"code": underlying_norm, "trade_date": trade_date_compact})
        if not df.empty:
            return _clean_float(df.iloc[0]["close_price"])
    except Exception:
        pass
    try:
        stock_prices_sql = text("""
            SELECT close FROM stock_prices
            WHERE UPPER(symbol) = :symbol
              AND date <= :trade_date
            ORDER BY date DESC LIMIT 1
        """)
        df = pd.read_sql(stock_prices_sql, engine, params={"symbol": underlying_norm, "trade_date": trade_date_dashed})
        if not df.empty:
            return _clean_float(df.iloc[0]["close"])
    except Exception:
        pass
    try:
        placeholders = ", ".join(f":code_{idx}" for idx, _ in enumerate(index_candidates))
        params = {f"code_{idx}": code for idx, code in enumerate(index_candidates)}
        params["trade_date"] = trade_date_compact
        index_sql = text("""
            SELECT close_price FROM index_price
            WHERE ts_code IN ({placeholders}) AND trade_date = :trade_date
            ORDER BY trade_date DESC LIMIT 1
        """.format(placeholders=placeholders))
        df = pd.read_sql(index_sql, engine, params=params)
        if not df.empty:
            return _clean_float(df.iloc[0]["close_price"])
    except Exception:
        pass
    return None


def fetch_underlying_close_yfinance(underlying: str, trade_date: str) -> float | None:
    try:
        import yfinance as yf
    except Exception:
        return None
    yf_symbols = {"SPX": "^GSPC", "NDX": "^NDX", "RUT": "^RUT", "VIX": "^VIX"}
    symbol = yf_symbols.get(underlying.upper(), underlying.upper())
    start = parse_date(trade_date)
    end = start + dt.timedelta(days=1)
    try:
        df = yf.download(symbol, start=start.isoformat(), end=end.isoformat(), progress=False, auto_adjust=False)
    except Exception:
        return None
    if df is None or df.empty:
        return None
    close = df["Close"].iloc[0]
    if hasattr(close, "iloc"):
        close = close.iloc[0]
    return _clean_float(close)


def get_underlying_close(engine, underlying: str, trade_date: str, fallback_yfinance: bool = True) -> float | None:
    price = fetch_underlying_close_from_db(engine, underlying, trade_date)
    if price is not None:
        return price
    if fallback_yfinance:
        return fetch_underlying_close_yfinance(underlying, trade_date)
    return None


def compute_implied_vol(
    option_price: float | None,
    underlying_price: float | None,
    strike: float | None,
    expiration_date: str,
    trade_date: str,
    call_put: str,
    risk_free_rate: float = 0.045,
) -> float | None:
    if vectorized_implied_volatility is None:
        return None
    option_price = _clean_float(option_price)
    underlying_price = _clean_float(underlying_price)
    strike = _clean_float(strike)
    if option_price is None or underlying_price is None or strike is None:
        return None
    if option_price <= 0 or underlying_price <= 0 or strike <= 0:
        return None
    dte = dte_for_trade_date(expiration_date, trade_date)
    if dte <= 0:
        return None
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            ivs = vectorized_implied_volatility(
                [option_price],
                [underlying_price],
                [strike],
                [dte / 365.0],
                risk_free_rate,
                [str(call_put).lower()],
                return_as="numpy",
            )
        return normalize_iv_value(float(ivs[0]))
    except Exception:
        return None


def snapshot_rows_for_underlying(
    client: MassiveOptionsClient,
    underlying: str,
    trade_date: str,
    short_strike_band_pct: float = 5.0,
    underlying_price_hint: float | None = None,
) -> tuple[list[OptionContract], list[OptionDaily], list[OptionIV], float | None]:
    raw_rows = client.option_chain_snapshot(underlying)
    underlying_price = _clean_float(underlying_price_hint)
    contracts: list[OptionContract] = []
    daily_rows: list[OptionDaily] = []
    iv_rows: list[OptionIV] = []
    for raw in raw_rows:
        contract = contract_from_snapshot(raw, underlying)
        if not contract:
            continue
        if underlying_price is None:
            underlying_asset = raw.get("underlying_asset") if isinstance(raw.get("underlying_asset"), dict) else {}
            underlying_price = _clean_float(underlying_asset.get("price"))
        if not should_keep_contract_for_storage(contract, trade_date, underlying_price, short_strike_band_pct):
            continue
        contracts.append(contract)
        daily_rows.append(daily_from_snapshot(raw, contract, trade_date))
        iv_rows.append(iv_from_snapshot(raw, contract, trade_date, underlying_price))
    return contracts, daily_rows, iv_rows, underlying_price


def _option_ticker_underlying_root(option_ticker: str, target_underlyings: set[str]) -> str | None:
    parsed = parse_option_ticker(option_ticker)
    if not parsed:
        return None
    roots = [parsed.root]
    if parsed.root.endswith(("W", "P")):
        roots.append(parsed.root[:-1])
    for underlying in target_underlyings:
        if underlying in roots:
            return underlying
    return None


def _iter_flatfile_rows_s3(
    trade_date: str,
    access_key: str,
    secret_key: str,
    endpoint_url: str = "https://files.massive.com",
) -> Iterator[dict[str, Any]]:
    try:
        import boto3
    except Exception as exc:  # pragma: no cover - optional integration path
        raise MassiveAPIError("boto3 is required for Massive flat files") from exc
    date = parse_date(trade_date)
    key = f"us_options_opra/day_aggs_v1/{date:%Y/%m}/{date:%Y-%m-%d}.csv.gz"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )
    try:
        obj = client.get_object(Bucket="flatfiles", Key=key)
    except Exception as exc:
        response = getattr(exc, "response", {}) or {}
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        code = str(error.get("Code") or "")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode") if isinstance(response, dict) else None
        if code in {"NoSuchKey", "404", "NotFound"} or status == 404:
            raise MassiveFlatFileMissingError(f"Missing flat file for {trade_date}: {key}") from exc
        raise
    with gzip.GzipFile(fileobj=obj["Body"]) as gz:
        text_stream = io.TextIOWrapper(gz, encoding="utf-8")
        yield from csv.DictReader(text_stream)


def flatfile_credentials_available() -> bool:
    if importlib.util.find_spec("boto3") is None:
        return False
    return bool(
        (os.getenv("MASSIVE_FLATFILES_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID"))
        and (os.getenv("MASSIVE_FLATFILES_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY"))
    )


def backfill_daily_from_flatfile(
    engine,
    trade_date: str,
    underlyings: Sequence[str] = DEFAULT_UNDERLYINGS,
    short_strike_band_pct: float = 5.0,
    use_test_tables: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    access_key = os.getenv("MASSIVE_FLATFILES_ACCESS_KEY") or os.getenv("AWS_ACCESS_KEY_ID") or ""
    secret_key = os.getenv("MASSIVE_FLATFILES_SECRET_KEY") or os.getenv("AWS_SECRET_ACCESS_KEY") or ""
    if not access_key or not secret_key:
        raise MassiveAPIError("Missing Massive flat-file S3 credentials")
    targets = {u.upper() for u in underlyings}
    contract_cache: dict[str, OptionContract] = {}
    daily_rows: list[OptionDaily] = []
    iv_rows: list[OptionIV] = []
    prices = {u: get_underlying_close(engine, u, trade_date, fallback_yfinance=False) for u in targets}
    for raw in _iter_flatfile_rows_s3(trade_date, access_key, secret_key):
        ticker = str(raw.get("ticker") or "").strip().upper()
        underlying = _option_ticker_underlying_root(ticker, targets)
        if not underlying:
            continue
        contract = contract_cache.get(ticker)
        if contract is None:
            parsed = parse_option_ticker(ticker)
            if not parsed:
                continue
            exp_type, settlement, root = classify_contract(ticker, underlying, parsed.expiration_date)
            contract = OptionContract(
                option_ticker=ticker,
                underlying=underlying,
                call_put=parsed.call_put,
                strike=parsed.strike,
                expiration_date=parsed.expiration_date,
                contract_root=root,
                expiration_type=exp_type,
                settlement_type=settlement,
                updated_at=_now_text(),
            )
            contract_cache[ticker] = contract
        price = prices.get(underlying)
        if not should_keep_contract_for_storage(contract, trade_date, price, short_strike_band_pct):
            continue
        close = _clean_float(raw.get("close"))
        daily_rows.append(OptionDaily(
            trade_date=compact_date(trade_date),
            option_ticker=ticker,
            underlying=underlying,
            open=_clean_float(raw.get("open")),
            high=_clean_float(raw.get("high")),
            low=_clean_float(raw.get("low")),
            close=close,
            volume=_clean_float(raw.get("volume")),
            vwap=_clean_float(raw.get("vwap")),
            transactions=_clean_float(raw.get("transactions")),
            updated_at=_now_text(),
        ))
        computed_iv = compute_implied_vol(close, price, contract.strike, contract.expiration_date, trade_date, contract.call_put)
        if computed_iv is not None:
            iv_rows.append(OptionIV(
                trade_date=compact_date(trade_date),
                option_ticker=ticker,
                underlying=underlying,
                computed_iv=computed_iv,
                iv_source="computed_close",
                underlying_price=price,
                updated_at=_now_text(),
            ))
    contracts = list(contract_cache.values())
    if not dry_run:
        ensure_us_option_tables(engine, use_test_tables)
        save_contracts(engine, contracts, use_test_tables)
        save_daily(engine, daily_rows, use_test_tables)
        save_iv(engine, iv_rows, use_test_tables)
    return {"contracts": len(contracts), "daily": len(daily_rows), "iv": len(iv_rows), "source": "flatfile"}


def backfill_daily_from_rest(
    engine,
    client: MassiveOptionsClient,
    trade_date: str,
    underlyings: Sequence[str] = DEFAULT_UNDERLYINGS,
    short_strike_band_pct: float = 5.0,
    use_test_tables: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    all_contracts: list[OptionContract] = []
    all_daily: list[OptionDaily] = []
    all_iv: list[OptionIV] = []
    per_underlying: dict[str, dict[str, Any]] = {}

    for underlying in underlyings:
        underlying_norm = underlying.upper()
        underlying_price = get_underlying_close(engine, underlying_norm, trade_date) if engine is not None else None
        contracts = client.list_contracts(underlying_norm, as_of=trade_date, expired=True)
        kept_contracts = [
            contract for contract in contracts
            if should_keep_contract_for_storage(contract, trade_date, underlying_price, short_strike_band_pct)
        ]
        daily_rows: list[OptionDaily] = []
        iv_rows: list[OptionIV] = []
        for contract in kept_contracts:
            aggs = client.aggregate_daily(contract.option_ticker, trade_date, trade_date)
            if not aggs:
                continue
            daily = daily_from_aggregate(aggs[0], contract, trade_date)
            daily_rows.append(daily)
            computed_iv = compute_implied_vol(
                daily.close,
                underlying_price,
                contract.strike,
                contract.expiration_date,
                trade_date,
                contract.call_put,
            )
            if computed_iv is not None:
                iv_rows.append(OptionIV(
                    trade_date=compact_date(trade_date),
                    option_ticker=contract.option_ticker,
                    underlying=contract.underlying,
                    computed_iv=computed_iv,
                    iv_source="computed_close",
                    underlying_price=underlying_price,
                    updated_at=_now_text(),
                ))
        all_contracts.extend(kept_contracts)
        all_daily.extend(daily_rows)
        all_iv.extend(iv_rows)
        per_underlying[underlying_norm] = {
            "underlying_price": underlying_price,
            "contracts_seen": len(contracts),
            "contracts_kept": len(kept_contracts),
            "daily": len(daily_rows),
            "iv": len(iv_rows),
        }

    if not dry_run:
        ensure_us_option_tables(engine, use_test_tables)
        save_contracts(engine, all_contracts, use_test_tables)
        save_daily(engine, all_daily, use_test_tables)
        save_iv(engine, all_iv, use_test_tables)
    return {
        "contracts": len(all_contracts),
        "daily": len(all_daily),
        "iv": len(all_iv),
        "per_underlying": per_underlying,
        "source": "rest_aggregates",
        "dry_run": bool(dry_run),
        "use_test_tables": bool(use_test_tables),
    }


def basic_probe_contract_candidates(
    contracts: Sequence[OptionContract],
    trade_date: str,
    underlying_price: float | None = None,
) -> list[OptionContract]:
    valid = []
    for contract in contracts:
        try:
            dte = dte_for_trade_date(contract.expiration_date, trade_date)
        except Exception:
            continue
        if dte >= 0:
            valid.append((contract, dte))
    if not valid:
        return []
    monthly = [item for item in valid if item[0].expiration_type == "monthly" and 20 <= item[1] <= 90]
    if monthly:
        if underlying_price and underlying_price > 0:
            monthly.sort(key=lambda item: (abs(item[0].strike - underlying_price), abs(item[1] - 45), item[0].call_put))
        else:
            monthly.sort(key=lambda item: (abs(item[1] - 45), item[0].strike, item[0].call_put))
        return [item[0] for item in monthly]
    if underlying_price and underlying_price > 0:
        valid.sort(key=lambda item: (abs(item[0].strike - underlying_price), item[1], item[0].call_put))
    else:
        valid.sort(key=lambda item: (item[1], item[0].strike, item[0].call_put))
    return [item[0] for item in valid]


def select_basic_probe_contract(
    contracts: Sequence[OptionContract],
    trade_date: str,
    underlying_price: float | None = None,
) -> OptionContract | None:
    candidates = basic_probe_contract_candidates(contracts, trade_date, underlying_price=underlying_price)
    return candidates[0] if candidates else None


def basic_probe(
    engine,
    client: MassiveOptionsClient,
    underlying: str = "SPY",
    trade_date: str | None = None,
    use_test_tables: bool = False,
    dry_run: bool = True,
) -> dict[str, Any]:
    trade_date = compact_date(trade_date) or default_trade_date()
    start = parse_date(trade_date)
    expiration_start = start + dt.timedelta(days=20)
    expiration_end = start + dt.timedelta(days=90)
    target_monthly_exp = first_third_friday_between(expiration_start, expiration_end)
    contract_exp_start = target_monthly_exp or expiration_start
    contract_exp_end = target_monthly_exp or expiration_end
    underlying_price = client.underlying_daily_close(underlying.upper(), trade_date)
    strike_gte = underlying_price * 0.95 if underlying_price else None
    strike_lte = underlying_price * 1.05 if underlying_price else None
    raw_contracts = client.contracts_page(
        underlying.upper(),
        expiration_gte=contract_exp_start.strftime("%Y%m%d"),
        expiration_lte=contract_exp_end.strftime("%Y%m%d"),
        strike_gte=strike_gte,
        strike_lte=strike_lte,
        limit=250,
    )
    contracts = [contract for raw in raw_contracts if (contract := normalize_contract(raw, underlying.upper()))]
    candidates = basic_probe_contract_candidates(contracts, trade_date, underlying_price=underlying_price)
    if not candidates:
        return {
            "status": "no_contract",
            "underlying": underlying.upper(),
            "trade_date": trade_date,
            "contracts_seen": len(raw_contracts),
            "underlying_price": underlying_price,
            "target_expiration": contract_exp_start.strftime("%Y-%m-%d"),
            "dry_run": bool(dry_run),
            "use_test_tables": bool(use_test_tables),
        }
    selected = candidates[0]
    aggs = []
    attempted: list[str] = []
    for candidate in candidates[:3]:
        attempted.append(candidate.option_ticker)
        aggs = client.aggregate_daily(candidate.option_ticker, trade_date, trade_date)
        if aggs:
            selected = candidate
            break
    daily_rows = [daily_from_aggregate(aggs[0], selected, trade_date)] if aggs else []
    if not dry_run:
        ensure_us_option_tables(engine, use_test_tables)
        save_contracts(engine, [selected], use_test_tables)
        save_daily(engine, daily_rows, use_test_tables)
    return {
        "status": "ok" if daily_rows else "no_daily_bar",
        "mode": "basic-probe",
        "underlying": underlying.upper(),
        "trade_date": trade_date,
        "underlying_price": underlying_price,
        "target_expiration": contract_exp_start.strftime("%Y-%m-%d"),
        "contracts_seen": len(raw_contracts),
        "contracts_attempted": attempted,
        "contract": asdict(selected),
        "daily_rows": len(daily_rows),
        "daily_sample": asdict(daily_rows[0]) if daily_rows else None,
        "dry_run": bool(dry_run),
        "use_test_tables": bool(use_test_tables),
        "note": "Basic probe uses reference contracts + one daily aggregate only; it does not test snapshot, provider IV, or open interest.",
    }


def live_update(
    engine,
    client: MassiveOptionsClient,
    underlyings: Sequence[str],
    trade_date: str,
    short_strike_band_pct: float = 5.0,
    use_test_tables: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    all_contracts: list[OptionContract] = []
    all_daily: list[OptionDaily] = []
    all_iv: list[OptionIV] = []
    per_underlying: dict[str, dict[str, Any]] = {}
    for underlying in underlyings:
        underlying_price_hint = get_underlying_close(engine, underlying.upper(), trade_date) if engine is not None else None
        contracts, daily_rows, iv_rows, price = snapshot_rows_for_underlying(
            client, underlying.upper(), trade_date, short_strike_band_pct, underlying_price_hint=underlying_price_hint
        )
        all_contracts.extend(contracts)
        all_daily.extend(daily_rows)
        all_iv.extend(iv_rows)
        open_interest_rows = sum(1 for row in daily_rows if row.open_interest is not None)
        provider_iv_rows = sum(1 for row in iv_rows if row.provider_iv is not None)
        computed_iv_rows = sum(1 for row in iv_rows if row.computed_iv is not None)
        per_underlying[underlying.upper()] = {
            "underlying_price": price,
            "contracts": len(contracts),
            "daily": len(daily_rows),
            "iv": len(iv_rows),
            "open_interest_rows": open_interest_rows,
            "provider_iv_rows": provider_iv_rows,
            "computed_iv_rows": computed_iv_rows,
        }
    if not dry_run:
        ensure_us_option_tables(engine, use_test_tables)
        save_contracts(engine, all_contracts, use_test_tables)
        save_daily(engine, all_daily, use_test_tables)
        save_iv(engine, all_iv, use_test_tables)
    metrics_count = 0
    metric_underlyings: set[str] = set()
    if not dry_run:
        metric_rows = compute_market_metrics_for_date(
            engine,
            underlyings,
            trade_date,
            use_test_tables=use_test_tables,
        )
        if metric_rows:
            metrics_count = save_market_metrics(engine, metric_rows, use_test_tables)
            metric_underlyings = {str(row.underlying).upper() for row in metric_rows}
    for underlying in per_underlying:
        per_underlying[underlying]["metrics"] = 1 if underlying in metric_underlyings else 0
    return {
        "contracts": len(all_contracts),
        "daily": len(all_daily),
        "iv": len(all_iv),
        "metrics": metrics_count,
        "open_interest_rows": sum(1 for row in all_daily if row.open_interest is not None),
        "provider_iv_rows": sum(1 for row in all_iv if row.provider_iv is not None),
        "computed_iv_rows": sum(1 for row in all_iv if row.computed_iv is not None),
        "per_underlying": per_underlying,
        "source": "snapshot",
        "dry_run": bool(dry_run),
        "use_test_tables": bool(use_test_tables),
    }


def backfill_range(
    engine,
    client: MassiveOptionsClient,
    start: str,
    end: str,
    underlyings: Sequence[str] = DEFAULT_UNDERLYINGS,
    short_strike_band_pct: float = 5.0,
    use_test_tables: bool = False,
    dry_run: bool = False,
    flatfile_only: bool = False,
    progress: bool = False,
) -> dict[str, Any]:
    start_date = parse_date(start)
    end_date = parse_date(end)
    current = start_date
    totals = {
        "contracts": 0,
        "daily": 0,
        "iv": 0,
        "metrics": 0,
        "days": 0,
        "flatfile_days": 0,
        "rest_days": 0,
        "skipped_days": 0,
    }
    while current <= end_date:
        if current.weekday() >= 5:
            current += dt.timedelta(days=1)
            continue
        trade_date = current.strftime("%Y%m%d")
        result: dict[str, Any]
        if flatfile_credentials_available():
            try:
                result = backfill_daily_from_flatfile(
                    engine,
                    trade_date,
                    underlyings=underlyings,
                    short_strike_band_pct=short_strike_band_pct,
                    use_test_tables=use_test_tables,
                    dry_run=dry_run,
                )
                totals["flatfile_days"] += 1
            except MassiveFlatFileMissingError as exc:
                result = {"contracts": 0, "daily": 0, "iv": 0, "source": "missing_flatfile", "error": str(exc)}
                totals["skipped_days"] += 1
            except MassiveAPIError:
                if flatfile_only:
                    raise
                result = backfill_daily_from_rest(
                    engine,
                    client,
                    trade_date,
                    underlyings=underlyings,
                    short_strike_band_pct=short_strike_band_pct,
                    use_test_tables=use_test_tables,
                    dry_run=dry_run,
                )
                totals["rest_days"] += 1
        else:
            if flatfile_only:
                raise MassiveAPIError("Flat-file credentials are unavailable; cannot run --flatfile-only")
            result = backfill_daily_from_rest(
                engine,
                client,
                underlyings=underlyings,
                trade_date=trade_date,
                short_strike_band_pct=short_strike_band_pct,
                use_test_tables=use_test_tables,
                dry_run=dry_run,
            )
            totals["rest_days"] += 1
        for key in ("contracts", "daily", "iv"):
            totals[key] += int(result.get(key) or 0)
        metrics_count = 0
        if not dry_run and int(result.get("daily") or 0) > 0:
            metrics_count = update_market_metrics_for_date(
                engine,
                underlyings,
                trade_date,
                use_test_tables=use_test_tables,
                dry_run=False,
            )
            totals["metrics"] += int(metrics_count or 0)
        totals["days"] += 1
        if progress:
            print(
                f"[backfill] {trade_date} source={result.get('source')} "
                f"contracts={result.get('contracts', 0)} daily={result.get('daily', 0)} "
                f"iv={result.get('iv', 0)} metrics={metrics_count}",
                file=sys.stderr,
                flush=True,
            )
        current += dt.timedelta(days=1)
    return totals


def get_us_option_chain_daily(
    underlying: str,
    trade_date: str,
    include_short_cycle: bool = True,
    use_test_tables: bool = False,
    engine=None,
) -> Any:
    engine = engine or get_db_engine()
    if engine is None:
        return pd.DataFrame() if pd is not None else []
    if pd is None:
        return []
    names = table_names(use_test_tables)
    contracts = _validate_table_name(names["contracts"])
    daily = _validate_table_name(names["daily"])
    iv = _validate_table_name(names["iv"])
    where_cycle = "" if include_short_cycle else "AND c.expiration_type = 'monthly'"
    sql = text(f"""
        SELECT d.trade_date, d.option_ticker, d.underlying, c.call_put, c.strike,
               c.expiration_date, c.expiration_type, c.settlement_type,
               d.open, d.high, d.low, d.close, d.volume, d.vwap, d.transactions,
               d.open_interest, h.provider_iv, h.computed_iv, h.iv_source,
               h.underlying_price
        FROM {daily} d
        JOIN {contracts} c ON d.option_ticker = c.option_ticker
        LEFT JOIN {iv} h ON d.trade_date = h.trade_date AND d.option_ticker = h.option_ticker
        WHERE d.underlying = :underlying
          AND d.trade_date = :trade_date
          {where_cycle}
        ORDER BY c.expiration_date ASC, c.strike ASC, c.call_put ASC
    """)
    try:
        return pd.read_sql(sql, engine, params={"underlying": underlying.upper(), "trade_date": compact_date(trade_date)})
    except Exception:
        return pd.DataFrame()


def get_us_underlying_iv_rank(
    underlying: str,
    window: int = 252,
    use_test_tables: bool = False,
    engine=None,
) -> dict[str, Any] | None:
    engine = engine or get_db_engine()
    if engine is None or pd is None:
        return None
    names = table_names(use_test_tables)
    contracts = _validate_table_name(names["contracts"])
    iv = _validate_table_name(names["iv"])
    limit = max(int(window) * 300, 1000)
    sql = text(f"""
        SELECT h.trade_date, h.option_ticker, h.underlying,
               h.provider_iv, h.computed_iv, h.open_interest, h.underlying_price,
               c.strike, c.call_put, c.expiration_date, c.expiration_type
        FROM {iv} h
        JOIN {contracts} c ON h.option_ticker = c.option_ticker
        WHERE h.underlying = :underlying
          AND c.expiration_type = 'monthly'
        ORDER BY h.trade_date DESC
        LIMIT {limit}
    """)
    try:
        df = pd.read_sql(sql, engine, params={"underlying": underlying.upper()})
    except Exception:
        return None
    if df.empty:
        return None
    df["iv"] = df.apply(lambda row: normalize_iv_value(row.get("provider_iv")) or normalize_iv_value(row.get("computed_iv")), axis=1)
    df = df.dropna(subset=["iv"])
    if df.empty:
        return None
    df["dte"] = df.apply(lambda row: dte_for_trade_date(row["expiration_date"], row["trade_date"]), axis=1)
    df = df[(df["dte"] >= 20) & (df["dte"] <= 90)]
    df = df[df["underlying_price"].astype(float) > 0]
    df = df[abs(df["strike"].astype(float) - df["underlying_price"].astype(float)) / df["underlying_price"].astype(float) <= 0.10]
    if "open_interest" in df.columns:
        df = df[(df["open_interest"].isna()) | (df["open_interest"].astype(float) > 0)]
    if df.empty:
        return None

    def aggregate(day: pd.DataFrame) -> float:
        weights = day["open_interest"].fillna(0).astype(float)
        if weights.sum() > 0:
            return float((day["iv"].astype(float) * weights).sum() / weights.sum())
        return float(day["iv"].astype(float).mean())

    daily_iv = df.groupby("trade_date").apply(aggregate).sort_index()
    if daily_iv.empty:
        return None
    daily_iv = daily_iv.tail(window)
    current = float(daily_iv.iloc[-1])
    high = float(daily_iv.max())
    low = float(daily_iv.min())
    rank = (current - low) / (high - low) * 100 if high > low else 0.0
    percentile = float((daily_iv < current).sum() / len(daily_iv) * 100)
    return {
        "underlying": underlying.upper(),
        "date": str(daily_iv.index[-1]),
        "current_iv": current * 100,
        "iv_rank": rank,
        "iv_percentile": percentile,
        "max_iv": high * 100,
        "min_iv": low * 100,
        "days": int(len(daily_iv)),
        "source": "us_option_iv_history",
    }


def compute_market_metrics_for_underlying(
    engine,
    underlying: str,
    trade_date: str,
    use_test_tables: bool = False,
) -> OptionMarketMetricDaily | None:
    if engine is None or pd is None:
        return None
    underlying_norm = str(underlying or "").strip().upper()
    trade_date_text = compact_date(trade_date)
    if not underlying_norm or len(trade_date_text) != 8:
        return None

    # Import lazily to avoid a module-level circular dependency. These helpers
    # are pure database/dataframe logic and do not call Massive APIs.
    from us_market_dashboard_data import (
        calculate_atm_iv_pct,
        calculate_volatility_positioning_metrics,
        load_iv_history,
        load_option_chain_daily,
        load_stock_daily,
        selected_underlying_price,
        summarize_option_chain,
    )

    stock_df = load_stock_daily(underlying_norm, limit=5000, engine=engine)
    underlying_price = selected_underlying_price(stock_df, trade_date_text)
    chain_df = load_option_chain_daily(
        underlying_norm,
        trade_date_text,
        include_short_cycle=True,
        use_test_tables=use_test_tables,
        underlying_price=underlying_price,
        engine=engine,
    )
    if (stock_df is None or stock_df.empty) and (chain_df is None or chain_df.empty):
        return None

    iv_history = load_iv_history(underlying_norm, window=5000, use_test_tables=use_test_tables, engine=engine)
    current_iv_pct = calculate_atm_iv_pct(chain_df, underlying_price=underlying_price)
    metrics = calculate_volatility_positioning_metrics(
        stock_df=stock_df,
        chain_df=chain_df,
        iv_history=iv_history,
        trade_date=trade_date_text,
        current_iv_pct=current_iv_pct,
        iv_rank=None,
    )
    summary = summarize_option_chain(chain_df)

    def metric_float(key: str) -> float | None:
        return _clean_float(metrics.get(key))

    def metric_int(key: str) -> int | None:
        value = summary.get(key)
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    return OptionMarketMetricDaily(
        trade_date=trade_date_text,
        underlying=underlying_norm,
        atm_iv_pct=metric_float("atm_iv_pct"),
        iv_change_1d=metric_float("iv_change_1d"),
        rv20_pct=metric_float("rv20_pct"),
        rv60_pct=metric_float("rv60_pct"),
        iv_rv20_spread=metric_float("iv_rv20_spread"),
        iv_30d=metric_float("iv_30d"),
        iv_60d=metric_float("iv_60d"),
        term_slope_30_60=metric_float("term_slope_30_60"),
        term_state=str(metrics.get("term_state") or "") or None,
        skew_expiration=str(metrics.get("skew_expiration") or "") or None,
        put_skew_5pct=metric_float("put_skew_5pct"),
        call_skew_5pct=metric_float("call_skew_5pct"),
        put_call_oi=metric_float("put_call_oi"),
        put_call_volume=metric_float("put_call_volume"),
        zero_dte_volume_share_pct=metric_float("zero_dte_volume_share_pct"),
        top_oi_strike=metric_float("top_oi_strike"),
        top_oi=metric_float("top_oi"),
        top5_oi_share_pct=metric_float("top5_oi_share_pct"),
        total_open_interest=metric_float("total_open_interest"),
        total_volume=metric_float("total_volume"),
        monthly_contract_count=metric_int("monthly"),
        short_cycle_contract_count=metric_int("short_cycle"),
        provider_iv_rows=metric_int("provider_iv_rows"),
        computed_iv_rows=metric_int("computed_iv_rows"),
        open_interest_rows=metric_int("open_interest_rows"),
        updated_at=_now_text(),
    )


def compute_market_metrics_for_date(
    engine,
    underlyings: Sequence[str],
    trade_date: str,
    use_test_tables: bool = False,
) -> list[OptionMarketMetricDaily]:
    rows: list[OptionMarketMetricDaily] = []
    for underlying in underlyings:
        row = compute_market_metrics_for_underlying(
            engine,
            underlying,
            trade_date,
            use_test_tables=use_test_tables,
        )
        if row is not None:
            rows.append(row)
    return rows


def update_market_metrics_for_date(
    engine,
    underlyings: Sequence[str],
    trade_date: str,
    use_test_tables: bool = False,
    dry_run: bool = False,
) -> int:
    rows = compute_market_metrics_for_date(
        engine,
        underlyings,
        trade_date,
        use_test_tables=use_test_tables,
    )
    if dry_run:
        return len(rows)
    if not rows:
        return 0
    ensure_us_option_tables(engine, use_test_tables)
    return save_market_metrics(engine, rows, use_test_tables)


def metrics_backfill_range(
    engine,
    start: str,
    end: str,
    underlyings: Sequence[str] = DEFAULT_UNDERLYINGS,
    use_test_tables: bool = False,
    dry_run: bool = False,
    progress: bool = False,
) -> dict[str, Any]:
    start_date = parse_date(start)
    end_date = parse_date(end)
    current = start_date
    totals = {"metrics": 0, "days": 0, "skipped_days": 0}
    if not dry_run:
        ensure_us_option_tables(engine, use_test_tables)
    while current <= end_date:
        if current.weekday() >= 5:
            current += dt.timedelta(days=1)
            continue
        trade_date = current.strftime("%Y%m%d")
        rows = compute_market_metrics_for_date(
            engine,
            underlyings,
            trade_date,
            use_test_tables=use_test_tables,
        )
        written = 0
        if rows:
            written = len(rows) if dry_run else save_market_metrics(engine, rows, use_test_tables)
        else:
            totals["skipped_days"] += 1
        totals["metrics"] += int(written or 0)
        totals["days"] += 1
        if progress:
            print(
                f"[metrics-backfill] {trade_date} metrics={written} dry_run={bool(dry_run)}",
                file=sys.stderr,
                flush=True,
            )
        current += dt.timedelta(days=1)
    totals["source"] = "local_metrics"
    totals["dry_run"] = bool(dry_run)
    totals["use_test_tables"] = bool(use_test_tables)
    return totals


def previous_weekday(date: dt.date) -> dt.date:
    current = date - dt.timedelta(days=1)
    while current.weekday() >= 5:
        current -= dt.timedelta(days=1)
    return current


def default_trade_date(now: dt.datetime | None = None) -> str:
    now = now or dt.datetime.now(dt.timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        eastern_now = now.astimezone(ZoneInfo("America/New_York"))
    except Exception:
        eastern_now = now
    if eastern_now.weekday() < 5 and eastern_now.hour >= 17:
        return eastern_now.date().strftime("%Y%m%d")
    return previous_weekday(eastern_now.date()).strftime("%Y%m%d")


def parse_underlyings(raw: str | None) -> list[str]:
    if not raw:
        return list(DEFAULT_UNDERLYINGS)
    return [part.strip().upper() for part in raw.split(",") if part.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Update US options data from Polygon/Massive.")
    parser.add_argument(
        "--mode",
        choices=["daily", "backfill", "live-test", "basic-probe", "metrics-backfill"],
        required=True,
    )
    parser.add_argument("--start", help="Backfill start date, YYYYMMDD")
    parser.add_argument("--end", help="Backfill end date, YYYYMMDD")
    parser.add_argument("--date", help="Trade date for daily/live-test, YYYYMMDD")
    parser.add_argument("--underlyings", default=",".join(DEFAULT_UNDERLYINGS))
    parser.add_argument("--short-strike-band-pct", type=float, default=5.0)
    parser.add_argument("--use-test-tables", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--flatfile-only", action="store_true", help="For backfill, skip/fail instead of falling back to slow REST.")
    parser.add_argument("--progress", action="store_true", help="Print per-day backfill progress to stderr.")
    return parser


def run_cli(args: argparse.Namespace | None = None) -> dict[str, Any]:
    if args is None:
        args = build_arg_parser().parse_args()
    underlyings = parse_underlyings(args.underlyings)
    if args.mode == "basic-probe":
        if not args.dry_run and not args.use_test_tables:
            raise RuntimeError("basic-probe only writes with --use-test-tables; use --dry-run for read-only probing.")
    needs_db = args.mode == "metrics-backfill" or not args.dry_run
    engine = get_db_engine() if needs_db else None
    if engine is None and needs_db:
        raise RuntimeError("Database env is incomplete. Set DB_USER/DB_PASSWORD/DB_HOST/DB_NAME or use --dry-run.")
    if args.mode == "metrics-backfill":
        if not args.start or not args.end:
            raise RuntimeError("--start and --end are required for metrics-backfill")
        return metrics_backfill_range(
            engine,
            args.start,
            args.end,
            underlyings=underlyings,
            use_test_tables=args.use_test_tables,
            dry_run=args.dry_run,
            progress=args.progress,
        )
    client = MassiveOptionsClient()
    if args.mode == "basic-probe":
        trade_date = compact_date(args.date) or default_trade_date()
        underlying = underlyings[0] if underlyings else "SPY"
        return basic_probe(
            engine,
            client,
            underlying=underlying,
            trade_date=trade_date,
            use_test_tables=args.use_test_tables,
            dry_run=args.dry_run,
        )
    if args.mode == "daily":
        trade_date = compact_date(args.date) or default_trade_date()
        return live_update(
            engine,
            client,
            underlyings,
            trade_date,
            short_strike_band_pct=args.short_strike_band_pct,
            use_test_tables=args.use_test_tables,
            dry_run=args.dry_run,
        )
    if args.mode == "live-test":
        trade_date = compact_date(args.date) or default_trade_date()
        use_test_tables = True if not args.dry_run else args.use_test_tables
        return live_update(
            engine,
            client,
            underlyings,
            trade_date,
            short_strike_band_pct=args.short_strike_band_pct,
            use_test_tables=use_test_tables,
            dry_run=args.dry_run,
        )
    if args.mode == "backfill":
        if not args.start or not args.end:
            raise RuntimeError("--start and --end are required for backfill")
        return backfill_range(
            engine,
            client,
            args.start,
            args.end,
            underlyings=underlyings,
            short_strike_band_pct=args.short_strike_band_pct,
            use_test_tables=args.use_test_tables,
            dry_run=args.dry_run,
            flatfile_only=args.flatfile_only,
            progress=args.progress,
        )
    raise RuntimeError(f"Unsupported mode: {args.mode}")

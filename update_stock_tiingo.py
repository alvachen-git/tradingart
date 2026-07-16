from __future__ import annotations

import argparse
import datetime
import gc
import os
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
import requests
try:
    import akshare as ak
except Exception:
    ak = None
try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):  # type: ignore[override]
        return False
from sqlalchemy import create_engine, text
from tiingo import TiingoClient

# Keep shell-exported vars (e.g. chunk rotation from run_daily2.sh) higher priority.
load_dotenv(override=False)


def _env_int(name: str, default: int) -> int:
    raw = str(os.getenv(name, default)).strip()
    try:
        return int(raw)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    raw = str(os.getenv(name, default)).strip()
    try:
        return float(raw)
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, str(default))).strip().lower()
    if raw in {"1", "true", "yes", "y", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _parse_source_priority(raw: str, fallback: str) -> list[str]:
    allowed = {"akshare", "tiingo", "twelvedata"}
    parts = [p.strip().lower() for p in str(raw or fallback).split(",") if p.strip()]
    out = []
    for p in parts:
        if p in allowed and p not in out:
            out.append(p)
    if out:
        return out
    return [p.strip() for p in fallback.split(",") if p.strip()]


TIINGO_KEY = (
    os.getenv("TIINGO_API_KEY")
    or os.getenv("TIINGO_KEY")
    or "ddb0de2f922b0e2e02c6b50516b2b87cb9dc1bda"
).strip()
TWELVEDATA_API_KEY = str(os.getenv("TWELVEDATA_API_KEY", "")).strip()

US_SOURCE_PRIORITY = _parse_source_priority(
    os.getenv("US_SOURCE_PRIORITY", ""),
    "tiingo,akshare,twelvedata",
)
US_BACKFILL_SOURCE_PRIORITY = _parse_source_priority(
    os.getenv("US_BACKFILL_SOURCE_PRIORITY", ""),
    "tiingo,akshare,twelvedata",
)
US_REQUIRE_ADJUSTED_SOURCE_FIRST = _env_bool("US_REQUIRE_ADJUSTED_SOURCE_FIRST", True)
if US_REQUIRE_ADJUSTED_SOURCE_FIRST:
    # Older deployments may still carry an AkShare-first value in .env.  Move
    # the only complete adjusted-OHLCV provider to the front unless an operator
    # explicitly disables the safety policy.
    if "tiingo" in US_SOURCE_PRIORITY:
        US_SOURCE_PRIORITY = ["tiingo", *(item for item in US_SOURCE_PRIORITY if item != "tiingo")]
    if "tiingo" in US_BACKFILL_SOURCE_PRIORITY:
        US_BACKFILL_SOURCE_PRIORITY = ["tiingo", *(item for item in US_BACKFILL_SOURCE_PRIORITY if item != "tiingo")]

US_BACKFILL_DAYS_PER_RUN = _env_int("US_BACKFILL_DAYS_PER_RUN", 120)
US_TARGET_HISTORY_DAYS = _env_int("US_TARGET_HISTORY_DAYS", 1095)
US_INCREMENTAL_LOOKBACK_DAYS = _env_int("US_INCREMENTAL_LOOKBACK_DAYS", 7)
US_ADJUSTMENT_REPAIR_LOOKBACK_DAYS = _env_int("US_ADJUSTMENT_REPAIR_LOOKBACK_DAYS", 550)
US_ADJUSTMENT_REPAIR_MIN_RATIO = _env_float("US_ADJUSTMENT_REPAIR_MIN_RATIO", 3.0)
US_BACKFILL_BATCH_SIZE = _env_int("US_BACKFILL_BATCH_SIZE", 6)
US_ENABLE_BACKFILL = _env_bool("US_ENABLE_BACKFILL", True)
US_MAX_SYMBOLS_PER_RUN = _env_int("US_MAX_SYMBOLS_PER_RUN", 0)
US_SYMBOL_CHUNK_TOTAL = max(1, _env_int("US_SYMBOL_CHUNK_TOTAL", 1))
US_SYMBOL_CHUNK_INDEX = _env_int("US_SYMBOL_CHUNK_INDEX", 0)

US_PROVIDER_COOLDOWN_SECONDS = _env_int("US_PROVIDER_COOLDOWN_SECONDS", 900)
US_PROVIDER_ERROR_COOLDOWN_SECONDS = _env_int("US_PROVIDER_ERROR_COOLDOWN_SECONDS", 600)
US_PROVIDER_ERROR_COOLDOWN_THRESHOLD = _env_int("US_PROVIDER_ERROR_COOLDOWN_THRESHOLD", 3)
US_REQUEST_SLEEP_AK = _env_float("US_REQUEST_SLEEP_AK", 1.0)
US_REQUEST_SLEEP_TIINGO = _env_float("US_REQUEST_SLEEP_TIINGO", 1.5)
US_REQUEST_SLEEP_TD = _env_float("US_REQUEST_SLEEP_TD", 8.0)

US_RETRY_MAX_AK = _env_int("US_RETRY_MAX_AK", 2)
US_RETRY_MAX_TIINGO = _env_int("US_RETRY_MAX_TIINGO", 2)
US_RETRY_MAX_TD = _env_int("US_RETRY_MAX_TD", 2)

COMMON_SPLIT_RATIOS = (1.5, 2.0, 3.0, 4.0, 5.0, 7.0, 8.0, 10.0, 15.0, 20.0, 25.0, 50.0, 100.0)
US_EXCHANGE_CODES = {
    "AMEX", "BATS", "CBOE", "IEX", "NASDAQ", "NYSE", "NYSEARCA", "NYSEMKT",
    "OTC", "OTCEXCHANGE",
}

# 数据库配置（优先 .env，兼容历史默认值）
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "alva13557941")
DB_HOST = os.getenv("DB_HOST", "39.102.215.198")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "finance_data")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

SECTOR_SYMBOLS = {
    # 科技与互联网龙头（半导体/云计算/软件/平台）
    "tech": [
        "AAPL", "MSFT", "NVDA", "GOOG", "GOOGL", "META", "AMZN", "TSM", "AVGO", "AMD", "INTC",
        "ORCL", "ADBE", "CRM", "IBM", "CSCO", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC","TSLA",
        "PANW", "CRWD", "PLTR", "SNOW", "NOW", "ANET", "CDNS", "SNPS", "INTU", "SHOP",
    ],
    # AI 主题补充（与 tech 并行维护，去重逻辑会自动处理重复代码）
    "ai_thematic": [
        "ARM", "MRVL", "SMCI", "DELL", "ASML", "TSM", "VRT",
    ],
    # 金融核心标的（银行/支付/资管/交易所）
    "financial": [
        "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SCHW", "AXP", "SPGI",
        "V", "MA", "PYPL", "COF", "USB", "PNC", "BK", "ICE", "CME", "CB",
    ],
    # 消费核心标的（可选消费 + 必选消费）
    "consumer": [
        "HD", "LOW", "NKE", "SBUX", "MCD", "CMG", "DIS", "NFLX", "BKNG",
        "COST", "WMT", "TGT", "KO", "PEP", "PG", "MDLZ", "PM", "MO", "EL",
    ],
    # 太空与军工航天
    "space": [
        "SPCX", "RKLB", "LUNR", "ASTS", "PL", "SPCE", "NOC", "LMT",
    ],
    # 稳定币/支付与交易基础设施（不含高波动矿股）
    "stablecoin_infra": [
        "COIN", "HOOD", "PYPL", "XYZ", "MSTR",
    ],
    # 期权高活跃补充池（配合美股期权页面新增标的，避免回补 IV 时临时补价）
    "option_active": [
        "BABA", "UBER", "SOFI", "RIVN", "MARA", "DRAM",
        "BA", "CVNA", "F", "GME", "LLY", "PANW", "PDD", "PYPL", "SHOP", "UNH",
        "ADBE", "APP", "C", "CAT", "CRM", "IBM", "PFE", "SNOW", "VRT", "WFC",
    ],
    # 石油与油服炼化
    "oil_energy": [
        "XOM", "CVX", "COP", "EOG", "OXY", "SLB", "HAL", "MPC", "VLO",
    ],
    # 美股宽基高流动性 ETF
    "us_etf_broad": [
        "SPY", "IVV", "VOO", "QQQ", "VTI", "IWM", "DIA",
    ],
    # 美股行业高流动性 ETF
    "us_etf_sector": [
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLC", "SMH", "SOXX", "KRE", "XBI",
    ],
    # 资产配置/宏观高流动性 ETF
    "us_etf_macro": [
        "GLD", "SLV", "USO", "TLT", "IEF", "HYG", "LQD", "EEM", "FXI",
    ],
    # 主流加密现货 ETF（不含杠杆/反向）
    "crypto_spot_etf": [
        "IBIT", "FBTC", "ETHA", "FETH",
    ],
}

SECTOR_LABELS = {
    "tech": "科技",
    "ai_thematic": "AI主题",
    "financial": "金融",
    "consumer": "消费",
    "space": "太空",
    "stablecoin_infra": "稳定币基础设施",
    "option_active": "期权活跃",
    "oil_energy": "石油能源",
    "us_etf_broad": "美股宽基ETF",
    "us_etf_sector": "美股行业ETF",
    "us_etf_macro": "宏观配置ETF",
    "crypto_spot_etf": "加密现货ETF",
}


def _build_symbols() -> list[str]:
    """按定义顺序去重，得到最终日更美股池。"""
    merged = []
    seen = set()
    for _, symbols in SECTOR_SYMBOLS.items():
        for s in symbols:
            code = str(s).strip().upper()
            if code and code not in seen:
                seen.add(code)
                merged.append(code)
    return merged


SYMBOLS = _build_symbols()
engine = create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


class ProviderError(Exception):
    pass


class RateLimitError(ProviderError):
    pass


class ProviderUnavailableError(ProviderError):
    pass


def _empty_ohlcv_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "adjClose", "splitFactor"])


def _looks_like_rate_limit(msg: str) -> bool:
    txt = str(msg or "").lower()
    return (
        "429" in txt
        or "too many requests" in txt
        or "hourly request allocation" in txt
        or "rate limit" in txt
        or "credits" in txt
    )


def _looks_like_network_abort(msg: str) -> bool:
    txt = str(msg or "").lower()
    return (
        "connection aborted" in txt
        or "remotedisconnected" in txt
        or "remote end closed connection without response" in txt
        or "connection reset" in txt
        or "read timed out" in txt
        or "connect timeout" in txt
    )


def _to_naive_dates(series: pd.Series) -> pd.Series:
    dt_series = pd.to_datetime(series, errors="coerce")
    try:
        if getattr(dt_series.dt, "tz", None) is not None:
            dt_series = dt_series.dt.tz_convert(None)
    except Exception:
        pass
    return dt_series.dt.date


def _normalize_ohlcv_df(
    df: pd.DataFrame,
    symbol: str,
    date_col: str,
    open_col: str,
    high_col: str,
    low_col: str,
    close_col: str,
    volume_col: str,
    adj_close_col: Optional[str] = None,
    split_factor_col: Optional[str] = None,
) -> pd.DataFrame:
    if df is None or df.empty:
        return _empty_ohlcv_df()

    if date_col not in df.columns or close_col not in df.columns:
        return _empty_ohlcv_df()

    out = pd.DataFrame()
    out["date"] = _to_naive_dates(df[date_col])
    out["symbol"] = str(symbol).upper()
    out["open"] = pd.to_numeric(df[open_col], errors="coerce") if open_col in df.columns else pd.NA
    out["high"] = pd.to_numeric(df[high_col], errors="coerce") if high_col in df.columns else pd.NA
    out["low"] = pd.to_numeric(df[low_col], errors="coerce") if low_col in df.columns else pd.NA
    out["close"] = pd.to_numeric(df[close_col], errors="coerce")
    out["volume"] = pd.to_numeric(df[volume_col], errors="coerce") if volume_col in df.columns else 0

    if adj_close_col and adj_close_col in df.columns:
        out["adjClose"] = pd.to_numeric(df[adj_close_col], errors="coerce")
    else:
        out["adjClose"] = out["close"]
    if split_factor_col and split_factor_col in df.columns:
        out["splitFactor"] = pd.to_numeric(df[split_factor_col], errors="coerce").fillna(1.0)
    else:
        out["splitFactor"] = 1.0

    for c in ["open", "high", "low"]:
        out[c] = out[c].fillna(out["close"])
    out["volume"] = out["volume"].fillna(0)
    out["adjClose"] = out["adjClose"].fillna(out["close"])

    out = out.dropna(subset=["date", "close"])
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return out[["date", "symbol", "open", "high", "low", "close", "volume", "adjClose", "splitFactor"]]


@dataclass
class ProviderState:
    name: str
    cooldown_seconds: int
    sleep_seconds: float
    blocked_until: Optional[datetime.datetime] = None
    stats: dict[str, int] = field(default_factory=lambda: {
        "success": 0,
        "failed": 0,
        "rate_limited": 0,
        "skipped": 0,
    })
    consecutive_errors: int = 0

    def in_cooldown(self) -> bool:
        return self.blocked_until is not None and datetime.datetime.utcnow() < self.blocked_until

    def set_cooldown(self, seconds: Optional[int] = None) -> None:
        sec = int(seconds if seconds is not None else self.cooldown_seconds)
        self.blocked_until = datetime.datetime.utcnow() + datetime.timedelta(seconds=max(1, sec))


class BaseProvider:
    def __init__(self, name: str, sleep_seconds: float, cooldown_seconds: int, max_retries: int = 2):
        self.state = ProviderState(name=name, cooldown_seconds=cooldown_seconds, sleep_seconds=sleep_seconds)
        self.max_retries = max(1, int(max_retries))

    def is_configured(self) -> bool:
        return True

    def fetch_daily(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        last_err: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._fetch_impl(symbol, start_date, end_date)
            except RateLimitError:
                raise
            except Exception as e:
                last_err = e
                if attempt >= self.max_retries:
                    break
                sleep_sec = (1.2 * attempt) + random.uniform(0.0, 0.8)
                print(f"      ⚠️ {self.state.name} 第{attempt}次失败，{sleep_sec:.1f}s 后重试: {e}")
                time.sleep(sleep_sec)
        if last_err is not None:
            raise ProviderError(str(last_err))
        raise ProviderError("unknown provider error")

    def _fetch_impl(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        raise NotImplementedError


class AkShareProvider(BaseProvider):
    def __init__(self):
        super().__init__(
            name="akshare",
            sleep_seconds=US_REQUEST_SLEEP_AK,
            cooldown_seconds=US_PROVIDER_COOLDOWN_SECONDS,
            max_retries=US_RETRY_MAX_AK,
        )
        self._symbol_map: Optional[dict[str, str]] = None

    def is_configured(self) -> bool:
        return ak is not None

    def _find_col(self, df: pd.DataFrame, candidates: list[str]) -> Optional[str]:
        for c in candidates:
            if c in df.columns:
                return c
        return None

    def _load_symbol_map(self) -> dict[str, str]:
        if self._symbol_map is not None:
            return self._symbol_map
        if ak is None:
            return {}
        mapping: dict[str, str] = {}
        try:
            spot_df = ak.stock_us_spot_em()
            if spot_df is None or spot_df.empty:
                self._symbol_map = mapping
                return mapping

            code_col = self._find_col(spot_df, ["代码", "symbol", "代码名称", "代码code"])
            if code_col is None:
                self._symbol_map = mapping
                return mapping

            for raw in spot_df[code_col].astype(str).tolist():
                token = raw.strip().upper()
                if not token:
                    continue
                if "." in token:
                    ticker = token.split(".")[-1]
                    if ticker and ticker not in mapping:
                        mapping[ticker] = token
                else:
                    mapping[token] = token
        except Exception:
            mapping = {}

        self._symbol_map = mapping
        return mapping

    def _resolve_ak_symbol(self, symbol: str) -> str:
        s = str(symbol).strip().upper()
        if "." in s and s.split(".")[0].isdigit():
            return s
        mapping = self._load_symbol_map()
        return mapping.get(s, s)

    def _fetch_impl(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        if ak is None:
            raise ProviderUnavailableError("akshare is not installed")

        ak_symbol = self._resolve_ak_symbol(symbol)
        s_date = start_date.strftime("%Y%m%d")
        e_date = (end_date or datetime.date.today()).strftime("%Y%m%d")

        try:
            df = ak.stock_us_hist(
                symbol=ak_symbol,
                period="daily",
                start_date=s_date,
                end_date=e_date,
                adjust="",
            )
        except Exception as e:
            if _looks_like_rate_limit(str(e)):
                raise RateLimitError(str(e))
            raise ProviderError(str(e))

        if df is None or df.empty:
            return _empty_ohlcv_df()

        date_col = self._find_col(df, ["日期", "date", "Date"])
        open_col = self._find_col(df, ["开盘", "open", "Open"])
        high_col = self._find_col(df, ["最高", "high", "High"])
        low_col = self._find_col(df, ["最低", "low", "Low"])
        close_col = self._find_col(df, ["收盘", "close", "Close"])
        volume_col = self._find_col(df, ["成交量", "volume", "Volume"])
        adj_col = self._find_col(df, ["复权收盘", "adjClose", "Adj Close", "adj_close"])

        if not date_col or not close_col:
            return _empty_ohlcv_df()

        return _normalize_ohlcv_df(
            df=df,
            symbol=symbol,
            date_col=date_col,
            open_col=open_col or close_col,
            high_col=high_col or close_col,
            low_col=low_col or close_col,
            close_col=close_col,
            volume_col=volume_col or close_col,
            adj_close_col=adj_col,
        )


class TiingoProvider(BaseProvider):
    def __init__(self, api_key: str):
        super().__init__(
            name="tiingo",
            sleep_seconds=US_REQUEST_SLEEP_TIINGO,
            cooldown_seconds=US_PROVIDER_COOLDOWN_SECONDS,
            max_retries=US_RETRY_MAX_TIINGO,
        )
        self.api_key = str(api_key or "").strip()
        self.client = TiingoClient({"session": True, "api_key": self.api_key}) if self.api_key else None
        self._metadata_cache: dict[str, dict[str, Any]] = {}

    def is_configured(self) -> bool:
        return bool(self.client is not None)

    @staticmethod
    def _normalize_exchange(value: Any) -> str:
        return "".join(ch for ch in str(value or "").upper() if ch.isalnum())

    def _validate_us_listing(self, symbol: str) -> None:
        """Reject same-ticker foreign listings before they can pollute US history."""
        code = str(symbol or "").strip().upper()
        metadata = self._metadata_cache.get(code)
        if metadata is None:
            try:
                raw = self.client.get_ticker_metadata(code) if self.client else None
            except Exception as exc:
                if _looks_like_rate_limit(str(exc)):
                    raise RateLimitError(str(exc))
                raise ProviderError(f"metadata lookup failed: {exc}")
            metadata = raw if isinstance(raw, dict) else {}
            self._metadata_cache[code] = metadata
        exchange = self._normalize_exchange(metadata.get("exchangeCode"))
        returned_ticker = str(metadata.get("ticker") or code).strip().upper()
        if returned_ticker != code:
            raise ProviderError(f"ticker mismatch: requested={code}, returned={returned_ticker}")
        if exchange not in US_EXCHANGE_CODES:
            raise ProviderError(f"non-US listing rejected: symbol={code}, exchange={exchange or 'unknown'}")

    def _fetch_impl(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        if not self.client:
            raise ProviderUnavailableError("TIINGO_API_KEY is missing")
        self._validate_us_listing(symbol)
        kwargs: dict[str, Any] = {
            "fmt": "json",
            "startDate": start_date.strftime("%Y-%m-%d"),
            "frequency": "daily",
        }
        if end_date is not None:
            kwargs["endDate"] = end_date.strftime("%Y-%m-%d")

        try:
            history_data = self.client.get_ticker_price(symbol.upper(), **kwargs)
        except Exception as e:
            if _looks_like_rate_limit(str(e)):
                raise RateLimitError(str(e))
            raise ProviderError(str(e))

        if not history_data:
            return _empty_ohlcv_df()

        df = pd.DataFrame(history_data)
        adjusted_columns = {"adjOpen", "adjHigh", "adjLow", "adjClose", "adjVolume"}
        use_adjusted = adjusted_columns.issubset(df.columns)
        return _normalize_ohlcv_df(
            df=df,
            symbol=symbol,
            date_col="date",
            open_col="adjOpen" if use_adjusted else "open",
            high_col="adjHigh" if use_adjusted else "high",
            low_col="adjLow" if use_adjusted else "low",
            close_col="adjClose" if use_adjusted else "close",
            volume_col="adjVolume" if use_adjusted else "volume",
            adj_close_col="adjClose",
            split_factor_col="splitFactor",
        )


class TwelveDataProvider(BaseProvider):
    BASE_URL = "https://api.twelvedata.com/time_series"

    def __init__(self, api_key: str):
        super().__init__(
            name="twelvedata",
            sleep_seconds=US_REQUEST_SLEEP_TD,
            cooldown_seconds=US_PROVIDER_COOLDOWN_SECONDS,
            max_retries=US_RETRY_MAX_TD,
        )
        self.api_key = str(api_key or "").strip()

    def is_configured(self) -> bool:
        return bool(self.api_key)

    def _fetch_impl(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        if not self.api_key:
            raise ProviderUnavailableError("TWELVEDATA_API_KEY is missing")

        params: dict[str, Any] = {
            "apikey": self.api_key,
            "symbol": symbol.upper(),
            "interval": "1day",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "order": "ASC",
            "format": "JSON",
            "outputsize": 5000,
        }
        if end_date is not None:
            params["end_date"] = end_date.strftime("%Y-%m-%d")

        try:
            resp = requests.get(self.BASE_URL, params=params, timeout=25)
        except Exception as e:
            raise ProviderError(str(e))

        if resp.status_code == 429:
            raise RateLimitError(f"http 429: {resp.text[:200]}")
        if resp.status_code >= 400:
            raise ProviderError(f"http {resp.status_code}: {resp.text[:200]}")

        try:
            payload = resp.json()
        except Exception as e:
            raise ProviderError(f"invalid json: {e}")

        if isinstance(payload, dict) and payload.get("status") == "error":
            code = str(payload.get("code", ""))
            msg = str(payload.get("message", payload))
            if code == "429" or _looks_like_rate_limit(msg):
                raise RateLimitError(msg)
            raise ProviderError(msg)

        values = payload.get("values") if isinstance(payload, dict) else None
        if not values:
            return _empty_ohlcv_df()

        df = pd.DataFrame(values)
        return _normalize_ohlcv_df(
            df=df,
            symbol=symbol,
            date_col="datetime",
            open_col="open",
            high_col="high",
            low_col="low",
            close_col="close",
            volume_col="volume",
            adj_close_col=None,
        )


PROVIDERS = {
    "akshare": AkShareProvider(),
    "tiingo": TiingoProvider(TIINGO_KEY),
    "twelvedata": TwelveDataProvider(TWELVEDATA_API_KEY),
}


def _query_single_date(symbol: str, agg_expr: str) -> Optional[datetime.datetime]:
    sql = text(f"SELECT {agg_expr} AS dt FROM stock_prices WHERE symbol = :symbol")
    with engine.connect() as conn:
        row = conn.execute(sql, {"symbol": symbol.upper()}).fetchone()
    if not row or row[0] is None:
        return None
    try:
        return pd.to_datetime(row[0]).to_pydatetime()
    except Exception:
        return None


def get_last_date_from_db(symbol: str) -> Optional[datetime.datetime]:
    return _query_single_date(symbol, "MAX(date)")


def get_first_date_from_db(symbol: str) -> Optional[datetime.datetime]:
    return _query_single_date(symbol, "MIN(date)")


def save_symbol_data(symbol: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    save_df = df.copy()
    save_df["symbol"] = symbol.upper()
    save_df = save_df[["date", "symbol", "open", "high", "low", "close", "volume", "adjClose"]]
    save_df = save_df.dropna(subset=["date", "close"])
    if save_df.empty:
        return 0

    start_date = save_df["date"].min()
    end_date = save_df["date"].max()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                DELETE FROM stock_prices
                WHERE symbol = :symbol
                  AND date >= :start_date
                  AND date <= :end_date
                """
            ),
            {
                "symbol": symbol.upper(),
                "start_date": start_date,
                "end_date": end_date,
            },
        )
    save_df.to_sql("stock_prices", con=engine, if_exists="append", index=False, method="multi")
    return int(len(save_df))


def _provider_cooldown_text(provider: BaseProvider) -> str:
    until = provider.state.blocked_until
    if until is None:
        return "-"
    return until.strftime("%Y-%m-%d %H:%M:%S")


def fetch_with_fallback(
    symbol: str,
    start_date: datetime.date,
    end_date: Optional[datetime.date] = None,
    source_priority: Optional[list[str]] = None,
) -> tuple[pd.DataFrame, Optional[str], int, list[str]]:
    priority = source_priority or US_SOURCE_PRIORITY
    switches = 0
    attempts: list[str] = []

    for name in priority:
        provider = PROVIDERS.get(name)
        if provider is None:
            attempts.append(f"{name}:unknown_provider")
            continue

        if provider.state.in_cooldown():
            provider.state.stats["skipped"] += 1
            attempts.append(f"{name}:cooldown")
            continue

        if not provider.is_configured():
            provider.state.stats["skipped"] += 1
            attempts.append(f"{name}:not_configured")
            continue

        try:
            data = provider.fetch_daily(symbol=symbol, start_date=start_date, end_date=end_date)
            provider.state.stats["success"] += 1
            provider.state.consecutive_errors = 0
            time.sleep(max(0.0, provider.state.sleep_seconds))
            return data, provider.state.name, switches, attempts
        except RateLimitError as e:
            provider.state.stats["failed"] += 1
            provider.state.stats["rate_limited"] += 1
            provider.state.consecutive_errors += 1
            provider.state.set_cooldown()
            attempts.append(f"{name}:rate_limit:{e}")
            switches += 1
            continue
        except Exception as e:
            provider.state.stats["failed"] += 1
            provider.state.consecutive_errors += 1
            if (
                provider.state.consecutive_errors >= US_PROVIDER_ERROR_COOLDOWN_THRESHOLD
                and _looks_like_network_abort(str(e))
            ):
                provider.state.set_cooldown(US_PROVIDER_ERROR_COOLDOWN_SECONDS)
                attempts.append(
                    f"{name}:error_cooldown:{e}"
                )
            else:
                attempts.append(f"{name}:error:{e}")
            switches += 1
            continue

    return _empty_ohlcv_df(), None, switches, attempts


def _get_incremental_start(last_date: Optional[datetime.datetime], today: datetime.date) -> Optional[datetime.date]:
    if last_date is None:
        return None
    suggested = (last_date + datetime.timedelta(days=1)).date()
    floor_date = today - datetime.timedelta(days=max(0, US_INCREMENTAL_LOOKBACK_DAYS))
    # Re-read a small overlap even when the DB is current.  Without overlap a
    # splitFactor that arrives on the split session is never observed and the
    # pre-split history cannot be repaired.
    return min(suggested, floor_date)


def _nearest_common_split_ratio(value: Any, *, tolerance: float = 0.12) -> Optional[float]:
    try:
        number = float(value)
    except Exception:
        return None
    if not pd.notna(number) or number <= 0:
        return None
    candidates = (*COMMON_SPLIT_RATIOS, *(1.0 / item for item in COMMON_SPLIT_RATIOS))
    for candidate in candidates:
        if abs(number - candidate) / candidate <= tolerance:
            return float(candidate)
    return None


def _has_split_event(df: pd.DataFrame) -> bool:
    if df is None or df.empty or "splitFactor" not in df.columns:
        return False
    values = pd.to_numeric(df["splitFactor"], errors="coerce").fillna(1.0)
    return bool((values.sub(1.0).abs() > 1e-9).any())


def _has_unresolved_scale_break(df: pd.DataFrame, *, minimum_scale: float = 1.45) -> bool:
    if df is None or df.empty or "close" not in df.columns:
        return False
    values = pd.to_numeric(df["close"], errors="coerce")
    steps = values / values.shift(1).replace(0, pd.NA)
    for value in steps.dropna():
        number = float(value)
        if number <= 0:
            continue
        scale = max(number, 1.0 / number)
        if scale >= minimum_scale and _nearest_common_split_ratio(number) is not None:
            return True
    return False


def _find_adjustment_repair_symbols(
    today: datetime.date,
    symbols: list[str],
) -> set[str]:
    """Find histories whose adjusted price or factor still contains a split-scale break.

    This scanner only selects symbols for a trusted re-fetch.  It never guesses
    a split factor and never mutates prices itself.
    """
    requested = {str(item).strip().upper() for item in symbols if str(item).strip()}
    if not requested:
        return set()
    start_date = today - datetime.timedelta(days=max(90, US_ADJUSTMENT_REPAIR_LOOKBACK_DAYS))
    try:
        with engine.connect() as conn:
            columns = {str(row[0]) for row in conn.execute(text("SHOW COLUMNS FROM stock_prices")).fetchall()}
            select_adj = ", adjClose" if "adjClose" in columns else ""
            history = pd.read_sql(
                text(
                    f"""
                    SELECT date, UPPER(symbol) AS symbol, close{select_adj}
                    FROM stock_prices
                    WHERE date >= :start_date
                    ORDER BY symbol, date
                    """
                ),
                conn,
                params={"start_date": start_date},
            )
    except Exception as exc:
        print(f"⚠️ 复权质量扫描失败，本轮继续使用读取端安全闸门: {exc}")
        return set()
    if history.empty:
        return set()
    history["symbol"] = history["symbol"].astype(str).str.upper().str.strip()
    history = history[history["symbol"].isin(requested)]
    repair: set[str] = set()
    for symbol, group in history.groupby("symbol"):
        group = group.sort_values("date")
        raw_close = pd.to_numeric(group["close"], errors="coerce")
        adjusted_close = (
            pd.to_numeric(group["adjClose"], errors="coerce")
            if "adjClose" in group.columns
            else raw_close.copy()
        )
        adjusted_close = adjusted_close.where(adjusted_close.gt(0), raw_close)
        price_steps = adjusted_close / adjusted_close.shift(1).replace(0, pd.NA)
        factor = adjusted_close / raw_close.replace(0, pd.NA)
        factor_steps = factor / factor.shift(1).replace(0, pd.NA)
        # A jump in adjClose/close is already positive split evidence, so even
        # a 3-for-2 or 2-for-1 event merits a full rebuild to repair volume.
        # A jump left in adjusted price is ambiguous with a real crash and is
        # auto-refetched only for larger (>= configured) scale breaks.
        series_thresholds = (
            (price_steps, max(1.0, US_ADJUSTMENT_REPAIR_MIN_RATIO)),
            (factor_steps, 1.45),
        )
        for series, minimum_scale in series_thresholds:
            for value in pd.to_numeric(series, errors="coerce").dropna():
                scale = max(float(value), 1.0 / float(value)) if float(value) > 0 else 0.0
                if scale < minimum_scale:
                    continue
                if _nearest_common_split_ratio(value) is not None:
                    repair.add(str(symbol))
                    break
            if symbol in repair:
                break
    return repair


def _select_symbols_for_run(all_symbols: list[str]) -> list[str]:
    symbols = [str(s).strip().upper() for s in all_symbols if str(s).strip()]
    if not symbols:
        return []

    if US_SYMBOL_CHUNK_TOTAL > 1:
        chunk_index = US_SYMBOL_CHUNK_INDEX % US_SYMBOL_CHUNK_TOTAL
        symbols = [s for i, s in enumerate(symbols) if (i % US_SYMBOL_CHUNK_TOTAL) == chunk_index]

    if US_MAX_SYMBOLS_PER_RUN > 0:
        symbols = symbols[:US_MAX_SYMBOLS_PER_RUN]
    return symbols


def _run_low_priority_backfill(today: datetime.date, candidate_symbols: Optional[list[str]] = None) -> dict[str, int]:
    stats = {"attempted": 0, "saved": 0, "failed": 0, "skipped": 0}
    if not US_ENABLE_BACKFILL:
        print("🔕 低优先级历史回填已关闭 (US_ENABLE_BACKFILL=false)")
        return stats

    target_start = today - datetime.timedelta(days=max(30, US_TARGET_HISTORY_DAYS))
    candidates: list[tuple[str, datetime.date]] = []
    use_symbols = candidate_symbols or SYMBOLS
    for symbol in use_symbols:
        first_date = get_first_date_from_db(symbol)
        if first_date is None:
            continue
        d = first_date.date()
        if d > target_start:
            candidates.append((symbol, d))

    if not candidates:
        print("📚 低优先级回填：暂无候选标的")
        return stats

    candidates = sorted(candidates, key=lambda x: x[0])
    offset = today.toordinal() % len(candidates)
    rotated = candidates[offset:] + candidates[:offset]
    batch = rotated[: max(1, US_BACKFILL_BATCH_SIZE)]

    print(f"📚 低优先级回填：候选{len(candidates)}只，本轮处理{len(batch)}只")
    for symbol, first_date in batch:
        stats["attempted"] += 1
        end_date = first_date - datetime.timedelta(days=1)
        start_date = max(
            target_start,
            end_date - datetime.timedelta(days=max(2, US_BACKFILL_DAYS_PER_RUN) - 1),
        )
        if start_date > end_date:
            stats["skipped"] += 1
            continue

        print(f"   ↪ 回填 {symbol}: {start_date} ~ {end_date}")
        data, source_name, switches, attempts = fetch_with_fallback(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            source_priority=US_BACKFILL_SOURCE_PRIORITY,
        )
        if source_name is None:
            stats["failed"] += 1
            print(f"     ❌ 回填失败，无可用源 | attempts={'; '.join(attempts)}")
            continue
        if data.empty:
            stats["skipped"] += 1
            print(f"     ⚠️ 回填无数据 (source={source_name}, switches={switches})")
            continue

        rows = save_symbol_data(symbol, data)
        stats["saved"] += rows
        print(f"     ✅ 回填成功 {rows} 条 (source={source_name}, switches={switches})")
        del data
        gc.collect()

    return stats


def update_stock_data(
    *,
    symbols: Optional[list[str]] = None,
    repair_only: bool = False,
) -> None:
    today = datetime.date.today()
    run_symbols = _select_symbols_for_run(symbols or SYMBOLS)
    adjustment_repair_symbols = _find_adjustment_repair_symbols(today, run_symbols)
    if repair_only:
        run_symbols = [item for item in run_symbols if item in adjustment_repair_symbols]
    print(f"🚀 开始更新美股日线，标的总数: {len(SYMBOLS)}，本轮处理: {len(run_symbols)}")
    print(f"🔗 源优先级: {US_SOURCE_PRIORITY}")
    print(f"🔁 回填源优先级: {US_BACKFILL_SOURCE_PRIORITY}")
    if not TWELVEDATA_API_KEY:
        print("⚠️ 未配置 TWELVEDATA_API_KEY，twelvedata 次源当前不可用")
    if US_SYMBOL_CHUNK_TOTAL > 1:
        print(f"🧩 分片模式: chunk {US_SYMBOL_CHUNK_INDEX % US_SYMBOL_CHUNK_TOTAL + 1}/{US_SYMBOL_CHUNK_TOTAL}")
    if US_MAX_SYMBOLS_PER_RUN > 0:
        print(f"🎯 本轮上限: {US_MAX_SYMBOLS_PER_RUN} 只")
    if adjustment_repair_symbols:
        print(
            "🧹 检测到需整段重建的复权/源冲突标的: "
            + ", ".join(sorted(adjustment_repair_symbols))
        )

    summary_items = []
    for key, symbols in SECTOR_SYMBOLS.items():
        label = SECTOR_LABELS.get(key, key)
        summary_items.append(f"{label}{len(symbols)}只")
    print("📦 标的池分类: " + ", ".join(summary_items))

    symbol_stats = {"success_symbols": 0, "failed_symbols": 0, "empty_symbols": 0, "saved_rows": 0, "skipped": 0}

    for symbol in run_symbols:
        symbol = symbol.upper()
        print(f"\n处理: {symbol}")
        try:
            last_date = get_last_date_from_db(symbol)
            force_adjustment_repair = symbol in adjustment_repair_symbols
            if force_adjustment_repair:
                start_date = today - datetime.timedelta(days=max(90, US_ADJUSTMENT_REPAIR_LOOKBACK_DAYS))
                print(f"   -> 复权质量修复，整段重拉起始日: {start_date}")
            else:
                incremental_start = _get_incremental_start(last_date, today)
                start_date = incremental_start
            if not force_adjustment_repair and start_date is not None:
                start_date = incremental_start
                print(f"   -> 增量更新起始日: {start_date}")
            elif not force_adjustment_repair:
                start_date = today - datetime.timedelta(days=max(2, US_BACKFILL_DAYS_PER_RUN))
                print(f"   -> 首次抓取分层回填，先拉近 {US_BACKFILL_DAYS_PER_RUN} 天: {start_date}")

            if start_date > today:
                symbol_stats["skipped"] += 1
                print("   -> 已是最新，无需更新")
                continue

            data, source_name, switches, attempts = fetch_with_fallback(
                symbol=symbol,
                start_date=start_date,
                end_date=None,
                source_priority=US_SOURCE_PRIORITY,
            )

            if source_name is None:
                symbol_stats["failed_symbols"] += 1
                print(f"   ❌ 全部数据源失败 | attempts={'; '.join(attempts)}")
                continue

            if data.empty:
                symbol_stats["empty_symbols"] += 1
                print(f"   ⚠️ 无新增数据 (source={source_name}, switches={switches})")
                continue

            if force_adjustment_repair and source_name != "tiingo" and _has_unresolved_scale_break(data):
                symbol_stats["failed_symbols"] += 1
                print(
                    f"   ❌ {source_name}整段历史仍有未复权尺度断层，本轮拒绝覆盖既有数据；"
                    "等待可信复权源恢复"
                )
                continue

            if source_name == "tiingo" and _has_split_event(data):
                repair_start = today - datetime.timedelta(days=max(90, US_ADJUSTMENT_REPAIR_LOOKBACK_DAYS))
                if start_date > repair_start:
                    print(f"   -> Tiingo返回拆股事件，改为整段重拉: {repair_start}")
                    repair_data, repair_source, repair_switches, repair_attempts = fetch_with_fallback(
                        symbol=symbol,
                        start_date=repair_start,
                        end_date=None,
                        source_priority=["tiingo"],
                    )
                    if repair_source != "tiingo" or repair_data.empty:
                        symbol_stats["failed_symbols"] += 1
                        print(
                            "   ❌ 拆股历史整段重拉失败，本轮不写入局部数据 | attempts="
                            + "; ".join(repair_attempts)
                        )
                        continue
                    data = repair_data
                    source_name = repair_source
                    switches += repair_switches

            rows = save_symbol_data(symbol, data)
            symbol_stats["success_symbols"] += 1
            symbol_stats["saved_rows"] += rows
            print(f"   ✅ 成功保存 {rows} 条 (source={source_name}, switches={switches})")

            del data
            gc.collect()
        except Exception as e:
            symbol_stats["failed_symbols"] += 1
            print(f"   ❌ 处理失败: {e}")
        finally:
            time.sleep(0.2)

    backfill_stats = (
        {"attempted": 0, "saved": 0, "failed": 0, "skipped": 0}
        if repair_only
        else _run_low_priority_backfill(today, run_symbols)
    )

    print("\n📊 源级汇总")
    for key in ["akshare", "tiingo", "twelvedata"]:
        provider = PROVIDERS[key]
        s = provider.state.stats
        print(
            f"   - {key}: success={s['success']}, failed={s['failed']}, "
            f"rate_limited={s['rate_limited']}, skipped={s['skipped']}, "
            f"cooldown_until={_provider_cooldown_text(provider)}"
        )

    print("\n📌 任务汇总")
    print(
        "   "
        + ", ".join(
            [
                f"symbol_success={symbol_stats['success_symbols']}",
                f"symbol_failed={symbol_stats['failed_symbols']}",
                f"symbol_empty={symbol_stats['empty_symbols']}",
                f"symbol_skipped={symbol_stats['skipped']}",
                f"saved_rows={symbol_stats['saved_rows']}",
                f"backfill_attempted={backfill_stats['attempted']}",
                f"backfill_saved={backfill_stats['saved']}",
                f"backfill_failed={backfill_stats['failed']}",
                f"backfill_skipped={backfill_stats['skipped']}",
            ]
        )
    )
    print("🏁 全部更新完成")


def _command_line() -> None:
    parser = argparse.ArgumentParser(description="更新并校验本地美股复权日线")
    parser.add_argument(
        "--audit-adjustments",
        action="store_true",
        help="只读扫描需重建的复权/源冲突标的，不写数据库",
    )
    parser.add_argument(
        "--repair-adjustments-only",
        action="store_true",
        help="仅重拉审计命中的异常标的，跳过普通标的和低优先级回填",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="可选，逗号分隔的代码白名单，例如 KLAC,BKNG,CRWD,BK",
    )
    args = parser.parse_args()
    selected = [item.strip().upper() for item in str(args.symbols).split(",") if item.strip()] or SYMBOLS
    if args.audit_adjustments:
        repair = sorted(_find_adjustment_repair_symbols(datetime.date.today(), selected))
        print("复权/源冲突审计：" + (", ".join(repair) if repair else "未发现需重建标的"))
        return
    update_stock_data(symbols=selected, repair_only=bool(args.repair_adjustments_only))


if __name__ == "__main__":
    _command_line()

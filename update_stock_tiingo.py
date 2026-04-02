from __future__ import annotations

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
    "akshare,tiingo,twelvedata",
)
US_BACKFILL_SOURCE_PRIORITY = _parse_source_priority(
    os.getenv("US_BACKFILL_SOURCE_PRIORITY", ""),
    "akshare,twelvedata,tiingo",
)

US_BACKFILL_DAYS_PER_RUN = _env_int("US_BACKFILL_DAYS_PER_RUN", 120)
US_TARGET_HISTORY_DAYS = _env_int("US_TARGET_HISTORY_DAYS", 1095)
US_INCREMENTAL_LOOKBACK_DAYS = _env_int("US_INCREMENTAL_LOOKBACK_DAYS", 1)
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
        "AAPL", "MSFT", "NVDA", "GOOG", "META", "AMZN", "TSM", "AVGO", "AMD", "INTC",
        "ORCL", "ADBE", "CRM", "CSCO", "QCOM", "TXN", "MU", "AMAT", "LRCX", "KLAC","TSLA",
        "PANW", "CRWD", "PLTR", "SNOW", "NOW", "ANET", "CDNS", "SNPS", "INTU", "SHOP",
    ],
    # AI 主题补充（与 tech 并行维护，去重逻辑会自动处理重复代码）
    "ai_thematic": [
        "ARM", "MRVL", "SMCI", "DELL", "ASML", "TSM",
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
        "RKLB", "LUNR", "ASTS", "PL", "SPCE", "NOC", "LMT",
    ],
    # 稳定币/支付与交易基础设施（不含高波动矿股）
    "stablecoin_infra": [
        "COIN", "HOOD", "PYPL", "SQ", "MSTR",
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
        "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLB", "XLC", "SMH", "SOXX",
    ],
    # 资产配置/宏观高流动性 ETF
    "us_etf_macro": [
        "GLD", "SLV", "USO", "TLT", "IEF", "HYG", "LQD",
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
    return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "adjClose"])


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

    for c in ["open", "high", "low"]:
        out[c] = out[c].fillna(out["close"])
    out["volume"] = out["volume"].fillna(0)
    out["adjClose"] = out["adjClose"].fillna(out["close"])

    out = out.dropna(subset=["date", "close"])
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return out[["date", "symbol", "open", "high", "low", "close", "volume", "adjClose"]]


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

    def is_configured(self) -> bool:
        return bool(self.client is not None)

    def _fetch_impl(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: Optional[datetime.date] = None,
    ) -> pd.DataFrame:
        if not self.client:
            raise ProviderUnavailableError("TIINGO_API_KEY is missing")
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
        return _normalize_ohlcv_df(
            df=df,
            symbol=symbol,
            date_col="date",
            open_col="open",
            high_col="high",
            low_col="low",
            close_col="close",
            volume_col="volume",
            adj_close_col="adjClose",
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
    if suggested < floor_date:
        return floor_date
    return suggested


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


def update_stock_data() -> None:
    today = datetime.date.today()
    run_symbols = _select_symbols_for_run(SYMBOLS)
    print(f"🚀 开始更新美股日线，标的总数: {len(SYMBOLS)}，本轮处理: {len(run_symbols)}")
    print(f"🔗 源优先级: {US_SOURCE_PRIORITY}")
    print(f"🔁 回填源优先级: {US_BACKFILL_SOURCE_PRIORITY}")
    if not TWELVEDATA_API_KEY:
        print("⚠️ 未配置 TWELVEDATA_API_KEY，twelvedata 次源当前不可用")
    if US_SYMBOL_CHUNK_TOTAL > 1:
        print(f"🧩 分片模式: chunk {US_SYMBOL_CHUNK_INDEX % US_SYMBOL_CHUNK_TOTAL + 1}/{US_SYMBOL_CHUNK_TOTAL}")
    if US_MAX_SYMBOLS_PER_RUN > 0:
        print(f"🎯 本轮上限: {US_MAX_SYMBOLS_PER_RUN} 只")

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
            incremental_start = _get_incremental_start(last_date, today)
            if incremental_start is not None:
                start_date = incremental_start
                print(f"   -> 增量更新起始日: {start_date}")
            else:
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

    backfill_stats = _run_low_priority_backfill(today, run_symbols)

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


if __name__ == "__main__":
    update_stock_data()

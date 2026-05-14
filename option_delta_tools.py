from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy import text

try:
    from py_vollib_vectorized import vectorized_delta
except Exception:  # pragma: no cover
    vectorized_delta = None

ETF_UNDERLYING_MAP = {
    "50ETF": "510050.SH",
    "上证50": "510050.SH",
    "300ETF": "510300.SH",
    "沪深300": "510300.SH",
    "500ETF": "510500.SH",
    "中证500": "510500.SH",
    "创业板": "159915.SZ",
    "创业板ETF": "159915.SZ",
    "科创50": "588000.SH",
    "科创50ETF": "588000.SH",
}

ETF_MULTIPLIER = 10000
INDEX_OPTION_MULTIPLIER = 100

INDEX_OPTION_PREFIX_TO_UNDERLYING = {
    "IO": "000300.SH",
    "HO": "000016.SH",
    "MO": "000852.SH",
}

INDEX_OPTION_UNDERLYING_TO_PREFIX = {v: k for k, v in INDEX_OPTION_PREFIX_TO_UNDERLYING.items()}

INDEX_OPTION_NAME_HINTS = {
    "沪深300": "IO",
    "上证50": "HO",
    "中证1000": "MO",
}

_RISK_PROFILE_MAP = {
    "保守型": "conservative",
    "稳健型": "balanced",
    "激进型": "aggressive",
}

_DELTA_TARGET_BANDS = {
    "conservative": {
        "bullish": (0.10, 0.25),
        "neutral": (-0.05, 0.05),
        "bearish": (-0.25, -0.10),
    },
    "balanced": {
        "bullish": (0.20, 0.40),
        "neutral": (-0.10, 0.10),
        "bearish": (-0.40, -0.20),
    },
    "aggressive": {
        "bullish": (0.50, 2.00),
        "neutral": (-0.20, 0.20),
        "bearish": (-2.00, -0.60),
    },
}

DELTA_EXECUTION_COVERAGE_THRESHOLD = 0.60

_SIDE_SIGN = {"买方": 1, "买入": 1, "卖方": -1, "卖出": -1}
_CP_MAP = {"认购": "c", "认沽": "p"}

_LEG_PATTERNS = [
    re.compile(
        r"(?:(?P<month>\d{1,2})月)?\s*(?P<strike>\d+(?:\.\d+)?)\s*"
        r"(?P<cp>认购|认沽)\s*(?P<side>买方|卖方|买入|卖出)\s*(?P<qty>\d+)\s*张"
    ),
    re.compile(
        r"(?:(?P<month>\d{1,2})月)?\s*(?P<strike>\d+(?:\.\d+)?)\s*"
        r"(?P<side>买方|卖方|买入|卖出)\s*(?P<cp>认购|认沽)\s*(?P<qty>\d+)\s*张"
    ),
]


def _load_project_env() -> None:
    """
    worktree 下运行时，.env 往往在上级主目录（.../tradingart/.env），
    这里做向上查找，避免因 cwd 不同导致 DB 配置丢失。
    """
    current_file = Path(__file__).resolve()
    for parent in [current_file.parent, *current_file.parents]:
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=False)
            return
    load_dotenv(override=False)


@lru_cache(maxsize=1)
def _get_db_engine_cached():
    _load_project_env()
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    if not all([db_user, db_password, db_host, db_name]):
        return None
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    try:
        return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
    except Exception:
        return None


@dataclass
class OptionLeg:
    month: Optional[int]
    strike: float
    cp_text: str
    side_text: str
    qty: int
    signed_qty: int
    option_flag: str
    source_text: str


def detect_etf_underlying(text: str, symbol_hint: str = "") -> str:
    text_u = f"{symbol_hint} {text}".upper()
    for name, code in ETF_UNDERLYING_MAP.items():
        if name.upper() in text_u:
            return code

    m = re.search(r"(510\d{3}|159\d{3}|588\d{3})", text_u)
    if m:
        raw = m.group(1)
        return f"{raw}.SZ" if raw.startswith("159") else f"{raw}.SH"

    hint = str(symbol_hint or "").strip().upper()
    if re.fullmatch(r"\d{6}(?:\.[A-Z]{2})?", hint):
        if "." in hint:
            return hint
        return f"{hint}.SZ" if hint.startswith("159") else f"{hint}.SH"
    return ""


def _normalize_underlying_hint(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    if raw in INDEX_OPTION_PREFIX_TO_UNDERLYING:
        return raw
    if raw in INDEX_OPTION_UNDERLYING_TO_PREFIX:
        return INDEX_OPTION_UNDERLYING_TO_PREFIX[raw]
    if raw in {"000300", "000300.SH"}:
        return "IO"
    if raw in {"000016", "000016.SH"}:
        return "HO"
    if raw in {"000852", "000852.SH"}:
        return "MO"
    if re.fullmatch(r"(510\d{3}|588\d{3})", raw):
        return f"{raw}.SH"
    if re.fullmatch(r"159\d{3}", raw):
        return f"{raw}.SZ"
    if re.fullmatch(r"\d{6}\.(SH|SZ)", raw):
        return raw
    return raw


def _normalize_option_contract_code(value: Any) -> str:
    code = str(value or "").strip().upper().replace(" ", "")
    if not code:
        return ""
    code = code.replace("SHSE:", "").replace("SZSE:", "")
    code = code.replace(".SSE", ".SH").replace(".SS", ".SH")
    return code


def _derive_option_cn_labels(
    option_flag: Any = None,
    cp_text: Any = None,
    side_text: Any = None,
    signed_qty: Any = None,
) -> Dict[str, str]:
    flag_raw = str(option_flag or "").strip().lower()
    cp_raw = str(cp_text or "").strip()
    if flag_raw in {"c", "call"} or cp_raw == "认购":
        cp_cn = "认购"
    elif flag_raw in {"p", "put"} or cp_raw == "认沽":
        cp_cn = "认沽"
    else:
        cp_cn = "待确认"

    side_raw = str(side_text or "").strip()
    if side_raw in {"买方", "买入"}:
        side_cn = "买方"
    elif side_raw in {"卖方", "卖出"}:
        side_cn = "卖方"
    else:
        if signed_qty is None:
            side_cn = "待确认"
        else:
            side_cn = "买方" if int(float(signed_qty)) >= 0 else "卖方"

    if cp_cn == "待确认" or side_cn == "待确认":
        direction_cn = "待确认"
    else:
        direction_cn = ("买" if side_cn == "买方" else "卖") + cp_cn
    return {"cp_cn": cp_cn, "side_cn": side_cn, "direction_cn": direction_cn}


def _attach_option_cn_labels(leg: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(leg or {})
    labels = _derive_option_cn_labels(
        option_flag=out.get("option_flag"),
        cp_text=out.get("cp_text"),
        side_text=out.get("side_text"),
        signed_qty=out.get("signed_qty"),
    )
    out.update(labels)
    return out


def detect_option_underlying(
    text: str,
    symbol_hint: str = "",
    vision_legs: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, str]:
    for leg in vision_legs or []:
        hint = _normalize_underlying_hint((leg or {}).get("underlying_hint"))
        if hint in INDEX_OPTION_PREFIX_TO_UNDERLYING:
            return {
                "asset_class": "index",
                "underlying_code": INDEX_OPTION_PREFIX_TO_UNDERLYING[hint],
                "contract_prefix": hint,
            }
        if re.fullmatch(r"(510\d{3}|159\d{3}|588\d{3})\.(SH|SZ)", hint):
            return {"asset_class": "etf", "underlying_code": hint, "contract_prefix": ""}

    text_u = f"{symbol_hint} {text}".upper()
    for prefix, code in INDEX_OPTION_PREFIX_TO_UNDERLYING.items():
        if re.search(rf"\b{prefix}\b", text_u):
            return {"asset_class": "index", "underlying_code": code, "contract_prefix": prefix}
    for name, prefix in INDEX_OPTION_NAME_HINTS.items():
        if name.upper() in text_u:
            return {
                "asset_class": "index",
                "underlying_code": INDEX_OPTION_PREFIX_TO_UNDERLYING[prefix],
                "contract_prefix": prefix,
            }

    etf_code = detect_etf_underlying(text=text, symbol_hint=symbol_hint)
    if etf_code:
        return {"asset_class": "etf", "underlying_code": etf_code, "contract_prefix": ""}

    return {"asset_class": "", "underlying_code": "", "contract_prefix": ""}


def parse_etf_option_legs(text: str) -> List[Dict[str, Any]]:
    raw = str(text or "")
    legs: List[OptionLeg] = []
    taken_spans = set()

    for pattern in _LEG_PATTERNS:
        for m in pattern.finditer(raw):
            span = m.span()
            if span in taken_spans:
                continue
            taken_spans.add(span)
            month = m.group("month")
            cp_text = m.group("cp")
            side_text = m.group("side")
            qty = int(m.group("qty"))
            signed_qty = _SIDE_SIGN.get(side_text, 0) * qty
            flag = _CP_MAP.get(cp_text)
            if not flag or signed_qty == 0:
                continue
            legs.append(
                OptionLeg(
                    month=int(month) if month else None,
                    strike=float(m.group("strike")),
                    cp_text=cp_text,
                    side_text=side_text,
                    qty=qty,
                    signed_qty=signed_qty,
                    option_flag=flag,
                    source_text=m.group(0),
                )
            )

    legs.sort(key=lambda x: x.source_text)
    return [
        _attach_option_cn_labels(
            {
            "month": leg.month,
            "strike": leg.strike,
            "cp_text": leg.cp_text,
            "side_text": leg.side_text,
            "qty": leg.qty,
            "signed_qty": leg.signed_qty,
            "option_flag": leg.option_flag,
            "source_text": leg.source_text,
            }
        )
        for leg in legs
    ]


class ETFOptionMarketLoader:
    def __init__(self, engine=None):
        self.engine = engine or _get_db_engine_cached()

    def get_underlying_spot(self, underlying_code: str) -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        table = "index_price" if str(underlying_code).upper() in INDEX_OPTION_UNDERLYING_TO_PREFIX else "stock_price"
        try:
            sql = text(
                f"""
                SELECT ts_code, trade_date, close_price
                FROM {table}
                WHERE ts_code = :code
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
            df = pd.read_sql(sql, self.engine, params={"code": underlying_code})
            if df.empty and table == "index_price":
                sql_stock = text(
                    """
                    SELECT ts_code, trade_date, close_price
                    FROM stock_price
                    WHERE ts_code = :code
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """
                )
                df = pd.read_sql(sql_stock, self.engine, params={"code": underlying_code})
        except Exception:
            return None
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            "ts_code": str(row.get("ts_code")),
            "trade_date": str(row.get("trade_date")),
            "close_price": float(row.get("close_price")),
        }

    def get_latest_iv(self, underlying_code: str) -> Optional[Dict[str, Any]]:
        if str(underlying_code).upper() in INDEX_OPTION_UNDERLYING_TO_PREFIX:
            return self.get_latest_option_iv(underlying_code=underlying_code)
        if self.engine is None:
            return None
        try:
            sql = text(
                """
                SELECT etf_code, trade_date, iv
                FROM etf_iv_history
                WHERE etf_code = :code
                ORDER BY trade_date DESC
                LIMIT 1
                """
            )
            df = pd.read_sql(sql, self.engine, params={"code": underlying_code})
        except Exception:
            return None
        if df.empty:
            return None
        row = df.iloc[0]
        return {
            "etf_code": str(row.get("etf_code")),
            "trade_date": str(row.get("trade_date")),
            "iv": float(row.get("iv")),
        }

    def get_latest_option_iv(self, underlying_code: str, contract_prefix: str = "") -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        code = str(underlying_code or "").upper()
        prefix = str(contract_prefix or INDEX_OPTION_UNDERLYING_TO_PREFIX.get(code, "")).upper()
        if prefix in INDEX_OPTION_PREFIX_TO_UNDERLYING:
            try:
                sql = text(
                    """
                    SELECT ts_code, trade_date, iv
                    FROM commodity_iv_history
                    WHERE ts_code LIKE :prefix
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """
                )
                df = pd.read_sql(sql, self.engine, params={"prefix": f"{prefix}%"})
            except Exception:
                return None
            if df.empty:
                return None
            row = df.iloc[0]
            return {
                "etf_code": code,
                "trade_date": str(row.get("trade_date")),
                "iv": float(row.get("iv")),
                "ts_code": str(row.get("ts_code")),
            }
        return self.get_latest_iv(code)

    def find_option_contract(
        self,
        underlying_code: str,
        option_flag: str,
        strike: float,
        month: Optional[int],
        as_of_yyyymmdd: str,
    ) -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        cp = option_flag.upper()
        is_index_underlying = str(underlying_code).upper() in INDEX_OPTION_UNDERLYING_TO_PREFIX
        if is_index_underlying:
            prefix = INDEX_OPTION_UNDERLYING_TO_PREFIX[str(underlying_code).upper()]
            sql = text(
                """
                SELECT ts_code, call_put, exercise_price, delist_date
                FROM option_basic
                WHERE call_put = :cp
                  AND delist_date >= :as_of_date
                  AND (underlying = :underlying OR ts_code LIKE :prefix_like)
                ORDER BY delist_date ASC
                """
            )
            params = {
                "underlying": underlying_code,
                "cp": cp,
                "as_of_date": as_of_yyyymmdd,
                "prefix_like": f"{prefix}%",
            }
        else:
            sql = text(
                """
                SELECT ts_code, call_put, exercise_price, delist_date
                FROM option_basic
                WHERE underlying = :underlying
                  AND call_put = :cp
                  AND delist_date >= :as_of_date
                ORDER BY delist_date ASC
                """
            )
            params = {"underlying": underlying_code, "cp": cp, "as_of_date": as_of_yyyymmdd}
        try:
            df = pd.read_sql(sql, self.engine, params=params)
        except Exception:
            return {
                "missing_reason": "查询合约失败（数据源异常）",
                "status": "loader_error",
            }
        if df.empty:
            return None

        work = df.copy()
        work["strike_num"] = pd.to_numeric(work["exercise_price"], errors="coerce")
        work["month_num"] = work["delist_date"].astype(str).str[4:6].astype(int)
        work = work.dropna(subset=["strike_num"])
        if month is not None:
            month_work = work[work["month_num"] == int(month)]
            if month_work.empty:
                return {
                    "missing_reason": f"未找到{month}月可交易合约",
                    "status": "missing_month",
                }
            work = month_work

        work["diff"] = (work["strike_num"] - float(strike)).abs()
        best = work.sort_values(["diff", "delist_date"]).iloc[0]
        matched_exact = float(best["diff"]) < 1e-8

        px_sql = text(
            """
            SELECT trade_date, close, vol, oi
            FROM option_daily
            WHERE ts_code = :ts_code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        try:
            df_px = pd.read_sql(px_sql, self.engine, params={"ts_code": str(best["ts_code"])})
        except Exception:
            return {
                "missing_reason": f"合约{best['ts_code']}价格查询失败（数据源异常）",
                "status": "loader_error",
                "ts_code": str(best["ts_code"]),
                "delist_date": str(best["delist_date"]),
                "exercise_price": float(best["strike_num"]),
            }
        if df_px.empty:
            return {
                "missing_reason": f"合约{best['ts_code']}暂无最新收盘数据",
                "status": "missing_price",
                "ts_code": str(best["ts_code"]),
                "delist_date": str(best["delist_date"]),
                "exercise_price": float(best["strike_num"]),
            }
        row_px = df_px.iloc[0]
        return {
            "status": "ok",
            "ts_code": str(best["ts_code"]),
            "delist_date": str(best["delist_date"]),
            "exercise_price": float(best["strike_num"]),
            "is_exact_strike": matched_exact,
            "trade_date": str(row_px.get("trade_date")),
            "close": float(row_px.get("close")),
            "vol": float(row_px.get("vol") or 0),
            "oi": float(row_px.get("oi") or 0),
        }

    def get_contract_by_ts_code(self, ts_code: str, as_of_yyyymmdd: str) -> Optional[Dict[str, Any]]:
        if self.engine is None:
            return None
        code = _normalize_option_contract_code(ts_code)
        if not code:
            return None
        candidates = [code]
        if "." not in code and re.fullmatch(r"\d{7,9}", code):
            candidates.extend([f"{code}.SH", f"{code}.SZ"])
        df = pd.DataFrame()
        sql = text(
            """
            SELECT ts_code, underlying, call_put, exercise_price, delist_date
            FROM option_basic
            WHERE ts_code = :ts_code
            LIMIT 1
            """
        )
        last_err = None
        matched_code = code
        for cand in candidates:
            try:
                df = pd.read_sql(sql, self.engine, params={"ts_code": cand})
            except Exception as e:
                last_err = e
                continue
            if not df.empty:
                matched_code = cand
                break
        if df.empty and last_err is not None:
            return {"status": "loader_error", "missing_reason": "合约元数据查询失败", "ts_code": code}
        if df.empty:
            return {"status": "missing_contract", "missing_reason": f"未找到合约{code}元数据", "ts_code": code}
        row = df.iloc[0]
        px_sql = text(
            """
            SELECT trade_date, close, vol, oi
            FROM option_daily
            WHERE ts_code = :ts_code
            ORDER BY trade_date DESC
            LIMIT 1
            """
        )
        try:
            df_px = pd.read_sql(px_sql, self.engine, params={"ts_code": matched_code})
        except Exception:
            return {"status": "loader_error", "missing_reason": f"合约{matched_code}价格查询失败", "ts_code": matched_code}
        if df_px.empty:
            return {"status": "missing_price", "missing_reason": f"合约{matched_code}暂无最新收盘数据", "ts_code": matched_code}
        row_px = df_px.iloc[0]
        return {
            "status": "ok",
            "ts_code": matched_code,
            "underlying": str(row.get("underlying") or ""),
            "call_put": str(row.get("call_put") or "").upper(),
            "delist_date": str(row.get("delist_date")),
            "exercise_price": float(row.get("exercise_price")),
            "trade_date": str(row_px.get("trade_date")),
            "close": float(row_px.get("close")),
            "vol": float(row_px.get("vol") or 0),
            "oi": float(row_px.get("oi") or 0),
        }


def _normalize_underlying_code_for_quote(value: Any) -> str:
    code = _normalize_underlying_hint(value)
    if not code:
        return ""
    if code in INDEX_OPTION_PREFIX_TO_UNDERLYING:
        return INDEX_OPTION_PREFIX_TO_UNDERLYING[code]
    if re.fullmatch(r"\d{6}", code):
        return f"{code}.SZ" if code.startswith("159") else f"{code}.SH"
    if re.fullmatch(r"\d{6}\.(SH|SZ)", code):
        return code
    return ""


def fetch_underlying_spot_map(
    underlyings: List[str],
    as_of_date: Optional[str] = None,
    loader: Optional[ETFOptionMarketLoader] = None,
) -> Dict[str, Dict[str, Any]]:
    """
    批量获取多标的权威现价（ETF/指数），缺失时显式标注 missing。
    返回结构：
    {
      "510500.SH": {
        "ts_code": "510500.SH",
        "close_price": 8.111,
        "trade_date": "20260415",
        "source": "stock_price",
        "missing": False
      },
      ...
    }
    """
    market_loader = loader or ETFOptionMarketLoader()
    as_of_yyyymmdd = _norm_date_str(as_of_date) if as_of_date else ""
    out: Dict[str, Dict[str, Any]] = {}
    normalized_codes: List[str] = []
    for item in underlyings or []:
        code = _normalize_underlying_code_for_quote(item)
        if code and code not in normalized_codes:
            normalized_codes.append(code)

    engine = getattr(market_loader, "engine", None)
    for code in normalized_codes:
        source_table = "index_price" if code in INDEX_OPTION_UNDERLYING_TO_PREFIX else "stock_price"
        quote: Optional[Dict[str, Any]] = None
        if engine is not None and as_of_yyyymmdd:
            try:
                sql = text(
                    f"""
                    SELECT ts_code, trade_date, close_price
                    FROM {source_table}
                    WHERE ts_code = :code AND trade_date <= :as_of
                    ORDER BY trade_date DESC
                    LIMIT 1
                    """
                )
                df = pd.read_sql(sql, engine, params={"code": code, "as_of": as_of_yyyymmdd})
                if df.empty and source_table == "index_price":
                    fallback_sql = text(
                        """
                        SELECT ts_code, trade_date, close_price
                        FROM stock_price
                        WHERE ts_code = :code AND trade_date <= :as_of
                        ORDER BY trade_date DESC
                        LIMIT 1
                        """
                    )
                    df = pd.read_sql(fallback_sql, engine, params={"code": code, "as_of": as_of_yyyymmdd})
                    source_table = "stock_price"
                if not df.empty:
                    row = df.iloc[0]
                    close_price = _coerce_positive_float(row.get("close_price"))
                    if close_price is not None:
                        quote = {
                            "ts_code": str(row.get("ts_code") or code).upper(),
                            "trade_date": str(row.get("trade_date") or ""),
                            "close_price": float(close_price),
                        }
            except Exception:
                quote = None
        if quote is None:
            try:
                quote = market_loader.get_underlying_spot(code)
            except Exception:
                quote = None

        if quote and _coerce_positive_float(quote.get("close_price")) is not None:
            out[code] = {
                "ts_code": str(quote.get("ts_code") or code).upper(),
                "close_price": float(quote.get("close_price")),
                "trade_date": str(quote.get("trade_date") or ""),
                "source": source_table,
                "missing": False,
            }
        else:
            out[code] = {
                "ts_code": code,
                "close_price": None,
                "trade_date": "",
                "source": source_table,
                "missing": True,
                "missing_reason": "未找到标的最新收盘价",
            }

    return out


def _normalize_iv(iv_value: Optional[float]) -> Optional[float]:
    if iv_value is None:
        return None
    iv = float(iv_value)
    if iv <= 0:
        return None
    if iv > 1:
        iv = iv / 100.0
    return max(iv, 1e-4)


def _coerce_positive_float(value: Any) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if v <= 0:
        return None
    return v


def _norm_date_str(dt: Optional[str] = None) -> str:
    if dt:
        return str(dt).replace("-", "")[:8]
    return datetime.now().strftime("%Y%m%d")


def _annualized_t(as_of_yyyymmdd: str, delist_yyyymmdd: str) -> float:
    try:
        as_of_dt = datetime.strptime(as_of_yyyymmdd, "%Y%m%d")
        exp_dt = datetime.strptime(str(delist_yyyymmdd)[:8], "%Y%m%d")
    except Exception:
        return 1.0 / 365.0
    days = max((exp_dt - as_of_dt).days, 1)
    return max(days / 365.0, 1.0 / 365.0)


def compute_delta_cash_metrics(
    legs: List[Dict[str, Any]],
    underlying_price: float,
    multiplier: int = ETF_MULTIPLIER,
) -> Dict[str, float]:
    gross_notional = 0.0
    for leg in legs:
        leg_multiplier = float(leg.get("multiplier") or multiplier)
        gross_notional += float(underlying_price) * leg_multiplier * abs(int(leg.get("signed_qty", 0)))
    total_delta_cash = 0.0
    valid = 0
    for leg in legs:
        delta = leg.get("delta")
        if delta is None:
            continue
        valid += 1
        leg_multiplier = float(leg.get("multiplier") or multiplier)
        total_delta_cash += (
            float(leg.get("signed_qty", 0)) * float(delta) * float(underlying_price) * leg_multiplier
        )
    delta_ratio = total_delta_cash / gross_notional if gross_notional > 0 else 0.0
    coverage_ratio = valid / len(legs) if legs else 0.0
    return {
        "total_delta_cash": total_delta_cash,
        "gross_notional": gross_notional,
        "delta_ratio": delta_ratio,
        "coverage_ratio": coverage_ratio,
    }


def _classify_delta_coverage(coverage_ratio: float, has_legs: bool) -> Dict[str, Any]:
    coverage = float(coverage_ratio or 0.0)
    if (not has_legs) or coverage <= 0.0:
        return {"coverage_tier": "gap", "displayable": False, "execution_ready": False}
    if coverage >= float(DELTA_EXECUTION_COVERAGE_THRESHOLD):
        return {"coverage_tier": "full", "displayable": True, "execution_ready": True}
    return {"coverage_tier": "partial", "displayable": True, "execution_ready": False}


def _normalize_risk_profile(risk_preference: str) -> str:
    text = re.sub(
        r"(不是|并非|不算|别按|不要按|不再按|不是很)\s*(偏?保守|保守型|conservative)",
        "",
        str(risk_preference or ""),
        flags=re.IGNORECASE,
    )
    text_l = text.lower()
    if "aggress" in text_l:
        return "aggressive"
    if any(k in text for k in ["偏保守", "保守", "低风险"]):
        return "conservative"
    if any(k in text for k in ["偏激进", "偏积极", "激进", "积极", "高风险"]):
        return "aggressive"
    for cn, key in _RISK_PROFILE_MAP.items():
        if cn in text:
            return key
    if "conserv" in text_l:
        return "conservative"
    return "balanced"


def _normalize_trend_signal(trend_signal: str) -> str:
    text = str(trend_signal or "")
    if any(k in text for k in ["看涨", "多头", "上涨", "bull", "Bull"]):
        return "bullish"
    if any(k in text for k in ["看跌", "空头", "下跌", "bear", "Bear"]):
        return "bearish"
    return "neutral"


def get_delta_target_band(trend_signal: str, risk_preference: str) -> Dict[str, Any]:
    risk_key = _normalize_risk_profile(risk_preference)
    trend_key = _normalize_trend_signal(trend_signal)
    low, high = _DELTA_TARGET_BANDS[risk_key][trend_key]
    return {
        "risk_key": risk_key,
        "trend_key": trend_key,
        "low": float(low),
        "high": float(high),
        "mid": float((low + high) / 2.0),
    }


def build_delta_adjustment(
    total_delta_cash: float,
    gross_notional: float,
    trend_signal: str,
    risk_preference: str,
    ratio_base: Optional[float] = None,
    ratio_basis: str = "gross_notional",
) -> Dict[str, Any]:
    band = get_delta_target_band(trend_signal=trend_signal, risk_preference=risk_preference)
    effective_base = _coerce_positive_float(ratio_base)
    if effective_base is None:
        effective_base = float(gross_notional)
    current_ratio = float(total_delta_cash) / effective_base if effective_base > 0 else 0.0
    mid = band["mid"]
    adjust_cash = (mid - current_ratio) * effective_base if effective_base > 0 else 0.0
    in_band = band["low"] <= current_ratio <= band["high"]
    if effective_base <= 0:
        action = "名义敞口为0，无法计算调整量"
    elif adjust_cash > 0:
        action = "需提高 Delta Cash（减空/加多）"
    elif adjust_cash < 0:
        action = "需降低 Delta Cash（减多/加空）"
    else:
        action = "当前已接近目标，无需调整"
    return {
        "band": band,
        "current_ratio": current_ratio,
        "target_mid": mid,
        "adjust_cash": adjust_cash,
        "in_band": in_band,
        "action": action,
        "ratio_base": effective_base,
        "ratio_basis": str(ratio_basis or "gross_notional"),
    }


def _normalize_structured_option_legs(legs: Optional[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in legs or []:
        if not isinstance(item, dict):
            continue
        strike = _coerce_positive_float(item.get("strike") or item.get("exercise_price") or item.get("行权价"))
        contract_code = _normalize_option_contract_code(item.get("contract_code") or item.get("ts_code")) or None
        cp_raw = str(
            item.get("cp")
            or item.get("option_flag")
            or item.get("call_put")
            or item.get("cp_text")
            or item.get("cp_cn")
            or ""
        ).strip().lower()
        if cp_raw in {"call", "认购", "c"}:
            option_flag = "c"
            cp_text = "认购"
        elif cp_raw in {"put", "认沽", "p"}:
            option_flag = "p"
            cp_text = "认沽"
        else:
            option_flag = ""
            cp_text = ""

        signed_qty = item.get("signed_qty")
        if signed_qty is None:
            qty = int(abs(float(item.get("qty") or item.get("quantity") or 0)))
            side_raw = str(
                item.get("side")
                or item.get("side_text")
                or item.get("side_cn")
                or item.get("direction_cn")
                or ""
            ).strip().lower()
            if side_raw in {"short", "卖方", "卖出"}:
                signed_qty = -qty
            elif side_raw in {"卖认购", "卖认沽"}:
                signed_qty = -qty
            else:
                signed_qty = qty
        signed_qty = int(signed_qty)
        if signed_qty == 0:
            continue

        qty = int(abs(signed_qty))
        side_text = "买方" if signed_qty > 0 else "卖方"
        month_val = item.get("month")
        month = int(month_val) if month_val not in (None, "") else None
        if strike is None and not contract_code:
            continue
        out.append(
            _attach_option_cn_labels(
                {
                "month": month,
                "strike": float(strike) if strike is not None else None,
                "cp_text": cp_text or "待确认",
                "side_text": side_text,
                "qty": qty,
                "signed_qty": signed_qty,
                "option_flag": option_flag or None,
                "source_text": str(item.get("source_text") or ""),
                "underlying_hint": _normalize_underlying_hint(item.get("underlying_hint") or item.get("underlying")),
                "contract_code": contract_code,
                }
            )
        )
    return out


def _infer_contract_multiplier(
    asset_class: str,
    underlying_code: str,
    contract: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    for key in ("multiplier", "contract_unit", "exercise_unit"):
        v = _coerce_positive_float((contract or {}).get(key))
        if v:
            return {"multiplier": float(v), "source": f"{key}"}
    if asset_class == "index" or str(underlying_code).upper() in INDEX_OPTION_UNDERLYING_TO_PREFIX:
        return {"multiplier": float(INDEX_OPTION_MULTIPLIER), "source": "index_default"}
    return {"multiplier": float(ETF_MULTIPLIER), "source": "etf_default"}


def _calc_delta(
    option_flag: str,
    spot: float,
    strike: float,
    t: float,
    r: float,
    sigma: float,
    q: float,
) -> Optional[float]:
    if vectorized_delta is None:
        return None
    try:
        res = vectorized_delta(
            flag=[option_flag],
            S=[spot],
            K=[strike],
            t=[t],
            r=[r],
            sigma=[sigma],
            q=[q],
            model="black_scholes",
            return_as="numpy",
        )
        if len(res) == 0:
            return None
        val = float(res[0])
        if val != val:  # NaN
            return None
        return val
    except Exception:
        return None


def _fmt_money(v: float) -> str:
    return f"{float(v):,.0f}"


def _build_delta_report(
    asset_class: str,
    underlying_code: str,
    spot_info: Optional[Dict[str, Any]],
    iv_info: Optional[Dict[str, Any]],
    legs: List[Dict[str, Any]],
    metrics: Dict[str, float],
    adjustment: Dict[str, Any],
    missing_notes: List[str],
    account_total_capital: Optional[float],
    displayable: bool,
    execution_ready: bool,
    coverage_tier: str,
) -> str:
    lines = []
    lines.append("### 【DeltaCash】")
    if not displayable:
        gap = "；".join(missing_notes) if missing_notes else "关键字段缺失，无法量化"
        lines.append(f"- 数据缺口: {gap}")
        lines.append("- 当前仅可给方向性建议，不输出金额级调仓量。")
        return "\n".join(lines)
    if spot_info:
        lines.append(
            f"- 标的: `{underlying_code}` | 现价: `{spot_info['close_price']:.4f}` | 日期: `{spot_info['trade_date']}`"
        )
    if iv_info:
        iv_raw = float(iv_info.get("iv", 0))
        lines.append(f"- 使用IV: `{iv_raw:.2f}%` | 日期: `{iv_info.get('trade_date')}`")
    if missing_notes:
        for note in missing_notes:
            lines.append(f"- 数据缺口: {note}")

    lines.append("")
    lines.append("| 腿 | 月份 | 方向 | 张数 | 行权价 | Delta | Delta Cash(元) | 状态 |")
    lines.append("|---|---:|---|---:|---:|---:|---:|---|")
    for i, leg in enumerate(legs, start=1):
        month_text = f"{leg.get('month')}月" if leg.get("month") else "待确认"
        direction = str(leg.get("direction_cn") or "").strip()
        if not direction or direction == "待确认":
            direction = f"{leg.get('cp_text')}{leg.get('side_text')}"
        delta = leg.get("delta")
        delta_text = f"{delta:+.4f}" if isinstance(delta, (int, float)) else "待确认"
        cash = leg.get("delta_cash")
        cash_text = _fmt_money(cash) if isinstance(cash, (int, float)) else "待确认"
        status = str(leg.get("status") or "")
        strike_num = _coerce_positive_float(leg.get("strike")) or _coerce_positive_float(leg.get("matched_strike"))
        strike_text = f"{float(strike_num):.3f}" if strike_num is not None else "待确认"
        lines.append(
            f"| {i} | {month_text} | {direction} | {int(leg.get('signed_qty', 0)):+d} | "
            f"{strike_text} | {delta_text} | {cash_text} | {status} |"
        )

    lines.append("")
    lines.append(f"- Total Delta Cash: `{_fmt_money(metrics.get('total_delta_cash', 0.0))}` 元")
    lines.append(f"- Gross Notional: `{_fmt_money(metrics.get('gross_notional', 0.0))}` 元")
    multiplier_items = []
    for leg in legs:
        m = leg.get("multiplier")
        if not isinstance(m, (int, float)):
            continue
        src = str(leg.get("multiplier_source") or "unknown")
        multiplier_items.append(f"{int(round(float(m)))}({src})")
    if multiplier_items:
        uniq = list(dict.fromkeys(multiplier_items))
        lines.append(f"- 合约乘数口径: `{', '.join(uniq)}`")
    lines.append(f"- 标的类别: `{asset_class or 'unknown'}`")
    ratio_basis = str(metrics.get("effective_ratio_basis", "gross_notional"))
    effective_ratio = float(metrics.get("effective_delta_ratio", metrics.get("delta_ratio", 0.0)))
    if ratio_basis == "account_total_capital":
        lines.append(f"- 账户总资金: `{_fmt_money(account_total_capital or 0.0)}` 元")
        lines.append(f"- 执行口径 Delta Ratio(账户): `{effective_ratio:+.4f}`")
        lines.append(f"- 参考口径 Delta Ratio(组合): `{metrics.get('delta_ratio', 0.0):+.4f}`")
    else:
        lines.append(f"- 执行口径 Delta Ratio(组合): `{effective_ratio:+.4f}`")
        lines.append("- ⚠️ 未提供账户总资金：请补充账户净资产/总资金，以便给出更精确的账户级Delta建议。")
    lines.append(f"- 覆盖率: `{metrics.get('coverage_ratio', 0.0) * 100:.1f}%`")
    lines.append(f"- 覆盖层级: `{coverage_tier}`")

    band = adjustment.get("band", {})
    lines.append(
        f"- 技术面目标区间: `[{band.get('low', 0.0):+.2f}, {band.get('high', 0.0):+.2f}]` "
        f"(目标中点 `{adjustment.get('target_mid', 0.0):+.2f}`)"
    )
    lines.append(
        f"- 当前偏离: `{adjustment.get('current_ratio', 0.0) - adjustment.get('target_mid', 0.0):+.4f}`"
    )
    if execution_ready:
        lines.append(
            f"- 建议调整量: `{_fmt_money(adjustment.get('adjust_cash', 0.0))}` 元 | {adjustment.get('action', '')}"
        )
    else:
        lines.append(
            f"- 建议方向: `{adjustment.get('action', '')}` | 覆盖率低于{int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%，暂不输出金额级调整量"
        )
        lines.append("- 补数清单: 请补齐缺失腿的IV/最新价/合约映射后再执行金额级调仓。")
    return "\n".join(lines)


def _normalize_trend_map(
    trend_map: Optional[Dict[str, str]],
    underlyings: List[str],
    default_trend: str,
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    raw = trend_map or {}
    for code in underlyings:
        trend = str(raw.get(code) or raw.get(code.split(".")[0]) or default_trend or "").strip()
        out[code] = trend or default_trend
    return out


def _resolve_leg_underlying_for_group(
    leg: Dict[str, Any],
    market_loader: Any,
    as_of_yyyymmdd: str,
) -> str:
    hint = _normalize_underlying_code_for_quote((leg or {}).get("underlying_hint"))
    if hint:
        return hint
    contract_code = _normalize_option_contract_code((leg or {}).get("contract_code"))
    if contract_code and hasattr(market_loader, "get_contract_by_ts_code"):
        try:
            cinfo = market_loader.get_contract_by_ts_code(contract_code, as_of_yyyymmdd=as_of_yyyymmdd)
        except Exception:
            cinfo = None
        if isinstance(cinfo, dict) and cinfo.get("status") == "ok":
            code = _normalize_underlying_code_for_quote(cinfo.get("underlying"))
            if code:
                return code
    return ""


def _group_legs_by_underlying(
    parsed_legs: List[Dict[str, Any]],
    market_loader: Any,
    as_of_yyyymmdd: str,
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    unresolved: List[Dict[str, Any]] = []
    for leg in parsed_legs:
        code = _resolve_leg_underlying_for_group(leg, market_loader=market_loader, as_of_yyyymmdd=as_of_yyyymmdd)
        if not code:
            unresolved.append(dict(leg))
            continue
        grouped.setdefault(code, []).append(dict(leg))
    return grouped, unresolved


def _build_risk_contribution_ranking(
    per_underlying: Dict[str, Dict[str, Any]],
    as_of_yyyymmdd: str,
    trend_map: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    abs_total = sum(abs(float((payload.get("metrics") or {}).get("total_delta_cash", 0.0))) for payload in per_underlying.values())
    ranking: List[Dict[str, Any]] = []
    as_of_date = datetime.strptime(as_of_yyyymmdd, "%Y%m%d").date()

    for code, payload in per_underlying.items():
        metrics = payload.get("metrics") or {}
        adjustment = payload.get("adjustment") or {}
        legs = [x for x in (payload.get("legs") or []) if isinstance(x, dict)]
        total_delta_cash = float(metrics.get("total_delta_cash", 0.0))
        contribution = (abs(total_delta_cash) / abs_total) if abs_total > 0 else 0.0
        trend_signal = str((trend_map or {}).get(code) or "")
        trend_key = _normalize_trend_signal(trend_signal)

        expiry_days: List[int] = []
        for leg in legs:
            delist_date = str(leg.get("delist_date") or "").strip()
            if not re.fullmatch(r"\d{8}", delist_date):
                continue
            try:
                expiry_days.append((datetime.strptime(delist_date, "%Y%m%d").date() - as_of_date).days)
            except Exception:
                continue
        min_expiry_days = min(expiry_days) if expiry_days else 999
        expiry_penalty = 0.20 if min_expiry_days <= 7 else (0.10 if min_expiry_days <= 15 else 0.0)

        short_call_qty = 0
        long_call_qty = 0
        low_liquidity_hits = 0
        for leg in legs:
            signed_qty = int(leg.get("signed_qty") or 0)
            cp_text = str(leg.get("cp_text") or "")
            if cp_text == "认购":
                if signed_qty < 0:
                    short_call_qty += abs(signed_qty)
                elif signed_qty > 0:
                    long_call_qty += signed_qty
            vol = float(leg.get("vol") or 0.0)
            oi = float(leg.get("oi") or 0.0)
            if vol <= 0 or oi <= 0 or vol < 100 or oi < 1000:
                low_liquidity_hits += 1
        naked_short_penalty = 0.15 if short_call_qty > long_call_qty else 0.0
        liquidity_penalty = min(0.12, 0.04 * low_liquidity_hits)

        score = contribution + expiry_penalty + naked_short_penalty + liquidity_penalty
        # 小风险腿默认不抢优先级；仅在看跌环境下提升保护优先级。
        if contribution < 0.20:
            if trend_key == "bearish":
                score += 0.05
            else:
                score *= 0.60
        ranking.append(
            {
                "underlying_code": code,
                "risk_contribution": contribution,
                "priority_score": score,
                "trend_signal": trend_signal,
                "total_delta_cash": total_delta_cash,
                "current_ratio": float(adjustment.get("current_ratio", 0.0)),
                "target_mid": float(adjustment.get("target_mid", 0.0)),
                "adjust_cash": float(adjustment.get("adjust_cash", 0.0)),
                "action": str(adjustment.get("action") or ""),
                "min_expiry_days": min_expiry_days if min_expiry_days < 900 else None,
                "naked_short_call_qty": short_call_qty,
                "low_liquidity_hits": low_liquidity_hits,
                "displayable": bool(payload.get("displayable")),
                "execution_ready": bool(payload.get("execution_ready")),
                "publishable": bool(payload.get("publishable")),
            }
        )
    ranking.sort(key=lambda x: x["priority_score"], reverse=True)
    for idx, row in enumerate(ranking, start=1):
        row["priority"] = f"P{idx}"
    return ranking


def _build_multi_delta_report(
    per_underlying: Dict[str, Dict[str, Any]],
    portfolio_summary: Dict[str, Any],
    risk_contribution_ranking: List[Dict[str, Any]],
    account_total_capital: Optional[float],
    execution_ready: bool,
) -> str:
    lines: List[str] = ["### 【DeltaCash】", ""]
    lines.append("| 标的 | Total Delta Cash(元) | Delta Ratio | 目标区间 | 当前偏离 | 建议调整量(元) | 数据状态 |")
    lines.append("|---|---:|---:|---|---:|---:|---|")
    for code, payload in per_underlying.items():
        metrics = payload.get("metrics") or {}
        adjustment = payload.get("adjustment") or {}
        band = adjustment.get("band") or {}
        row_displayable = bool(payload.get("displayable"))
        row_execution_ready = bool(payload.get("execution_ready"))
        if row_execution_ready:
            status = "可执行"
        elif row_displayable:
            status = "部分可用"
        else:
            status = "数据缺口"
        if not row_displayable:
            miss = payload.get("blocking_missing_notes") or payload.get("missing_notes") or []
            if miss:
                status = f"数据缺口:{str(miss[0])}"
        adjust_text = _fmt_money(adjustment.get("adjust_cash", 0.0)) if row_execution_ready else "-"
        lines.append(
            f"| {code} | {_fmt_money(metrics.get('total_delta_cash', 0.0))} | {float(metrics.get('effective_delta_ratio', metrics.get('delta_ratio', 0.0))):+.4f} | "
            f"[{float(band.get('low', 0.0)):+.2f}, {float(band.get('high', 0.0)):+.2f}] | "
            f"{float(adjustment.get('current_ratio', 0.0) - adjustment.get('target_mid', 0.0)):+.4f} | "
            f"{adjust_text} | {status} |"
        )

    lines.append("")
    lines.append(f"- 组合 Total Delta Cash: `{_fmt_money(portfolio_summary.get('total_delta_cash', 0.0))}` 元")
    lines.append(f"- 组合 Gross Notional: `{_fmt_money(portfolio_summary.get('gross_notional', 0.0))}` 元")
    basis = str(portfolio_summary.get("effective_ratio_basis") or "gross_notional")
    effective_ratio = float(portfolio_summary.get("effective_delta_ratio", portfolio_summary.get("delta_ratio", 0.0)))
    if basis == "account_total_capital":
        lines.append(f"- 账户总资金: `{_fmt_money(account_total_capital or 0.0)}` 元")
        lines.append(f"- 组合执行口径 Delta Ratio(账户): `{effective_ratio:+.4f}`")
    else:
        lines.append(f"- 组合执行口径 Delta Ratio(组合): `{effective_ratio:+.4f}`")
        lines.append("- ⚠️ 未提供账户总资金：请补充账户净资产/总资金，以便给出更精确的账户级Delta建议。")
    lines.append(f"- 组合覆盖率: `{float(portfolio_summary.get('coverage_ratio', 0.0)) * 100:.1f}%`")
    lines.append(f"- 覆盖层级: `{portfolio_summary.get('coverage_tier', 'gap')}`")

    portfolio_adjustment = portfolio_summary.get("adjustment") or {}
    band = portfolio_adjustment.get("band") or {}
    lines.append(
        f"- 组合技术面目标区间: `[{float(band.get('low', 0.0)):+.2f}, {float(band.get('high', 0.0)):+.2f}]` "
        f"(目标中点 `{float(portfolio_adjustment.get('target_mid', 0.0)):+.2f}`)"
    )
    lines.append(
        f"- 组合当前偏离: `{float(portfolio_adjustment.get('current_ratio', 0.0) - portfolio_adjustment.get('target_mid', 0.0)):+.4f}`"
    )
    if execution_ready:
        lines.append(
            f"- 组合建议调整量: `{_fmt_money(portfolio_adjustment.get('adjust_cash', 0.0))}` 元 | {portfolio_adjustment.get('action', '')}"
        )
    else:
        lines.append(
            f"- 组合建议方向: `{portfolio_adjustment.get('action', '')}` | 覆盖率低于{int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%，暂不输出金额级调整量"
        )
        lines.append("- 补数清单: 优先补齐缺数据标的的IV/合约最新价，再做金额级再平衡。")

    lines.append("")
    lines.append("#### 调仓优先队列（风险贡献最大腿优先）")
    lines.append("| 优先级 | 标的 | 风险贡献 | 优先分 | 建议方向 | 关键原因 |")
    lines.append("|---|---|---:|---:|---|---|")
    for row in risk_contribution_ranking:
        reasons: List[str] = []
        if row.get("min_expiry_days") is not None:
            reasons.append(f"近月到期{int(row['min_expiry_days'])}天")
        if int(row.get("naked_short_call_qty") or 0) > 0:
            reasons.append(f"净卖认购{int(row['naked_short_call_qty'])}张")
        if int(row.get("low_liquidity_hits") or 0) > 0:
            reasons.append(f"低流动性腿{int(row['low_liquidity_hits'])}个")
        if not reasons:
            reasons.append("风险贡献驱动")
        lines.append(
            f"| {row.get('priority')} | {row.get('underlying_code')} | {float(row.get('risk_contribution', 0.0)) * 100:.1f}% | "
            f"{float(row.get('priority_score', 0.0)):.3f} | {row.get('action', '')} | {'；'.join(reasons)} |"
        )
    return "\n".join(lines)


def _compute_option_delta_cash_multi(
    user_query: str,
    grouped_legs: Dict[str, List[Dict[str, Any]]],
    unresolved_legs: List[Dict[str, Any]],
    symbol_hint: str,
    trend_signal: str,
    trend_map: Optional[Dict[str, str]],
    risk_preference: str,
    loader: Any,
    as_of_date: Optional[str],
    r: float,
    q: float,
    account_total_capital: Optional[float],
    allowed_asset_classes: tuple,
) -> Dict[str, Any]:
    underlyings = sorted(grouped_legs.keys())
    normalized_trend_map = _normalize_trend_map(trend_map=trend_map, underlyings=underlyings, default_trend=trend_signal)

    per_underlying: Dict[str, Dict[str, Any]] = {}
    missing_notes: List[str] = []
    for code in underlyings:
        sub = compute_option_delta_cash(
            user_query=user_query,
            symbol_hint=code or symbol_hint,
            vision_legs=grouped_legs.get(code) or None,
            vision_domain="option",
            trend_signal=normalized_trend_map.get(code, trend_signal),
            trend_map=None,
            risk_preference=risk_preference,
            loader=loader,
            as_of_date=as_of_date,
            r=r,
            q=q,
            account_total_capital=None,
            allowed_asset_classes=allowed_asset_classes,
        )
        per_underlying[code] = sub
        for note in (sub.get("missing_notes") or []):
            prefixed = f"{code}: {note}"
            if prefixed not in missing_notes:
                missing_notes.append(prefixed)

    if unresolved_legs:
        msg = f"存在{len(unresolved_legs)}条腿未识别标的，未纳入Delta计算"
        if msg not in missing_notes:
            missing_notes.append(msg)

    total_delta_cash = sum(float((payload.get("metrics") or {}).get("total_delta_cash", 0.0)) for payload in per_underlying.values())
    gross_notional = sum(float((payload.get("metrics") or {}).get("gross_notional", 0.0)) for payload in per_underlying.values())
    coverage_num = sum(float((payload.get("metrics") or {}).get("coverage_ratio", 0.0)) * len(payload.get("legs") or []) for payload in per_underlying.values())
    leg_count = sum(len(payload.get("legs") or []) for payload in per_underlying.values())
    coverage_ratio = (coverage_num / leg_count) if leg_count > 0 else 0.0
    account_capital = _coerce_positive_float(account_total_capital)
    effective_base = account_capital if account_capital else gross_notional
    effective_basis = "account_total_capital" if account_capital else "gross_notional"
    effective_ratio = total_delta_cash / effective_base if effective_base > 0 else 0.0
    portfolio_adjustment = build_delta_adjustment(
        total_delta_cash=total_delta_cash,
        gross_notional=gross_notional,
        trend_signal=trend_signal,
        risk_preference=risk_preference,
        ratio_base=effective_base,
        ratio_basis=effective_basis,
    )
    portfolio_summary = {
        "total_delta_cash": total_delta_cash,
        "gross_notional": gross_notional,
        "delta_ratio": (total_delta_cash / gross_notional) if gross_notional > 0 else 0.0,
        "coverage_ratio": coverage_ratio,
        "effective_ratio_base": effective_base,
        "effective_ratio_basis": effective_basis,
        "effective_delta_ratio": effective_ratio,
        "adjustment": portfolio_adjustment,
    }
    if account_capital:
        portfolio_summary["account_total_capital"] = account_capital
        portfolio_summary["account_delta_ratio"] = effective_ratio

    risk_contribution_ranking = _build_risk_contribution_ranking(
        per_underlying=per_underlying,
        as_of_yyyymmdd=_norm_date_str(as_of_date),
        trend_map=normalized_trend_map,
    )

    displayable_any = any(bool(payload.get("displayable")) for payload in per_underlying.values())
    execution_ready_any = any(bool(payload.get("execution_ready")) for payload in per_underlying.values())
    coverage_state = _classify_delta_coverage(coverage_ratio=coverage_ratio, has_legs=(leg_count > 0))
    portfolio_execution_ready = bool(coverage_state["execution_ready"] and execution_ready_any)
    portfolio_displayable = bool(displayable_any)
    portfolio_summary["coverage_tier"] = str(coverage_state["coverage_tier"])
    portfolio_summary["displayable"] = portfolio_displayable
    portfolio_summary["execution_ready"] = portfolio_execution_ready

    report = _build_multi_delta_report(
        per_underlying=per_underlying,
        portfolio_summary=portfolio_summary,
        risk_contribution_ranking=risk_contribution_ranking,
        account_total_capital=account_capital,
        execution_ready=portfolio_execution_ready,
    ) if portfolio_displayable else (
        "### 【DeltaCash】\n- 数据缺口: 多标的腿均未形成可展示Delta，请补齐IV/最新价/合约映射后重算。"
    )

    blocking_missing_notes: List[str] = []
    if not portfolio_displayable:
        blocking_missing_notes.append("多标的Delta均未达到可展示条件")
    elif not portfolio_execution_ready:
        blocking_missing_notes.append(
            f"组合覆盖率低于{int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%，仅支持方向性建议"
        )
    return {
        "is_etf": False,
        "asset_class": "multi",
        "contract_prefix": "",
        "underlying_code": ",".join(underlyings),
        "legs": [leg for payload in per_underlying.values() for leg in (payload.get("legs") or [])],
        "metrics": portfolio_summary,
        "adjustment": portfolio_adjustment,
        "per_underlying": per_underlying,
        "portfolio_summary": portfolio_summary,
        "risk_contribution_ranking": risk_contribution_ranking,
        "trend_map_used": normalized_trend_map,
        "missing_notes": missing_notes,
        "blocking_missing_notes": blocking_missing_notes,
        "displayable": portfolio_displayable,
        "execution_ready": portfolio_execution_ready,
        "coverage_tier": str(coverage_state["coverage_tier"]),
        "publishable": portfolio_execution_ready,
        "report": report,
    }


def _build_empty_delta_result(
    trend_signal: str,
    risk_preference: str,
    is_etf: bool = False,
    underlying_code: str = "",
    missing_notes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    miss = [str(x) for x in (missing_notes or []) if str(x).strip()]
    adjustment = build_delta_adjustment(0.0, 0.0, trend_signal, risk_preference)
    metrics = {"total_delta_cash": 0.0, "gross_notional": 0.0, "delta_ratio": 0.0, "coverage_ratio": 0.0}
    return {
        "is_etf": bool(is_etf),
        "underlying_code": underlying_code,
        "legs": [],
        "metrics": metrics,
        "adjustment": adjustment,
        "missing_notes": miss,
        "blocking_missing_notes": miss,
        "displayable": False,
        "execution_ready": False,
        "coverage_tier": "gap",
        "publishable": False,
        "per_underlying": {},
        "portfolio_summary": {
            **metrics,
            "adjustment": adjustment,
            "displayable": False,
            "execution_ready": False,
            "coverage_tier": "gap",
        },
        "risk_contribution_ranking": [],
        "trend_map_used": {},
        "report": (
            f"### 【DeltaCash】\n- 数据缺口: {'；'.join(miss)}"
            if miss
            else ""
        ),
    }


def compute_option_delta_cash(
    user_query: str,
    symbol_hint: str = "",
    vision_legs: Optional[List[Dict[str, Any]]] = None,
    vision_domain: str = "",
    trend_signal: str = "",
    trend_map: Optional[Dict[str, str]] = None,
    risk_preference: str = "稳健型",
    loader: Optional[ETFOptionMarketLoader] = None,
    as_of_date: Optional[str] = None,
    r: float = 0.015,
    q: float = 0.0,
    account_total_capital: Optional[float] = None,
    allowed_asset_classes: tuple = ("etf", "index"),
) -> Dict[str, Any]:
    as_of_yyyymmdd = _norm_date_str(as_of_date)
    parsed_legs = _normalize_structured_option_legs(vision_legs) if vision_legs else parse_etf_option_legs(user_query)
    market_loader = loader or ETFOptionMarketLoader()

    normalized_leg_underlyings: List[str] = []
    for leg in parsed_legs:
        hint = _normalize_underlying_hint((leg or {}).get("underlying_hint"))
        if hint in INDEX_OPTION_PREFIX_TO_UNDERLYING:
            normalized_leg_underlyings.append(INDEX_OPTION_PREFIX_TO_UNDERLYING[hint])
            continue
        if re.fullmatch(r"(510\d{3}|159\d{3}|588\d{3})\.(SH|SZ)", hint):
            normalized_leg_underlyings.append(hint)
    unique_leg_underlyings = sorted({x for x in normalized_leg_underlyings if x})
    if len(unique_leg_underlyings) > 1:
        grouped_legs, unresolved_legs = _group_legs_by_underlying(
            parsed_legs=parsed_legs,
            market_loader=market_loader,
            as_of_yyyymmdd=as_of_yyyymmdd,
        )
        if len(grouped_legs) > 1:
            return _compute_option_delta_cash_multi(
                user_query=user_query,
                grouped_legs=grouped_legs,
                unresolved_legs=unresolved_legs,
                symbol_hint=symbol_hint,
                trend_signal=trend_signal,
                trend_map=trend_map,
                risk_preference=risk_preference,
                loader=market_loader,
                as_of_date=as_of_date,
                r=r,
                q=q,
                account_total_capital=account_total_capital,
                allowed_asset_classes=allowed_asset_classes,
            )
        if len(grouped_legs) == 1 and unresolved_legs:
            parsed_legs = list(grouped_legs.values())[0] + unresolved_legs
        elif len(grouped_legs) == 1:
            parsed_legs = list(grouped_legs.values())[0]
            unique_leg_underlyings = list(grouped_legs.keys())
        else:
            return _build_empty_delta_result(
                trend_signal=trend_signal,
                risk_preference=risk_preference,
                missing_notes=["识别到多标的期权腿，但无法确认每条腿对应标的，请补充合约代码或标的信息。"],
            )

    if not parsed_legs:
        return _build_empty_delta_result(
            trend_signal=trend_signal,
            risk_preference=risk_preference,
            missing_notes=["未识别到可计算的期权持仓腿，请补充“月份/行权价/认购认沽/买卖方向/张数”"],
        )

    detection = detect_option_underlying(user_query, symbol_hint=symbol_hint, vision_legs=parsed_legs)
    asset_class = str(detection.get("asset_class") or "").strip().lower()
    underlying_code = str(detection.get("underlying_code") or "").strip().upper()
    contract_prefix = str(detection.get("contract_prefix") or "").strip().upper()
    if asset_class and asset_class not in set(allowed_asset_classes):
        return _build_empty_delta_result(
            trend_signal=trend_signal,
            risk_preference=risk_preference,
            is_etf=(asset_class == "etf"),
            underlying_code=underlying_code,
            missing_notes=[f"当前仅支持{','.join(allowed_asset_classes)}期权计算"],
        )
    if not underlying_code and parsed_legs:
        for leg in parsed_legs:
            contract_code = str(leg.get("contract_code") or "").strip().upper()
            if not contract_code or not hasattr(market_loader, "get_contract_by_ts_code"):
                continue
            try:
                cinfo = market_loader.get_contract_by_ts_code(contract_code, as_of_yyyymmdd=as_of_yyyymmdd)
            except Exception:
                cinfo = None
            if not isinstance(cinfo, dict) or cinfo.get("status") != "ok":
                continue
            c_underlying = _normalize_underlying_hint(cinfo.get("underlying"))
            if c_underlying in INDEX_OPTION_PREFIX_TO_UNDERLYING:
                asset_class = "index"
                underlying_code = INDEX_OPTION_PREFIX_TO_UNDERLYING[c_underlying]
                contract_prefix = c_underlying
            else:
                asset_class = "etf"
                underlying_code = c_underlying
                contract_prefix = ""
            break

    if not underlying_code:
        return _build_empty_delta_result(
            trend_signal=trend_signal,
            risk_preference=risk_preference,
            missing_notes=["未识别到可计算的ETF/股指期权标的"],
        )

    try:
        spot_info = market_loader.get_underlying_spot(underlying_code)
    except Exception:
        spot_info = None
    try:
        if hasattr(market_loader, "get_latest_option_iv"):
            iv_info = market_loader.get_latest_option_iv(underlying_code, contract_prefix=contract_prefix)
        else:
            iv_info = market_loader.get_latest_iv(underlying_code)
    except Exception:
        iv_info = None
    sigma = _normalize_iv(iv_info.get("iv")) if iv_info else None

    missing_notes: List[str] = []
    blocking_notes: List[str] = []
    if not spot_info:
        msg = "未找到标的最新收盘价"
        missing_notes.append(msg)
        blocking_notes.append(msg)
    if sigma is None:
        msg = "未找到可用IV数据"
        missing_notes.append(msg)
        blocking_notes.append(msg)

    legs_out: List[Dict[str, Any]] = []
    for leg in parsed_legs:
        leg_out = dict(leg)
        leg_out.update({"status": "ok", "delta": None, "delta_cash": None, "multiplier": None, "multiplier_source": ""})
        leg_out = _attach_option_cn_labels(leg_out)
        leg_contract_code = _normalize_option_contract_code(leg.get("contract_code"))
        option_flag = str(leg.get("option_flag") or "").strip().lower() or None
        strike_val = _coerce_positive_float(leg.get("strike"))
        month_val = leg.get("month")
        underlying_for_leg = str(underlying_code)

        contract = None
        if leg_contract_code and (option_flag is None or strike_val is None):
            try:
                contract = market_loader.get_contract_by_ts_code(
                    ts_code=leg_contract_code,
                    as_of_yyyymmdd=as_of_yyyymmdd,
                )
            except Exception:
                contract = {"status": "loader_error", "missing_reason": "合约元数据查询异常", "ts_code": leg_contract_code}
            if contract and contract.get("status") == "ok":
                cp_from_contract = str(contract.get("call_put") or "").strip().lower()
                if cp_from_contract in {"c", "p"}:
                    option_flag = cp_from_contract
                strike_from_contract = _coerce_positive_float(contract.get("exercise_price"))
                if strike_val is None and strike_from_contract is not None:
                    strike_val = strike_from_contract
                if month_val in (None, ""):
                    try:
                        month_val = int(str(contract.get("delist_date"))[4:6])
                    except Exception:
                        month_val = None
                underlying_from_contract = str(contract.get("underlying") or "").strip().upper()
                if underlying_from_contract:
                    underlying_for_leg = underlying_from_contract

        # 如果当前腿识别出更明确的标的，用它替换全局标的
        if underlying_for_leg and underlying_for_leg != underlying_code:
            normalized_leg_underlying = _normalize_underlying_hint(underlying_for_leg)
            if normalized_leg_underlying in INDEX_OPTION_PREFIX_TO_UNDERLYING:
                underlying_for_leg = INDEX_OPTION_PREFIX_TO_UNDERLYING[normalized_leg_underlying]
            else:
                underlying_for_leg = normalized_leg_underlying
        leg_out["option_flag"] = option_flag
        if option_flag in {"c", "p"}:
            leg_out["cp_text"] = "认购" if option_flag == "c" else "认沽"

        if not option_flag or strike_val is None:
            leg_out["status"] = "数据缺口"
            leg_out["missing_reason"] = "缺少行权价或认购认沽方向，无法计算Delta"
            blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(_attach_option_cn_labels(leg_out))
            continue

        if contract is None:
            try:
                contract = market_loader.find_option_contract(
                    underlying_code=underlying_for_leg,
                    option_flag=option_flag,
                    strike=float(strike_val),
                    month=month_val,
                    as_of_yyyymmdd=as_of_yyyymmdd,
                )
            except Exception:
                contract = {"status": "loader_error", "missing_reason": "合约查询异常"}
        if not contract:
            leg_out["status"] = "缺合约"
            leg_out["missing_reason"] = "未找到可交易合约"
            blocking_notes.append("存在未匹配合约腿")
            legs_out.append(_attach_option_cn_labels(leg_out))
            continue

        leg_out.update(
            {
                "ts_code": contract.get("ts_code"),
                "matched_strike": contract.get("exercise_price"),
                "delist_date": contract.get("delist_date"),
                "option_close": contract.get("close"),
                "option_trade_date": contract.get("trade_date"),
                "match_exact": contract.get("is_exact_strike"),
            }
        )
        if leg_out.get("strike") is None and contract.get("exercise_price") is not None:
            leg_out["strike"] = float(contract.get("exercise_price"))
        if leg_out.get("option_flag") in (None, "") and contract.get("call_put"):
            cp_val = str(contract.get("call_put")).strip().lower()
            if cp_val in {"c", "p"}:
                leg_out["option_flag"] = cp_val
                leg_out["cp_text"] = "认购" if cp_val == "c" else "认沽"
        if contract.get("status") != "ok":
            leg_out["status"] = "数据缺口"
            leg_out["missing_reason"] = contract.get("missing_reason", "合约信息缺失")
            if contract.get("status") in {"missing_price", "loader_error"}:
                blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(_attach_option_cn_labels(leg_out))
            continue

        multiplier_meta = _infer_contract_multiplier(
            asset_class=asset_class,
            underlying_code=underlying_for_leg,
            contract=contract,
        )
        leg_out["multiplier"] = float(multiplier_meta["multiplier"])
        leg_out["multiplier_source"] = str(multiplier_meta["source"])
        if not spot_info or sigma is None:
            leg_out["status"] = "数据缺口"
            leg_out["missing_reason"] = "缺现价或IV，无法计算Delta"
            blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(_attach_option_cn_labels(leg_out))
            continue

        t = _annualized_t(as_of_yyyymmdd, str(contract.get("delist_date")))
        delta = _calc_delta(
            option_flag=option_flag,
            spot=float(spot_info["close_price"]),
            strike=float(contract.get("exercise_price")),
            t=t,
            r=float(r),
            sigma=float(sigma),
            q=float(q),
        )
        if delta is None:
            leg_out["status"] = "Delta失败"
            leg_out["missing_reason"] = "Delta计算失败"
            blocking_notes.append(str(leg_out["missing_reason"]))
            legs_out.append(_attach_option_cn_labels(leg_out))
            continue

        leg_out["delta"] = float(delta)
        leg_out["delta_cash"] = (
            float(leg["signed_qty"]) * float(delta) * float(spot_info["close_price"]) * float(leg_out["multiplier"])
        )
        if leg_out.get("match_exact") is False:
            leg_out["status"] = "近似行权价"
            leg_out["missing_reason"] = "未命中精确行权价，已就近匹配"
        else:
            leg_out["status"] = "已计算"
        legs_out.append(_attach_option_cn_labels(leg_out))

    spot_for_metrics = float(spot_info["close_price"]) if spot_info else 0.0
    default_multiplier = ETF_MULTIPLIER if asset_class == "etf" else INDEX_OPTION_MULTIPLIER
    metrics = compute_delta_cash_metrics(legs_out, underlying_price=spot_for_metrics, multiplier=default_multiplier)
    account_capital = _coerce_positive_float(account_total_capital)
    effective_base = account_capital if account_capital else float(metrics["gross_notional"])
    effective_basis = "account_total_capital" if account_capital else "gross_notional"
    effective_ratio = float(metrics["total_delta_cash"]) / effective_base if effective_base > 0 else 0.0
    metrics["effective_ratio_base"] = effective_base
    metrics["effective_ratio_basis"] = effective_basis
    metrics["effective_delta_ratio"] = effective_ratio
    if account_capital:
        metrics["account_total_capital"] = account_capital
        metrics["account_delta_ratio"] = effective_ratio

    adjustment = build_delta_adjustment(
        total_delta_cash=metrics["total_delta_cash"],
        gross_notional=metrics["gross_notional"],
        trend_signal=trend_signal,
        risk_preference=risk_preference,
        ratio_base=effective_base,
        ratio_basis=effective_basis,
    )
    coverage_state = _classify_delta_coverage(
        coverage_ratio=float(metrics.get("coverage_ratio", 0.0)),
        has_legs=bool(legs_out),
    )
    displayable = bool(coverage_state.get("displayable"))
    execution_ready = bool(coverage_state.get("execution_ready"))
    report = _build_delta_report(
        asset_class=asset_class,
        underlying_code=underlying_code,
        spot_info=spot_info,
        iv_info=iv_info,
        legs=legs_out,
        metrics=metrics,
        adjustment=adjustment,
        missing_notes=missing_notes,
        account_total_capital=account_capital,
        displayable=displayable,
        execution_ready=execution_ready,
        coverage_tier=str(coverage_state.get("coverage_tier", "gap")),
    )
    if displayable and not execution_ready:
        blocking_notes.append(
            f"覆盖率低于{int(DELTA_EXECUTION_COVERAGE_THRESHOLD * 100)}%，仅支持方向性建议"
        )
    if not displayable and not blocking_notes:
        blocking_notes.append("Delta未达到可展示条件")

    # 去重并保持顺序
    dedup_blocking = list(dict.fromkeys([x for x in blocking_notes if str(x).strip()]))
    dedup_missing = list(dict.fromkeys([x for x in missing_notes if str(x).strip()]))
    if dedup_blocking:
        for note in dedup_blocking:
            if note not in dedup_missing:
                dedup_missing.append(note)

    single_payload = {
        "is_etf": (asset_class == "etf"),
        "asset_class": asset_class,
        "contract_prefix": contract_prefix,
        "underlying_code": underlying_code,
        "legs": legs_out,
        "metrics": metrics,
        "adjustment": adjustment,
        "missing_notes": dedup_missing,
        "blocking_missing_notes": dedup_blocking,
        "displayable": displayable,
        "execution_ready": execution_ready,
        "coverage_tier": str(coverage_state.get("coverage_tier", "gap")),
        "publishable": execution_ready,
        "report": report,
    }
    single_payload["per_underlying"] = {underlying_code: dict(single_payload)}
    single_payload["portfolio_summary"] = {
        "total_delta_cash": metrics.get("total_delta_cash", 0.0),
        "gross_notional": metrics.get("gross_notional", 0.0),
        "delta_ratio": metrics.get("delta_ratio", 0.0),
        "coverage_ratio": metrics.get("coverage_ratio", 0.0),
        "effective_ratio_base": metrics.get("effective_ratio_base", 0.0),
        "effective_ratio_basis": metrics.get("effective_ratio_basis", "gross_notional"),
        "effective_delta_ratio": metrics.get("effective_delta_ratio", metrics.get("delta_ratio", 0.0)),
        "coverage_tier": str(coverage_state.get("coverage_tier", "gap")),
        "displayable": displayable,
        "execution_ready": execution_ready,
        "adjustment": adjustment,
    }
    single_payload["risk_contribution_ranking"] = [
        {
            "priority": "P1",
            "underlying_code": underlying_code,
            "risk_contribution": 1.0 if abs(float(metrics.get("total_delta_cash", 0.0))) > 0 else 0.0,
            "priority_score": 1.0 if abs(float(metrics.get("total_delta_cash", 0.0))) > 0 else 0.0,
            "total_delta_cash": float(metrics.get("total_delta_cash", 0.0)),
            "current_ratio": float(adjustment.get("current_ratio", 0.0)),
            "target_mid": float(adjustment.get("target_mid", 0.0)),
            "adjust_cash": float(adjustment.get("adjust_cash", 0.0)),
            "action": str(adjustment.get("action") or ""),
            "min_expiry_days": None,
            "naked_short_call_qty": 0,
            "low_liquidity_hits": 0,
            "displayable": displayable,
            "execution_ready": execution_ready,
            "publishable": execution_ready,
        }
    ]
    single_payload["trend_map_used"] = {underlying_code: trend_signal}
    return single_payload

def compute_option_delta_cash_from_legs(
    legs: List[Dict[str, Any]],
    trend_signal: str = "",
    trend_map: Optional[Dict[str, str]] = None,
    risk_preference: str = "稳健型",
    loader: Optional[ETFOptionMarketLoader] = None,
    as_of_date: Optional[str] = None,
    r: float = 0.015,
    q: float = 0.0,
    account_total_capital: Optional[float] = None,
) -> Dict[str, Any]:
    return compute_option_delta_cash(
        user_query="",
        symbol_hint="",
        vision_legs=legs,
        vision_domain="option",
        trend_signal=trend_signal,
        trend_map=trend_map,
        risk_preference=risk_preference,
        loader=loader,
        as_of_date=as_of_date,
        r=r,
        q=q,
        account_total_capital=account_total_capital,
        allowed_asset_classes=("etf", "index"),
    )


def compute_etf_option_delta_cash(
    user_query: str,
    symbol_hint: str = "",
    trend_signal: str = "",
    trend_map: Optional[Dict[str, str]] = None,
    risk_preference: str = "稳健型",
    loader: Optional[ETFOptionMarketLoader] = None,
    as_of_date: Optional[str] = None,
    r: float = 0.015,
    q: float = 0.0,
    account_total_capital: Optional[float] = None,
) -> Dict[str, Any]:
    return compute_option_delta_cash(
        user_query=user_query,
        symbol_hint=symbol_hint,
        vision_legs=None,
        vision_domain="option",
        trend_signal=trend_signal,
        trend_map=trend_map,
        risk_preference=risk_preference,
        loader=loader,
        as_of_date=as_of_date,
        r=r,
        q=q,
        account_total_capital=account_total_capital,
        allowed_asset_classes=("etf",),
    )

"""
Utilities for resolving futures daily K-line trading day.

Rule summary:
- Day session uses natural day.
- Night-session products opened at 21:00 belong to next trading day.
- If trading calendar is unavailable, fallback to next weekday.
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Optional


_NIGHT_SESSION_PRODUCTS = {
    # SHFE / INE
    "au", "ag", "cu", "al", "zn", "pb", "ni", "sn", "rb", "hc", "ss",
    "fu", "bu", "ru", "sp", "sc", "lu", "bc", "ao",
    # DCE
    "a", "b", "m", "y", "p", "c", "cs", "jd", "l", "pp", "v", "eb",
    "eg", "j", "jm", "i", "rr", "pg", "lh",
    # CZCE
    "sr", "cf", "ta", "ma", "rm", "oi", "zc", "fg", "sa", "ur", "ap",
    "cj", "lc", "si", "ps", "pr", "sf", "sm", "pf", "cy",
}

_SH_TZ = timezone(timedelta(hours=8))
_TRADE_CAL_CACHE_TTL = 3600

_trade_days: list[str] = []
_trade_range: tuple[str, str] = ("", "")
_trade_loaded_ts: float = 0.0
_trade_lock = Lock()

_ts_client = None
_ts_init_failed = False


def _now_shanghai() -> datetime:
    return datetime.now(_SH_TZ)


def _next_weekday(day_yyyymmdd: str) -> str:
    d = datetime.strptime(day_yyyymmdd, "%Y%m%d") + timedelta(days=1)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.strftime("%Y%m%d")


def _product_of_contract(contract: str) -> str:
    m = re.match(r"^([A-Za-z]+)\d+$", contract or "")
    return m.group(1).lower() if m else ""


def _is_night_session_product(contract: str) -> bool:
    return _product_of_contract(contract) in _NIGHT_SESSION_PRODUCTS


def _is_night_open_window(now_sh: datetime) -> bool:
    hhmm = now_sh.hour * 60 + now_sh.minute
    return 21 * 60 <= hhmm <= 23 * 60 + 59


def _get_tushare_client():
    global _ts_client, _ts_init_failed
    if _ts_client is not None:
        return _ts_client
    if _ts_init_failed:
        return None

    token = os.getenv("TUSHARE_TOKEN", "").strip()
    if not token:
        _ts_init_failed = True
        return None

    try:
        import tushare as ts

        ts.set_token(token)
        _ts_client = ts.pro_api(timeout=10)
    except Exception:
        _ts_init_failed = True
        return None
    return _ts_client


def _load_trade_days(anchor_day: str) -> list[str]:
    global _trade_days, _trade_range, _trade_loaded_ts

    client = _get_tushare_client()
    if client is None:
        return []

    anchor = datetime.strptime(anchor_day, "%Y%m%d")
    start = (anchor - timedelta(days=7)).strftime("%Y%m%d")
    end = (anchor + timedelta(days=45)).strftime("%Y%m%d")
    now_ts = time.time()

    with _trade_lock:
        if (
            _trade_days
            and (now_ts - _trade_loaded_ts) < _TRADE_CAL_CACHE_TTL
            and _trade_range[0] <= start
            and _trade_range[1] >= end
        ):
            return _trade_days

    try:
        df = client.trade_cal(exchange="SHFE", start_date=start, end_date=end, is_open="1")
        if df is None or df.empty:
            return []
        days = sorted(df["cal_date"].astype(str).tolist())
    except Exception:
        return []

    with _trade_lock:
        _trade_days = days
        _trade_range = (start, end)
        _trade_loaded_ts = now_ts
        return _trade_days


def _next_open_trade_day(day_yyyymmdd: str) -> Optional[str]:
    days = _load_trade_days(day_yyyymmdd)
    if not days:
        return None
    for d in days:
        if d > day_yyyymmdd:
            return d
    return None


def resolve_trading_day_for_contract(contract: str, now_sh: Optional[datetime] = None) -> str:
    now_sh = now_sh or _now_shanghai()
    natural_day = now_sh.strftime("%Y%m%d")

    if not _is_night_session_product(contract):
        return natural_day

    if _is_night_open_window(now_sh):
        return _next_open_trade_day(natural_day) or _next_weekday(natural_day)

    # 00:00-02:30 belongs to natural day, matching the same trading day as previous night.
    return natural_day


def enrich_prices_payload_with_trading_day(payload: dict) -> dict:
    contracts = payload.get("contracts")
    if not isinstance(contracts, dict):
        return payload

    now_sh = _now_shanghai()
    natural_day = now_sh.strftime("%Y%m%d")
    night_trade_day = ""
    if _is_night_open_window(now_sh):
        night_trade_day = _next_open_trade_day(natural_day) or _next_weekday(natural_day)

    for code, item in contracts.items():
        if not isinstance(item, dict):
            continue
        if night_trade_day and _is_night_session_product(str(code)):
            item["trading_day"] = night_trade_day
        else:
            item["trading_day"] = natural_day
    return payload
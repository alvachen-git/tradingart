"""Read-only K-line data adapter for card roguelike mode."""

from __future__ import annotations

import importlib
import os
import random
from typing import Dict, List, Optional, Set

import pandas as pd
from sqlalchemy import create_engine, text


def _build_fallback_engine():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT") or "3306"
    db_name = os.getenv("DB_NAME")
    if all([db_user, db_password, db_host, db_name]):
        db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)
    return None


_ENGINE = None


def _get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    try:
        from data_engine import engine as shared_engine  # type: ignore
        _ENGINE = shared_engine
        return _ENGINE
    except Exception:
        _ENGINE = _build_fallback_engine()
        return _ENGINE


def _get_name(conn, table_name: str, symbol: str) -> str:
    try:
        row = conn.execute(
            text(f"SELECT name FROM {table_name} WHERE ts_code = :code AND name IS NOT NULL LIMIT 1"),
            {"code": symbol},
        ).fetchone()
        if row and row[0]:
            return str(row[0])
    except Exception:
        pass
    return symbol


def _fallback_get_random_kline_data(bars=100, history_bars=20, _attempt=1, _max_attempts=12):
    total_bars = max(30, int(bars) + int(history_bars))
    eng = _get_engine()
    if eng is None:
        return None, None, None, None

    table_map = {
        "stock": "stock_price",
        "index": "index_price",
        "future": "futures_price",
    }
    market_type = random.choices(["stock", "index", "future"], weights=[40, 10, 50], k=1)[0]
    table_name = table_map[market_type]

    try:
        with eng.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT ts_code
                    FROM {table_name}
                    WHERE open_price IS NOT NULL
                      AND close_price IS NOT NULL
                    GROUP BY ts_code
                    HAVING COUNT(*) >= :min_bars
                    """
                ),
                {"min_bars": total_bars + 30},
            ).fetchall()
            symbols = [str(r[0]) for r in rows]
            if not symbols:
                return None, None, None, None

            symbol = random.choice(symbols)
            count_row = conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM {table_name}
                    WHERE ts_code = :code
                      AND open_price IS NOT NULL
                      AND close_price IS NOT NULL
                    """
                ),
                {"code": symbol},
            ).fetchone()
            total_count = int(count_row[0]) if count_row else 0
            if total_count < total_bars:
                return None, None, None, None

            max_offset = max(0, total_count - total_bars - 1)
            offset = random.randint(0, max_offset) if max_offset > 0 else 0
            df = pd.read_sql(
                text(
                    f"""
                    SELECT trade_date, open_price, high_price, low_price, close_price, COALESCE(vol, 0) AS vol
                    FROM {table_name}
                    WHERE ts_code = :code
                      AND open_price IS NOT NULL
                      AND close_price IS NOT NULL
                    ORDER BY trade_date
                    LIMIT :bars OFFSET :offset
                    """
                ),
                conn,
                params={"code": symbol, "bars": total_bars, "offset": offset},
            )
            if df.empty or len(df) < total_bars:
                return None, None, None, None

            avg_vol = float(df["vol"].fillna(0).mean()) if "vol" in df.columns else 0.0
            if avg_vol < 1000:
                print(f"[FALLBACK] ❌ 成交量不足: avg(vol)={avg_vol:.2f} < 1000, 标的 {symbol}")
                if _attempt < _max_attempts:
                    return _fallback_get_random_kline_data(bars, history_bars, _attempt + 1, _max_attempts)
                return None, None, None, None

            try:
                df["trade_date"] = pd.to_datetime(df["trade_date"])
                df.set_index("trade_date", inplace=True)
            except Exception:
                pass
            symbol_name = _get_name(conn, table_name, symbol)
            return symbol, symbol_name, market_type, df
    except Exception:
        return None, None, None, None


def _load_kline_game_module():
    try:
        return importlib.import_module("kline_game")
    except Exception:
        return None


class _KlineGameProxy:
    def get_random_kline_data(self, bars=100, history_bars=20, _attempt=1, _max_attempts=12):
        mod = _load_kline_game_module()
        if mod and hasattr(mod, "get_random_kline_data"):
            try:
                return mod.get_random_kline_data(bars=bars, history_bars=history_bars, _attempt=_attempt, _max_attempts=_max_attempts)
            except Exception:
                pass
        return _fallback_get_random_kline_data(
            bars=bars,
            history_bars=history_bars,
            _attempt=_attempt,
            _max_attempts=_max_attempts,
        )


kg = _KlineGameProxy()


def _safe_float(v) -> Optional[float]:
    try:
        f = float(v)
    except Exception:
        return None
    if pd.isna(f):
        return None
    return f


def _df_to_bars(df: pd.DataFrame) -> List[Dict[str, object]]:
    bars: List[Dict[str, object]] = []
    if df is None or df.empty:
        return bars

    for idx, row in df.iterrows():
        o = _safe_float(row.get("open_price"))
        h = _safe_float(row.get("high_price"))
        l = _safe_float(row.get("low_price"))
        c = _safe_float(row.get("close_price"))
        v = _safe_float(row.get("vol"))
        if None in (o, h, l, c):
            continue
        if v is None or v < 0:
            v = 0.0
        if hasattr(idx, "strftime"):
            ds = idx.strftime("%Y-%m-%d")
        else:
            ds = str(idx)[:10]
        bars.append(
            {
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v),
                "date": ds,
            }
        )
    return bars


def _fetch_one_pack(total_bars: int, exclude_symbols: Optional[Set[str]] = None) -> Optional[Dict[str, object]]:
    excluded = exclude_symbols or set()
    symbol, symbol_name, symbol_type, df = kg.get_random_kline_data(bars=max(1, total_bars - 20), history_bars=20)
    if not symbol or df is None or len(df) < total_bars:
        return None
    if symbol in excluded:
        return None

    bars = _df_to_bars(df)
    if len(bars) < total_bars:
        return None

    bars = bars[:total_bars]
    return {
        "symbol": symbol,
        "symbol_name": symbol_name or symbol,
        "symbol_type": symbol_type or "unknown",
        "bars": bars,
    }


def get_stage_candidates(stage_no: int, count: int = 3, seed: Optional[int] = None) -> List[Dict[str, object]]:
    rng = random.Random((seed or 0) + int(stage_no) * 29)
    candidates: List[Dict[str, object]] = []
    used: Set[str] = set()

    attempts = 0
    while len(candidates) < max(1, int(count)) and attempts < 20:
        attempts += 1
        pack = _fetch_one_pack(total_bars=120, exclude_symbols=used)
        if not pack:
            continue
        used.add(str(pack["symbol"]))
        candidates.append(pack)

    rng.shuffle(candidates)
    return candidates


def get_boss_stage_candidate(stage_no: int, seed: Optional[int] = None) -> Dict[str, object]:
    rng = random.Random((seed or 0) + int(stage_no) * 43 + 7)
    attempts = 0
    while attempts < 10:
        attempts += 1
        pack = _fetch_one_pack(total_bars=120, exclude_symbols=None)
        if pack:
            return pack
    # fallback with explicit empty payload to avoid None propagation
    rng_seed = rng.randint(1, 999999999)
    symbol, symbol_name, symbol_type, df = kg.get_random_kline_data(bars=100, history_bars=20, _attempt=1, _max_attempts=12)
    bars = _df_to_bars(df)[:120] if df is not None else []
    return {
        "symbol": symbol or f"BOSS-{rng_seed}",
        "symbol_name": symbol_name or symbol or "Boss",
        "symbol_type": symbol_type or "unknown",
        "bars": bars,
    }

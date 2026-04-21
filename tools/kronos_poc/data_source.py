from __future__ import annotations

import os
from typing import List

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from .config import MIN_SAFETY_MARGIN, PilotAsset


load_dotenv(override=True)

_ENGINE = None
_TABLE_COLUMNS: dict[str, set[str]] = {}


def get_db_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    if not all([db_user, db_password, db_host, db_name]):
        return None
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    _ENGINE = create_engine(db_url, pool_recycle=7200, pool_pre_ping=True)
    return _ENGINE


def _get_table_columns(table_name: str) -> set[str]:
    if table_name in _TABLE_COLUMNS:
        return _TABLE_COLUMNS[table_name]
    engine = get_db_engine()
    if engine is None:
        return set()
    try:
        df = pd.read_sql(text(f"SHOW COLUMNS FROM {table_name}"), engine)
        cols = set(df["Field"].astype(str).tolist()) if "Field" in df.columns else set()
    except Exception:
        cols = set()
    _TABLE_COLUMNS[table_name] = cols
    return cols


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    required = ["trade_date", "open_price", "high_price", "low_price", "close_price", "vol"]
    optional = ["amount", "oi"]
    for col in required + optional:
        if col not in df.columns:
            df[col] = None
    df = df[["trade_date", "open_price", "high_price", "low_price", "close_price", "vol", "amount", "oi", "ts_code"]]
    df["trade_date"] = df["trade_date"].astype(str)
    df = df.drop_duplicates(subset=["trade_date"], keep="last").sort_values("trade_date").reset_index(drop=True)
    for col in ["open_price", "high_price", "low_price", "close_price", "vol", "amount", "oi"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def fetch_history(asset: PilotAsset, lookback_window: int, safety_margin: int = MIN_SAFETY_MARGIN) -> pd.DataFrame:
    engine = get_db_engine()
    if engine is None:
        raise RuntimeError("数据库连接未配置（DB_USER/DB_PASSWORD/DB_HOST/DB_NAME）")
    limit_n = int(lookback_window) + int(safety_margin) + 5
    if asset.asset_type in {"index", "etf"}:
        table_name = "index_price" if asset.asset_type == "index" else "stock_price"
        table_cols = _get_table_columns(table_name)
        select_cols = ["trade_date", "ts_code", "open_price", "high_price", "low_price", "close_price"]
        if "vol" in table_cols:
            select_cols.append("vol")
        if "amount" in table_cols:
            select_cols.append("amount")
        sql = text(
            f"""
            SELECT {", ".join(select_cols)}
            FROM {table_name}
            WHERE ts_code = :code
            ORDER BY trade_date DESC
            LIMIT :limit_n
            """
        )
        df = pd.read_sql(sql, engine, params={"code": asset.db_code, "limit_n": limit_n})
    else:
        table_cols = _get_table_columns("futures_price")
        select_cols = ["trade_date", "ts_code", "open_price", "high_price", "low_price", "close_price"]
        if "vol" in table_cols:
            select_cols.append("vol")
        if "amount" in table_cols:
            select_cols.append("amount")
        if "oi" in table_cols:
            select_cols.append("oi")
        codes: List[str] = [asset.db_code.upper()]
        if asset.legacy_code:
            codes.append(asset.legacy_code.upper())
        placeholders = ", ".join([f":c{i}" for i in range(len(codes))])
        params = {f"c{i}": code for i, code in enumerate(codes)}
        params["limit_n"] = limit_n
        sql = text(
            f"""
            SELECT {", ".join(select_cols)}
            FROM futures_price
            WHERE UPPER(ts_code) IN ({placeholders})
            ORDER BY trade_date DESC
            LIMIT :limit_n
            """
        )
        df = pd.read_sql(sql, engine, params=params)
    return _normalize_frame(df)


def detect_futures_roll_warning(asset: PilotAsset, df: pd.DataFrame) -> list[str]:
    if asset.asset_type != "future" or df.empty or len(df) < 10:
        return []
    warnings: list[str] = []
    closes = df["close_price"].dropna()
    if len(closes) >= 2:
        last_ret = float((closes.iloc[-1] / closes.iloc[-2]) - 1) if closes.iloc[-2] else 0.0
        if abs(last_ret) >= 0.06:
            warnings.append("近期价格波动较大，可能受主连换月或事件冲击影响，预测稳定性会下降。")
    if asset.risk_note:
        warnings.append(asset.risk_note)
    return warnings

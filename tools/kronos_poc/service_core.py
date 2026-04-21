from __future__ import annotations

import time
from typing import Any, Dict, Iterable, List

import pandas as pd

from .cache import JsonDiskCache
from .config import (
    CACHE_DIR,
    DEFAULT_HORIZON,
    DEFAULT_LOOKBACK,
    DEFAULT_QUANTILES,
    MIN_SAFETY_MARGIN,
    MODEL_VERSION,
    PREPROCESS_VERSION,
    resolve_pilot_asset,
)
from .data_source import detect_futures_roll_warning, fetch_history
from .engine import KronosAdapter


_CACHE = JsonDiskCache(CACHE_DIR)
_ENGINE = KronosAdapter()


def _make_error(error_code: str, message: str, latest_trade_date: str | None = None, symbol: str | None = None) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "ok": False,
        "error_code": error_code,
        "message": message,
    }
    if latest_trade_date is not None:
        payload["latest_trade_date"] = latest_trade_date
    if symbol is not None:
        payload["symbol"] = symbol
    return payload


def _normalize_quantiles(qs: Iterable[float] | None) -> List[float]:
    if not qs:
        return list(DEFAULT_QUANTILES)
    q_vals = sorted({round(float(q), 4) for q in qs})
    if any(q <= 0 or q >= 1 for q in q_vals):
        raise ValueError("quantiles 必须在 (0,1) 区间内")
    return q_vals


def _cache_key(*, symbol: str, asset_type: str, latest_trade_date: str, lookback_window: int, horizon: int, quantiles: List[float]) -> Dict[str, Any]:
    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "latest_trade_date": latest_trade_date,
        "lookback_window": int(lookback_window),
        "horizon": int(horizon),
        "quantiles": quantiles,
        "model_version": MODEL_VERSION,
        "preprocess_version": PREPROCESS_VERSION,
    }


def _prediction_fields_present(preds: List[dict], quantiles: List[float]) -> bool:
    if not preds:
        return False
    for row in preds:
        if "step" not in row:
            return False
        for q in quantiles:
            key = f"p{int(q * 100):02d}_close"
            if key not in row:
                return False
    return True


def predict_eod_interval(payload: Dict[str, Any]) -> Dict[str, Any]:
    t0 = time.time()
    symbol_input = str(payload.get("symbol", "")).strip()
    if not symbol_input:
        return _make_error("INVALID_REQUEST", "缺少 symbol")

    try:
        asset = resolve_pilot_asset(symbol_input)
    except KeyError:
        return _make_error("INVALID_SYMBOL", f"不在试点范围内: {symbol_input}", symbol=symbol_input)

    lookback_window = int(payload.get("lookback_window") or DEFAULT_LOOKBACK)
    horizon = int(payload.get("horizon") or DEFAULT_HORIZON)
    force_refresh = bool(payload.get("force_refresh", False))
    try:
        quantiles = _normalize_quantiles(payload.get("quantiles"))
    except Exception as e:
        return _make_error("INVALID_REQUEST", str(e), symbol=asset.label)

    if horizon != 3:
        return _make_error("INVALID_REQUEST", "首期 PoC 仅支持 horizon=3", symbol=asset.label)
    if lookback_window < 40:
        return _make_error("INVALID_REQUEST", "lookback_window 过小，至少为40", symbol=asset.label)

    try:
        df = fetch_history(asset, lookback_window=lookback_window, safety_margin=MIN_SAFETY_MARGIN)
    except Exception as e:
        return _make_error("DATA_READ_ERROR", f"读取历史数据失败: {e}", symbol=asset.label)

    if df.empty:
        return _make_error("DATA_NOT_FOUND", "未找到该标的历史数据", symbol=asset.label)
    latest_trade_date = str(df["trade_date"].max())
    if len(df) < lookback_window + MIN_SAFETY_MARGIN:
        return _make_error(
            "INSUFFICIENT_HISTORY",
            f"历史数据不足，当前 {len(df)} 条，至少需要 {lookback_window + MIN_SAFETY_MARGIN} 条",
            latest_trade_date=latest_trade_date,
            symbol=asset.label,
        )

    df_model = df.tail(lookback_window).reset_index(drop=True)
    key = _cache_key(
        symbol=asset.label,
        asset_type=asset.asset_type,
        latest_trade_date=latest_trade_date,
        lookback_window=lookback_window,
        horizon=horizon,
        quantiles=quantiles,
    )
    if not force_refresh:
        cached = _CACHE.get(key)
        if cached:
            history_plot = (((cached or {}).get("debug") or {}).get("history_plot") or {})
            if history_plot.get("open_prices") and history_plot.get("high_prices") and history_plot.get("low_prices"):
                cached["cache_hit"] = True
                return cached

    warnings = detect_futures_roll_warning(asset, df_model)
    try:
        preds, engine_warnings, engine_debug = _ENGINE.predict(df_model, horizon=horizon, quantiles=quantiles)
    except Exception as e:
        return _make_error("PREDICT_ERROR", f"预测失败: {e}", latest_trade_date=latest_trade_date, symbol=asset.label)
    warnings.extend(engine_warnings)

    if not _prediction_fields_present(preds, quantiles):
        return _make_error("PREDICT_SCHEMA_ERROR", "预测结果字段不完整", latest_trade_date=latest_trade_date, symbol=asset.label)

    latency_ms = int((time.time() - t0) * 1000)
    response: Dict[str, Any] = {
        "ok": True,
        "symbol": asset.label,
        "symbol_key": asset.key,
        "asset_type": asset.asset_type,
        "latest_trade_date": latest_trade_date,
        "cache_hit": False,
        "model_version": MODEL_VERSION,
        "preprocess_version": PREPROCESS_VERSION,
        "data_points_used": int(len(df_model)),
        "latency_ms": latency_ms,
        "predictions": preds,
        "warnings": list(dict.fromkeys([w for w in warnings if w])),
        "debug": {
            "engine": engine_debug,
            "history_plot": {
                "trade_dates": df_model["trade_date"].astype(str).tolist(),
                "open_prices": pd.to_numeric(df_model["open_price"], errors="coerce").round(6).fillna(0).tolist(),
                "high_prices": pd.to_numeric(df_model["high_price"], errors="coerce").round(6).fillna(0).tolist(),
                "low_prices": pd.to_numeric(df_model["low_price"], errors="coerce").round(6).fillna(0).tolist(),
                "close_prices": pd.to_numeric(df_model["close_price"], errors="coerce").round(6).fillna(0).tolist(),
            },
            "request": {
                "lookback_window": lookback_window,
                "horizon": horizon,
                "quantiles": quantiles,
                "force_refresh": force_refresh,
            },
        },
    }

    _CACHE.set(key, response)
    return response


def cache_invalidate(symbol: str | None = None) -> Dict[str, Any]:
    removed = _CACHE.invalidate(symbol=symbol)
    return {"ok": True, "removed": removed, "symbol": symbol}

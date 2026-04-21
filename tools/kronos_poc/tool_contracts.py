"""Reserved I/O contracts for phase-2 agent tool integration (not wired yet)."""

from __future__ import annotations

from typing import Any, Dict, TypedDict


class KronosPredictIntervalToolInput(TypedDict, total=False):
    symbol: str
    lookback_window: int
    horizon: int
    force_refresh: bool


class KronosPredictIntervalToolOutput(TypedDict, total=False):
    ok: bool
    symbol: str
    latest_trade_date: str
    cache_hit: bool
    predictions: list[dict]
    warnings: list[str]
    message: str
    error_code: str


class KronosAnomalyScoreToolInput(TypedDict, total=False):
    symbol: str
    lookback_window: int
    metric: str


class KronosAnomalyScoreToolOutput(TypedDict, total=False):
    ok: bool
    symbol: str
    latest_trade_date: str
    anomaly_score: float
    threshold: float
    warnings: list[str]
    message: str
    error_code: str


def normalize_phase2_predict_output(api_resp: Dict[str, Any]) -> KronosPredictIntervalToolOutput:
    """Adapter helper for future agent tool integration."""
    if not api_resp:
        return {"ok": False, "message": "空响应", "error_code": "EMPTY_RESPONSE"}
    return {
        "ok": bool(api_resp.get("ok")),
        "symbol": api_resp.get("symbol", ""),
        "latest_trade_date": api_resp.get("latest_trade_date", ""),
        "cache_hit": bool(api_resp.get("cache_hit", False)),
        "predictions": api_resp.get("predictions", []) or [],
        "warnings": api_resp.get("warnings", []) or [],
        "message": api_resp.get("message", ""),
        "error_code": api_resp.get("error_code", ""),
    }


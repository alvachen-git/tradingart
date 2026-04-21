from __future__ import annotations

import os
from typing import Any, Dict

import requests

from .config import SERVICE_BASE_URL


def get_service_base_url() -> str:
    return os.getenv("KRONOS_POC_SERVICE_URL", SERVICE_BASE_URL).rstrip("/")


def get_health(timeout: float = 2.0) -> Dict[str, Any]:
    url = f"{get_service_base_url()}/health"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def predict_eod_interval(
    *,
    symbol: str,
    lookback_window: int = 120,
    horizon: int = 3,
    quantiles: list[float] | None = None,
    force_refresh: bool = False,
    timeout: float = 15.0,
) -> Dict[str, Any]:
    url = f"{get_service_base_url()}/predict/eod-interval"
    payload = {
        "symbol": symbol,
        "lookback_window": lookback_window,
        "horizon": horizon,
        "quantiles": quantiles or [0.1, 0.5, 0.9],
        "force_refresh": force_refresh,
    }
    resp = requests.post(url, json=payload, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


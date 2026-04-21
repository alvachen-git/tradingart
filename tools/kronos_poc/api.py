from __future__ import annotations

from typing import List, Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from .service_core import cache_invalidate, predict_eod_interval


class PredictRequest(BaseModel):
    symbol: str = Field(..., description="试点标的名称或标识")
    force_refresh: bool = Field(default=False)
    lookback_window: int = Field(default=120, ge=40, le=500)
    horizon: int = Field(default=3)
    quantiles: List[float] = Field(default_factory=lambda: [0.1, 0.5, 0.9])


class CacheInvalidateRequest(BaseModel):
    symbol: Optional[str] = None


def create_app() -> FastAPI:
    app = FastAPI(
        title="TradingArt Kronos PoC API",
        version="0.1.0",
        description="Experimental end-of-day interval forecast service for TradingArt.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    def health():
        return {"ok": True, "service": "kronos-poc", "version": "0.1.0"}

    @app.post("/predict/eod-interval")
    def predict(req: PredictRequest):
        payload = req.model_dump() if hasattr(req, "model_dump") else req.dict()
        return predict_eod_interval(payload)

    @app.post("/cache/invalidate")
    def invalidate(req: CacheInvalidateRequest):
        return cache_invalidate(symbol=req.symbol)

    return app


app = create_app()

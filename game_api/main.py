"""FastAPI entrypoint for K-line card roguelike backend."""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import kline_card_storage as storage
import kline_card_map_storage as map_storage
from game_api.routes import map_router, router as card_router


def create_app() -> FastAPI:
    app = FastAPI(
        title="TradingArt K-Line Card API",
        version="0.1.0",
        description="Dedicated API for Godot/Web game client.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(card_router)
    app.include_router(map_router)

    @app.on_event("startup")
    def _startup() -> None:
        storage.init_card_game_schema()
        map_storage.init_map_schema()

    return app


app = create_app()

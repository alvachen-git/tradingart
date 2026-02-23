"""Pydantic schemas for card game API."""

from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class BaseResp(BaseModel):
    ok: bool = True
    message: Optional[str] = None


class MetaUpgradeReq(BaseModel):
    upgrade_code: str


class AuthLoginReq(BaseModel):
    account: str
    password: str


class AuthRestoreReq(BaseModel):
    username: str
    token: str


class RunCreateReq(BaseModel):
    seed: Optional[int] = None


class StageStartReq(BaseModel):
    run_id: int
    stage_no: int
    symbol_choice: Optional[str] = None


class TurnPlayReq(BaseModel):
    run_id: int
    type: Literal["pass", "play", "combo", "discard"] = "pass"
    card_id: Optional[str] = None
    cards: List[str] = Field(default_factory=list)


class RunReq(BaseModel):
    run_id: int


class StageUpgradeReq(BaseModel):
    run_id: int
    upgrade_code: str


class MapRunCreateReq(BaseModel):
    seed: Optional[int] = None
    restart_existing_active: bool = False
    new_game_setup: Optional["MapNewGameSetupReq"] = None


class MapNewGameSetupReq(BaseModel):
    player_name: str
    traits: List[str] = Field(default_factory=list, min_length=4, max_length=4)
    style_answers: Dict[str, str] = Field(default_factory=dict)
    god_mode: bool = False


class MapRunReq(BaseModel):
    map_run_id: int


class MapMoveReq(BaseModel):
    map_run_id: int
    to_location: Literal["home", "association"]


class MapDeckSaveReq(BaseModel):
    map_run_id: int
    deck_cards: List[str] = Field(default_factory=list)


class MapBattleStartReq(BaseModel):
    map_run_id: int


class MapBattleCommitReq(BaseModel):
    map_run_id: int
    battle_run_id: int

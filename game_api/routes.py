"""HTTP routes for K-line card roguelike service."""

from __future__ import annotations

from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, status

import kline_card_storage as storage
import kline_card_map_storage as map_storage
from game_api.auth import require_user
from game_api.schemas import (
    AuthLoginReq,
    AuthRestoreReq,
    MapBattleCommitReq,
    MapBattleStartReq,
    MapDeckSaveReq,
    MapMoveReq,
    MapRunCreateReq,
    MapRunReq,
    MetaUpgradeReq,
    RunCreateReq,
    RunReq,
    StageStartReq,
    StageUpgradeReq,
    TurnPlayReq,
)


router = APIRouter(prefix="/v1/card", tags=["kline-card"])
map_router = APIRouter(prefix="/v1/map", tags=["kline-map"])


def _not_ok(payload: Dict[str, Any], status_code: int = status.HTTP_400_BAD_REQUEST) -> None:
    raise HTTPException(status_code=status_code, detail=payload.get("message", "request failed"))


def _assert_run_owner(run_id: int, user_id: str) -> Dict[str, Any]:
    state = storage.get_run_state(int(run_id))
    if not state.get("ok"):
        _not_ok(state, status.HTTP_404_NOT_FOUND)
    run = dict(state.get("run") or {})
    owner = str(run.get("user_id") or "").strip()
    if owner != str(user_id).strip():
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="run ownership mismatch")
    return state


def _assert_map_run_owner(map_run_id: int, user_id: str) -> Dict[str, Any]:
    state = map_storage.get_map_state(int(map_run_id))
    if not state.get("ok"):
        _not_ok(state, status.HTTP_404_NOT_FOUND)
    run = dict(state.get("map_run") or {})
    owner = str(run.get("user_id") or "").strip()
    if owner != str(user_id).strip():
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="map run ownership mismatch")
    return state


@router.get("/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "service": "kline-card-api"}


@router.post("/auth/login")
def auth_login(req: AuthLoginReq) -> Dict[str, Any]:
    try:
        import auth_utils as auth
    except Exception as exc:
        _not_ok({"message": f"auth backend unavailable: {exc}"}, status.HTTP_500_INTERNAL_SERVER_ERROR)
    ok, message, token, username = auth.login_user(req.account.strip(), req.password)
    if not ok or not token or not username:
        _not_ok({"message": message or "login failed"}, status.HTTP_401_UNAUTHORIZED)
    return {"ok": True, "username": str(username), "token": str(token), "message": "login success"}


@router.post("/auth/restore")
def auth_restore(req: AuthRestoreReq) -> Dict[str, Any]:
    username = req.username.strip()
    token = req.token.strip()
    if not username or not token:
        _not_ok({"message": "username/token required"}, status.HTTP_400_BAD_REQUEST)
    try:
        import auth_utils as auth
    except Exception as exc:
        _not_ok({"message": f"auth backend unavailable: {exc}"}, status.HTTP_500_INTERNAL_SERVER_ERROR)
    if not auth.check_token(username, token):
        _not_ok({"message": "invalid or expired token"}, status.HTTP_401_UNAUTHORIZED)
    return {"ok": True, "username": username, "token": token, "message": "restore success"}


@router.post("/meta/get")
def get_meta(user_id: str = Depends(require_user)) -> Dict[str, Any]:
    payload = storage.get_card_meta(user_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/meta/upgrade")
def upgrade_meta(req: MetaUpgradeReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    payload = storage.apply_card_meta_upgrade(user_id, req.upgrade_code)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/run/create")
def create_run(req: RunCreateReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    run_id = storage.create_run(user_id=user_id, seed=req.seed)
    if not run_id:
        _not_ok({"message": "create run failed"})
    return {"ok": True, "run_id": int(run_id)}


@router.post("/run/resume")
def resume_run(user_id: str = Depends(require_user)) -> Dict[str, Any]:
    run = storage.get_resume_run(user_id)
    return {"ok": True, "run": run}


@router.post("/run/state")
def run_state(req: RunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    payload = storage.get_run_state(req.run_id)
    if not payload.get("ok"):
        _not_ok(payload, status.HTTP_404_NOT_FOUND)
    return payload


@router.post("/stage/start")
def start_stage(req: StageStartReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    payload = storage.start_stage(req.run_id, req.stage_no, req.symbol_choice)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/turn/play")
def play_turn(req: TurnPlayReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    action: Dict[str, Any] = {"type": req.type}
    if req.type == "play":
        action["card_id"] = req.card_id
    elif req.type == "combo" or req.type == "discard":
        action["cards"] = req.cards
        if req.card_id:
            action["card_id"] = req.card_id
    payload = storage.play_turn(req.run_id, action)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/stage/finish")
def finish_stage(req: RunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    payload = storage.finish_stage(req.run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/stage/upgrade")
def apply_stage_upgrade(req: StageUpgradeReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    payload = storage.apply_stage_upgrade(req.run_id, req.upgrade_code)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/run/finish")
def finish_run(req: RunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    payload = storage.finish_run(req.run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@router.post("/run/abort")
def abort_run(req: RunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_run_owner(req.run_id, user_id)
    payload = storage.abort_run(req.run_id, reason="user_exit_to_map")
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.get("/health")
def map_health() -> Dict[str, Any]:
    return {"ok": True, "service": "kline-map-api"}


@map_router.post("/run/create")
def create_map_run(req: MapRunCreateReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    setup_payload = req.new_game_setup.model_dump() if req.new_game_setup is not None else None
    map_run_id = map_storage.create_map_run(
        user_id=user_id,
        seed=req.seed,
        setup=setup_payload,
        restart_existing=bool(req.restart_existing_active),
    )
    if not map_run_id:
        _not_ok({"message": "create map run failed"})
    resp: Dict[str, Any] = {"ok": True, "map_run_id": int(map_run_id)}
    if setup_payload:
        created = map_storage.get_map_state(int(map_run_id))
        if created.get("ok"):
            mr = dict(created.get("map_run") or {})
            resp["applied_setup_summary"] = {
                "player_name": mr.get("player_name"),
                "traits": list(mr.get("traits") or []),
                "style_answers": dict(mr.get("style_answers") or {}),
                "god_mode": bool(mr.get("god_mode", False)),
                "initial_deck_size": len(list(mr.get("home_deck") or [])),
            }
    return resp


@map_router.post("/run/resume")
def resume_map_run(user_id: str = Depends(require_user)) -> Dict[str, Any]:
    run = map_storage.get_resume_map_run(user_id)
    return {"ok": True, "run": run}


@map_router.post("/run/state")
def map_run_state(req: MapRunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.get_map_state(req.map_run_id)
    if not payload.get("ok"):
        _not_ok(payload, status.HTTP_404_NOT_FOUND)
    return payload


@map_router.post("/location/move")
def map_move_location(req: MapMoveReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.move_location(req.map_run_id, req.to_location)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.post("/turn/rest")
def map_rest_turn(req: MapRunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.rest_and_advance_turn(req.map_run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.post("/home/deck/get")
def map_get_home_deck(req: MapRunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.get_home_deck(req.map_run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.post("/home/deck/save")
def map_save_home_deck(req: MapDeckSaveReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.save_home_deck(req.map_run_id, req.deck_cards)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.post("/battle/start")
def map_start_battle(req: MapBattleStartReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.start_battle_from_map(req.map_run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.post("/battle/commit")
def map_commit_battle(req: MapBattleCommitReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.commit_battle_result(req.map_run_id, req.battle_run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload


@map_router.post("/run/finish")
def map_finish_run(req: MapRunReq, user_id: str = Depends(require_user)) -> Dict[str, Any]:
    _assert_map_run_owner(req.map_run_id, user_id)
    payload = map_storage.finish_map_run(req.map_run_id)
    if not payload.get("ok"):
        _not_ok(payload)
    return payload

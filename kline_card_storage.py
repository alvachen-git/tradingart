"""Storage and run-state service for K-line card roguelike MVP.

Hard constraints:
- Only writes to kline_card_* tables.
- Never writes to users.experience, kline_game_records, kline_game_stats.
"""

from __future__ import annotations

import json
import os
import random
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import create_engine, text

import kline_card_data as card_data
import kline_card_rules as rules


FINAL_RUN_STATUSES = {"failed", "cleared"}
_STAGE_BARS_CACHE_MAX = 128
_STAGE_BARS_CACHE: "OrderedDict[Tuple[int, int, int, int], List[Dict[str, Any]]]" = OrderedDict()


def _build_default_engine():
    try:
        from data_engine import engine as shared_engine  # type: ignore
        return shared_engine
    except Exception:
        pass

    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT") or "3306"
    db_name = os.getenv("DB_NAME")
    if all([db_user, db_password, db_host, db_name]):
        db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        return create_engine(db_url, pool_pre_ping=True, pool_recycle=3600)

    # Test/dev fallback. Test suites monkeypatch this engine anyway.
    return create_engine("sqlite:///:memory:", future=True)


engine = _build_default_engine()


def _now() -> datetime:
    return datetime.now()


def _gen_id() -> int:
    return int(time.time() * 1000) * 1000 + random.randint(0, 999)


def _json_dump(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def _json_load(payload: Any, default: Any) -> Any:
    if payload is None:
        return default
    if isinstance(payload, (dict, list)):
        return payload
    try:
        return json.loads(payload)
    except Exception:
        return default


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _to_str(v: Any, default: str = "") -> str:
    if v is None:
        return default
    try:
        return str(v)
    except Exception:
        return default


def init_card_game_schema() -> None:
    with engine.begin() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS kline_card_runs (
                run_id BIGINT PRIMARY KEY,
                user_id VARCHAR(128) NOT NULL,
                seed BIGINT NOT NULL DEFAULT 0,
                status VARCHAR(32) NOT NULL DEFAULT 'created',
                current_stage INT NOT NULL DEFAULT 1,
                current_turn INT NOT NULL DEFAULT 1,
                current_stage_score INT NOT NULL DEFAULT 0,
                total_score INT NOT NULL DEFAULT 0,
                cleared_stages INT NOT NULL DEFAULT 0,
                confidence INT NOT NULL DEFAULT 80,
                starting_confidence INT NOT NULL DEFAULT 80,
                hand_limit INT NOT NULL DEFAULT 10,
                deck_json TEXT NULL,
                hand_json TEXT NULL,
                discard_json TEXT NULL,
                run_effects_json TEXT NULL,
                pending_upgrade_json TEXT NULL,
                current_symbol VARCHAR(32) NULL,
                current_symbol_name VARCHAR(64) NULL,
                current_symbol_type VARCHAR(16) NULL,
                reward_claimed TINYINT NOT NULL DEFAULT 0,
                reward_exp INT NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                finished_at DATETIME NULL
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS kline_card_stage_logs (
                run_id BIGINT NOT NULL,
                stage_no INT NOT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'pending',
                is_boss TINYINT NOT NULL DEFAULT 0,
                symbol VARCHAR(32) NULL,
                symbol_name VARCHAR(64) NULL,
                symbol_type VARCHAR(16) NULL,
                target_score INT NOT NULL DEFAULT 0,
                stage_score INT NOT NULL DEFAULT 0,
                current_turn INT NOT NULL DEFAULT 1,
                visible_end INT NOT NULL DEFAULT 20,
                candidate_json TEXT NULL,
                bars_json TEXT NULL,
                event_json TEXT NULL,
                last_result_json TEXT NULL,
                started_at DATETIME NOT NULL,
                ended_at DATETIME NULL,
                PRIMARY KEY (run_id, stage_no)
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS kline_card_turn_logs (
                turn_id BIGINT PRIMARY KEY,
                run_id BIGINT NOT NULL,
                stage_no INT NOT NULL,
                turn_no INT NOT NULL,
                action_json TEXT NULL,
                result_json TEXT NULL,
                created_at DATETIME NOT NULL
            )
            """
        ))
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS kline_card_user_meta (
                user_id VARCHAR(128) PRIMARY KEY,
                level INT NOT NULL DEFAULT 1,
                exp INT NOT NULL DEFAULT 0,
                skill_points INT NOT NULL DEFAULT 0,
                spent_points INT NOT NULL DEFAULT 0,
                upgrades_json TEXT NULL,
                games_played INT NOT NULL DEFAULT 0,
                games_cleared INT NOT NULL DEFAULT 0,
                best_score INT NOT NULL DEFAULT 0,
                total_score BIGINT NOT NULL DEFAULT 0,
                created_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL
            )
            """
        ))


def _get_run_row(run_id: int) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM kline_card_runs WHERE run_id = :rid LIMIT 1"),
            {"rid": int(run_id)},
        ).fetchone()
    return dict(row._mapping) if row else None


def _get_stage_row(run_id: int, stage_no: int) -> Optional[Dict[str, Any]]:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM kline_card_stage_logs WHERE run_id = :rid AND stage_no = :sn LIMIT 1"),
            {"rid": int(run_id), "sn": int(stage_no)},
        ).fetchone()
    return dict(row._mapping) if row else None


def _serialize_run(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    out["deck"] = _json_load(out.pop("deck_json", None), [])
    out["hand"] = _json_load(out.pop("hand_json", None), [])
    out["discard"] = _json_load(out.pop("discard_json", None), [])
    out["run_effects"] = _json_load(out.pop("run_effects_json", None), {})
    out["pending_upgrades"] = _json_load(out.pop("pending_upgrade_json", None), [])
    return out


def _serialize_stage(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    run_id = _to_int(out.get("run_id"), 0)
    stage_no = _to_int(out.get("stage_no"), 0)
    raw_bars = out.pop("bars_json", None)
    out["candidate_pool"] = _json_load(out.pop("candidate_json", None), [])
    out["bars"] = _load_stage_bars_cached(run_id, stage_no, raw_bars)
    out["event_state"] = _json_load(out.pop("event_json", None), {})
    out["last_result"] = _json_load(out.pop("last_result_json", None), {})
    return out


def _bars_cache_key(run_id: int, stage_no: int, raw: Any) -> Optional[Tuple[int, int, int, int]]:
    if raw is None:
        return None
    if isinstance(raw, list):
        payload = _json_dump(raw)
    elif isinstance(raw, str):
        payload = raw
    else:
        payload = _json_dump(raw)
    return (int(run_id), int(stage_no), len(payload), hash(payload))


def _load_stage_bars_cached(run_id: int, stage_no: int, raw: Any) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    cache_key = _bars_cache_key(run_id, stage_no, raw)
    if cache_key and cache_key in _STAGE_BARS_CACHE:
        bars = _STAGE_BARS_CACHE.pop(cache_key)
        _STAGE_BARS_CACHE[cache_key] = bars
        return bars
    bars = _json_load(raw, [])
    if not isinstance(bars, list):
        bars = []
    if cache_key:
        _STAGE_BARS_CACHE[cache_key] = bars
        if len(_STAGE_BARS_CACHE) > _STAGE_BARS_CACHE_MAX:
            _STAGE_BARS_CACHE.popitem(last=False)
    return bars


def _evict_stage_bars_cache(run_id: int, stage_no: int) -> None:
    if not _STAGE_BARS_CACHE:
        return
    prefix = (int(run_id), int(stage_no))
    hit_keys = [k for k in _STAGE_BARS_CACHE.keys() if k[0] == prefix[0] and k[1] == prefix[1]]
    for k in hit_keys:
        _STAGE_BARS_CACHE.pop(k, None)


def _get_or_create_meta(user_id: str) -> Dict[str, Any]:
    uid = _to_str(user_id).strip()
    if not uid:
        return {}

    init_card_game_schema()
    with engine.begin() as conn:
        row = conn.execute(
            text("SELECT * FROM kline_card_user_meta WHERE user_id = :uid LIMIT 1"),
            {"uid": uid},
        ).fetchone()
        if not row:
            now = _now()
            conn.execute(
                text(
                    """
                    INSERT INTO kline_card_user_meta
                    (user_id, level, exp, skill_points, spent_points, upgrades_json, games_played, games_cleared,
                     best_score, total_score, created_at, updated_at)
                    VALUES
                    (:uid, 1, 0, 0, 0, :up, 0, 0, 0, 0, :now, :now)
                    """
                ),
                {"uid": uid, "up": _json_dump({}), "now": now},
            )
            row = conn.execute(
                text("SELECT * FROM kline_card_user_meta WHERE user_id = :uid LIMIT 1"),
                {"uid": uid},
            ).fetchone()

    data = dict(row._mapping) if row else {}
    data["upgrades"] = _json_load(data.pop("upgrades_json", None), {})
    return data


def get_card_meta(user_id: str) -> Dict[str, Any]:
    meta = _get_or_create_meta(user_id)
    if not meta:
        return {"ok": False, "message": "missing user_id"}

    return {
        "ok": True,
        "user_id": _to_str(user_id),
        "level": _to_int(meta.get("level"), 1),
        "exp": _to_int(meta.get("exp"), 0),
        "skill_points": _to_int(meta.get("skill_points"), 0),
        "spent_points": _to_int(meta.get("spent_points"), 0),
        "upgrades": meta.get("upgrades", {}),
        "games_played": _to_int(meta.get("games_played"), 0),
        "games_cleared": _to_int(meta.get("games_cleared"), 0),
        "best_score": _to_int(meta.get("best_score"), 0),
        "total_score": _to_int(meta.get("total_score"), 0),
    }


def _advance_meta_level(exp_value: int) -> int:
    return int(exp_value // 500) + 1


def apply_card_meta_upgrade(user_id: str, upgrade_code: str) -> Dict[str, Any]:
    uid = _to_str(user_id).strip()
    code = _to_str(upgrade_code).strip()
    if not uid or not code:
        return {"ok": False, "message": "missing user_id/upgrade_code"}
    if code not in rules.META_UPGRADES:
        return {"ok": False, "message": "invalid upgrade_code"}

    init_card_game_schema()
    meta = _get_or_create_meta(uid)
    upgrades = dict(meta.get("upgrades") or {})
    current_level = _to_int(upgrades.get(code), 0)
    max_level = _to_int(rules.META_UPGRADES[code].get("max_level"), 0)
    cost = _to_int(rules.META_UPGRADES[code].get("cost"), 1)
    skill_points = _to_int(meta.get("skill_points"), 0)

    if current_level >= max_level:
        return {"ok": False, "message": "upgrade maxed"}
    if skill_points < cost:
        return {"ok": False, "message": "not enough skill points"}

    upgrades[code] = current_level + 1
    now = _now()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_user_meta
                SET skill_points = :sp,
                    spent_points = :spent,
                    upgrades_json = :up,
                    updated_at = :now
                WHERE user_id = :uid
                """
            ),
            {
                "uid": uid,
                "sp": skill_points - cost,
                "spent": _to_int(meta.get("spent_points"), 0) + cost,
                "up": _json_dump(upgrades),
                "now": now,
            },
        )

    return {"ok": True, "meta": get_card_meta(uid)}


def create_run(user_id: str, seed: Optional[int] = None, deck_override: Optional[List[str]] = None) -> int:
    uid = _to_str(user_id).strip()
    if not uid:
        return 0

    init_card_game_schema()
    meta = _get_or_create_meta(uid)
    upgrades = dict(meta.get("upgrades") or {})
    bonuses = rules.compute_meta_bonuses(upgrades)

    run_seed = int(seed if seed is not None else random.randint(1, 2_000_000_000))
    run_id = _gen_id()
    starting_confidence = 80 + _to_int(bonuses.get("starting_confidence_bonus"), 0)
    hand_limit = 10 + _to_int(bonuses.get("hand_limit_bonus"), 0)

    run_effects = {
        "rules_version": rules.RULE_VERSION,
        "draw_quality": _to_int(bonuses.get("draw_quality"), 0),
        "score_multiplier": float(bonuses.get("score_multiplier", 1.0)),
        "momentum": 0,
        "score_streak": 0,
        "extra_draw_next_turn": 0,
        "extra_draw_pending_turn": 0,
        "dynamic_adjust_pending_turn": 0,
        "dynamic_adjust_pending_once": 0,
    }

    deck = rules.build_initial_deck(seed=run_seed)
    if isinstance(deck_override, list):
        override = [str(cid).strip() for cid in deck_override if str(cid).strip() in rules.CARD_LIBRARY]
        if override:
            deck = override
    deck, hand, discard = rules.draw_cards(
        deck=deck,
        hand=[],
        discard=[],
        draw_count=3,
        hand_limit=hand_limit,
        run_effects=run_effects,
        seed=run_seed + 11,
    )

    now = _now()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO kline_card_runs
                (run_id, user_id, seed, status, current_stage, current_turn, current_stage_score, total_score,
                 cleared_stages, confidence, starting_confidence, hand_limit, deck_json, hand_json, discard_json,
                 run_effects_json, pending_upgrade_json, current_symbol, current_symbol_name, current_symbol_type,
                 reward_claimed, reward_exp, created_at, updated_at, finished_at)
                VALUES
                (:rid, :uid, :seed, 'await_stage_start', 1, 1, 0, 0, 0, :conf, :start_conf, :hand_limit,
                 :deck, :hand, :discard, :effects, :pending, NULL, NULL, NULL, 0, 0, :now, :now, NULL)
                """
            ),
            {
                "rid": run_id,
                "uid": uid,
                "seed": run_seed,
                "conf": starting_confidence,
                "start_conf": starting_confidence,
                "hand_limit": hand_limit,
                "deck": _json_dump(deck),
                "hand": _json_dump(hand),
                "discard": _json_dump(discard),
                "effects": _json_dump(run_effects),
                "pending": _json_dump([]),
                "now": now,
            },
        )
        conn.execute(
            text(
                """
                UPDATE kline_card_user_meta
                SET games_played = games_played + 1,
                    updated_at = :now
                WHERE user_id = :uid
                """
            ),
            {"uid": uid, "now": now},
        )

    return run_id


def get_resume_run(user_id: str) -> Optional[Dict[str, Any]]:
    uid = _to_str(user_id).strip()
    if not uid:
        return None
    init_card_game_schema()
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM kline_card_runs
                WHERE user_id = :uid
                  AND status NOT IN ('failed', 'cleared')
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {"uid": uid},
        ).fetchone()
    if not row:
        return None

    run = _serialize_run(dict(row._mapping))
    if not _is_v2_run(run):
        return None
    stage = _get_stage_row(_to_int(run["run_id"]), _to_int(run["current_stage"]))
    run["stage_state"] = _serialize_stage(stage) if stage else None
    return run


def _stage_snapshot(run: Dict[str, Any], stage: Dict[str, Any]) -> Dict[str, Any]:
    bars = list(stage.get("bars") or [])
    visible_end = _to_int(stage.get("visible_end"), 20)
    run_effects = dict(run.get("run_effects") or {})
    pending_discard = max(0, _to_int(run_effects.get("pending_discard"), 0))
    return {
        "ok": True,
        "run_id": _to_int(run.get("run_id")),
        "status": _to_str(run.get("status")),
        "stage_no": _to_int(stage.get("stage_no"), _to_int(run.get("current_stage"), 1)),
        "symbol": _to_str(stage.get("symbol")),
        "symbol_name": _to_str(stage.get("symbol_name")),
        "symbol_type": _to_str(stage.get("symbol_type")),
        "current_turn": _to_int(stage.get("current_turn"), 1),
        "confidence": _to_int(run.get("confidence"), 0),
        "stage_score": _to_int(stage.get("stage_score"), 0),
        "target_score": _to_int(stage.get("target_score"), 0),
        "total_score": _to_int(run.get("total_score"), 0),
        "hand": list(run.get("hand") or []),
        "deck_count": len(run.get("deck") or []),
        "discard_count": len(run.get("discard") or []),
        "visible_bars": bars[:max(0, visible_end)],
        "event_state": stage.get("event_state") or {},
        "pending_upgrades": run.get("pending_upgrades") or [],
        "pending_discard": pending_discard,
        "need_discard": pending_discard > 0,
        "hand_limit": _to_int(run.get("hand_limit"), 10),
    }


def _upsert_stage(
    run_id: int,
    stage_no: int,
    status: str,
    is_boss: int,
    started_at: datetime,
    candidate_pool: Optional[List[Dict[str, Any]]] = None,
    bars: Optional[List[Dict[str, Any]]] = None,
    event_state: Optional[Dict[str, Any]] = None,
    symbol: Optional[str] = None,
    symbol_name: Optional[str] = None,
    symbol_type: Optional[str] = None,
    target_score: Optional[int] = None,
    stage_score: Optional[int] = None,
    current_turn: Optional[int] = None,
    visible_end: Optional[int] = None,
    last_result: Optional[Dict[str, Any]] = None,
    ended_at: Optional[datetime] = None,
) -> None:
    existing = _get_stage_row(run_id, stage_no)
    payload = {
        "rid": int(run_id),
        "sn": int(stage_no),
        "status": status,
        "is_boss": int(is_boss),
        "symbol": symbol,
        "symbol_name": symbol_name,
        "symbol_type": symbol_type,
        "target_score": int(target_score or 0),
        "stage_score": int(stage_score or 0),
        "current_turn": int(current_turn or 1),
        "visible_end": int(visible_end or 20),
        "candidate_json": _json_dump(candidate_pool or []),
        "bars_json": _json_dump(bars or []),
        "event_json": _json_dump(event_state or {}),
        "last_result_json": _json_dump(last_result or {}),
        "started_at": started_at,
        "ended_at": ended_at,
    }

    with engine.begin() as conn:
        if existing:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_stage_logs
                    SET status = :status,
                        is_boss = :is_boss,
                        symbol = :symbol,
                        symbol_name = :symbol_name,
                        symbol_type = :symbol_type,
                        target_score = :target_score,
                        stage_score = :stage_score,
                        current_turn = :current_turn,
                        visible_end = :visible_end,
                        candidate_json = :candidate_json,
                        bars_json = :bars_json,
                        event_json = :event_json,
                        last_result_json = :last_result_json,
                        ended_at = :ended_at
                    WHERE run_id = :rid
                      AND stage_no = :sn
                    """
                ),
                payload,
            )
        else:
            conn.execute(
                text(
                    """
                    INSERT INTO kline_card_stage_logs
                    (run_id, stage_no, status, is_boss, symbol, symbol_name, symbol_type,
                     target_score, stage_score, current_turn, visible_end, candidate_json, bars_json,
                     event_json, last_result_json, started_at, ended_at)
                    VALUES
                    (:rid, :sn, :status, :is_boss, :symbol, :symbol_name, :symbol_type,
                     :target_score, :stage_score, :current_turn, :visible_end, :candidate_json, :bars_json,
                     :event_json, :last_result_json, :started_at, :ended_at)
                    """
                ),
                payload,
            )
    _evict_stage_bars_cache(run_id, stage_no)


def _update_stage_after_turn(
    run_id: int,
    stage_no: int,
    status: str,
    stage_score: int,
    current_turn: int,
    visible_end: int,
    event_state: Dict[str, Any],
    last_result: Dict[str, Any],
    ended_at: Optional[datetime],
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_stage_logs
                SET status = :status,
                    stage_score = :stage_score,
                    current_turn = :current_turn,
                    visible_end = :visible_end,
                    event_json = :event_json,
                    last_result_json = :last_result_json,
                    ended_at = :ended_at
                WHERE run_id = :rid
                  AND stage_no = :sn
                """
            ),
            {
                "rid": int(run_id),
                "sn": int(stage_no),
                "status": status,
                "stage_score": int(stage_score),
                "current_turn": int(current_turn),
                "visible_end": int(visible_end),
                "event_json": _json_dump(event_state or {}),
                "last_result_json": _json_dump(last_result or {}),
                "ended_at": ended_at,
            },
        )


def _load_run_or_error(run_id: int) -> Dict[str, Any]:
    run = _get_run_row(run_id)
    if not run:
        return {"ok": False, "message": "run not found"}
    payload = _serialize_run(run)
    if not _is_v2_run(payload):
        return {"ok": False, "message": "run is legacy and cannot continue, please create a new run"}
    return {"ok": True, "run": payload}


def _is_v2_run(run: Dict[str, Any]) -> bool:
    effects = dict(run.get("run_effects") or {})
    return _to_str(effects.get("rules_version")) == rules.RULE_VERSION


def start_stage(
    run_id: int,
    stage_no: int,
    symbol_choice: Optional[str] = None,
    auto_random_choice: bool = False,
) -> Dict[str, Any]:
    init_card_game_schema()
    loaded = _load_run_or_error(run_id)
    if not loaded["ok"]:
        return loaded
    run = loaded["run"]

    if _to_str(run.get("status")) in FINAL_RUN_STATUSES:
        return {"ok": False, "message": "run already finished"}

    current_stage = _to_int(run.get("current_stage"), 1)
    stage_num = _to_int(stage_no, current_stage)
    if stage_num != current_stage:
        return {"ok": False, "message": f"invalid stage_no, expected {current_stage}"}

    stage = _get_stage_row(run_id, stage_num)
    stage_data = _serialize_stage(stage) if stage else None
    now = _now()

    if stage_data and _to_str(stage_data.get("status")) == "playing":
        return _stage_snapshot(run, stage_data)

    is_boss = 1 if stage_num == 5 else 0
    selected = None
    candidates: List[Dict[str, Any]] = []
    if stage_num < 5:
        if not symbol_choice:
            if auto_random_choice:
                if stage_data and _to_str(stage_data.get("status")) == "choose_symbol" and stage_data.get("candidate_pool"):
                    candidates = list(stage_data.get("candidate_pool") or [])
                else:
                    candidates = card_data.get_stage_candidates(
                        stage_no=stage_num,
                        count=1,
                        seed=_to_int(run.get("seed"), 0) + stage_num * 37,
                    )
                if not candidates:
                    return {"ok": False, "message": "no available stage candidate"}
                selected = random.choice(candidates)
                candidates = [selected]
            else:
                if stage_data and _to_str(stage_data.get("status")) == "choose_symbol" and stage_data.get("candidate_pool"):
                    candidates = list(stage_data.get("candidate_pool") or [])
                else:
                    candidates = card_data.get_stage_candidates(
                        stage_no=stage_num,
                        count=3,
                        seed=_to_int(run.get("seed"), 0) + stage_num * 37,
                    )
                _upsert_stage(
                    run_id=run_id,
                    stage_no=stage_num,
                    status="choose_symbol",
                    is_boss=is_boss,
                    started_at=now,
                    candidate_pool=candidates,
                    bars=[],
                    event_state={},
                    target_score=rules.get_stage_target(stage_num),
                    stage_score=0,
                    current_turn=1,
                    visible_end=20,
                    last_result={},
                )
                return {"ok": True, "need_choice": True, "run_id": run_id, "stage_no": stage_num, "candidates": candidates}

        if selected is None:
            if stage_data and stage_data.get("candidate_pool"):
                candidates = list(stage_data.get("candidate_pool") or [])
            else:
                candidates = card_data.get_stage_candidates(
                    stage_no=stage_num,
                    count=3,
                    seed=_to_int(run.get("seed"), 0) + stage_num * 37,
                )

            choice = _to_str(symbol_choice).strip()
            for c in candidates:
                if _to_str(c.get("symbol")) == choice:
                    selected = c
                    break
            if not selected:
                return {"ok": False, "message": "symbol_choice not in candidates"}
    else:
        selected = card_data.get_boss_stage_candidate(
            stage_no=stage_num,
            seed=_to_int(run.get("seed"), 0) + stage_num * 53,
        )
        candidates = [selected]

    bars = list(selected.get("bars") or [])
    if len(bars) < 120:
        return {"ok": False, "message": "bars not enough for stage"}

    event_state = rules.roll_stage_event(_to_int(run.get("seed"), 0), stage_num)
    target = rules.get_stage_target(stage_num)

    _upsert_stage(
        run_id=run_id,
        stage_no=stage_num,
        status="playing",
        is_boss=is_boss,
        started_at=now,
        candidate_pool=candidates if stage_num < 5 else [],
        bars=bars,
        event_state=event_state,
        symbol=_to_str(selected.get("symbol")),
        symbol_name=_to_str(selected.get("symbol_name")),
        symbol_type=_to_str(selected.get("symbol_type")),
        target_score=target,
        stage_score=0,
        current_turn=1,
        visible_end=20,
        last_result={},
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_runs
                SET status = 'playing',
                    current_turn = 1,
                    current_stage_score = 0,
                    pending_upgrade_json = :pending,
                    current_symbol = :sym,
                    current_symbol_name = :sym_name,
                    current_symbol_type = :sym_type,
                    updated_at = :now
                WHERE run_id = :rid
                """
            ),
            {
                "rid": int(run_id),
                "pending": _json_dump([]),
                "sym": _to_str(selected.get("symbol")),
                "sym_name": _to_str(selected.get("symbol_name")),
                "sym_type": _to_str(selected.get("symbol_type")),
                "now": now,
            },
        )

    run_latest = _serialize_run(_get_run_row(run_id) or {})
    stage_latest = _serialize_stage(_get_stage_row(run_id, stage_num) or {})
    return _stage_snapshot(run_latest, stage_latest)


def _bars_slice_for_turn(stage_bars: List[Dict[str, Any]], visible_end: int) -> Dict[str, Any]:
    reveal_start = int(visible_end)
    reveal_end = reveal_start + 5
    # V2: 突破牌必须只对比“当前时点前已可见历史”，因此 context 传全部已揭示 K 线。
    context = stage_bars[:reveal_start]
    future = stage_bars[reveal_start:reveal_end]
    return {
        "context": context,
        "future": future,
        "reveal_start": reveal_start,
        "reveal_end": reveal_end,
    }


def play_turn(run_id: int, action: Dict[str, Any]) -> Dict[str, Any]:
    init_card_game_schema()
    loaded = _load_run_or_error(run_id)
    if not loaded["ok"]:
        return loaded
    run = loaded["run"]

    if _to_str(run.get("status")) in FINAL_RUN_STATUSES:
        return {"ok": False, "message": "run already finished"}

    stage_no = _to_int(run.get("current_stage"), 1)
    stage_row = _get_stage_row(run_id, stage_no)
    if not stage_row:
        return {"ok": False, "message": "stage not prepared"}
    stage = _serialize_stage(stage_row)
    if _to_str(stage.get("status")) != "playing":
        return {"ok": False, "message": "stage not in playing state"}

    turn_no = _to_int(stage.get("current_turn"), 1)
    if turn_no > 20:
        return {"ok": False, "message": "stage already completed"}

    hand_limit = _to_int(run.get("hand_limit"), 10)
    deck = list(run.get("deck") or [])
    hand = list(run.get("hand") or [])
    discard = list(run.get("discard") or [])
    run_effects = dict(run.get("run_effects") or {})
    pending_discard = max(0, _to_int(run_effects.get("pending_discard"), 0))
    bars = list(stage.get("bars") or [])
    visible_end = _to_int(stage.get("visible_end"), 20)
    stage_score = _to_int(stage.get("stage_score"), 0)
    confidence = _to_int(run.get("confidence"), 0)
    total_score = _to_int(run.get("total_score"), 0)
    run_effects["rules_version"] = rules.RULE_VERSION
    extra_draw_pending_turn = max(0, _to_int(run_effects.get("extra_draw_pending_turn"), 0))
    extra_draw_carry = 0
    if extra_draw_pending_turn == turn_no:
        extra_draw_carry = max(0, _to_int(run_effects.get("extra_draw_next_turn"), 0))
    extra_draw_applied_this_turn = extra_draw_carry

    dynamic_adjust_pending_turn = max(0, _to_int(run_effects.get("dynamic_adjust_pending_turn"), 0))
    dynamic_adjust_applied_this_turn = False
    dynamic_adjust_discarded_count = 0
    if dynamic_adjust_pending_turn == turn_no:
        old_hand_count = len(hand)
        if old_hand_count > 0:
            discard.extend(hand)
            hand = []
            dynamic_adjust_discarded_count = old_hand_count
        deck, hand, discard = rules.draw_cards(
            deck=deck,
            hand=hand,
            discard=discard,
            draw_count=old_hand_count + 2 + extra_draw_carry,
            hand_limit=9999,
            run_effects=run_effects,
            seed=_to_int(run.get("seed"), 0) + stage_no * 100 + turn_no + 900,
        )
        dynamic_adjust_applied_this_turn = True
        extra_draw_carry = 0  # 已在动态调整重抽时消耗
        pending_discard = max(0, len(hand) - hand_limit)
        if pending_discard > 0:
            run_effects["pending_discard"] = pending_discard
        else:
            run_effects.pop("pending_discard", None)
        run_effects["dynamic_adjust_pending_turn"] = 0
        run_effects["dynamic_adjust_pending_once"] = 0
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET deck_json = :deck,
                        hand_json = :hand,
                        discard_json = :discard,
                        run_effects_json = :effects,
                        updated_at = :now
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": int(run_id),
                    "deck": _json_dump(deck),
                    "hand": _json_dump(hand),
                    "discard": _json_dump(discard),
                    "effects": _json_dump(run_effects),
                    "now": _now(),
                },
            )

    action = action or {}
    card_id = _to_str(action.get("card_id")).strip()
    action_type = "play"
    played_cards: List[str] = []
    action_kind = _to_str(action.get("type")).lower()
    if pending_discard > 0 and action_kind != "discard":
        return {
            "ok": False,
            "message": f"discard required before next action: {pending_discard}",
            "need_discard": True,
            "pending_discard": pending_discard,
            "hand": hand,
            "hand_limit": hand_limit,
        }
    if action_kind == "discard":
        raw_cards = action.get("cards")
        selected: List[str] = []
        if isinstance(raw_cards, list):
            selected = [_to_str(x).strip() for x in raw_cards if _to_str(x).strip()]
        if not selected and card_id:
            selected = [card_id]
        if not selected:
            return {"ok": False, "message": "discard cards is empty"}
        if len(selected) > pending_discard:
            return {"ok": False, "message": f"discard too many cards, need {pending_discard}"}
        hand_count: Dict[str, int] = {}
        for h in hand:
            hand_count[h] = hand_count.get(h, 0) + 1
        for cid in selected:
            if hand_count.get(cid, 0) <= 0:
                return {"ok": False, "message": f"discard card not in hand: {cid}"}
            hand_count[cid] -= 1
        for cid in selected:
            hand.remove(cid)
            discard.append(cid)
        pending_discard = max(0, pending_discard - len(selected))
        if pending_discard > 0:
            run_effects["pending_discard"] = pending_discard
        else:
            run_effects.pop("pending_discard", None)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET hand_json = :hand,
                        discard_json = :discard,
                        run_effects_json = :effects,
                        updated_at = :now
                    WHERE run_id = :rid
                    """
                ),
                {
                    "rid": int(run_id),
                    "hand": _json_dump(hand),
                    "discard": _json_dump(discard),
                    "effects": _json_dump(run_effects),
                    "now": _now(),
                },
            )
        return {
            "ok": True,
            "run_id": run_id,
            "stage_no": stage_no,
            "turn_no": turn_no,
            "action_type": "discard",
            "discarded_cards": selected,
            "pending_discard": pending_discard,
            "need_discard": pending_discard > 0,
            "hand": hand,
            "hand_limit": hand_limit,
            "deck_count": len(deck),
            "discard_count": len(discard),
            "run_status": _to_str(run.get("status")),
            "stage_status": _to_str(stage.get("status")),
        }
    if action_kind == "combo":
        raw_cards = action.get("cards")
        if not isinstance(raw_cards, list):
            return {"ok": False, "message": "combo cards must be list"}
        normalized = [_to_str(x).strip() for x in raw_cards if _to_str(x).strip()]
        if not normalized:
            return {"ok": False, "message": "combo cards is empty"}
        conflict = rules.validate_combo_direction_conflict(normalized)
        if not conflict.get("ok", False):
            return {"ok": False, "message": _to_str(conflict.get("message"), "trend direction conflict")}
        hand_count: Dict[str, int] = {}
        for h in hand:
            hand_count[h] = hand_count.get(h, 0) + 1
        for cid in normalized:
            if hand_count.get(cid, 0) <= 0:
                return {"ok": False, "message": f"card not enough in hand: {cid}"}
            hand_count[cid] -= 1
        for cid in normalized:
            hand.remove(cid)
        played_cards = normalized
        action_type = "combo"
    elif action_kind == "pass" or not card_id:
        action_type = "pass"
        card_id = ""
    elif card_id not in hand:
        return {"ok": False, "message": "card not in hand"}
    else:
        hand.remove(card_id)
        played_cards = [card_id]

    bars_slice = _bars_slice_for_turn(bars, visible_end)
    if len(bars_slice["future"]) < 5:
        return {"ok": False, "message": "insufficient bars for settlement"}

    if action_type == "pass":
        combo_cards: List[str] = []
    elif action_type == "play":
        combo_cards = [card_id]
    else:
        combo_cards = list(played_cards)
    resolve_effects = dict(run_effects)
    resolve_effects["_confidence_current"] = confidence
    resolve = rules.resolve_turn_combo(
        card_ids=combo_cards,
        context_bars=bars_slice["context"],
        future_bars=bars_slice["future"],
        stage_no=stage_no,
        run_effects=resolve_effects,
        seed=_to_int(run.get("seed"), 0) + turn_no * 13 + stage_no * 7,
    )
    if not resolve.get("ok", True):
        return {"ok": False, "message": _to_str(resolve.get("message"), "turn resolve failed")}

    mechanics = dict(resolve.get("mechanics") or {})
    confidence_events: List[Dict[str, Any]] = []
    score_streak_before = max(0, _to_int(run_effects.get("score_streak"), 0))
    score_streak_after = score_streak_before
    momentum_before = max(0, _to_int(run_effects.get("momentum"), 0))
    momentum_after = max(0, _to_int(mechanics.get("momentum_after"), momentum_before))

    confidence_before = confidence
    stage_score_before = stage_score
    total_score_before = total_score

    if action_type == "pass":
        confidence -= 2
        confidence_events.append({"code": "pass_penalty", "delta": -2})

    turn_score = _to_int(resolve.get("turn_score"), 0)
    if turn_score < 0:
        confidence -= 4
        confidence_events.append({"code": "turn_negative_penalty", "delta": -4})
    if turn_score > 50:
        confidence += 4
        confidence_events.append({"code": "turn_gt_50_bonus", "delta": 4})

    confidence_from_cards = _to_int(mechanics.get("confidence_delta_from_cards"), 0)
    if confidence_from_cards != 0:
        confidence += confidence_from_cards
        confidence_events.append({"code": "card_mechanics", "delta": confidence_from_cards})

    if turn_score > 0:
        score_streak_after = score_streak_before + 1
        if score_streak_after % 3 == 0:
            confidence += 6
            confidence_events.append({"code": "three_turn_score_streak", "delta": 6})
    else:
        score_streak_after = 0

    stage_score += turn_score
    total_score += turn_score
    if total_score < 0:
        confidence -= 6
        confidence_events.append({"code": "total_score_negative_end", "delta": -6})
    visible_end += 5
    if action_type == "play" and card_id:
        discard.append(card_id)
    if action_type == "combo" and played_cards:
        discard.extend(played_cards)

    stage_complete = False
    stage_status = "playing"
    run_status = "playing"
    target_score = _to_int(stage.get("target_score"), rules.get_stage_target(stage_no))
    pending_upgrades: List[Dict[str, Any]] = []
    cleared_stages = _to_int(run.get("cleared_stages"), 0)
    finished_at = None

    if confidence <= 0:
        stage_complete = True
        stage_status = "failed"
        run_status = "failed"
        finished_at = _now()
    elif turn_no >= 20:
        stage_complete = True
        if stage_score >= target_score:
            stage_status = "cleared"
            if stage_no >= 5:
                run_status = "cleared"
                cleared_stages = max(cleared_stages, 5)
                finished_at = _now()
            else:
                run_status = "stage_cleared"
                cleared_stages = max(cleared_stages, stage_no)
        else:
            stage_status = "failed"
            run_status = "failed"
            finished_at = _now()

    drawn_cards: List[str] = []
    run_effects["score_streak"] = score_streak_after
    run_effects["momentum"] = momentum_after
    next_extra_draw_gain = max(0, _to_int(mechanics.get("extra_draw_next_turn_gain"), 0))
    if next_extra_draw_gain > 0:
        run_effects["extra_draw_next_turn"] = next_extra_draw_gain
        run_effects["extra_draw_pending_turn"] = turn_no + 1
    else:
        run_effects["extra_draw_next_turn"] = 0
        run_effects["extra_draw_pending_turn"] = 0
    next_dynamic_adjust = bool(mechanics.get("dynamic_adjust_next_turn", False))
    if next_dynamic_adjust:
        run_effects["dynamic_adjust_pending_turn"] = turn_no + 1
        run_effects["dynamic_adjust_pending_once"] = 1
    else:
        run_effects["dynamic_adjust_pending_turn"] = 0
        run_effects["dynamic_adjust_pending_once"] = 0
    if not stage_complete:
        hand_before_draw = len(hand)
        deck, hand, discard = rules.draw_cards(
            deck=deck,
            hand=hand,
            discard=discard,
            draw_count=2 + extra_draw_carry,
            hand_limit=9999,
            run_effects=run_effects,
            seed=_to_int(run.get("seed"), 0) + stage_no * 100 + turn_no + 1000,
        )
        if len(hand) > hand_before_draw:
            drawn_cards = list(hand[hand_before_draw:])
        pending_discard = max(0, len(hand) - hand_limit)
        if pending_discard > 0:
            run_effects["pending_discard"] = pending_discard
        else:
            run_effects.pop("pending_discard", None)
        if len(hand) <= 0:
            confidence -= 5
            confidence_events.append({"code": "empty_hand_penalty", "delta": -5})
    else:
        run_effects.pop("pending_discard", None)

    confidence = max(0, confidence)
    if confidence <= 0 and run_status != "failed":
        stage_complete = True
        stage_status = "failed"
        run_status = "failed"
        finished_at = _now()

    turn_mechanics_out: Dict[str, Any] = dict(mechanics)
    turn_mechanics_out["rule_version"] = rules.RULE_VERSION
    turn_mechanics_out["momentum_before"] = momentum_before
    turn_mechanics_out["momentum_after"] = momentum_after
    turn_mechanics_out["momentum_delta"] = momentum_after - momentum_before
    turn_mechanics_out["trend_gain"] = _to_int(mechanics.get("trend_gain"), 0)
    turn_mechanics_out["trend_loss"] = _to_int(mechanics.get("trend_loss"), 0)
    turn_mechanics_out["score_streak_before"] = score_streak_before
    turn_mechanics_out["score_streak_after"] = score_streak_after
    turn_mechanics_out["extra_draw_applied_this_turn"] = extra_draw_applied_this_turn
    turn_mechanics_out["extra_draw_next_turn_gain"] = next_extra_draw_gain
    turn_mechanics_out["dynamic_adjust_applied_this_turn"] = dynamic_adjust_applied_this_turn
    turn_mechanics_out["dynamic_adjust_next_turn"] = next_dynamic_adjust
    turn_mechanics_out["dynamic_adjust_discarded_count"] = dynamic_adjust_discarded_count
    turn_mechanics_out["confidence_events"] = confidence_events
    turn_mechanics_out["tactic_chain"] = list(mechanics.get("tactic_chain") or [])

    next_turn = turn_no + 1 if not stage_complete else turn_no
    last_result = {
        "turn_no": turn_no,
        "card_id": card_id,
        "played_cards": played_cards,
        "action_type": action_type,
        "turn_score": turn_score,
        "threshold": 0,
        "penalty": 0,
        "confidence_before": confidence_before,
        "confidence_after": confidence,
        "confidence_delta": confidence - confidence_before,
        "stage_score_before": stage_score_before,
        "stage_score_after": stage_score,
        "stage_score_delta": stage_score - stage_score_before,
        "total_score_before": total_score_before,
        "total_score_after": total_score,
        "total_score_delta": total_score - total_score_before,
        "event_message": "",
        "event_score_delta": 0,
        "event_confidence_delta": 0,
        "drawn_cards": drawn_cards,
        "pending_discard": pending_discard,
        "need_discard": pending_discard > 0,
        "stage_complete": stage_complete,
        "stage_status": stage_status,
        "card_results": list(resolve.get("card_results") or []),
        "rule_version": rules.RULE_VERSION,
        "mechanics": turn_mechanics_out,
    }

    _update_stage_after_turn(
        run_id=run_id,
        stage_no=stage_no,
        status=stage_status,
        stage_score=stage_score,
        current_turn=next_turn,
        visible_end=visible_end,
        event_state={},
        last_result=last_result,
        ended_at=_now() if stage_complete else None,
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_runs
                SET status = :status,
                    current_turn = :turn,
                    current_stage_score = :stage_score,
                    total_score = :total_score,
                    cleared_stages = :cleared_stages,
                    confidence = :confidence,
                    deck_json = :deck,
                    hand_json = :hand,
                    discard_json = :discard,
                    run_effects_json = :effects,
                    pending_upgrade_json = :pending_up,
                    updated_at = :now,
                    finished_at = :finished_at
                WHERE run_id = :rid
                """
            ),
            {
                "rid": int(run_id),
                "status": run_status,
                "turn": next_turn,
                "stage_score": stage_score,
                "total_score": total_score,
                "cleared_stages": cleared_stages,
                "confidence": confidence,
                "deck": _json_dump(deck),
                "hand": _json_dump(hand),
                "discard": _json_dump(discard),
                "effects": _json_dump(run_effects),
                "pending_up": _json_dump(pending_upgrades),
                "now": _now(),
                "finished_at": finished_at,
            },
        )
        conn.execute(
            text(
                """
                INSERT INTO kline_card_turn_logs
                (turn_id, run_id, stage_no, turn_no, action_json, result_json, created_at)
                VALUES
                (:tid, :rid, :sn, :tn, :action, :result, :now)
                """
            ),
            {
                "tid": _gen_id(),
                "rid": int(run_id),
                "sn": stage_no,
                "tn": turn_no,
                "action": _json_dump(action),
                "result": _json_dump(last_result),
                "now": _now(),
            },
        )

    return {
        "ok": True,
        "run_id": run_id,
        "stage_no": stage_no,
        "turn_no": turn_no,
        "played_card": card_id,
        "played_cards": played_cards,
        "action_type": action_type,
        "card_results": list(resolve.get("card_results") or []),
        "reveal_bars": bars_slice["future"],
        "turn_score": turn_score,
        "threshold": 0,
        "penalty": 0,
        "confidence_before": confidence_before,
        "confidence": confidence,
        "confidence_delta": confidence - confidence_before,
        "stage_score_before": stage_score_before,
        "stage_score": stage_score,
        "stage_score_delta": stage_score - stage_score_before,
        "target_score": target_score,
        "total_score_before": total_score_before,
        "total_score": total_score,
        "total_score_delta": total_score - total_score_before,
        "hand": hand,
        "deck_count": len(deck),
        "discard_count": len(discard),
        "stage_complete": stage_complete,
        "stage_status": stage_status,
        "run_status": run_status,
        "event_message": "",
        "pending_upgrades": pending_upgrades,
        "drawn_cards": drawn_cards,
        "pending_discard": pending_discard,
        "need_discard": pending_discard > 0,
        "hand_limit": hand_limit,
        "rule_version": rules.RULE_VERSION,
        "mechanics": turn_mechanics_out,
    }


def apply_stage_upgrade(run_id: int, upgrade_code: str) -> Dict[str, Any]:
    _ = run_id
    _ = upgrade_code
    return {"ok": False, "message": "stage upgrade is disabled in v2"}


def finish_stage(run_id: int) -> Dict[str, Any]:
    init_card_game_schema()
    loaded = _load_run_or_error(run_id)
    if not loaded["ok"]:
        return loaded
    run = loaded["run"]

    stage_no = _to_int(run.get("current_stage"), 1)
    stage_row = _get_stage_row(run_id, stage_no)
    if not stage_row:
        return {"ok": False, "message": "stage not found"}
    stage = _serialize_stage(stage_row)
    status = _to_str(stage.get("status"))

    if status in {"playing", "choose_symbol", "pending"}:
        return {"ok": False, "message": "stage not finished yet"}

    if status == "failed":
        if _to_str(run.get("status")) != "failed":
            with engine.begin() as conn:
                conn.execute(
                    text(
                        """
                        UPDATE kline_card_runs
                        SET status = 'failed',
                            finished_at = :now,
                            updated_at = :now
                        WHERE run_id = :rid
                        """
                    ),
                    {"rid": int(run_id), "now": _now()},
                )
        return {
            "ok": True,
            "stage_result": "failed",
            "run_status": "failed",
            "stage_no": stage_no,
            "stage_score": _to_int(stage.get("stage_score"), 0),
            "target_score": _to_int(stage.get("target_score"), 0),
        }

    # cleared
    if stage_no >= 5:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_runs
                    SET status = 'cleared',
                        cleared_stages = 5,
                        finished_at = COALESCE(finished_at, :now),
                        updated_at = :now
                    WHERE run_id = :rid
                    """
                ),
                {"rid": int(run_id), "now": _now()},
            )
        return {
            "ok": True,
            "stage_result": "cleared",
            "run_status": "cleared",
            "stage_no": stage_no,
            "stage_score": _to_int(stage.get("stage_score"), 0),
            "target_score": _to_int(stage.get("target_score"), 0),
        }

    pending = list(run.get("pending_upgrades") or [])
    if pending:
        return {
            "ok": True,
            "stage_result": "cleared",
            "run_status": "stage_cleared",
            "stage_no": stage_no,
            "need_upgrade": True,
            "upgrade_options": pending,
        }

    # fallback: if pending options are unexpectedly empty, auto advance
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_runs
                SET status = 'await_stage_start',
                    current_stage = :next_stage,
                    current_turn = 1,
                    current_stage_score = 0,
                    updated_at = :now
                WHERE run_id = :rid
                """
            ),
            {"rid": int(run_id), "next_stage": stage_no + 1, "now": _now()},
        )
    return {
        "ok": True,
        "stage_result": "cleared",
        "run_status": "await_stage_start",
        "stage_no": stage_no,
        "need_upgrade": False,
    }


def _grant_meta_exp(user_id: str, exp_gain: int, cleared: bool, run_score: int) -> Dict[str, Any]:
    meta = _get_or_create_meta(user_id)
    old_exp = _to_int(meta.get("exp"), 0)
    old_level = _to_int(meta.get("level"), 1)
    new_exp = old_exp + max(0, int(exp_gain))
    new_level = _advance_meta_level(new_exp)
    level_delta = max(0, new_level - old_level)
    new_skill_points = _to_int(meta.get("skill_points"), 0) + level_delta
    now = _now()

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_user_meta
                SET exp = :exp,
                    level = :level,
                    skill_points = :sp,
                    games_cleared = games_cleared + :gc,
                    best_score = CASE WHEN best_score < :score THEN :score ELSE best_score END,
                    total_score = total_score + :score,
                    updated_at = :now
                WHERE user_id = :uid
                """
            ),
            {
                "uid": user_id,
                "exp": new_exp,
                "level": new_level,
                "sp": new_skill_points,
                "gc": 1 if cleared else 0,
                "score": max(0, int(run_score)),
                "now": now,
            },
        )
    return get_card_meta(user_id)


def finish_run(run_id: int) -> Dict[str, Any]:
    init_card_game_schema()
    loaded = _load_run_or_error(run_id)
    if not loaded["ok"]:
        return loaded
    run = loaded["run"]
    status = _to_str(run.get("status"))
    if status not in FINAL_RUN_STATUSES:
        return {"ok": False, "message": "run is not in final status"}

    if _to_int(run.get("reward_claimed"), 0) == 1:
        return {
            "ok": True,
            "run_id": run_id,
            "status": status,
            "reward_exp": _to_int(run.get("reward_exp"), 0),
            "meta": get_card_meta(_to_str(run.get("user_id"))),
            "already_claimed": True,
        }

    total_score = _to_int(run.get("total_score"), 0)
    cleared_stages = _to_int(run.get("cleared_stages"), 0)
    exp_gain = rules.compute_run_exp(total_score, cleared_stages, status == "cleared")
    meta = _grant_meta_exp(_to_str(run.get("user_id")), exp_gain, status == "cleared", total_score)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_runs
                SET reward_claimed = 1,
                    reward_exp = :exp,
                    updated_at = :now,
                    finished_at = COALESCE(finished_at, :now)
                WHERE run_id = :rid
                """
            ),
            {"rid": int(run_id), "exp": exp_gain, "now": _now()},
        )

    return {
        "ok": True,
        "run_id": run_id,
        "status": status,
        "reward_exp": exp_gain,
        "meta": meta,
    }


def abort_run(run_id: int, reason: str = "manual_abort") -> Dict[str, Any]:
    init_card_game_schema()
    loaded = _load_run_or_error(run_id)
    if not loaded["ok"]:
        return loaded
    run = loaded["run"]
    status = _to_str(run.get("status"))
    if status in FINAL_RUN_STATUSES:
        return {
            "ok": True,
            "run_id": int(run_id),
            "status": status,
            "already_final": True,
            "aborted": False,
        }

    stage_no = _to_int(run.get("current_stage"), 1)
    stage = _serialize_stage(_get_stage_row(run_id, stage_no) or {})
    stage_status = _to_str(stage.get("status"), "")
    now = _now()
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_runs
                SET status = 'failed',
                    finished_at = COALESCE(finished_at, :now),
                    updated_at = :now
                WHERE run_id = :rid
                """
            ),
            {"rid": int(run_id), "now": now},
        )
        if stage_status in {"playing", "choose_symbol", "pending", "await_stage_start"}:
            conn.execute(
                text(
                    """
                    UPDATE kline_card_stage_logs
                    SET status = 'failed',
                        ended_at = COALESCE(ended_at, :now)
                    WHERE run_id = :rid
                      AND stage_no = :sn
                    """
                ),
                {"rid": int(run_id), "sn": int(stage_no), "now": now},
            )
    return {
        "ok": True,
        "run_id": int(run_id),
        "status": "failed",
        "aborted": True,
        "reason": _to_str(reason, "manual_abort"),
    }


def get_run_state(run_id: int) -> Dict[str, Any]:
    loaded = _load_run_or_error(run_id)
    if not loaded["ok"]:
        return loaded
    run = loaded["run"]
    stage = _get_stage_row(run_id, _to_int(run.get("current_stage"), 1))
    stage_data = _serialize_stage(stage) if stage else {}
    return {"ok": True, "run": run, "stage": stage_data}

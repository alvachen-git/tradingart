"""Map-layer storage and rules for K-line card map MVP.

Hard constraints:
- Only writes to kline_card_* / kline_card_map_* tables.
- Never writes users.experience, kline_game_records, kline_game_stats.
"""

from __future__ import annotations

import json
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import inspect as sa_inspect
from sqlalchemy import text

import kline_card_rules as rules
import kline_card_storage as card_storage


MAP_RUN_FINAL_STATUSES = {"ended"}
VALID_LOCATIONS = {"home", "association"}
LOCATION_NAMES = {
    "home": "住宅",
    "association": "基金业协会",
}

BASE_YEAR = 2030
MAX_TURNS = 72
STAMINA_CAP = 100
RESTORE_PER_REST = 60
MOVE_COST = 10
ACTION_POINTS_CAP = 20
ACTION_POINTS_RESTORE_PER_REST = 10
STRESS_CAP = 200
CONFIDENCE_CAP = 100

DEFAULT_MONEY = 100_000
DEFAULT_MANAGEMENT_AUM = 2_000_000
DEFAULT_STAMINA = 100
DEFAULT_ACTION_POINTS = 10
DEFAULT_STRESS = 0
DEFAULT_FAME = 0
DEFAULT_EXP = 0
DEFAULT_CONFIDENCE = 60

TRAIT_PAIRS = [
    ("外向", "内向"),
    ("谦虚", "自信"),
    ("喜欢规则", "喜欢改变"),
    ("看重自由", "看重平等"),
]
TRAIT_SET = {v for pair in TRAIT_PAIRS for v in pair}
STYLE_DEFAULTS = {
    "horizon_preference": "long",
    "risk_preference": "avoid_loss",
    "priority_preference": "skill",
}
STYLE_ALLOWED = {
    "horizon_preference": {"short", "long"},
    "risk_preference": {"avoid_loss", "seek_profit"},
    "priority_preference": {"skill", "mindset"},
}

BATTLE_REWARD_CLEARED = {"money": 150, "fame": 6, "exp": 20, "stamina": 0}
BATTLE_PENALTY_FAILED = {"money": -120, "fame": -4, "exp": 0, "stamina": -20}


def _engine():
    return card_storage.engine


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


def _calendar_from_turn(global_turn: int) -> Tuple[int, int, str]:
    turn = max(1, int(global_turn))
    year_no = ((turn - 1) // 24) + 1
    turn_in_year = ((turn - 1) % 24) + 1
    month_no = ((turn_in_year - 1) // 2) + 1
    month_half = "上" if (turn_in_year % 2 == 1) else "下"
    return year_no, month_no, month_half


def _format_date_label(year_no: int, month_no: int, month_half: str) -> str:
    year = BASE_YEAR + max(0, int(year_no) - 1)
    return f"{year}年{int(month_no)}月{month_half}"


def _default_home_deck(seed: Optional[int] = None) -> List[str]:
    run_seed = int(seed if seed is not None else random.randint(1, 2_000_000_000))
    built = rules.build_initial_deck(run_seed)
    if not isinstance(built, list):
        built = list(rules.CARD_LIBRARY.keys())
    return [str(cid) for cid in built[:12] if str(cid) in rules.CARD_LIBRARY]


def _base_initial_deck_counts() -> Dict[str, int]:
    return {
        "short_short_novice": 3,
        "short_long_novice": 3,
        "trend_short_novice": 3,
        "trend_long_novice": 3,
        "tactic_quick_cancel": 1,
        "tactic_meditation": 1,
        "tactic_risk_control": 1,
    }


def _god_mode_deck_counts() -> Dict[str, int]:
    # God mode starts with one copy of every currently defined card for combo testing.
    return {str(cid): 1 for cid in rules.CARD_LIBRARY.keys()}


def _apply_style_modifiers(deck_counts: Dict[str, int], style_answers: Dict[str, str]) -> Dict[str, int]:
    counts = dict(deck_counts or {})

    def _bump(cid: str, delta: int) -> None:
        counts[cid] = max(0, _to_int(counts.get(cid), 0) + int(delta))

    # Fixed order for determinism: horizon -> risk -> priority
    if _to_str(style_answers.get("horizon_preference")) == "short":
        _bump("trend_short_novice", -1)
        _bump("trend_long_novice", -1)
        _bump("short_short_novice", +1)
        _bump("short_long_novice", +1)

    if _to_str(style_answers.get("risk_preference")) == "seek_profit":
        _bump("tactic_risk_control", -1)
        _bump("tactic_quick_cancel", -1)
        _bump("trend_short_novice", +1)
        _bump("trend_long_novice", +1)

    if _to_str(style_answers.get("priority_preference")) == "mindset":
        _bump("short_short_novice", -1)
        _bump("short_long_novice", -1)
        _bump("tactic_meditation", +2)

    return counts


def _expand_deck_counts(deck_counts: Dict[str, int]) -> List[str]:
    out: List[str] = []
    for cid in sorted((deck_counts or {}).keys()):
        if cid not in rules.CARD_LIBRARY:
            continue
        n = max(0, _to_int(deck_counts.get(cid), 0))
        out.extend([cid] * n)
    return out


def _normalize_traits(raw_traits: Any) -> List[str]:
    if not isinstance(raw_traits, list):
        return []
    values = [_to_str(v).strip() for v in raw_traits]
    if len(values) != 4:
        return []
    normalized: List[str] = []
    for a, b in TRAIT_PAIRS:
        picked = [v for v in values if v in {a, b}]
        if len(picked) != 1:
            return []
        normalized.append(picked[0])
    return normalized


def _normalize_style_answers(raw_style: Any) -> Dict[str, str]:
    if not isinstance(raw_style, dict):
        return dict(STYLE_DEFAULTS)
    out = dict(STYLE_DEFAULTS)
    for key, allowed in STYLE_ALLOWED.items():
        v = _to_str(raw_style.get(key, out[key])).strip()
        out[key] = v if v in allowed else out[key]
    return out


def _normalize_new_game_setup(user_id: str, setup: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = dict(setup or {})
    player_name = _to_str(payload.get("player_name") or user_id).strip()
    if not player_name:
        player_name = _to_str(user_id, "玩家").strip() or "玩家"
    player_name = player_name[:20]

    traits = _normalize_traits(payload.get("traits"))
    if not traits:
        # Deterministic default: first option from each pair
        traits = [pair[0] for pair in TRAIT_PAIRS]

    style_answers = _normalize_style_answers(payload.get("style_answers", {}))
    god_mode = bool(payload.get("god_mode", False))
    return {
        "player_name": player_name,
        "traits": traits,
        "style_answers": style_answers,
        "god_mode": god_mode,
    }


def _build_initial_home_deck_from_setup(setup_norm: Dict[str, Any], seed: int) -> List[str]:
    if bool(setup_norm.get("god_mode", False)):
        deck = _expand_deck_counts(_god_mode_deck_counts())
        return _normalize_deck_cards(deck, 1, max(15, len(deck))) or _default_home_deck(seed)

    counts = _base_initial_deck_counts()
    counts = _apply_style_modifiers(counts, dict(setup_norm.get("style_answers") or {}))
    deck = _expand_deck_counts(counts)
    deck = _normalize_deck_cards(deck, 10, 15)
    if deck:
        return deck
    return _normalize_deck_cards(_expand_deck_counts(_base_initial_deck_counts()), 10, 15) or _default_home_deck(seed)


def _normalize_deck_cards(cards: List[Any], min_len: int = 10, max_len: int = 15) -> List[str]:
    cleaned: List[str] = []
    for one in list(cards or []):
        cid = _to_str(one).strip()
        if not cid:
            continue
        if cid not in rules.CARD_LIBRARY:
            continue
        cleaned.append(cid)
    if len(cleaned) < int(min_len) or len(cleaned) > int(max_len):
        return []
    return cleaned


def init_map_schema() -> None:
    card_storage.init_card_game_schema()
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kline_card_map_runs (
                    map_run_id BIGINT PRIMARY KEY,
                    user_id VARCHAR(128) NOT NULL,
                    seed BIGINT NOT NULL DEFAULT 0,
                    status VARCHAR(32) NOT NULL DEFAULT 'playing',
                    year_no INT NOT NULL DEFAULT 1,
                    turn_index INT NOT NULL DEFAULT 1,
                    month_no INT NOT NULL DEFAULT 1,
                    month_half VARCHAR(8) NOT NULL DEFAULT '上',
                    date_label VARCHAR(32) NOT NULL DEFAULT '2030年1月上',
                    location VARCHAR(32) NOT NULL DEFAULT 'home',
                    player_name VARCHAR(128) NOT NULL DEFAULT '',
                    stamina INT NOT NULL DEFAULT 100,
                    money INT NOT NULL DEFAULT 1000,
                    management_aum BIGINT NOT NULL DEFAULT 2000000,
                    action_points INT NOT NULL DEFAULT 10,
                    stress INT NOT NULL DEFAULT 0,
                    confidence INT NOT NULL DEFAULT 60,
                    fame INT NOT NULL DEFAULT 10,
                    exp INT NOT NULL DEFAULT 0,
                    traits_json TEXT NULL,
                    style_answers_json TEXT NULL,
                    god_mode TINYINT NOT NULL DEFAULT 0,
                    home_deck_json TEXT NULL,
                    deck_pending_apply TINYINT NOT NULL DEFAULT 0,
                    linked_battle_run_id BIGINT NULL,
                    battle_state VARCHAR(32) NOT NULL DEFAULT 'idle',
                    ended_reason VARCHAR(64) NULL,
                    result_json TEXT NULL,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    finished_at DATETIME NULL
                )
                """
            )
        )
    _ensure_map_schema_columns()


def _ensure_map_schema_columns() -> None:
    required = {
        "management_aum": "ALTER TABLE kline_card_map_runs ADD COLUMN management_aum BIGINT NOT NULL DEFAULT 2000000",
        "action_points": "ALTER TABLE kline_card_map_runs ADD COLUMN action_points INT NOT NULL DEFAULT 10",
        "stress": "ALTER TABLE kline_card_map_runs ADD COLUMN stress INT NOT NULL DEFAULT 0",
        "confidence": "ALTER TABLE kline_card_map_runs ADD COLUMN confidence INT NOT NULL DEFAULT 60",
        "traits_json": "ALTER TABLE kline_card_map_runs ADD COLUMN traits_json TEXT NULL",
        "style_answers_json": "ALTER TABLE kline_card_map_runs ADD COLUMN style_answers_json TEXT NULL",
        "god_mode": "ALTER TABLE kline_card_map_runs ADD COLUMN god_mode TINYINT NOT NULL DEFAULT 0",
    }
    try:
        inspector = sa_inspect(_engine())
        existing = {str(col.get("name")) for col in inspector.get_columns("kline_card_map_runs")}
    except Exception:
        try:
            with _engine().connect() as conn:
                rows = conn.execute(text("PRAGMA table_info(kline_card_map_runs)")).fetchall()
            existing = {str(r[1]) for r in rows}
        except Exception:
            # Best-effort fallback only when neither inspector nor sqlite pragma is available.
            return
    with _engine().begin() as conn:
        for sql in [sql for col, sql in required.items() if col not in existing]:
            conn.execute(text(sql))
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kline_card_map_turn_logs (
                    turn_log_id BIGINT PRIMARY KEY,
                    map_run_id BIGINT NOT NULL,
                    turn_index INT NOT NULL,
                    action VARCHAR(64) NOT NULL,
                    payload_json TEXT NULL,
                    result_json TEXT NULL,
                    created_at DATETIME NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS kline_card_map_event_logs (
                    event_id BIGINT PRIMARY KEY,
                    map_run_id BIGINT NOT NULL,
                    event_type VARCHAR(64) NOT NULL,
                    payload_json TEXT NULL,
                    created_at DATETIME NOT NULL
                )
                """
            )
        )


def _get_map_row(map_run_id: int) -> Optional[Dict[str, Any]]:
    with _engine().connect() as conn:
        row = conn.execute(
            text("SELECT * FROM kline_card_map_runs WHERE map_run_id = :rid LIMIT 1"),
            {"rid": int(map_run_id)},
        ).fetchone()
    return dict(row._mapping) if row else None


def _serialize_map_run(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    out["home_deck"] = _json_load(out.pop("home_deck_json", None), [])
    out["traits"] = _json_load(out.pop("traits_json", None), [])
    out["style_answers"] = _json_load(out.pop("style_answers_json", None), {})
    out["deck_template"] = list(out["home_deck"])
    out["deck_pending_apply"] = bool(_to_int(out.get("deck_pending_apply"), 0))
    out["god_mode"] = bool(_to_int(out.get("god_mode"), 0))
    out["money"] = _to_int(out.get("money"), DEFAULT_MONEY)
    out["management_aum"] = _to_int(out.get("management_aum"), DEFAULT_MANAGEMENT_AUM)
    out["stamina"] = max(0, min(STAMINA_CAP, _to_int(out.get("stamina"), DEFAULT_STAMINA)))
    out["action_points"] = max(0, min(ACTION_POINTS_CAP, _to_int(out.get("action_points"), DEFAULT_ACTION_POINTS)))
    out["stress"] = max(0, min(STRESS_CAP, _to_int(out.get("stress"), DEFAULT_STRESS)))
    out["confidence"] = max(0, min(CONFIDENCE_CAP, _to_int(out.get("confidence"), DEFAULT_CONFIDENCE)))
    out["fame"] = max(0, _to_int(out.get("fame"), DEFAULT_FAME))
    out["exp"] = max(0, _to_int(out.get("exp"), DEFAULT_EXP))
    out["player_name"] = _to_str(out.get("player_name"), "").strip()
    if not out["player_name"]:
        out["player_name"] = _to_str(out.get("user_id"), "玩家")
    if not isinstance(out["traits"], list) or len(out["traits"]) != 4:
        out["traits"] = [pair[0] for pair in TRAIT_PAIRS]
    else:
        norm_traits = _normalize_traits(out["traits"])
        out["traits"] = norm_traits or [pair[0] for pair in TRAIT_PAIRS]
    if not isinstance(out["style_answers"], dict):
        out["style_answers"] = dict(STYLE_DEFAULTS)
    else:
        out["style_answers"] = _normalize_style_answers(out["style_answers"])
    out["result"] = _json_load(out.pop("result_json", None), {})
    out["location_name"] = LOCATION_NAMES.get(_to_str(out.get("location"), "home"), "未知地点")
    out["global_turn"] = _to_int(out.get("turn_index"), 1)
    return out


def _replace_active_map_runs_for_user(user_id: str) -> int:
    uid = _to_str(user_id).strip()
    if not uid:
        return 0
    now = _now()
    with _engine().begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT map_run_id
                FROM kline_card_map_runs
                WHERE user_id = :uid
                  AND status <> 'ended'
                """
            ),
            {"uid": uid},
        ).fetchall()
        ids = [int(r[0]) for r in rows]
        if not ids:
            return 0
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET status = 'ended',
                    ended_reason = 'restart_replaced',
                    updated_at = :now,
                    finished_at = COALESCE(finished_at, :now)
                WHERE user_id = :uid
                  AND status <> 'ended'
                """
            ),
            {"uid": uid, "now": now},
        )
    for rid in ids:
        _append_event_log(rid, "map_run_ended", {"reason": "restart_replaced"})
    return len(ids)


def _append_turn_log(map_run_id: int, turn_index: int, action: str, payload: Dict[str, Any], result: Dict[str, Any]) -> None:
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO kline_card_map_turn_logs
                (turn_log_id, map_run_id, turn_index, action, payload_json, result_json, created_at)
                VALUES
                (:tid, :rid, :tn, :action, :payload, :result, :now)
                """
            ),
            {
                "tid": _gen_id(),
                "rid": int(map_run_id),
                "tn": int(turn_index),
                "action": _to_str(action),
                "payload": _json_dump(payload or {}),
                "result": _json_dump(result or {}),
                "now": _now(),
            },
        )


def _append_event_log(map_run_id: int, event_type: str, payload: Dict[str, Any]) -> None:
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO kline_card_map_event_logs
                (event_id, map_run_id, event_type, payload_json, created_at)
                VALUES
                (:eid, :rid, :etype, :payload, :now)
                """
            ),
            {
                "eid": _gen_id(),
                "rid": int(map_run_id),
                "etype": _to_str(event_type),
                "payload": _json_dump(payload or {}),
                "now": _now(),
            },
        )


def _load_map_or_error(map_run_id: int) -> Dict[str, Any]:
    row = _get_map_row(map_run_id)
    if not row:
        return {"ok": False, "message": "map run not found"}
    return {"ok": True, "map_run": _serialize_map_run(row)}


def _resource_delta(money: int = 0, stamina: int = 0, fame: int = 0, exp: int = 0) -> Dict[str, int]:
    return {
        "money": int(money),
        "stamina": int(stamina),
        "fame": int(fame),
        "exp": int(exp),
    }


def create_map_run(
    user_id: str,
    seed: Optional[int] = None,
    setup: Optional[Dict[str, Any]] = None,
    restart_existing: bool = False,
) -> int:
    uid = _to_str(user_id).strip()
    if not uid:
        return 0
    init_map_schema()
    if bool(restart_existing):
        _replace_active_map_runs_for_user(uid)

    run_seed = int(seed if seed is not None else random.randint(1, 2_000_000_000))
    map_run_id = _gen_id()
    year_no, month_no, month_half = _calendar_from_turn(1)
    now = _now()
    setup_norm = _normalize_new_game_setup(uid, setup)
    home_deck = _build_initial_home_deck_from_setup(setup_norm, run_seed)
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO kline_card_map_runs
                (map_run_id, user_id, seed, status, year_no, turn_index, month_no, month_half, date_label,
                 location, player_name, stamina, money, management_aum, action_points, stress, confidence, fame, exp,
                 traits_json, style_answers_json, god_mode,
                 home_deck_json, deck_pending_apply,
                 linked_battle_run_id, battle_state, ended_reason, result_json, created_at, updated_at, finished_at)
                VALUES
                (:rid, :uid, :seed, 'playing', :year_no, 1, :month_no, :month_half, :date_label,
                 'home', :player_name, :stamina, :money, :management_aum, :action_points, :stress, :confidence, :fame, :exp,
                 :traits_json, :style_answers_json, :god_mode,
                 :home_deck, 0,
                 NULL, 'idle', NULL, :result_json, :now, :now, NULL)
                """
            ),
            {
                "rid": map_run_id,
                "uid": uid,
                "seed": run_seed,
                "year_no": year_no,
                "month_no": month_no,
                "month_half": month_half,
                "date_label": _format_date_label(year_no, month_no, month_half),
                "player_name": setup_norm["player_name"],
                "stamina": DEFAULT_STAMINA,
                "money": DEFAULT_MONEY,
                "management_aum": DEFAULT_MANAGEMENT_AUM,
                "action_points": DEFAULT_ACTION_POINTS,
                "stress": DEFAULT_STRESS,
                "confidence": DEFAULT_CONFIDENCE,
                "fame": DEFAULT_FAME,
                "exp": DEFAULT_EXP,
                "traits_json": _json_dump(setup_norm["traits"]),
                "style_answers_json": _json_dump(setup_norm["style_answers"]),
                "god_mode": 1 if setup_norm["god_mode"] else 0,
                "home_deck": _json_dump(home_deck),
                "result_json": _json_dump({}),
                "now": now,
            },
        )

    _append_event_log(
        map_run_id,
        "create_map_run",
        {
            "seed": run_seed,
            "player_name": setup_norm["player_name"],
            "traits": setup_norm["traits"],
            "style_answers": setup_norm["style_answers"],
            "god_mode": bool(setup_norm["god_mode"]),
            "initial_deck_size": len(home_deck),
        },
    )
    _append_turn_log(map_run_id, 1, "create", {}, {"status": "playing"})
    return map_run_id


def get_resume_map_run(user_id: str) -> Optional[Dict[str, Any]]:
    uid = _to_str(user_id).strip()
    if not uid:
        return None
    init_map_schema()
    with _engine().connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT *
                FROM kline_card_map_runs
                WHERE user_id = :uid
                  AND status <> 'ended'
                ORDER BY updated_at DESC
                LIMIT 1
                """
            ),
            {"uid": uid},
        ).fetchone()
    return _serialize_map_run(dict(row._mapping)) if row else None


def get_map_state(map_run_id: int) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    return {"ok": True, "map_run": loaded["map_run"]}


def move_location(map_run_id: int, to_location: str) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])

    if _to_str(run.get("status")) in MAP_RUN_FINAL_STATUSES:
        return {"ok": False, "message": "map run already ended", "locked": True}

    target = _to_str(to_location).strip().lower()
    if target not in VALID_LOCATIONS:
        return {"ok": False, "message": "invalid location"}

    current = _to_str(run.get("location"), "home")
    stamina_before = _to_int(run.get("stamina"), 0)
    cost = 0
    if target != current and target != "home":
        cost = MOVE_COST
    if cost > 0 and stamina_before < cost:
        return {"ok": False, "message": "stamina not enough for move"}

    stamina_after = max(0, stamina_before - cost)
    now = _now()
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET location = :loc,
                    stamina = :stamina,
                    updated_at = :now
                WHERE map_run_id = :rid
                """
            ),
            {"loc": target, "stamina": stamina_after, "now": now, "rid": int(map_run_id)},
        )

    log_line = "地点切换：%s -> %s，体力%s" % (
        LOCATION_NAMES.get(current, current),
        LOCATION_NAMES.get(target, target),
        "-%s" % str(cost) if cost > 0 else "+0",
    )
    delta = _resource_delta(stamina=-cost)
    _append_event_log(map_run_id, "move_location", {"from": current, "to": target, "cost": cost})
    _append_turn_log(map_run_id, _to_int(run.get("turn_index"), 1), "move", {"to": target}, {"stamina_cost": cost})

    latest = _serialize_map_run(_get_map_row(map_run_id) or {})
    return {"ok": True, "map_run": latest, "resource_delta": delta, "log_line": log_line, "locked": False}


def _maybe_end_by_turn_limit(run: Dict[str, Any], turn_index: int) -> Tuple[bool, Dict[str, Any]]:
    if turn_index < MAX_TURNS:
        return False, {}
    result = {
        "summary": "结局评分待规划",
        "turn_limit": MAX_TURNS,
    }
    return True, result


def rest_and_advance_turn(map_run_id: int) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])

    if _to_str(run.get("status")) in MAP_RUN_FINAL_STATUSES:
        return {"ok": False, "message": "map run already ended", "locked": True}
    if _to_str(run.get("location"), "home") != "home":
        return {"ok": False, "message": "rest action is only available at home"}

    turn_before = _to_int(run.get("turn_index"), 1)
    stamina_before = _to_int(run.get("stamina"), 0)
    ap_before = _to_int(run.get("action_points"), DEFAULT_ACTION_POINTS)
    stamina_after = min(STAMINA_CAP, stamina_before + RESTORE_PER_REST)
    ap_after = min(ACTION_POINTS_CAP, ap_before + ACTION_POINTS_RESTORE_PER_REST)

    next_turn = turn_before + 1
    year_no, month_no, month_half = _calendar_from_turn(next_turn)
    date_label = _format_date_label(year_no, month_no, month_half)
    ended, result = _maybe_end_by_turn_limit(run, next_turn)

    status = "ended" if ended else "playing"
    ended_reason = "time_up_3y" if ended else None
    now = _now()
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET status = :status,
                    year_no = :year_no,
                    turn_index = :turn_index,
                    month_no = :month_no,
                    month_half = :month_half,
                    date_label = :date_label,
                    stamina = :stamina,
                    action_points = :action_points,
                    ended_reason = :ended_reason,
                    result_json = :result_json,
                    updated_at = :now,
                    finished_at = :finished_at
                WHERE map_run_id = :rid
                """
            ),
            {
                "status": status,
                "year_no": year_no,
                "turn_index": next_turn,
                "month_no": month_no,
                "month_half": month_half,
                "date_label": date_label,
                "stamina": stamina_after,
                "action_points": ap_after,
                "ended_reason": ended_reason,
                "result_json": _json_dump(result),
                "now": now,
                "finished_at": now if ended else None,
                "rid": int(map_run_id),
            },
        )

    delta = _resource_delta(stamina=stamina_after - stamina_before)
    stamina_gain = _to_int(_resource_delta(stamina=stamina_after - stamina_before).get("stamina"), 0)
    log_line = "休息完成：体力%s，日期推进到 %s" % (
        ("%+d" % stamina_gain),
        date_label,
    )
    log_line += "，行动点%+d" % (ap_after - ap_before)
    _append_turn_log(
        map_run_id,
        next_turn,
        "rest",
        {},
        {
            "stamina_before": stamina_before,
            "stamina_after": stamina_after,
            "action_points_before": ap_before,
            "action_points_after": ap_after,
        },
    )
    if ended:
        _append_event_log(map_run_id, "map_run_ended", {"reason": ended_reason, "result": result})

    latest = _serialize_map_run(_get_map_row(map_run_id) or {})
    return {
        "ok": True,
        "map_run": latest,
        "resource_delta": delta,
        "log_line": log_line,
        "locked": ended,
    }


def get_home_deck(map_run_id: int) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])
    return {
        "ok": True,
        "map_run_id": int(map_run_id),
        "deck_cards": list(run.get("home_deck") or []),
        "min_cards": 10,
        "max_cards": 15,
        "deck_pending_apply": bool(run.get("deck_pending_apply", False)),
    }


def save_home_deck(map_run_id: int, deck_cards: List[str]) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])
    if _to_str(run.get("status")) in MAP_RUN_FINAL_STATUSES:
        return {"ok": False, "message": "map run already ended", "locked": True}

    raw_cards = [str(cid).strip() for cid in list(deck_cards or []) if str(cid).strip()]
    invalid = [cid for cid in raw_cards if cid not in rules.CARD_LIBRARY]
    if invalid:
        return {"ok": False, "message": f"invalid card id: {invalid[0]}"}
    cards = _normalize_deck_cards(raw_cards)
    if len(cards) < 10 or len(cards) > 15:
        return {"ok": False, "message": "deck cards size must be between 10 and 15"}

    now = _now()
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET home_deck_json = :deck,
                    deck_pending_apply = 1,
                    updated_at = :now
                WHERE map_run_id = :rid
                """
            ),
            {
                "deck": _json_dump(cards),
                "now": now,
                "rid": int(map_run_id),
            },
        )

    _append_event_log(map_run_id, "save_home_deck", {"deck_size": len(cards)})
    _append_turn_log(map_run_id, _to_int(run.get("turn_index"), 1), "save_deck", {"deck_size": len(cards)}, {})
    latest = _serialize_map_run(_get_map_row(map_run_id) or {})
    return {
        "ok": True,
        "map_run": latest,
        "resource_delta": _resource_delta(),
        "log_line": "住宅卡组已保存，下次新战斗生效。",
        "locked": False,
    }


def start_battle_from_map(map_run_id: int) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])

    if _to_str(run.get("status")) in MAP_RUN_FINAL_STATUSES:
        return {"ok": False, "message": "map run already ended", "locked": True}

    linked_battle_run_id = _to_int(run.get("linked_battle_run_id"), 0)
    battle_state = _to_str(run.get("battle_state"), "idle")
    if linked_battle_run_id > 0 and battle_state == "pending_commit":
        return {
            "ok": True,
            "map_run": run,
            "battle_run_id": linked_battle_run_id,
            "pending": True,
            "message": "battle already started and waiting for commit",
        }

    uid = _to_str(run.get("user_id"), "")
    seed_base = _to_int(run.get("seed"), 0)
    turn_index = _to_int(run.get("turn_index"), 1)
    battle_seed = seed_base + turn_index * 17 + random.randint(1, 997)
    deck = list(run.get("home_deck") or [])

    battle_run_id = card_storage.create_run(uid, seed=battle_seed, deck_override=deck)
    if battle_run_id <= 0:
        return {"ok": False, "message": "failed to create battle run"}

    # Pre-warm battle stage here with a single random symbol directly.
    # This avoids generating a 3-candidate pool and speeds up map->battle transition.
    stage_prepare = card_storage.start_stage(battle_run_id, 1, None, auto_random_choice=True)
    if not stage_prepare.get("ok"):
        return {"ok": False, "message": f"battle stage prepare failed: {stage_prepare.get('message', 'unknown')}"}
    if bool(stage_prepare.get("need_choice", False)):
        return {"ok": False, "message": "battle stage prepare returned unexpected need_choice"}

    now = _now()
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET linked_battle_run_id = :bid,
                    battle_state = 'pending_commit',
                    deck_pending_apply = 0,
                    updated_at = :now
                WHERE map_run_id = :rid
                """
            ),
            {
                "bid": int(battle_run_id),
                "now": now,
                "rid": int(map_run_id),
            },
        )

    _append_event_log(map_run_id, "battle_started", {"battle_run_id": battle_run_id})
    _append_turn_log(map_run_id, turn_index, "battle_start", {"battle_run_id": battle_run_id}, {})
    latest = _serialize_map_run(_get_map_row(map_run_id) or {})
    return {
        "ok": True,
        "map_run": latest,
        "battle_run_id": int(battle_run_id),
        "pending": False,
    }


def _battle_resource_delta(run_status: str) -> Dict[str, int]:
    if run_status == "cleared":
        return dict(BATTLE_REWARD_CLEARED)
    if run_status == "failed":
        return dict(BATTLE_PENALTY_FAILED)
    return _resource_delta()


def commit_battle_result(map_run_id: int, battle_run_id: int) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])

    linked_battle_run_id = _to_int(run.get("linked_battle_run_id"), 0)
    battle_state = _to_str(run.get("battle_state"), "idle")
    target_battle_run = _to_int(battle_run_id, 0)
    if target_battle_run <= 0:
        return {"ok": False, "message": "invalid battle_run_id"}

    if battle_state != "pending_commit":
        return {
            "ok": True,
            "map_run": run,
            "resource_delta": _resource_delta(),
            "log_line": "当前无待回写战斗结果。",
            "pending": False,
        }
    if linked_battle_run_id > 0 and linked_battle_run_id != target_battle_run:
        return {"ok": False, "message": "battle run mismatch for commit"}

    battle_state_res = card_storage.get_run_state(target_battle_run)
    if not battle_state_res.get("ok"):
        return {"ok": False, "message": "battle run state not found"}

    battle_run = dict(battle_state_res.get("run") or {})
    run_status = _to_str(battle_run.get("status"), "")
    if run_status not in card_storage.FINAL_RUN_STATUSES:
        return {
            "ok": True,
            "map_run": run,
            "resource_delta": _resource_delta(),
            "log_line": "战斗尚未结束，结果暂不可回写。",
            "pending": True,
            "battle_status": run_status,
        }

    delta = _battle_resource_delta(run_status)
    money_after = _to_int(run.get("money"), 0) + _to_int(delta.get("money"), 0)
    fame_after = max(0, _to_int(run.get("fame"), 0) + _to_int(delta.get("fame"), 0))
    exp_after = max(0, _to_int(run.get("exp"), 0) + _to_int(delta.get("exp"), 0))
    stamina_after = max(0, _to_int(run.get("stamina"), 0) + _to_int(delta.get("stamina"), 0))
    current_conf = _to_int(run.get("confidence"), DEFAULT_CONFIDENCE)
    battle_conf = battle_run.get("confidence", None)
    if battle_conf is None:
        confidence_after = current_conf
    else:
        confidence_after = max(0, min(CONFIDENCE_CAP, _to_int(battle_conf, current_conf)))
    ended_by_conf = confidence_after <= 0
    result_payload = None
    ended_reason = None
    status_after = _to_str(run.get("status"), "playing")
    finished_at = None
    if ended_by_conf:
        status_after = "ended"
        ended_reason = "confidence_zero"
        result_payload = {"summary": "结局评分待规划（信心归零）"}
        finished_at = now = _now()
    else:
        now = _now()

    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET money = :money,
                    status = :status,
                    fame = :fame,
                    exp = :exp,
                    stamina = :stamina,
                    confidence = :confidence,
                    linked_battle_run_id = NULL,
                    battle_state = 'idle',
                    ended_reason = COALESCE(:ended_reason, ended_reason),
                    result_json = COALESCE(:result_json, result_json),
                    updated_at = :now
                    , finished_at = COALESCE(:finished_at, finished_at)
                WHERE map_run_id = :rid
                """
            ),
            {
                "money": int(money_after),
                "status": status_after,
                "fame": int(fame_after),
                "exp": int(exp_after),
                "stamina": int(stamina_after),
                "confidence": int(confidence_after),
                "ended_reason": ended_reason,
                "result_json": _json_dump(result_payload) if result_payload is not None else None,
                "finished_at": finished_at,
                "now": now,
                "rid": int(map_run_id),
            },
        )

    log_line = "战斗结算回写：%s，金钱%+d，名气%+d，经验%+d，体力%+d" % (
        "胜利" if run_status == "cleared" else "失败",
        _to_int(delta.get("money"), 0),
        _to_int(delta.get("fame"), 0),
        _to_int(delta.get("exp"), 0),
        _to_int(delta.get("stamina"), 0),
    )
    if battle_conf is not None:
        log_line += "，信心=%d" % int(confidence_after)
    if ended_by_conf:
        log_line += "（信心归零，地图局结束）"
    _append_event_log(
        map_run_id,
        "battle_committed",
        {
            "battle_run_id": target_battle_run,
            "battle_status": run_status,
            "resource_delta": delta,
            "confidence_after": int(confidence_after),
            "ended_by_confidence_zero": bool(ended_by_conf),
        },
    )
    _append_turn_log(
        map_run_id,
        _to_int(run.get("turn_index"), 1),
        "battle_commit",
        {"battle_run_id": target_battle_run, "battle_status": run_status},
        {"resource_delta": delta, "confidence_after": int(confidence_after)},
    )
    if ended_by_conf:
        _append_event_log(map_run_id, "map_run_ended", {"reason": "confidence_zero", "result": result_payload})

    latest = _serialize_map_run(_get_map_row(map_run_id) or {})
    return {
        "ok": True,
        "map_run": latest,
        "resource_delta": delta,
        "log_line": log_line,
        "pending": False,
        "battle_status": run_status,
        "locked": bool(ended_by_conf),
    }


def finish_map_run(map_run_id: int) -> Dict[str, Any]:
    init_map_schema()
    loaded = _load_map_or_error(map_run_id)
    if not loaded.get("ok"):
        return loaded
    run = dict(loaded["map_run"])
    if _to_str(run.get("status")) in MAP_RUN_FINAL_STATUSES:
        return {"ok": True, "map_run": run, "locked": True}

    now = _now()
    result = {
        "summary": "结局评分待规划",
        "turn_limit": MAX_TURNS,
    }
    with _engine().begin() as conn:
        conn.execute(
            text(
                """
                UPDATE kline_card_map_runs
                SET status = 'ended',
                    ended_reason = 'manual_finish',
                    result_json = :result_json,
                    updated_at = :now,
                    finished_at = :now
                WHERE map_run_id = :rid
                """
            ),
            {
                "rid": int(map_run_id),
                "result_json": _json_dump(result),
                "now": now,
            },
        )

    _append_event_log(map_run_id, "map_run_ended", {"reason": "manual_finish", "result": result})
    latest = _serialize_map_run(_get_map_row(map_run_id) or {})
    return {
        "ok": True,
        "map_run": latest,
        "locked": True,
        "resource_delta": _resource_delta(),
        "log_line": "地图局已结束。",
    }

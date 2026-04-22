import re
import threading
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional


CHAT_FEEDBACK_ALLOWED_TYPES = {"up", "down"}
CHAT_FEEDBACK_REASON_CODES = {
    "not_personalized",
    "too_generic",
    "wrong_fact",
    "not_actionable",
}
CHAT_FEEDBACK_SAMPLE_STATUSES = {
    "new",
    "reviewed",
    "accepted",
    "rejected",
    "fixed",
}
CHAT_FEEDBACK_SAMPLE_OPTIMIZATION_TYPES = {
    "prompt",
    "rag",
    "rule",
    "fine_tune",
}

_CHAT_FEEDBACK_SCHEMA_LOCK = threading.Lock()
_CHAT_FEEDBACK_SCHEMA_READY = False
_CHAT_FEEDBACK_SCHEMA_ENGINE_ID = ""
_CHAT_FEEDBACK_SCHEMA_VERSION = "v2_samples"


def generate_chat_trace_id() -> str:
    return f"trace_{uuid.uuid4().hex}"


def generate_chat_answer_id() -> str:
    return f"answer_{uuid.uuid4().hex}"


def normalize_feedback_prompt_key(prompt_text: str) -> str:
    normalized = re.sub(r"\s+", " ", str(prompt_text or "").strip().lower())
    return normalized[:200]


def _normalize_datetime_filter(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value).strip()


def generate_feedback_sample_key(prompt_text: str, reason_code: str, intent_domain: str) -> str:
    prompt_key = normalize_feedback_prompt_key(prompt_text)
    return f"{prompt_key}|{str(reason_code or '').strip().lower()}|{str(intent_domain or 'general').strip().lower()}"


def default_feedback_sample_optimization_type(reason_code: str) -> str:
    normalized_reason = str(reason_code or "").strip().lower()
    if normalized_reason == "wrong_fact":
        return "rag"
    if normalized_reason in {"not_personalized", "too_generic", "not_actionable"}:
        return "prompt"
    return "prompt"


def _normalize_sample_status(value: Any, *, allow_empty: bool = True) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized and allow_empty:
        return ""
    if normalized not in CHAT_FEEDBACK_SAMPLE_STATUSES:
        return ""
    return normalized


def _normalize_sample_optimization_type(value: Any, *, reason_code: str = "", allow_empty: bool = True) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        if allow_empty:
            return ""
        return default_feedback_sample_optimization_type(reason_code)
    if normalized not in CHAT_FEEDBACK_SAMPLE_OPTIMIZATION_TYPES:
        return ""
    return normalized


def ensure_chat_feedback_tables(engine) -> bool:
    global _CHAT_FEEDBACK_SCHEMA_READY, _CHAT_FEEDBACK_SCHEMA_ENGINE_ID

    if engine is None:
        return False

    engine_id = f"{id(engine)}:{_CHAT_FEEDBACK_SCHEMA_VERSION}"
    if _CHAT_FEEDBACK_SCHEMA_READY and _CHAT_FEEDBACK_SCHEMA_ENGINE_ID == engine_id:
        return True

    dialect = str(getattr(engine.dialect, "name", "") or "").lower()
    if dialect == "sqlite":
        answer_sql = """
            CREATE TABLE IF NOT EXISTS chat_answer_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                answer_id VARCHAR(80) NOT NULL UNIQUE,
                trace_id VARCHAR(80) NOT NULL,
                task_id VARCHAR(80) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                prompt_text TEXT NOT NULL,
                response_text TEXT NOT NULL,
                intent_domain VARCHAR(64) NOT NULL DEFAULT 'general',
                feedback_allowed INTEGER NOT NULL DEFAULT 1,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL
            )
        """
        feedback_sql = """
            CREATE TABLE IF NOT EXISTS chat_feedback_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                answer_id VARCHAR(80) NOT NULL,
                trace_id VARCHAR(80) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                prompt_text TEXT NOT NULL,
                response_text TEXT NOT NULL,
                intent_domain VARCHAR(64) NOT NULL DEFAULT 'general',
                feedback_type VARCHAR(16) NOT NULL,
                reason_code VARCHAR(64) NULL,
                feedback_text TEXT NULL,
                created_at VARCHAR(40) NOT NULL
            )
        """
        sample_sql = """
            CREATE TABLE IF NOT EXISTS chat_feedback_samples (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_key VARCHAR(255) NOT NULL UNIQUE,
                prompt_text TEXT NOT NULL,
                reason_code VARCHAR(64) NOT NULL,
                intent_domain VARCHAR(64) NOT NULL DEFAULT 'general',
                sample_answer_id VARCHAR(80) NULL,
                sample_trace_id VARCHAR(80) NULL,
                sample_response_text TEXT NULL,
                latest_feedback_text TEXT NULL,
                occurrence_count INTEGER NOT NULL DEFAULT 1,
                sample_status VARCHAR(32) NOT NULL DEFAULT 'new',
                optimization_type VARCHAR(32) NOT NULL DEFAULT 'prompt',
                review_notes TEXT NULL,
                created_by VARCHAR(255) NULL,
                reviewed_by VARCHAR(255) NULL,
                first_seen_at VARCHAR(40) NOT NULL,
                last_seen_at VARCHAR(40) NOT NULL,
                reviewed_at VARCHAR(40) NULL,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL
            )
        """
    else:
        answer_sql = """
            CREATE TABLE IF NOT EXISTS chat_answer_events (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                answer_id VARCHAR(80) NOT NULL UNIQUE,
                trace_id VARCHAR(80) NOT NULL,
                task_id VARCHAR(80) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                prompt_text TEXT NOT NULL,
                response_text MEDIUMTEXT NOT NULL,
                intent_domain VARCHAR(64) NOT NULL DEFAULT 'general',
                feedback_allowed TINYINT(1) NOT NULL DEFAULT 1,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL
            )
        """
        feedback_sql = """
            CREATE TABLE IF NOT EXISTS chat_feedback_events (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                answer_id VARCHAR(80) NOT NULL,
                trace_id VARCHAR(80) NOT NULL,
                user_id VARCHAR(255) NOT NULL,
                prompt_text TEXT NOT NULL,
                response_text MEDIUMTEXT NOT NULL,
                intent_domain VARCHAR(64) NOT NULL DEFAULT 'general',
                feedback_type VARCHAR(16) NOT NULL,
                reason_code VARCHAR(64) NULL,
                feedback_text TEXT NULL,
                created_at VARCHAR(40) NOT NULL
            )
        """
        sample_sql = """
            CREATE TABLE IF NOT EXISTS chat_feedback_samples (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                sample_key VARCHAR(255) NOT NULL UNIQUE,
                prompt_text TEXT NOT NULL,
                reason_code VARCHAR(64) NOT NULL,
                intent_domain VARCHAR(64) NOT NULL DEFAULT 'general',
                sample_answer_id VARCHAR(80) NULL,
                sample_trace_id VARCHAR(80) NULL,
                sample_response_text MEDIUMTEXT NULL,
                latest_feedback_text TEXT NULL,
                occurrence_count INT NOT NULL DEFAULT 1,
                sample_status VARCHAR(32) NOT NULL DEFAULT 'new',
                optimization_type VARCHAR(32) NOT NULL DEFAULT 'prompt',
                review_notes TEXT NULL,
                created_by VARCHAR(255) NULL,
                reviewed_by VARCHAR(255) NULL,
                first_seen_at VARCHAR(40) NOT NULL,
                last_seen_at VARCHAR(40) NOT NULL,
                reviewed_at VARCHAR(40) NULL,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL
            )
        """

    try:
        from sqlalchemy import text as _text

        with _CHAT_FEEDBACK_SCHEMA_LOCK:
            if _CHAT_FEEDBACK_SCHEMA_READY and _CHAT_FEEDBACK_SCHEMA_ENGINE_ID == engine_id:
                return True
            with engine.begin() as conn:
                conn.execute(_text(answer_sql))
                conn.execute(_text(feedback_sql))
                conn.execute(_text(sample_sql))
            _CHAT_FEEDBACK_SCHEMA_READY = True
            _CHAT_FEEDBACK_SCHEMA_ENGINE_ID = engine_id
            return True
    except Exception as exc:
        print(f"[chat-feedback] ensure tables failed err={exc}")
        return False


def save_chat_answer_event(
    engine,
    *,
    task_id: str,
    user_id: str,
    trace_id: str,
    answer_id: str,
    prompt_text: str,
    response_text: str,
    intent_domain: str = "general",
    feedback_allowed: bool = True,
) -> bool:
    if not answer_id or not trace_id or not user_id:
        return False
    if not ensure_chat_feedback_tables(engine):
        return False

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            exists = conn.execute(
                _text("SELECT 1 FROM chat_answer_events WHERE answer_id = :answer_id LIMIT 1"),
                {"answer_id": answer_id},
            ).fetchone()
            now_iso = datetime.now().isoformat()
            if exists:
                conn.execute(
                    _text(
                        """
                        UPDATE chat_answer_events
                        SET response_text = :response_text,
                            prompt_text = :prompt_text,
                            intent_domain = :intent_domain,
                            feedback_allowed = :feedback_allowed,
                            updated_at = :updated_at
                        WHERE answer_id = :answer_id
                        """
                    ),
                    {
                        "answer_id": answer_id,
                        "response_text": str(response_text or ""),
                        "prompt_text": str(prompt_text or ""),
                        "intent_domain": str(intent_domain or "general"),
                        "feedback_allowed": 1 if feedback_allowed else 0,
                        "updated_at": now_iso,
                    },
                )
                return True

            conn.execute(
                _text(
                    """
                    INSERT INTO chat_answer_events (
                        answer_id, trace_id, task_id, user_id, prompt_text, response_text,
                        intent_domain, feedback_allowed, created_at, updated_at
                    )
                    VALUES (
                        :answer_id, :trace_id, :task_id, :user_id, :prompt_text, :response_text,
                        :intent_domain, :feedback_allowed, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "answer_id": answer_id,
                    "trace_id": trace_id,
                    "task_id": str(task_id or ""),
                    "user_id": str(user_id or ""),
                    "prompt_text": str(prompt_text or ""),
                    "response_text": str(response_text or ""),
                    "intent_domain": str(intent_domain or "general"),
                    "feedback_allowed": 1 if feedback_allowed else 0,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                },
            )
        return True
    except Exception as exc:
        print(f"[chat-feedback] save answer event failed answer_id={answer_id} err={exc}")
        return False


def get_chat_answer_event(engine, answer_id: str) -> Dict[str, Any]:
    if not answer_id or not ensure_chat_feedback_tables(engine):
        return {}

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            row = conn.execute(
                _text(
                    """
                    SELECT answer_id, trace_id, task_id, user_id, prompt_text, response_text,
                           intent_domain, feedback_allowed, created_at, updated_at
                    FROM chat_answer_events
                    WHERE answer_id = :answer_id
                    LIMIT 1
                    """
                ),
                {"answer_id": answer_id},
            ).mappings().fetchone()
            return dict(row) if row else {}
    except Exception as exc:
        print(f"[chat-feedback] get answer event failed answer_id={answer_id} err={exc}")
        return {}


def get_user_feedback_for_answer(engine, *, answer_id: str, user_id: str) -> Dict[str, Any]:
    if not answer_id or not user_id or not ensure_chat_feedback_tables(engine):
        return {}

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            row = conn.execute(
                _text(
                    """
                    SELECT answer_id, trace_id, user_id, feedback_type, reason_code, feedback_text, created_at
                    FROM chat_feedback_events
                    WHERE answer_id = :answer_id AND user_id = :user_id
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {"answer_id": answer_id, "user_id": user_id},
            ).mappings().fetchone()
            return dict(row) if row else {}
    except Exception as exc:
        print(f"[chat-feedback] get user feedback failed answer_id={answer_id} user_id={user_id} err={exc}")
        return {}


def save_chat_feedback_event(
    engine,
    *,
    answer_id: str,
    trace_id: str,
    user_id: str,
    prompt_text: str,
    response_text: str,
    intent_domain: str,
    feedback_type: str,
    reason_code: str = "",
    feedback_text: str = "",
) -> bool:
    if not answer_id or not trace_id or not user_id:
        return False
    if not ensure_chat_feedback_tables(engine):
        return False

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            conn.execute(
                _text(
                    """
                    INSERT INTO chat_feedback_events (
                        answer_id, trace_id, user_id, prompt_text, response_text, intent_domain,
                        feedback_type, reason_code, feedback_text, created_at
                    )
                    VALUES (
                        :answer_id, :trace_id, :user_id, :prompt_text, :response_text, :intent_domain,
                        :feedback_type, :reason_code, :feedback_text, :created_at
                    )
                    """
                ),
                {
                    "answer_id": answer_id,
                    "trace_id": trace_id,
                    "user_id": str(user_id or ""),
                    "prompt_text": str(prompt_text or ""),
                    "response_text": str(response_text or ""),
                    "intent_domain": str(intent_domain or "general"),
                    "feedback_type": str(feedback_type or ""),
                    "reason_code": str(reason_code or "") or None,
                    "feedback_text": str(feedback_text or ""),
                    "created_at": datetime.now().isoformat(),
                },
            )
        return True
    except Exception as exc:
        print(f"[chat-feedback] save feedback failed answer_id={answer_id} err={exc}")
        return False


def submit_chat_feedback(
    engine,
    *,
    answer_id: str,
    trace_id: str,
    user_id: str,
    feedback_type: str,
    reason_code: str = "",
    feedback_text: str = "",
) -> Dict[str, Any]:
    normalized_type = str(feedback_type or "").strip().lower()
    if normalized_type not in CHAT_FEEDBACK_ALLOWED_TYPES:
        return {"ok": False, "code": "unsupported_feedback_type"}

    normalized_reason = str(reason_code or "").strip().lower()
    if normalized_type == "down":
        if normalized_reason not in CHAT_FEEDBACK_REASON_CODES:
            return {"ok": False, "code": "invalid_reason_code"}
    else:
        normalized_reason = ""

    answer_event = get_chat_answer_event(engine, answer_id)
    if not answer_event:
        return {"ok": False, "code": "answer_not_found"}
    if str(answer_event.get("user_id") or "").strip() != str(user_id or "").strip():
        return {"ok": False, "code": "forbidden"}
    if str(answer_event.get("trace_id") or "").strip() != str(trace_id or "").strip():
        return {"ok": False, "code": "trace_mismatch"}

    existing_feedback = get_user_feedback_for_answer(engine, answer_id=answer_id, user_id=user_id)
    if existing_feedback:
        return {
            "ok": True,
            "code": "already_submitted",
            "feedback": existing_feedback,
        }

    saved = save_chat_feedback_event(
        engine,
        answer_id=answer_id,
        trace_id=trace_id,
        user_id=user_id,
        prompt_text=str(answer_event.get("prompt_text") or ""),
        response_text=str(answer_event.get("response_text") or ""),
        intent_domain=str(answer_event.get("intent_domain") or "general"),
        feedback_type=normalized_type,
        reason_code=normalized_reason,
        feedback_text=str(feedback_text or "").strip(),
    )
    if not saved:
        return {"ok": False, "code": "save_failed"}
    return {"ok": True, "code": "ok"}


def list_chat_feedback_events(
    engine,
    *,
    limit: int = 100,
    feedback_type: str = "",
    answer_id: str = "",
    user_id: str = "",
    intent_domain: str = "",
    reason_code: str = "",
    keyword: str = "",
    start_at: Any = "",
    end_at: Any = "",
) -> List[Dict[str, Any]]:
    if limit <= 0 or not ensure_chat_feedback_tables(engine):
        return []

    clauses = []
    params: Dict[str, Any] = {"limit": int(limit)}
    normalized_type = str(feedback_type or "").strip().lower()
    if normalized_type:
        clauses.append("feedback_type = :feedback_type")
        params["feedback_type"] = normalized_type
    normalized_answer_id = str(answer_id or "").strip()
    if normalized_answer_id:
        clauses.append("answer_id = :answer_id")
        params["answer_id"] = normalized_answer_id
    normalized_user_id = str(user_id or "").strip()
    if normalized_user_id:
        clauses.append("user_id = :user_id")
        params["user_id"] = normalized_user_id
    normalized_domain = str(intent_domain or "").strip()
    if normalized_domain:
        clauses.append("LOWER(intent_domain) LIKE :intent_domain")
        params["intent_domain"] = f"%{normalized_domain.lower()}%"
    normalized_reason = str(reason_code or "").strip().lower()
    if normalized_reason:
        clauses.append("reason_code = :reason_code")
        params["reason_code"] = normalized_reason
    normalized_keyword = str(keyword or "").strip()
    if normalized_keyword:
        clauses.append(
            "(LOWER(prompt_text) LIKE :keyword OR LOWER(feedback_text) LIKE :keyword OR LOWER(response_text) LIKE :keyword)"
        )
        params["keyword"] = f"%{normalized_keyword.lower()}%"
    normalized_start_at = _normalize_datetime_filter(start_at)
    if normalized_start_at:
        clauses.append("created_at >= :start_at")
        params["start_at"] = normalized_start_at
    normalized_end_at = _normalize_datetime_filter(end_at)
    if normalized_end_at:
        clauses.append("created_at <= :end_at")
        params["end_at"] = normalized_end_at

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            rows = conn.execute(
                _text(
                    f"""
                    SELECT answer_id, trace_id, user_id, prompt_text, response_text, intent_domain,
                           feedback_type, reason_code, feedback_text, created_at
                    FROM chat_feedback_events
                    {where_sql}
                    ORDER BY created_at DESC
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().fetchall()
            return [dict(row) for row in rows]
    except Exception as exc:
        print(f"[chat-feedback] list feedback events failed err={exc}")
        return []


def list_chat_feedback_failure_candidates(
    engine,
    limit: int = 20,
    *,
    intent_domain: str = "",
    reason_code: str = "",
    keyword: str = "",
    start_at: Any = "",
    end_at: Any = "",
    min_occurrence: int = 1,
) -> List[Dict[str, Any]]:
    if limit <= 0 or not ensure_chat_feedback_tables(engine):
        return []

    clauses = ["feedback_type = 'down'"]
    params: Dict[str, Any] = {}
    normalized_domain = str(intent_domain or "").strip()
    if normalized_domain:
        clauses.append("LOWER(intent_domain) LIKE :intent_domain")
        params["intent_domain"] = f"%{normalized_domain.lower()}%"
    normalized_reason = str(reason_code or "").strip().lower()
    if normalized_reason:
        clauses.append("reason_code = :reason_code")
        params["reason_code"] = normalized_reason
    normalized_keyword = str(keyword or "").strip()
    if normalized_keyword:
        clauses.append(
            "(LOWER(prompt_text) LIKE :keyword OR LOWER(feedback_text) LIKE :keyword OR LOWER(response_text) LIKE :keyword)"
        )
        params["keyword"] = f"%{normalized_keyword.lower()}%"
    normalized_start_at = _normalize_datetime_filter(start_at)
    if normalized_start_at:
        clauses.append("created_at >= :start_at")
        params["start_at"] = normalized_start_at
    normalized_end_at = _normalize_datetime_filter(end_at)
    if normalized_end_at:
        clauses.append("created_at <= :end_at")
        params["end_at"] = normalized_end_at
    where_sql = "WHERE " + " AND ".join(clauses)

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            rows = conn.execute(
                _text(
                    f"""
                    SELECT answer_id, trace_id, user_id, prompt_text, response_text, intent_domain,
                           feedback_type, reason_code, feedback_text, created_at
                    FROM chat_feedback_events
                    {where_sql}
                    ORDER BY created_at DESC
                    """
                ),
                params,
            ).mappings().fetchall()
    except Exception as exc:
        print(f"[chat-feedback] list failure candidates failed err={exc}")
        return []

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        prompt_text = str(item.get("prompt_text") or "").strip()
        reason_code = str(item.get("reason_code") or "").strip()
        key = f"{normalize_feedback_prompt_key(prompt_text)}|{reason_code}"
        existing = grouped.get(key)
        if not existing:
            grouped[key] = {
                "prompt_text": prompt_text,
                "reason_code": reason_code,
                "intent_domain": str(item.get("intent_domain") or "general"),
                "occurrence_count": 1,
                "latest_feedback_at": str(item.get("created_at") or ""),
                "latest_feedback_text": str(item.get("feedback_text") or "").strip(),
                "sample_answer_id": str(item.get("answer_id") or ""),
                "sample_trace_id": str(item.get("trace_id") or ""),
                "sample_response_text": str(item.get("response_text") or "").strip(),
            }
            continue

        existing["occurrence_count"] += 1
        if str(item.get("created_at") or "") >= str(existing.get("latest_feedback_at") or ""):
            existing["latest_feedback_at"] = str(item.get("created_at") or "")
            if str(item.get("feedback_text") or "").strip():
                existing["latest_feedback_text"] = str(item.get("feedback_text") or "").strip()
            existing["sample_answer_id"] = str(item.get("answer_id") or "")
            existing["sample_trace_id"] = str(item.get("trace_id") or "")
            existing["sample_response_text"] = str(item.get("response_text") or "").strip()

    ranked = sorted(
        [
            item
            for item in grouped.values()
            if int(item.get("occurrence_count", 0)) >= max(1, int(min_occurrence or 1))
        ],
        key=lambda item: (-int(item.get("occurrence_count", 0)), str(item.get("latest_feedback_at") or "")),
    )
    return ranked[:limit]


def get_chat_feedback_sample(engine, *, sample_key: str) -> Dict[str, Any]:
    normalized_key = str(sample_key or "").strip()
    if not normalized_key or not ensure_chat_feedback_tables(engine):
        return {}

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            row = conn.execute(
                _text(
                    """
                    SELECT sample_key, prompt_text, reason_code, intent_domain, sample_answer_id,
                           sample_trace_id, sample_response_text, latest_feedback_text, occurrence_count,
                           sample_status, optimization_type, review_notes, created_by, reviewed_by,
                           first_seen_at, last_seen_at, reviewed_at, created_at, updated_at
                    FROM chat_feedback_samples
                    WHERE sample_key = :sample_key
                    LIMIT 1
                    """
                ),
                {"sample_key": normalized_key},
            ).mappings().fetchone()
            return dict(row) if row else {}
    except Exception as exc:
        print(f"[chat-feedback] get sample failed sample_key={normalized_key} err={exc}")
        return {}


def upsert_chat_feedback_sample(
    engine,
    *,
    prompt_text: str,
    reason_code: str,
    intent_domain: str = "general",
    occurrence_count: int = 1,
    latest_feedback_at: Any = "",
    latest_feedback_text: str = "",
    sample_answer_id: str = "",
    sample_trace_id: str = "",
    sample_response_text: str = "",
    created_by: str = "",
    sample_status: str = "new",
    optimization_type: str = "",
    review_notes: str = "",
) -> Dict[str, Any]:
    normalized_prompt = str(prompt_text or "").strip()
    normalized_reason = str(reason_code or "").strip().lower()
    normalized_domain = str(intent_domain or "general").strip() or "general"
    if not normalized_prompt or normalized_reason not in CHAT_FEEDBACK_REASON_CODES:
        return {"ok": False, "code": "invalid_candidate"}
    if not ensure_chat_feedback_tables(engine):
        return {"ok": False, "code": "schema_unavailable"}

    normalized_status = _normalize_sample_status(sample_status, allow_empty=False) or "new"
    normalized_opt_type = _normalize_sample_optimization_type(
        optimization_type,
        reason_code=normalized_reason,
        allow_empty=False,
    )
    normalized_occurrence = max(1, int(occurrence_count or 1))
    normalized_latest_at = _normalize_datetime_filter(latest_feedback_at) or datetime.now().isoformat()
    normalized_key = generate_feedback_sample_key(normalized_prompt, normalized_reason, normalized_domain)
    now_iso = datetime.now().isoformat()

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            existing = conn.execute(
                _text(
                    """
                    SELECT sample_key, occurrence_count, sample_status, optimization_type, review_notes,
                           first_seen_at, last_seen_at, created_by
                    FROM chat_feedback_samples
                    WHERE sample_key = :sample_key
                    LIMIT 1
                    """
                ),
                {"sample_key": normalized_key},
            ).mappings().fetchone()

            if existing:
                existing_dict = dict(existing)
                merged_status = normalized_status or str(existing_dict.get("sample_status") or "new")
                merged_opt_type = normalized_opt_type or str(existing_dict.get("optimization_type") or "")
                merged_notes = str(review_notes or "").strip() or str(existing_dict.get("review_notes") or "").strip()
                merged_created_by = str(existing_dict.get("created_by") or created_by or "").strip()
                merged_first_seen = str(existing_dict.get("first_seen_at") or normalized_latest_at or now_iso)
                merged_last_seen = max(str(existing_dict.get("last_seen_at") or ""), normalized_latest_at)
                merged_occurrence = max(int(existing_dict.get("occurrence_count") or 1), normalized_occurrence)
                conn.execute(
                    _text(
                        """
                        UPDATE chat_feedback_samples
                        SET prompt_text = :prompt_text,
                            reason_code = :reason_code,
                            intent_domain = :intent_domain,
                            sample_answer_id = :sample_answer_id,
                            sample_trace_id = :sample_trace_id,
                            sample_response_text = :sample_response_text,
                            latest_feedback_text = :latest_feedback_text,
                            occurrence_count = :occurrence_count,
                            sample_status = :sample_status,
                            optimization_type = :optimization_type,
                            review_notes = :review_notes,
                            created_by = :created_by,
                            first_seen_at = :first_seen_at,
                            last_seen_at = :last_seen_at,
                            updated_at = :updated_at
                        WHERE sample_key = :sample_key
                        """
                    ),
                    {
                        "sample_key": normalized_key,
                        "prompt_text": normalized_prompt,
                        "reason_code": normalized_reason,
                        "intent_domain": normalized_domain,
                        "sample_answer_id": str(sample_answer_id or ""),
                        "sample_trace_id": str(sample_trace_id or ""),
                        "sample_response_text": str(sample_response_text or ""),
                        "latest_feedback_text": str(latest_feedback_text or "").strip(),
                        "occurrence_count": merged_occurrence,
                        "sample_status": merged_status,
                        "optimization_type": merged_opt_type,
                        "review_notes": merged_notes,
                        "created_by": merged_created_by,
                        "first_seen_at": merged_first_seen,
                        "last_seen_at": merged_last_seen,
                        "updated_at": now_iso,
                    },
                )
                return {
                    "ok": True,
                    "code": "updated",
                    "sample": get_chat_feedback_sample(engine, sample_key=normalized_key),
                }

            conn.execute(
                _text(
                    """
                    INSERT INTO chat_feedback_samples (
                        sample_key, prompt_text, reason_code, intent_domain, sample_answer_id,
                        sample_trace_id, sample_response_text, latest_feedback_text, occurrence_count,
                        sample_status, optimization_type, review_notes, created_by, reviewed_by,
                        first_seen_at, last_seen_at, reviewed_at, created_at, updated_at
                    )
                    VALUES (
                        :sample_key, :prompt_text, :reason_code, :intent_domain, :sample_answer_id,
                        :sample_trace_id, :sample_response_text, :latest_feedback_text, :occurrence_count,
                        :sample_status, :optimization_type, :review_notes, :created_by, :reviewed_by,
                        :first_seen_at, :last_seen_at, :reviewed_at, :created_at, :updated_at
                    )
                    """
                ),
                {
                    "sample_key": normalized_key,
                    "prompt_text": normalized_prompt,
                    "reason_code": normalized_reason,
                    "intent_domain": normalized_domain,
                    "sample_answer_id": str(sample_answer_id or ""),
                    "sample_trace_id": str(sample_trace_id or ""),
                    "sample_response_text": str(sample_response_text or ""),
                    "latest_feedback_text": str(latest_feedback_text or "").strip(),
                    "occurrence_count": normalized_occurrence,
                    "sample_status": normalized_status,
                    "optimization_type": normalized_opt_type,
                    "review_notes": str(review_notes or "").strip(),
                    "created_by": str(created_by or "").strip(),
                    "reviewed_by": "",
                    "first_seen_at": normalized_latest_at,
                    "last_seen_at": normalized_latest_at,
                    "reviewed_at": None,
                    "created_at": now_iso,
                    "updated_at": now_iso,
                },
            )
        return {
            "ok": True,
            "code": "created",
            "sample": get_chat_feedback_sample(engine, sample_key=normalized_key),
        }
    except Exception as exc:
        print(f"[chat-feedback] upsert sample failed sample_key={normalized_key} err={exc}")
        return {"ok": False, "code": "save_failed"}


def update_chat_feedback_sample(
    engine,
    *,
    sample_key: str,
    sample_status: Optional[str] = None,
    optimization_type: Optional[str] = None,
    review_notes: Optional[str] = None,
    reviewed_by: str = "",
) -> Dict[str, Any]:
    normalized_key = str(sample_key or "").strip()
    if not normalized_key:
        return {"ok": False, "code": "sample_key_required"}
    if not ensure_chat_feedback_tables(engine):
        return {"ok": False, "code": "schema_unavailable"}

    existing = get_chat_feedback_sample(engine, sample_key=normalized_key)
    if not existing:
        return {"ok": False, "code": "sample_not_found"}

    next_status = _normalize_sample_status(sample_status, allow_empty=True) or str(existing.get("sample_status") or "new")
    next_opt_type = _normalize_sample_optimization_type(
        optimization_type,
        reason_code=str(existing.get("reason_code") or ""),
        allow_empty=True,
    ) or str(existing.get("optimization_type") or "")
    next_notes = (
        str(review_notes).strip()
        if review_notes is not None
        else str(existing.get("review_notes") or "").strip()
    )
    reviewed_at = datetime.now().isoformat() if str(reviewed_by or "").strip() else existing.get("reviewed_at")
    now_iso = datetime.now().isoformat()

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            conn.execute(
                _text(
                    """
                    UPDATE chat_feedback_samples
                    SET sample_status = :sample_status,
                        optimization_type = :optimization_type,
                        review_notes = :review_notes,
                        reviewed_by = :reviewed_by,
                        reviewed_at = :reviewed_at,
                        updated_at = :updated_at
                    WHERE sample_key = :sample_key
                    """
                ),
                {
                    "sample_key": normalized_key,
                    "sample_status": next_status,
                    "optimization_type": next_opt_type,
                    "review_notes": next_notes,
                    "reviewed_by": str(reviewed_by or "").strip(),
                    "reviewed_at": reviewed_at,
                    "updated_at": now_iso,
                },
            )
        return {"ok": True, "code": "ok", "sample": get_chat_feedback_sample(engine, sample_key=normalized_key)}
    except Exception as exc:
        print(f"[chat-feedback] update sample failed sample_key={normalized_key} err={exc}")
        return {"ok": False, "code": "save_failed"}


def list_chat_feedback_samples(
    engine,
    *,
    limit: int = 100,
    sample_status: str = "",
    optimization_type: str = "",
    intent_domain: str = "",
    reason_code: str = "",
    keyword: str = "",
) -> List[Dict[str, Any]]:
    if limit <= 0 or not ensure_chat_feedback_tables(engine):
        return []

    clauses = []
    params: Dict[str, Any] = {"limit": int(limit)}
    normalized_status = _normalize_sample_status(sample_status, allow_empty=True)
    if normalized_status:
        clauses.append("sample_status = :sample_status")
        params["sample_status"] = normalized_status
    normalized_opt_type = _normalize_sample_optimization_type(optimization_type, allow_empty=True)
    if normalized_opt_type:
        clauses.append("optimization_type = :optimization_type")
        params["optimization_type"] = normalized_opt_type
    normalized_domain = str(intent_domain or "").strip()
    if normalized_domain:
        clauses.append("LOWER(intent_domain) LIKE :intent_domain")
        params["intent_domain"] = f"%{normalized_domain.lower()}%"
    normalized_reason = str(reason_code or "").strip().lower()
    if normalized_reason:
        clauses.append("reason_code = :reason_code")
        params["reason_code"] = normalized_reason
    normalized_keyword = str(keyword or "").strip()
    if normalized_keyword:
        clauses.append(
            "(LOWER(prompt_text) LIKE :keyword OR LOWER(latest_feedback_text) LIKE :keyword OR LOWER(review_notes) LIKE :keyword)"
        )
        params["keyword"] = f"%{normalized_keyword.lower()}%"

    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    try:
        from sqlalchemy import text as _text

        with engine.begin() as conn:
            rows = conn.execute(
                _text(
                    f"""
                    SELECT sample_key, prompt_text, reason_code, intent_domain, sample_answer_id,
                           sample_trace_id, sample_response_text, latest_feedback_text, occurrence_count,
                           sample_status, optimization_type, review_notes, created_by, reviewed_by,
                           first_seen_at, last_seen_at, reviewed_at, created_at, updated_at
                    FROM chat_feedback_samples
                    {where_sql}
                    ORDER BY updated_at DESC, occurrence_count DESC
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().fetchall()
            return [dict(row) for row in rows]
    except Exception as exc:
        print(f"[chat-feedback] list samples failed err={exc}")
        return []

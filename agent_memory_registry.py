import hashlib
import json
from datetime import datetime
from typing import Any, Dict, List

from sqlalchemy import text


MEMORY_STATUS_ACTIVE = "active"
MEMORY_STATUS_SUPERSEDED = "superseded"
MEMORY_STATUS_PENDING = "pending"
MEMORY_STATUS_DELETED = "deleted"
MEMORY_STATUS_REJECTED = "rejected"
MEMORY_STATUS_EXPIRED = "expired"

MEMORY_TYPE_SEMANTIC = "semantic"
MEMORY_TYPE_EPISODIC = "episodic"
MEMORY_TYPE_PROCEDURAL = "procedural"
MEMORY_TYPE_CONVERSATION = "conversation"

SOURCE_PROFILE_MEMORY = "user_profile_memory"
SOURCE_CHROMA_CHAT_HISTORY = "chroma_user_chat_history"
SOURCE_CHAT_FEEDBACK_EVENT = "chat_feedback_event"
SOURCE_CHAT_FEEDBACK_SAMPLE = "chat_feedback_sample"

_VALID_STATUSES = {
    MEMORY_STATUS_ACTIVE,
    MEMORY_STATUS_SUPERSEDED,
    MEMORY_STATUS_PENDING,
    MEMORY_STATUS_DELETED,
    MEMORY_STATUS_REJECTED,
    MEMORY_STATUS_EXPIRED,
}
_VALID_TYPES = {
    MEMORY_TYPE_SEMANTIC,
    MEMORY_TYPE_EPISODIC,
    MEMORY_TYPE_PROCEDURAL,
    MEMORY_TYPE_CONVERSATION,
}
_SCHEMA_READY_ENGINE_IDS: set[str] = set()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _clean_text(value: Any, max_len: int = 1000) -> str:
    return str(value or "").strip()[:max_len]


def _json_dumps(value: Any) -> str:
    if value is None:
        value = {}
    if isinstance(value, str):
        value = {"text": value}
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        return json.dumps({"text": str(value)}, ensure_ascii=False, sort_keys=True)


def _json_loads(value: Any) -> Any:
    raw = str(value or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {"text": raw}


def _stable_hash(value: str) -> str:
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def normalize_memory_status(status: Any) -> str:
    value = _clean_text(status, 32).lower()
    return value if value in _VALID_STATUSES else MEMORY_STATUS_ACTIVE


def normalize_memory_type(memory_type: Any) -> str:
    value = _clean_text(memory_type, 32).lower()
    return value if value in _VALID_TYPES else MEMORY_TYPE_CONVERSATION


def normalize_domain(domain: Any) -> str:
    value = _clean_text(domain, 64).lower()
    return value or "general"


def build_memory_namespace(user_id: str = "", *, kind: str, domain: str = "") -> str:
    normalized_kind = _clean_text(kind, 64).lower() or "memory"
    normalized_domain = normalize_domain(domain)
    uid = _clean_text(user_id, 255)
    if uid:
        if normalized_kind == "conversation":
            return f"user:{uid}:conversation:{normalized_domain}"
        return f"user:{uid}:{normalized_kind}"
    return f"global:{normalized_kind}:{normalized_domain}"


def _build_memory_uid(
    *,
    user_id: str,
    namespace: str,
    memory_type: str,
    memory_key: str,
    source_type: str,
    source_id: str,
    value_json: str,
    text_summary: str,
) -> str:
    stable_source = source_id or _stable_hash(f"{value_json}|{text_summary}")
    basis = "|".join(
        [
            _clean_text(user_id, 255),
            _clean_text(namespace, 255),
            _clean_text(memory_type, 32),
            _clean_text(memory_key, 255),
            _clean_text(source_type, 64),
            _clean_text(stable_source, 255),
        ]
    )
    return _stable_hash(basis)[:64]


def build_chroma_memory_source_id(
    *,
    user_id: str,
    timestamp: str,
    topic: str,
    source: str,
    user_input: str,
    ai_response: str,
) -> str:
    return _stable_hash(
        "|".join(
            [
                _clean_text(user_id, 255),
                _clean_text(timestamp, 40),
                normalize_domain(topic),
                _clean_text(source, 64),
                _clean_text(user_input, 2000),
                _clean_text(ai_response, 4000),
            ]
        )
    )[:64]


def ensure_agent_memory_tables(engine) -> bool:
    if engine is None:
        return False
    dialect = str(getattr(engine.dialect, "name", "") or "").lower()
    db_name = str(getattr(getattr(engine, "url", None), "database", "") or "")
    cacheable_schema = not (dialect == "sqlite" and db_name in {"", ":memory:"})
    engine_id = f"{id(engine)}:agent_memories_v1"
    if cacheable_schema and engine_id in _SCHEMA_READY_ENGINE_IDS:
        return True

    if dialect == "sqlite":
        ddl = """
            CREATE TABLE IF NOT EXISTS agent_memories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_memory_uid VARCHAR(80) NOT NULL UNIQUE,
                user_id VARCHAR(255) NOT NULL DEFAULT '',
                namespace VARCHAR(255) NOT NULL,
                memory_type VARCHAR(32) NOT NULL,
                domain VARCHAR(64) NOT NULL DEFAULT 'general',
                memory_key VARCHAR(255) NOT NULL DEFAULT '',
                value_json TEXT NOT NULL,
                text_summary TEXT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'active',
                confidence FLOAT NOT NULL DEFAULT 0.8,
                source_type VARCHAR(64) NOT NULL DEFAULT '',
                source_id VARCHAR(255) NOT NULL DEFAULT '',
                vector_ref TEXT NULL,
                expires_at VARCHAR(40) NULL,
                last_used_at VARCHAR(40) NULL,
                use_count INTEGER NOT NULL DEFAULT 0,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL
            )
        """
    else:
        ddl = """
            CREATE TABLE IF NOT EXISTS agent_memories (
                id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                agent_memory_uid VARCHAR(80) NOT NULL UNIQUE,
                user_id VARCHAR(255) NOT NULL DEFAULT '',
                namespace VARCHAR(255) NOT NULL,
                memory_type VARCHAR(32) NOT NULL,
                domain VARCHAR(64) NOT NULL DEFAULT 'general',
                memory_key VARCHAR(255) NOT NULL DEFAULT '',
                value_json MEDIUMTEXT NOT NULL,
                text_summary TEXT NULL,
                status VARCHAR(32) NOT NULL DEFAULT 'active',
                confidence FLOAT NOT NULL DEFAULT 0.8,
                source_type VARCHAR(64) NOT NULL DEFAULT '',
                source_id VARCHAR(255) NOT NULL DEFAULT '',
                vector_ref TEXT NULL,
                expires_at VARCHAR(40) NULL,
                last_used_at VARCHAR(40) NULL,
                use_count INT NOT NULL DEFAULT 0,
                created_at VARCHAR(40) NOT NULL,
                updated_at VARCHAR(40) NOT NULL,
                INDEX idx_agent_memories_lookup (user_id, namespace, memory_type, status),
                INDEX idx_agent_memories_source (source_type, source_id)
            )
        """
    try:
        with engine.begin() as conn:
            conn.execute(text(ddl))
        if cacheable_schema:
            _SCHEMA_READY_ENGINE_IDS.add(engine_id)
        return True
    except Exception as exc:
        print(f"[agent-memory] ensure table failed err={exc}")
        return False


def upsert_agent_memory(
    engine,
    *,
    user_id: str = "",
    namespace: str,
    memory_type: str,
    domain: str = "general",
    memory_key: str = "",
    value: Any = None,
    text_summary: str = "",
    status: str = MEMORY_STATUS_ACTIVE,
    confidence: float = 0.8,
    source_type: str = "",
    source_id: str = "",
    vector_ref: Any = None,
    expires_at: str = "",
    supersede_scope: bool = False,
) -> Dict[str, Any]:
    if not ensure_agent_memory_tables(engine):
        return {"ok": False, "code": "schema_unavailable"}

    uid = _clean_text(user_id, 255)
    ns = _clean_text(namespace, 255)
    if not ns:
        return {"ok": False, "code": "invalid_namespace"}
    mem_type = normalize_memory_type(memory_type)
    mem_status = normalize_memory_status(status)
    mem_domain = normalize_domain(domain)
    mem_key = _clean_text(memory_key, 255)
    value_json = _json_dumps(value)
    summary = _clean_text(text_summary, 4000) or _clean_text(value_json, 4000)
    src_type = _clean_text(source_type, 64)
    src_id = _clean_text(source_id, 255)
    vector_json = _json_dumps(vector_ref) if vector_ref else ""
    now_iso = _now_iso()
    memory_uid = _build_memory_uid(
        user_id=uid,
        namespace=ns,
        memory_type=mem_type,
        memory_key=mem_key,
        source_type=src_type,
        source_id=src_id,
        value_json=value_json,
        text_summary=summary,
    )

    try:
        with engine.begin() as conn:
            if supersede_scope and mem_status == MEMORY_STATUS_ACTIVE:
                conn.execute(
                    text(
                        """
                        UPDATE agent_memories
                        SET status=:superseded, updated_at=:updated_at
                        WHERE user_id=:user_id AND namespace=:namespace
                          AND memory_type=:memory_type AND memory_key=:memory_key
                          AND status=:active AND agent_memory_uid<>:agent_memory_uid
                        """
                    ),
                    {
                        "user_id": uid,
                        "namespace": ns,
                        "memory_type": mem_type,
                        "memory_key": mem_key,
                        "active": MEMORY_STATUS_ACTIVE,
                        "superseded": MEMORY_STATUS_SUPERSEDED,
                        "agent_memory_uid": memory_uid,
                        "updated_at": now_iso,
                    },
                )

            existing = conn.execute(
                text("SELECT id FROM agent_memories WHERE agent_memory_uid=:uid LIMIT 1"),
                {"uid": memory_uid},
            ).fetchone()
            params = {
                "agent_memory_uid": memory_uid,
                "user_id": uid,
                "namespace": ns,
                "memory_type": mem_type,
                "domain": mem_domain,
                "memory_key": mem_key,
                "value_json": value_json,
                "text_summary": summary,
                "status": mem_status,
                "confidence": max(0.0, min(float(confidence or 0.8), 1.0)),
                "source_type": src_type,
                "source_id": src_id,
                "vector_ref": vector_json or None,
                "expires_at": _clean_text(expires_at, 40) or None,
                "created_at": now_iso,
                "updated_at": now_iso,
            }
            if existing:
                conn.execute(
                    text(
                        """
                        UPDATE agent_memories
                        SET user_id=:user_id,
                            namespace=:namespace,
                            memory_type=:memory_type,
                            domain=:domain,
                            memory_key=:memory_key,
                            value_json=:value_json,
                            text_summary=:text_summary,
                            status=:status,
                            confidence=:confidence,
                            source_type=:source_type,
                            source_id=:source_id,
                            vector_ref=:vector_ref,
                            expires_at=:expires_at,
                            updated_at=:updated_at
                        WHERE agent_memory_uid=:agent_memory_uid
                        """
                    ),
                    params,
                )
                code = "updated"
            else:
                conn.execute(
                    text(
                        """
                        INSERT INTO agent_memories (
                            agent_memory_uid, user_id, namespace, memory_type, domain,
                            memory_key, value_json, text_summary, status, confidence,
                            source_type, source_id, vector_ref, expires_at,
                            last_used_at, use_count, created_at, updated_at
                        )
                        VALUES (
                            :agent_memory_uid, :user_id, :namespace, :memory_type, :domain,
                            :memory_key, :value_json, :text_summary, :status, :confidence,
                            :source_type, :source_id, :vector_ref, :expires_at,
                            NULL, 0, :created_at, :updated_at
                        )
                        """
                    ),
                    params,
                )
                code = "created"
        return {"ok": True, "code": code, "agent_memory_uid": memory_uid}
    except Exception as exc:
        print(f"[agent-memory] upsert failed uid={memory_uid} err={exc}")
        return {"ok": False, "code": "upsert_failed"}


def list_agent_memories(
    engine,
    *,
    user_id: str = "",
    namespace: str = "",
    memory_type: str = "",
    domain: str = "",
    status: str = MEMORY_STATUS_ACTIVE,
    source_type: str = "",
    source_id: str = "",
    limit: int = 50,
) -> List[Dict[str, Any]]:
    if limit <= 0 or not ensure_agent_memory_tables(engine):
        return []
    clauses = []
    params: Dict[str, Any] = {"limit": min(max(int(limit or 50), 1), 500)}
    if user_id:
        clauses.append("user_id=:user_id")
        params["user_id"] = _clean_text(user_id, 255)
    if namespace:
        clauses.append("namespace=:namespace")
        params["namespace"] = _clean_text(namespace, 255)
    if memory_type:
        clauses.append("memory_type=:memory_type")
        params["memory_type"] = normalize_memory_type(memory_type)
    if domain:
        clauses.append("domain=:domain")
        params["domain"] = normalize_domain(domain)
    if status:
        clauses.append("status=:status")
        params["status"] = normalize_memory_status(status)
    if source_type:
        clauses.append("source_type=:source_type")
        params["source_type"] = _clean_text(source_type, 64)
    if source_id:
        clauses.append("source_id=:source_id")
        params["source_id"] = _clean_text(source_id, 255)
    where_sql = "WHERE " + " AND ".join(clauses) if clauses else ""
    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT id, agent_memory_uid, user_id, namespace, memory_type, domain,
                           memory_key, value_json, text_summary, status, confidence,
                           source_type, source_id, vector_ref, expires_at,
                           last_used_at, use_count, created_at, updated_at
                    FROM agent_memories
                    {where_sql}
                    ORDER BY updated_at DESC, id DESC
                    LIMIT :limit
                    """
                ),
                params,
            ).mappings().fetchall()
        out = []
        for row in rows:
            item = dict(row)
            item["value"] = _json_loads(item.get("value_json"))
            item["vector"] = _json_loads(item.get("vector_ref"))
            out.append(item)
        return out
    except Exception as exc:
        print(f"[agent-memory] list failed err={exc}")
        return []


def is_agent_memory_active(engine, *, source_type: str, source_id: str) -> bool:
    src_type = _clean_text(source_type, 64)
    src_id = _clean_text(source_id, 255)
    if not src_type or not src_id:
        return True
    rows = list_agent_memories(
        engine,
        source_type=src_type,
        source_id=src_id,
        status="",
        limit=1,
    )
    if not rows:
        return True
    return str(rows[0].get("status") or "") == MEMORY_STATUS_ACTIVE


def record_agent_memory_use(
    engine,
    *,
    agent_memory_uid: str = "",
    source_type: str = "",
    source_id: str = "",
) -> bool:
    if not ensure_agent_memory_tables(engine):
        return False
    clauses = []
    params: Dict[str, Any] = {"last_used_at": _now_iso()}
    uid = _clean_text(agent_memory_uid, 80)
    if uid:
        clauses.append("agent_memory_uid=:agent_memory_uid")
        params["agent_memory_uid"] = uid
    else:
        src_type = _clean_text(source_type, 64)
        src_id = _clean_text(source_id, 255)
        if not src_type or not src_id:
            return False
        clauses.append("source_type=:source_type")
        clauses.append("source_id=:source_id")
        params["source_type"] = src_type
        params["source_id"] = src_id
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"""
                    UPDATE agent_memories
                    SET last_used_at=:last_used_at,
                        use_count=use_count + 1,
                        updated_at=:last_used_at
                    WHERE {' AND '.join(clauses)}
                    """
                ),
                params,
            )
        return int(getattr(result, "rowcount", 0) or 0) > 0
    except Exception as exc:
        print(f"[agent-memory] record use failed err={exc}")
        return False


def update_agent_memories_status(
    engine,
    *,
    user_id: str = "",
    namespace: str = "",
    memory_type: str = "",
    memory_key: str = "",
    source_type: str = "",
    source_id: str = "",
    status: str,
) -> int:
    if not ensure_agent_memory_tables(engine):
        return 0
    clauses = ["status=:active"]
    params: Dict[str, Any] = {
        "active": MEMORY_STATUS_ACTIVE,
        "status": normalize_memory_status(status),
        "updated_at": _now_iso(),
    }
    if user_id:
        clauses.append("user_id=:user_id")
        params["user_id"] = _clean_text(user_id, 255)
    if namespace:
        clauses.append("namespace=:namespace")
        params["namespace"] = _clean_text(namespace, 255)
    if memory_type:
        clauses.append("memory_type=:memory_type")
        params["memory_type"] = normalize_memory_type(memory_type)
    if memory_key:
        clauses.append("memory_key=:memory_key")
        params["memory_key"] = _clean_text(memory_key, 255)
    if source_type:
        clauses.append("source_type=:source_type")
        params["source_type"] = _clean_text(source_type, 64)
    if source_id:
        clauses.append("source_id=:source_id")
        params["source_id"] = _clean_text(source_id, 255)
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    f"""
                    UPDATE agent_memories
                    SET status=:status, updated_at=:updated_at
                    WHERE {' AND '.join(clauses)}
                    """
                ),
                params,
            )
        return int(getattr(result, "rowcount", 0) or 0)
    except Exception as exc:
        print(f"[agent-memory] status update failed err={exc}")
        return 0


def register_profile_memory(
    engine,
    *,
    user_id: str,
    memory_key: str,
    memory_value: str,
    confidence: float = 0.9,
    source_text: str = "",
    status: str = MEMORY_STATUS_ACTIVE,
) -> Dict[str, Any]:
    uid = _clean_text(user_id, 255)
    key = _clean_text(memory_key, 255)
    value = _clean_text(memory_value, 1000)
    return upsert_agent_memory(
        engine,
        user_id=uid,
        namespace=build_memory_namespace(uid, kind="profile"),
        memory_type=MEMORY_TYPE_SEMANTIC,
        domain="profile",
        memory_key=key,
        value={
            "memory_key": key,
            "memory_value": value,
            "source_text": _clean_text(source_text, 1000),
        },
        text_summary=value,
        status=status,
        confidence=confidence,
        source_type=SOURCE_PROFILE_MEMORY,
        source_id=f"{uid}:{key}:{_stable_hash(value)[:16]}",
        supersede_scope=True,
    )


def register_conversation_memory(
    engine,
    *,
    user_id: str,
    topic: str,
    source: str,
    source_id: str,
    user_input: str,
    ai_response: str,
    timestamp: str,
    vector_ref: Any = None,
) -> Dict[str, Any]:
    domain = normalize_domain(topic)
    return upsert_agent_memory(
        engine,
        user_id=user_id,
        namespace=build_memory_namespace(user_id, kind="conversation", domain=domain),
        memory_type=MEMORY_TYPE_CONVERSATION,
        domain=domain,
        memory_key=_clean_text(source_id, 255),
        value={
            "user_input": _clean_text(user_input, 2000),
            "ai_response": _clean_text(ai_response, 4000),
            "timestamp": _clean_text(timestamp, 40),
            "source": _clean_text(source, 64),
        },
        text_summary=f"{_clean_text(user_input, 500)}\n{_clean_text(ai_response, 1000)}",
        status=MEMORY_STATUS_ACTIVE,
        confidence=0.75,
        source_type=SOURCE_CHROMA_CHAT_HISTORY,
        source_id=source_id,
        vector_ref=vector_ref,
    )


def register_feedback_event_memory(
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
) -> Dict[str, Any]:
    domain = normalize_domain(intent_domain)
    source_id = f"{_clean_text(answer_id, 80)}:{_clean_text(user_id, 255)}"
    return upsert_agent_memory(
        engine,
        user_id=user_id,
        namespace=build_memory_namespace(user_id, kind="feedback", domain=domain),
        memory_type=MEMORY_TYPE_EPISODIC,
        domain=domain,
        memory_key=source_id,
        value={
            "answer_id": _clean_text(answer_id, 80),
            "trace_id": _clean_text(trace_id, 80),
            "prompt_text": _clean_text(prompt_text, 2000),
            "response_text": _clean_text(response_text, 4000),
            "feedback_type": _clean_text(feedback_type, 16),
            "reason_code": _clean_text(reason_code, 64),
            "feedback_text": _clean_text(feedback_text, 1000),
        },
        text_summary=f"{_clean_text(feedback_type, 16)} {_clean_text(reason_code, 64)} {_clean_text(prompt_text, 500)}",
        status=MEMORY_STATUS_ACTIVE,
        confidence=0.8,
        source_type=SOURCE_CHAT_FEEDBACK_EVENT,
        source_id=source_id,
    )


def register_feedback_sample_memory(
    engine,
    *,
    sample_key: str,
    prompt_text: str,
    reason_code: str,
    intent_domain: str,
    occurrence_count: int = 1,
    latest_feedback_text: str = "",
    sample_status: str = "new",
    optimization_type: str = "",
    review_notes: str = "",
) -> Dict[str, Any]:
    domain = normalize_domain(intent_domain)
    status = MEMORY_STATUS_REJECTED if _clean_text(sample_status, 32).lower() == "rejected" else MEMORY_STATUS_ACTIVE
    memory_type = MEMORY_TYPE_PROCEDURAL if optimization_type else MEMORY_TYPE_EPISODIC
    return upsert_agent_memory(
        engine,
        user_id="",
        namespace=build_memory_namespace("", kind="feedback", domain=domain),
        memory_type=memory_type,
        domain=domain,
        memory_key=_clean_text(sample_key, 255),
        value={
            "sample_key": _clean_text(sample_key, 255),
            "prompt_text": _clean_text(prompt_text, 2000),
            "reason_code": _clean_text(reason_code, 64),
            "occurrence_count": max(1, int(occurrence_count or 1)),
            "latest_feedback_text": _clean_text(latest_feedback_text, 1000),
            "sample_status": _clean_text(sample_status, 32),
            "optimization_type": _clean_text(optimization_type, 32),
            "review_notes": _clean_text(review_notes, 2000),
        },
        text_summary=f"{_clean_text(reason_code, 64)} {_clean_text(prompt_text, 500)}",
        status=status,
        confidence=0.85,
        source_type=SOURCE_CHAT_FEEDBACK_SAMPLE,
        source_id=_clean_text(sample_key, 255),
    )


def _safe_fetch_rows(engine, sql: str) -> List[Dict[str, Any]]:
    try:
        with engine.begin() as conn:
            rows = conn.execute(text(sql)).mappings().fetchall()
        return [dict(row) for row in rows]
    except Exception:
        return []


def backfill_agent_memories(
    engine,
    *,
    include_chroma: bool = True,
    vector_store: Any = None,
    max_chroma_records: int = 5000,
) -> Dict[str, Any]:
    if not ensure_agent_memory_tables(engine):
        return {"ok": False, "code": "schema_unavailable", "counts": {}}

    counts = {"profile": 0, "feedback_events": 0, "feedback_samples": 0, "conversation": 0}

    profile_rows = _safe_fetch_rows(
        engine,
        """
        SELECT user_id, memory_key, memory_value, confidence, source_text, status
        FROM user_profile_memory
        """,
    )
    for row in profile_rows:
        status = normalize_memory_status(row.get("status") or MEMORY_STATUS_ACTIVE)
        result = register_profile_memory(
            engine,
            user_id=str(row.get("user_id") or ""),
            memory_key=str(row.get("memory_key") or ""),
            memory_value=str(row.get("memory_value") or ""),
            confidence=float(row.get("confidence") or 0.8),
            source_text=str(row.get("source_text") or ""),
            status=status,
        )
        if result.get("ok"):
            counts["profile"] += 1

    feedback_event_rows = _safe_fetch_rows(
        engine,
        """
        SELECT answer_id, trace_id, user_id, prompt_text, response_text, intent_domain,
               feedback_type, reason_code, feedback_text
        FROM chat_feedback_events
        """,
    )
    for row in feedback_event_rows:
        result = register_feedback_event_memory(
            engine,
            answer_id=str(row.get("answer_id") or ""),
            trace_id=str(row.get("trace_id") or ""),
            user_id=str(row.get("user_id") or ""),
            prompt_text=str(row.get("prompt_text") or ""),
            response_text=str(row.get("response_text") or ""),
            intent_domain=str(row.get("intent_domain") or "general"),
            feedback_type=str(row.get("feedback_type") or ""),
            reason_code=str(row.get("reason_code") or ""),
            feedback_text=str(row.get("feedback_text") or ""),
        )
        if result.get("ok"):
            counts["feedback_events"] += 1

    feedback_sample_rows = _safe_fetch_rows(
        engine,
        """
        SELECT sample_key, prompt_text, reason_code, intent_domain, latest_feedback_text,
               occurrence_count, sample_status, optimization_type, review_notes
        FROM chat_feedback_samples
        """,
    )
    for row in feedback_sample_rows:
        result = register_feedback_sample_memory(
            engine,
            sample_key=str(row.get("sample_key") or ""),
            prompt_text=str(row.get("prompt_text") or ""),
            reason_code=str(row.get("reason_code") or ""),
            intent_domain=str(row.get("intent_domain") or "general"),
            occurrence_count=int(row.get("occurrence_count") or 1),
            latest_feedback_text=str(row.get("latest_feedback_text") or ""),
            sample_status=str(row.get("sample_status") or "new"),
            optimization_type=str(row.get("optimization_type") or ""),
            review_notes=str(row.get("review_notes") or ""),
        )
        if result.get("ok"):
            counts["feedback_samples"] += 1

    if include_chroma:
        store = vector_store
        if store is None:
            try:
                from memory_utils import get_vector_store

                store = get_vector_store()
            except Exception as exc:
                print(f"[agent-memory] chroma backfill skipped err={exc}")
                store = None
        if store is not None:
            try:
                collection = getattr(store, "_collection", None)
                getter = collection.get if collection is not None and hasattr(collection, "get") else store.get
                data = getter(limit=max(1, int(max_chroma_records or 5000)))
                docs = data.get("documents") or []
                metas = data.get("metadatas") or []
                ids = data.get("ids") or []
                for i in range(max(len(docs), len(metas))):
                    doc = str(docs[i] if i < len(docs) else "")
                    meta = metas[i] if i < len(metas) and isinstance(metas[i], dict) else {}
                    user_id = str(meta.get("user_id") or "").strip()
                    if not user_id:
                        continue
                    topic = normalize_domain(meta.get("topic") or "general")
                    timestamp = str(meta.get("timestamp") or "")
                    source = str(meta.get("source") or "")
                    chroma_id = str(ids[i] if i < len(ids) else "")
                    source_id = str(meta.get("memory_source_id") or chroma_id or _stable_hash(doc)[:64])
                    result = register_conversation_memory(
                        engine,
                        user_id=user_id,
                        topic=topic,
                        source=source,
                        source_id=source_id,
                        user_input=doc[:1000],
                        ai_response=doc[1000:5000],
                        timestamp=timestamp,
                        vector_ref={
                            "collection": "user_chat_history",
                            "chroma_id": chroma_id,
                            "source_id": source_id,
                        },
                    )
                    if result.get("ok"):
                        counts["conversation"] += 1
            except Exception as exc:
                print(f"[agent-memory] chroma backfill failed err={exc}")

    return {"ok": True, "code": "ok", "counts": counts}

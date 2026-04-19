from __future__ import annotations

import hashlib
import json
import secrets
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from data_engine import engine
import payment_service as pay_svc


DEFAULT_INVITE_REWARD_POINTS = 300
INVITE_BIZ_ID_PREFIX = "invite_reward:"


def _normalize_invite_code(code: str) -> str:
    raw = "".join(ch for ch in str(code or "").strip().upper() if ch.isalnum())
    return raw[:64]


def _hash_value(value: Optional[str]) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _generate_invite_code() -> str:
    # 例：AIB1F3C9A0B，长度固定且可读
    return f"AIB{secrets.token_hex(4).upper()}"


def _ensure_invite_tables(conn) -> None:
    if conn.dialect.name == "sqlite":
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_invite_codes (
                    user_id TEXT NOT NULL PRIMARY KEY,
                    invite_code TEXT NOT NULL UNIQUE,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_invite_relations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    inviter_user_id TEXT NOT NULL,
                    invitee_user_id TEXT NOT NULL UNIQUE,
                    invite_code TEXT NOT NULL,
                    reward_points INTEGER NOT NULL DEFAULT 300,
                    status TEXT NOT NULL DEFAULT 'pending_reward',
                    register_ip_hash TEXT,
                    device_hash TEXT,
                    reject_reason TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    rewarded_at DATETIME
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_inv_rel_inviter_status ON user_invite_relations(inviter_user_id, status)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_inv_rel_created ON user_invite_relations(created_at)"
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_inv_rel_ip_device ON user_invite_relations(register_ip_hash, device_hash)"
            )
        )
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS invite_landing_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    invite_code TEXT NOT NULL,
                    inviter_user_id TEXT,
                    event_type TEXT NOT NULL,
                    session_id TEXT,
                    invitee_user_id TEXT,
                    register_ip_hash TEXT,
                    device_hash TEXT,
                    extra_json TEXT,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_inv_evt_code_type_created "
                "ON invite_landing_events(invite_code, event_type, created_at)"
            )
        )
        return

    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS user_invite_codes (
                user_id VARCHAR(100) NOT NULL,
                invite_code VARCHAR(64) NOT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id),
                UNIQUE KEY uq_user_invite_code (invite_code),
                KEY idx_invite_code_created (created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS user_invite_relations (
                id BIGINT NOT NULL AUTO_INCREMENT,
                inviter_user_id VARCHAR(100) NOT NULL,
                invitee_user_id VARCHAR(100) NOT NULL,
                invite_code VARCHAR(64) NOT NULL,
                reward_points INT NOT NULL DEFAULT 300,
                status VARCHAR(32) NOT NULL DEFAULT 'pending_reward',
                register_ip_hash VARCHAR(128) NULL,
                device_hash VARCHAR(128) NULL,
                reject_reason VARCHAR(100) NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                rewarded_at DATETIME NULL,
                PRIMARY KEY (id),
                UNIQUE KEY uq_invitee_user (invitee_user_id),
                KEY idx_inviter_status (inviter_user_id, status),
                KEY idx_created_at (created_at),
                KEY idx_ip_device (register_ip_hash, device_hash)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
    )
    conn.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS invite_landing_events (
                id BIGINT NOT NULL AUTO_INCREMENT,
                invite_code VARCHAR(64) NOT NULL,
                inviter_user_id VARCHAR(100) NULL,
                event_type VARCHAR(32) NOT NULL,
                session_id VARCHAR(120) NULL,
                invitee_user_id VARCHAR(100) NULL,
                register_ip_hash VARCHAR(128) NULL,
                device_hash VARCHAR(128) NULL,
                extra_json TEXT NULL,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (id),
                KEY idx_code_type_created (invite_code, event_type, created_at),
                KEY idx_session_created (session_id, created_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
    )


def get_or_create_invite_code(user_id: str) -> str:
    user = str(user_id or "").strip()
    if not user:
        return ""

    with engine.begin() as conn:
        _ensure_invite_tables(conn)
        row = conn.execute(
            text("SELECT invite_code FROM user_invite_codes WHERE user_id = :uid LIMIT 1"),
            {"uid": user},
        ).fetchone()
        if row and row[0]:
            return str(row[0])

        for _ in range(12):
            code = _generate_invite_code()
            try:
                conn.execute(
                    text(
                        """
                        INSERT INTO user_invite_codes (user_id, invite_code)
                        VALUES (:uid, :code)
                        """
                    ),
                    {"uid": user, "code": code},
                )
                return code
            except IntegrityError:
                row_retry = conn.execute(
                    text("SELECT invite_code FROM user_invite_codes WHERE user_id = :uid LIMIT 1"),
                    {"uid": user},
                ).fetchone()
                if row_retry and row_retry[0]:
                    return str(row_retry[0])
                continue

    return ""


def get_invite_context(invite_code: str) -> Dict[str, Any]:
    code = _normalize_invite_code(invite_code)
    if not code:
        return {
            "invite_code": "",
            "is_valid": False,
            "inviter_user_id": "",
            "reward_points": DEFAULT_INVITE_REWARD_POINTS,
        }

    try:
        with engine.begin() as conn:
            _ensure_invite_tables(conn)
            row = conn.execute(
                text("SELECT user_id FROM user_invite_codes WHERE invite_code = :code LIMIT 1"),
                {"code": code},
            ).fetchone()
        return {
            "invite_code": code,
            "is_valid": bool(row and row[0]),
            "inviter_user_id": str((row[0] if row else "") or ""),
            "reward_points": DEFAULT_INVITE_REWARD_POINTS,
        }
    except Exception as e:
        print(f"[invite][context] code={code} err={e}")
        return {
            "invite_code": code,
            "is_valid": False,
            "inviter_user_id": "",
            "reward_points": DEFAULT_INVITE_REWARD_POINTS,
        }


def get_invite_stats(user_id: str) -> Dict[str, int]:
    user = str(user_id or "").strip()
    if not user:
        return {"invited_count": 0, "rewarded_points": 0, "pending_count": 0}

    try:
        with engine.begin() as conn:
            _ensure_invite_tables(conn)
            rewarded_cnt = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM user_invite_relations
                    WHERE inviter_user_id = :uid AND status = 'rewarded'
                    """
                ),
                {"uid": user},
            ).scalar()
            rewarded_sum = conn.execute(
                text(
                    """
                    SELECT COALESCE(SUM(reward_points), 0)
                    FROM user_invite_relations
                    WHERE inviter_user_id = :uid AND status = 'rewarded'
                    """
                ),
                {"uid": user},
            ).scalar()
            pending_cnt = conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM user_invite_relations
                    WHERE inviter_user_id = :uid AND status = 'pending_reward'
                    """
                ),
                {"uid": user},
            ).scalar()
        return {
            "invited_count": int(rewarded_cnt or 0),
            "rewarded_points": int(rewarded_sum or 0),
            "pending_count": int(pending_cnt or 0),
        }
    except Exception as e:
        print(f"[invite][stats] user={user} err={e}")
        return {"invited_count": 0, "rewarded_points": 0, "pending_count": 0}


def track_invite_event(
    invite_code: str,
    event_type: str,
    *,
    session_id: str = "",
    invitee_user_id: str = "",
    register_ip: str | None = None,
    device_fingerprint: str | None = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> bool:
    code = _normalize_invite_code(invite_code)
    event = str(event_type or "").strip().lower()
    if not code or not event:
        return False

    context = get_invite_context(code)
    extra_json = ""
    if metadata:
        try:
            extra_json = json.dumps(metadata, ensure_ascii=False, sort_keys=True)
        except Exception:
            extra_json = ""

    try:
        with engine.begin() as conn:
            _ensure_invite_tables(conn)
            conn.execute(
                text(
                    """
                    INSERT INTO invite_landing_events (
                        invite_code, inviter_user_id, event_type, session_id,
                        invitee_user_id, register_ip_hash, device_hash, extra_json
                    ) VALUES (
                        :invite_code, :inviter_user_id, :event_type, :session_id,
                        :invitee_user_id, :register_ip_hash, :device_hash, :extra_json
                    )
                    """
                ),
                {
                    "invite_code": code,
                    "inviter_user_id": str(context.get("inviter_user_id") or "") or None,
                    "event_type": event,
                    "session_id": str(session_id or "").strip() or None,
                    "invitee_user_id": str(invitee_user_id or "").strip() or None,
                    "register_ip_hash": _hash_value(register_ip) or None,
                    "device_hash": _hash_value(device_fingerprint) or None,
                    "extra_json": extra_json or None,
                },
            )
        return True
    except Exception as e:
        print(f"[invite][event] code={code} event={event} err={e}")
        return False


def _count_recent_valid_by_ip_or_device(conn, ip_hash: str, device_hash: str, hours: int = 24) -> int:
    if not ip_hash and not device_hash:
        return 0
    cutoff = datetime.now() - timedelta(hours=max(1, int(hours or 24)))
    row = conn.execute(
        text(
            """
            SELECT COUNT(*)
            FROM user_invite_relations
            WHERE status IN ('pending_reward', 'rewarded')
              AND created_at >= :cutoff
              AND (
                    (:iph <> '' AND register_ip_hash = :iph)
                 OR (:dh <> '' AND device_hash = :dh)
              )
            """
        ),
        {"cutoff": cutoff, "iph": ip_hash or "", "dh": device_hash or ""},
    ).scalar()
    return int(row or 0)


def apply_invite_on_register(
    invitee_user_id: str,
    invite_code: str | None,
    register_ip: str | None,
    device_fingerprint: str | None,
) -> Dict[str, Any]:
    invitee = str(invitee_user_id or "").strip()
    code = _normalize_invite_code(str(invite_code or ""))
    ip_hash = _hash_value(register_ip)
    device_hash = _hash_value(device_fingerprint)

    if not invitee:
        return {"applied": False, "rewarded": False, "reason": "invalid_invitee"}
    if not code:
        return {"applied": False, "rewarded": False, "reason": "missing_invite_code"}

    inviter = ""
    reward_points = DEFAULT_INVITE_REWARD_POINTS

    with engine.begin() as conn:
        _ensure_invite_tables(conn)

        rel_row = conn.execute(
            text(
                """
                SELECT inviter_user_id, status, reward_points
                FROM user_invite_relations
                WHERE invitee_user_id = :invitee
                LIMIT 1
                """
            ),
            {"invitee": invitee},
        ).fetchone()
        if rel_row:
            status = str(rel_row[1] or "")
            if status == "rewarded":
                return {
                    "applied": True,
                    "rewarded": True,
                    "reason": "already_rewarded",
                    "inviter_user_id": str(rel_row[0] or ""),
                }
            return {
                "applied": False,
                "rewarded": False,
                "reason": "already_bound",
                "status": status,
                "inviter_user_id": str(rel_row[0] or ""),
            }

        inviter_row = conn.execute(
            text("SELECT user_id FROM user_invite_codes WHERE invite_code = :code LIMIT 1"),
            {"code": code},
        ).fetchone()
        if not inviter_row:
            return {"applied": False, "rewarded": False, "reason": "invalid_invite_code"}

        inviter = str(inviter_row[0] or "").strip()
        if not inviter:
            return {"applied": False, "rewarded": False, "reason": "invalid_inviter"}

        if inviter == invitee:
            conn.execute(
                text(
                    """
                    INSERT INTO user_invite_relations (
                        inviter_user_id, invitee_user_id, invite_code, reward_points,
                        status, register_ip_hash, device_hash, reject_reason
                    ) VALUES (
                        :inviter, :invitee, :code, :points,
                        'rejected', :ip_hash, :device_hash, 'self_invite'
                    )
                    """
                ),
                {
                    "inviter": inviter,
                    "invitee": invitee,
                    "code": code,
                    "points": reward_points,
                    "ip_hash": ip_hash or None,
                    "device_hash": device_hash or None,
                },
            )
            return {"applied": False, "rewarded": False, "reason": "self_invite"}

        if _count_recent_valid_by_ip_or_device(conn, ip_hash, device_hash, hours=24) > 0:
            conn.execute(
                text(
                    """
                    INSERT INTO user_invite_relations (
                        inviter_user_id, invitee_user_id, invite_code, reward_points,
                        status, register_ip_hash, device_hash, reject_reason
                    ) VALUES (
                        :inviter, :invitee, :code, :points,
                        'rejected', :ip_hash, :device_hash, 'ip_or_device_rate_limited'
                    )
                    """
                ),
                {
                    "inviter": inviter,
                    "invitee": invitee,
                    "code": code,
                    "points": reward_points,
                    "ip_hash": ip_hash or None,
                    "device_hash": device_hash or None,
                },
            )
            return {"applied": False, "rewarded": False, "reason": "ip_or_device_rate_limited"}

        conn.execute(
            text(
                """
                INSERT INTO user_invite_relations (
                    inviter_user_id, invitee_user_id, invite_code, reward_points,
                    status, register_ip_hash, device_hash
                ) VALUES (
                    :inviter, :invitee, :code, :points,
                    'pending_reward', :ip_hash, :device_hash
                )
                """
            ),
            {
                "inviter": inviter,
                "invitee": invitee,
                "code": code,
                "points": reward_points,
                "ip_hash": ip_hash or None,
                "device_hash": device_hash or None,
            },
        )

    biz_id = f"{INVITE_BIZ_ID_PREFIX}{invitee}"
    ok, reason = pay_svc.credit_points(
        inviter,
        reward_points,
        ref_id=invitee,
        description=f"邀请用户 {invitee} 注册奖励",
        tx_type="admin_grant",
        biz_id=biz_id,
    )
    rewarded = bool(ok or reason == "already_processed")

    with engine.begin() as conn:
        _ensure_invite_tables(conn)
        if rewarded:
            conn.execute(
                text(
                    """
                    UPDATE user_invite_relations
                    SET status = 'rewarded',
                        rewarded_at = :rewarded_at,
                        reject_reason = NULL
                    WHERE invitee_user_id = :invitee
                    """
                ),
                {"invitee": invitee, "rewarded_at": datetime.now()},
            )
        else:
            conn.execute(
                text(
                    """
                    UPDATE user_invite_relations
                    SET status = 'reward_failed',
                        reject_reason = :reason
                    WHERE invitee_user_id = :invitee
                    """
                ),
                {"invitee": invitee, "reason": str(reason or "reward_failed")},
            )

    return {
        "applied": True,
        "rewarded": rewarded,
        "reason": "ok" if rewarded else str(reason or "reward_failed"),
        "inviter_user_id": inviter,
        "reward_points": reward_points,
    }

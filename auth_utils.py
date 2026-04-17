"""
Authentication utilities.

This module now supports:
- username + password as primary login flow
- two-step registration with phone binding (SMS verification)
- legacy email flows for registration/reset compatibility
"""

from __future__ import annotations

import random
import secrets
import uuid
from datetime import datetime, timedelta

import bcrypt
import streamlit as st
from sqlalchemy import text

from data_engine import engine
import subscription_service as sub_svc
from email_utils import (
    send_register_code,
    verify_register_code,
    send_reset_password_code,
    verify_reset_password_code,
    send_login_code,
    verify_login_code,
    send_bind_email_code,
    verify_bind_email_code,
)
from sms_utils import (
    normalize_cn_phone,
    send_register_sms_code,
    verify_register_sms_code,
    send_login_sms_code,
    verify_login_sms_code,
)


def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode(), str(hashed).encode())
    except Exception:
        return False


def generate_token() -> str:
    return str(uuid.uuid4())


def ensure_sessions_table() -> None:
    """Ensure session table exists for multi-device login."""
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS user_sessions (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        username VARCHAR(100) NOT NULL,
                        session_token VARCHAR(100) NOT NULL,
                        token_expire DATETIME NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE KEY uq_token (session_token),
                        INDEX idx_username (username)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
            )
    except Exception as e:
        print(f"ensure_sessions_table failed: {e}")


def ensure_users_phone_columns() -> None:
    """Ensure users table has phone fields and unique index."""
    try:
        with engine.begin() as conn:
            has_phone = conn.execute(
                text(
                    """
                    SELECT COUNT(1)
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'users'
                      AND COLUMN_NAME = 'phone'
                    """
                )
            ).scalar()
            if not has_phone:
                conn.execute(text("ALTER TABLE users ADD COLUMN phone VARCHAR(20) NULL"))

            has_phone_verified = conn.execute(
                text(
                    """
                    SELECT COUNT(1)
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'users'
                      AND COLUMN_NAME = 'phone_verified'
                    """
                )
            ).scalar()
            if not has_phone_verified:
                conn.execute(
                    text("ALTER TABLE users ADD COLUMN phone_verified TINYINT(1) NOT NULL DEFAULT 0")
                )

            has_phone_unique = conn.execute(
                text(
                    """
                    SELECT COUNT(1)
                    FROM information_schema.STATISTICS
                    WHERE TABLE_SCHEMA = DATABASE()
                      AND TABLE_NAME = 'users'
                      AND NON_UNIQUE = 0
                      AND COLUMN_NAME = 'phone'
                    """
                )
            ).scalar()
            if not has_phone_unique:
                conn.execute(text("ALTER TABLE users ADD UNIQUE KEY uq_users_phone (phone)"))
    except Exception as e:
        print(f"ensure_users_phone_columns failed: {e}")


def _ensure_user_profile(conn, username: str) -> None:
    profile_exists = conn.execute(
        text("SELECT 1 FROM user_profile WHERE user_id = :uid LIMIT 1"),
        {"uid": username},
    ).fetchone()
    if not profile_exists:
        conn.execute(
            text(
                """
                INSERT INTO user_profile (user_id, risk_preference, focus_assets, current_mood)
                VALUES (:uid, '未知', '暂无', '平静')
                """
            ),
            {"uid": username},
        )


def _create_user_session(conn, username: str) -> str:
    token = generate_token()
    now = datetime.now()
    expire_time = now + timedelta(days=30)
    conn.execute(
        text(
            """
            INSERT INTO user_sessions (username, session_token, token_expire, created_at)
            VALUES (:u, :t, :e, :now)
            """
        ),
        {"u": username, "t": token, "e": expire_time, "now": now},
    )
    conn.execute(text("UPDATE users SET last_login = :now WHERE username = :u"), {"now": now, "u": username})
    conn.execute(
        text("DELETE FROM user_sessions WHERE username = :u AND token_expire <= :now"),
        {"u": username, "now": now},
    )
    return token


def create_user_session(username: str):
    if not username:
        return False, "用户名不能为空", None
    try:
        with engine.begin() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u AND is_active = 1"),
                {"u": username},
            ).fetchone()
            if not exists:
                return False, "账号不存在或已禁用", None
            token = _create_user_session(conn, username)
            return True, "ok", token
    except Exception as e:
        print(f"create_user_session failed: {e}")
        return False, "创建会话失败", None


def send_register_phone_code(phone: str, client_ip: str = None):
    return send_register_sms_code(phone, client_ip=client_ip)


def verify_register_phone_code(phone: str, code: str):
    ok, normalized_phone, phone_msg = normalize_cn_phone(phone)
    if not ok:
        return False, phone_msg, ""
    verify_ok, verify_msg = verify_register_sms_code(normalized_phone, code)
    return verify_ok, verify_msg, normalized_phone


def send_login_phone_code(phone: str, client_ip: str = None):
    return send_login_sms_code(phone, client_ip=client_ip)


def validate_register_step1(username: str, password: str, password_confirm: str):
    """Step-1 validation for account registration (username + password)."""
    username = str(username or "").strip()
    password = str(password or "")
    password_confirm = str(password_confirm or "")

    if not username:
        return False, "账号是必填项", ""
    if len(username) < 3:
        return False, "账号至少3个字符", ""
    if not password or len(password) < 6:
        return False, "密码至少6位", ""
    if password != password_confirm:
        return False, "两次密码不一致", ""

    try:
        with engine.begin() as conn:
            exists_username = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u"),
                {"u": username},
            ).fetchone()
            if exists_username:
                return False, "该账号已存在，请更换", ""
    except Exception as e:
        print(f"validate_register_step1 failed: {e}")
        return False, "账号校验失败，请稍后重试", ""

    return True, "ok", username


def register_with_email(email: str, password: str, email_code: str, username: str = None):
    code_valid, code_msg = verify_register_code(email, email_code)
    if not code_valid:
        return False, code_msg

    if len(str(password or "")) < 6:
        return False, "密码长度不能少于6位"

    username = str(username or "").strip()
    if not username:
        username = str(email).split("@")[0] or "user"
    if len(username) < 3:
        username = f"user_{username}"[:30]

    hashed = hash_password(password)
    try:
        registered_username = None
        with engine.begin() as conn:
            check_email = conn.execute(text("SELECT 1 FROM users WHERE email = :e"), {"e": email}).fetchone()
            if check_email:
                return False, "该邮箱已注册，请直接登录或找回密码"

            check_user = conn.execute(text("SELECT 1 FROM users WHERE username = :u"), {"u": username}).fetchone()
            if check_user:
                username = f"{username}_{random.randint(1000, 9999)}"

            conn.execute(
                text(
                    """
                    INSERT INTO users (username, email, password_hash, level, experience, capital,
                                       is_active, email_verified, phone_verified, created_at)
                    VALUES (:u, :e, :h, 1, 0, 1000000, 1, 1, 0, :now)
                    """
                ),
                {"u": username, "e": email, "h": hashed, "now": datetime.now()},
            )
            _ensure_user_profile(conn, username)
            registered_username = username

        trial_ok, trial_msg = sub_svc.grant_new_user_trial_all_reports(registered_username)
        if not trial_ok:
            print(
                f"[auth][trial_grant] register_with_email user={registered_username} "
                f"status=failed reason={trial_msg}"
            )
        return True, f"注册成功，您的用户名是：{registered_username}"
    except Exception as e:
        print(f"register_with_email failed: {e}")
        return False, "注册失败，请稍后重试"


def register_with_phone(phone: str, sms_code: str, username: str):
    username = str(username or "").strip()
    if len(username) < 3:
        return False, "昵称至少3个字符"

    ok, normalized_phone, phone_msg = normalize_cn_phone(phone)
    if not ok:
        return False, phone_msg

    code_ok, code_msg = verify_register_sms_code(normalized_phone, sms_code)
    if not code_ok:
        return False, code_msg

    internal_hash = hash_password(secrets.token_urlsafe(24))
    try:
        registered_username = None
        with engine.begin() as conn:
            exists_phone = conn.execute(text("SELECT 1 FROM users WHERE phone = :p"), {"p": normalized_phone}).fetchone()
            if exists_phone:
                return False, "该手机号已注册，请更换手机号"

            exists_username = conn.execute(text("SELECT 1 FROM users WHERE username = :u"), {"u": username}).fetchone()
            if exists_username:
                return False, "该昵称已被占用，请更换"

            conn.execute(
                text(
                    """
                    INSERT INTO users (
                        username, phone, phone_verified, password_hash,
                        level, experience, capital, is_active, email_verified, created_at
                    ) VALUES (
                        :u, :p, 1, :h,
                        1, 0, 1000000, 1, 0, :now
                    )
                    """
                ),
                {"u": username, "p": normalized_phone, "h": internal_hash, "now": datetime.now()},
            )
            _ensure_user_profile(conn, username)
            registered_username = username

        trial_ok, trial_msg = sub_svc.grant_new_user_trial_all_reports(registered_username)
        if not trial_ok:
            print(
                f"[auth][trial_grant] register_with_phone user={registered_username} "
                f"status=failed reason={trial_msg}"
            )
        return True, f"注册成功，欢迎你：{registered_username}"
    except Exception as e:
        print(f"register_with_phone failed: {e}")
        return False, "注册失败，请稍后重试"


def register_with_phone_password(
    phone: str,
    password: str,
    username: str,
    invite_code: str | None = None,
    register_ip: str | None = None,
    device_fingerprint: str | None = None,
):
    """Register by phone + password + username."""
    username = str(username or "").strip()
    password = str(password or "")
    if len(username) < 3:
        return False, "账号至少3个字符"
    if len(password) < 6:
        return False, "密码长度不能少于6位"

    ok, normalized_phone, phone_msg = normalize_cn_phone(phone)
    if not ok:
        return False, phone_msg

    hashed = hash_password(password)
    try:
        registered_username = None
        with engine.begin() as conn:
            exists_phone = conn.execute(
                text("SELECT 1 FROM users WHERE phone = :p"),
                {"p": normalized_phone},
            ).fetchone()
            if exists_phone:
                return False, "该手机号已注册，请更换手机号"

            exists_username = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u"),
                {"u": username},
            ).fetchone()
            if exists_username:
                return False, "该账号已存在，请更换"

            conn.execute(
                text(
                    """
                    INSERT INTO users (
                        username, phone, phone_verified, password_hash,
                        level, experience, capital, is_active, email_verified, created_at
                    ) VALUES (
                        :u, :p, 1, :h,
                        1, 0, 1000000, 1, 0, :now
                    )
                    """
                ),
                {"u": username, "p": normalized_phone, "h": hashed, "now": datetime.now()},
            )
            _ensure_user_profile(conn, username)
            registered_username = username

        trial_ok, trial_msg = sub_svc.grant_new_user_trial_all_reports(registered_username)
        if not trial_ok:
            print(
                f"[auth][trial_grant] register_with_phone_password user={registered_username} "
                f"status=failed reason={trial_msg}"
            )

        # 邀请奖励为附加流程：失败不影响注册主链路。
        try:
            from invite_service import apply_invite_on_register

            invite_result = apply_invite_on_register(
                invitee_user_id=registered_username,
                invite_code=invite_code,
                register_ip=register_ip,
                device_fingerprint=device_fingerprint,
            )
            if invite_result.get("applied") and invite_result.get("rewarded"):
                print(
                    f"[auth][invite] register user={registered_username} inviter={invite_result.get('inviter_user_id')} "
                    f"reward={invite_result.get('reward_points')}"
                )
            elif invite_result.get("reason") not in {"missing_invite_code", "invalid_invite_code"}:
                print(f"[auth][invite] register user={registered_username} result={invite_result}")
        except Exception as invite_err:
            print(f"[auth][invite] register user={registered_username} err={invite_err}")

        return True, f"注册成功，欢迎你：{registered_username}"
    except Exception as e:
        print(f"register_with_phone_password failed: {e}")
        return False, "注册失败，请稍后重试"


def register_with_username_phone(
    username: str,
    password: str,
    phone: str,
    invite_code: str | None = None,
    register_ip: str | None = None,
    device_fingerprint: str | None = None,
):
    """Final step registration: username + password + verified phone."""
    return register_with_phone_password(
        phone=phone,
        password=password,
        username=username,
        invite_code=invite_code,
        register_ip=register_ip,
        device_fingerprint=device_fingerprint,
    )


def register_user(username, password):
    """Legacy register: username + password."""
    username = str(username or "").strip()
    password = str(password or "")

    if not username or not password:
        return False, "用户名和密码不能为空"
    if len(username) < 3:
        return False, "用户名长度不能少于3位"
    if len(password) < 6:
        return False, "密码长度不能少于6位"

    hashed = hash_password(password)
    try:
        registered_username = None
        with engine.begin() as conn:
            check = conn.execute(text("SELECT 1 FROM users WHERE username = :u"), {"u": username}).fetchone()
            if check:
                return False, "用户名已存在"

            conn.execute(
                text(
                    """
                    INSERT INTO users (username, password_hash, level, experience, capital, is_active,
                                       phone_verified, created_at)
                    VALUES (:u, :p, 1, 0, 1000000, 1, 0, :now)
                    """
                ),
                {"u": username, "p": hashed, "now": datetime.now()},
            )
            _ensure_user_profile(conn, username)
            registered_username = username

        trial_ok, trial_msg = sub_svc.grant_new_user_trial_all_reports(registered_username)
        if not trial_ok:
            print(
                f"[auth][trial_grant] register_user user={registered_username} "
                f"status=failed reason={trial_msg}"
            )
        return True, "注册成功，请登录"
    except Exception as e:
        print(f"register_user failed: {e}")
        return False, "注册失败"


def login_with_phone_code(phone: str, sms_code: str):
    """Login by phone + sms code. Returns: (success, message, token, username)."""
    ok, normalized_phone, phone_msg = normalize_cn_phone(phone)
    if not ok:
        return False, phone_msg, None, None

    code_ok, code_msg = verify_login_sms_code(normalized_phone, sms_code)
    if not code_ok:
        return False, code_msg, None, None

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text("SELECT username, is_active FROM users WHERE phone = :p"),
                {"p": normalized_phone},
            ).fetchone()
            if not row:
                return False, "该手机号未注册，请先注册", None, None

            username, is_active = row
            if not is_active:
                return False, "账号已禁用", None, None

            token = _create_user_session(conn, username)
            return True, "登录成功", token, username
    except Exception as e:
        print(f"login_with_phone_code failed: {e}")
        return False, "登录失败，请稍后重试", None, None


def login_user(account: str, password: str):
    """Username + password login (account only)."""
    if not account or not password:
        return False, "请输入账号和密码", None, None

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT username, password_hash, is_active
                    FROM users
                    WHERE username = :a
                    """
                ),
                {"a": account},
            ).fetchone()

            if not result:
                return False, "账号不存在", None, None

            username, stored_hash, is_active = result
            if not is_active:
                return False, "账号已被禁用", None, None
            if not stored_hash or not verify_password(password, stored_hash):
                return False, "密码错误", None, None

            token = _create_user_session(conn, username)
            return True, "登录成功", token, username
    except Exception as e:
        print(f"login_user failed: {e}")
        return False, "登录失败，请稍后重试", None, None


def login_with_email_code(email: str, email_code: str):
    code_valid, code_msg = verify_login_code(email, email_code)
    if not code_valid:
        return False, code_msg, None, None

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT username, is_active FROM users WHERE email = :e"),
                {"e": email},
            ).fetchone()
            if not result:
                return False, "该邮箱未注册", None, None

            username, is_active = result
            if not is_active:
                return False, "账号已被禁用", None, None

            token = _create_user_session(conn, username)
            return True, "登录成功", token, username
    except Exception as e:
        print(f"login_with_email_code failed: {e}")
        return False, "登录失败，请稍后重试", None, None


def change_password_with_old(username: str, old_password: str, new_password: str):
    if len(str(new_password or "")) < 6:
        return False, "新密码长度不能少于6位"

    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("SELECT password_hash FROM users WHERE username = :u"),
                {"u": username},
            ).fetchone()
            if not result:
                return False, "用户不存在"
            if not verify_password(old_password, result[0]):
                return False, "旧密码错误"

            conn.execute(
                text("UPDATE users SET password_hash = :h WHERE username = :u"),
                {"h": hash_password(new_password), "u": username},
            )
            conn.execute(text("DELETE FROM user_sessions WHERE username = :u"), {"u": username})
            return True, "密码修改成功，请重新登录"
    except Exception as e:
        print(f"change_password_with_old failed: {e}")
        return False, "修改密码失败"


def reset_password_with_email(email: str, email_code: str, new_password: str):
    code_valid, code_msg = verify_reset_password_code(email, email_code)
    if not code_valid:
        return False, code_msg

    if len(str(new_password or "")) < 6:
        return False, "新密码长度不能少于6位"

    try:
        with engine.begin() as conn:
            check = conn.execute(
                text("SELECT username FROM users WHERE email = :e"),
                {"e": email},
            ).fetchone()
            if not check:
                return False, "该邮箱未注册"

            conn.execute(
                text("UPDATE users SET password_hash = :h WHERE email = :e"),
                {"h": hash_password(new_password), "e": email},
            )
            conn.execute(text("DELETE FROM user_sessions WHERE username = :u"), {"u": check[0]})
            return True, "密码重置成功，请使用新密码登录"
    except Exception as e:
        print(f"reset_password_with_email failed: {e}")
        return False, "重置密码失败"


def bind_email(username: str, email: str, email_code: str):
    code_valid, code_msg = verify_bind_email_code(email, email_code)
    if not code_valid:
        return False, code_msg

    try:
        with engine.begin() as conn:
            check = conn.execute(
                text("SELECT username FROM users WHERE email = :e AND username != :u"),
                {"e": email, "u": username},
            ).fetchone()
            if check:
                return False, "该邮箱已被其他账号绑定"

            conn.execute(
                text(
                    """
                    UPDATE users
                    SET email = :e, email_verified = 1
                    WHERE username = :u
                    """
                ),
                {"e": email, "u": username},
            )
            return True, "邮箱绑定成功"
    except Exception as e:
        print(f"bind_email failed: {e}")
        return False, "绑定失败"


def logout_user(username: str = None, token: str = None):
    try:
        with engine.begin() as conn:
            if token and username:
                conn.execute(
                    text("DELETE FROM user_sessions WHERE username = :u AND session_token = :t"),
                    {"u": username, "t": token},
                )
            elif token:
                conn.execute(
                    text("DELETE FROM user_sessions WHERE session_token = :t"),
                    {"t": token},
                )
            else:
                conn.execute(text("DELETE FROM user_sessions WHERE username = :u"), {"u": username})
            return True
    except Exception as e:
        print(f"logout_user failed: {e}")
        return False


def check_token(username, token, strict: bool = False):
    if not username or not token:
        return False

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT s.token_expire, u.is_active
                    FROM user_sessions s
                    JOIN users u ON u.username = s.username
                    WHERE s.username = :u AND s.session_token = :t
                    """
                ),
                {"u": username, "t": token},
            ).fetchone()
            if not result:
                return False

            expire_time, is_active = result
            if not is_active:
                return False

            if expire_time and expire_time > datetime.now():
                return True

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM user_sessions WHERE session_token = :t"), {"t": token})
        return False
    except Exception as e:
        print(f"check_token failed: {e}")
        if strict:
            raise
        return False


def get_username_by_token(token: str, strict: bool = False):
    if not token:
        return ""

    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT s.username, s.token_expire, u.is_active
                    FROM user_sessions s
                    JOIN users u ON u.username = s.username
                    WHERE s.session_token = :t
                    LIMIT 1
                    """
                ),
                {"t": token},
            ).fetchone()
            if not result:
                return ""

            username, expire_time, is_active = result
            if not is_active:
                return ""
            if expire_time and expire_time > datetime.now():
                return str(username or "")

        with engine.begin() as conn:
            conn.execute(text("DELETE FROM user_sessions WHERE session_token = :t"), {"t": token})
        return ""
    except Exception as e:
        print(f"get_username_by_token failed: {e}")
        if strict:
            raise
        return ""


def restore_login_from_cookies(cookies: dict) -> bool:
    if st.session_state.get("is_logged_in") and st.session_state.get("user_id"):
        return True

    cookies = cookies or {}
    c_user = str(cookies.get("username") or "").strip()
    c_token = str(cookies.get("token") or "").strip()
    if not c_token:
        return False

    # Prefer username+token check when both are present.
    if c_user and check_token(c_user, c_token):
        st.session_state["is_logged_in"] = True
        st.session_state["user_id"] = c_user
        st.session_state["token"] = c_token
        return True

    # Fallback: cookie reads can be partial during page transitions.
    # If token is valid, recover username from DB and restore session safely.
    token_user = get_username_by_token(c_token)
    if not token_user:
        return False

    st.session_state["is_logged_in"] = True
    st.session_state["user_id"] = str(token_user)
    st.session_state["token"] = c_token
    return True


def get_user_info(username):
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    """
                    SELECT username, email, phone, email_verified, phone_verified,
                           level, capital, is_active, last_login, created_at
                    FROM users
                    WHERE username = :u
                    """
                ),
                {"u": username},
            ).fetchone()
            if not result:
                return None

            return {
                "username": result[0],
                "email": result[1],
                "phone": result[2],
                "email_verified": result[3],
                "phone_verified": result[4],
                "level": result[5],
                "capital": result[6],
                "is_active": result[7],
                "last_login": result[8],
                "created_at": result[9],
            }
    except Exception as e:
        print(f"get_user_info failed: {e}")
        return None


def get_masked_email(username):
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT email FROM users WHERE username = :u"), {"u": username}).fetchone()
            if result and result[0]:
                email = result[0]
                parts = email.split("@")
                if len(parts) == 2:
                    name, domain = parts
                    masked_name = (name[:2] if len(name) > 2 else name[:1]) + "***"
                    return f"{masked_name}@{domain}"
            return None
    except Exception:
        return None


ensure_sessions_table()
ensure_users_phone_columns()

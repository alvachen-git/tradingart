"""
用户认证工具类 (邮箱验证版)
- 新用户注册：强制邮箱验证
- 邮箱：不能重复
- 老用户：可在个人资料页绑定邮箱
"""

import bcrypt
from sqlalchemy import text
import streamlit as st
import uuid
from datetime import datetime, timedelta

# 导入数据库引擎
from data_engine import engine
import subscription_service as sub_svc

# 导入邮箱服务
from email_utils import (
    send_register_code, verify_register_code,
    send_reset_password_code, verify_reset_password_code,
    send_login_code, verify_login_code,
    send_bind_email_code, verify_bind_email_code
)


# ============================================
# 密码处理
# ============================================

def hash_password(password):
    """密码加密"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password, hashed):
    """验证密码"""
    return bcrypt.checkpw(password.encode(), hashed.encode())


def generate_token():
    """生成会话Token"""
    return str(uuid.uuid4())


# ============================================
# 多设备会话表（自动初始化）
# ============================================

def ensure_sessions_table():
    """确保 user_sessions 表存在，支持同账号多设备并行登录"""
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(100) NOT NULL,
                    session_token VARCHAR(100) NOT NULL,
                    token_expire DATETIME NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE KEY uq_token (session_token),
                    INDEX idx_username (username)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """))
    except Exception as e:
        print(f"创建 user_sessions 表失败: {e}")


ensure_sessions_table()


# ============================================
# 🔥 新用户注册（强制邮箱验证）
# ============================================

def register_with_email(email: str, password: str, email_code: str, username: str = None):
    """
    使用邮箱注册（强制验证）

    Args:
        email: 邮箱地址（必填，且不能重复）
        password: 密码
        email_code: 邮箱验证码
        username: 用户名（可选，默认从邮箱生成）

    Returns:
        (success, message)
    """
    # 1. 验证邮箱验证码
    code_valid, code_msg = verify_register_code(email, email_code)
    if not code_valid:
        return False, code_msg

    # 2. 验证密码强度
    if len(password) < 6:
        return False, "密码长度不能少于6位"

    # 3. 用户名处理
    if not username:
        # 从邮箱生成默认用户名
        username = email.split('@')[0]
        if len(username) < 3:
            username = f"用户{username}"

    # 4. 密码加密
    hashed = hash_password(password)

    try:
        registered_username = None
        with engine.begin() as conn:
            # 🔥 检查邮箱是否已注册（强制唯一）
            check_email = conn.execute(
                text("SELECT 1 FROM users WHERE email = :e"),
                {"e": email}
            ).fetchone()

            if check_email:
                return False, "该邮箱已注册，请直接登录或找回密码"

            # 检查用户名是否已存在
            check_user = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u"),
                {"u": username}
            ).fetchone()

            if check_user:
                # 自动生成唯一用户名
                import random
                username = f"{username}_{random.randint(1000, 9999)}"

            # 插入用户（邮箱已验证）
            sql_user = text("""
                            INSERT INTO users (username, email, password_hash,
                                               level, experience, capital,
                                               is_active, email_verified, created_at)
                            VALUES (:u, :e, :h,
                                    1, 0, 1000000,
                                    1, 1, :now)
                            """)
            conn.execute(sql_user, {
                "u": username,
                "e": email,
                "h": hashed,
                "now": datetime.now()
            })

            # 初始化用户画像
            sql_profile = text("""
                               INSERT INTO user_profile (user_id, risk_preference, focus_assets, current_mood)
                               VALUES (:uid, '未知', '暂无', '平静')
                               """)
            conn.execute(sql_profile, {"uid": username})

            registered_username = username

        trial_ok, trial_msg = sub_svc.grant_new_user_trial(registered_username)
        if not trial_ok:
            print(
                f"[auth][trial_grant] register_with_email user={registered_username} "
                f"status=failed reason={trial_msg}"
            )
        return True, f"注册成功！您的用户名是：{registered_username}"

    except Exception as e:
        print(f"注册失败: {e}")
        return False, "注册失败，请稍后重试"


# ============================================
# 登录功能
# ============================================

def login_user(account: str, password: str):
    """
    用户名/邮箱 + 密码登录

    Returns:
        (success, message, token, username)  # 🔥 新增返回 username
    """
    if not account or not password:
        return False, "请输入账号和密码", None, None

    try:
        with engine.begin() as conn:
            # 支持用户名或邮箱登录
            sql = text("""
                       SELECT username, password_hash, is_active
                       FROM users
                       WHERE username = :a
                          OR email = :a
                       """)
            result = conn.execute(sql, {"a": account}).fetchone()

            if not result:
                return False, "账号不存在", None, None

            username, stored_hash, is_active = result

            if not is_active:
                return False, "账号已被禁用", None, None

            if not verify_password(password, stored_hash):
                return False, "密码错误", None, None

            # 生成Token（每台设备独立，不覆盖其他设备）
            token = generate_token()
            expire_time = datetime.now() + timedelta(days=30)
            now = datetime.now()

            # 插入新会话（支持多设备并行）
            conn.execute(text("""
                INSERT INTO user_sessions (username, session_token, token_expire, created_at)
                VALUES (:u, :t, :e, :now)
            """), {"u": username, "t": token, "e": expire_time, "now": now})

            # 仅更新最后登录时间，不触碰 session_token
            conn.execute(text("UPDATE users SET last_login = :now WHERE username = :u"),
                         {"now": now, "u": username})

            # 清理该用户已过期的旧会话
            conn.execute(text("""
                DELETE FROM user_sessions WHERE username = :u AND token_expire <= :now
            """), {"u": username, "now": now})

            return True, "登录成功", token, username

    except Exception as e:
        print(f"登录错误: {e}")
        return False, "登录失败，请稍后重试", None, None


def login_with_email_code(email: str, email_code: str):
    """
    邮箱验证码登录

    Returns:
        (success, message, token, username)  # 🔥 返回 username
    """
    # 1. 验证邮箱验证码
    code_valid, code_msg = verify_login_code(email, email_code)
    if not code_valid:
        return False, code_msg, None, None

    try:
        with engine.begin() as conn:
            sql = text("""
                       SELECT username, is_active
                       FROM users
                       WHERE email = :e
                       """)
            result = conn.execute(sql, {"e": email}).fetchone()

            if not result:
                return False, "该邮箱未注册", None, None

            username, is_active = result

            if not is_active:
                return False, "账号已被禁用", None, None

            # 生成Token（每台设备独立，不覆盖其他设备）
            token = generate_token()
            expire_time = datetime.now() + timedelta(days=30)
            now = datetime.now()

            conn.execute(text("""
                INSERT INTO user_sessions (username, session_token, token_expire, created_at)
                VALUES (:u, :t, :e, :now)
            """), {"u": username, "t": token, "e": expire_time, "now": now})

            conn.execute(text("UPDATE users SET last_login = :now WHERE username = :u"),
                         {"now": now, "u": username})

            conn.execute(text("""
                DELETE FROM user_sessions WHERE username = :u AND token_expire <= :now
            """), {"u": username, "now": now})

            return True, "登录成功", token, username

    except Exception as e:
        print(f"登录错误: {e}")
        return False, "登录失败，请稍后重试", None, None


# ============================================
# 修改密码
# ============================================

def change_password_with_old(username: str, old_password: str, new_password: str):
    """通过旧密码修改密码"""
    if len(new_password) < 6:
        return False, "新密码长度不能少于6位"

    try:
        with engine.begin() as conn:
            sql = text("SELECT password_hash FROM users WHERE username = :u")
            result = conn.execute(sql, {"u": username}).fetchone()

            if not result:
                return False, "用户不存在"

            if not verify_password(old_password, result[0]):
                return False, "旧密码错误"

            new_hash = hash_password(new_password)
            conn.execute(
                text("UPDATE users SET password_hash = :h WHERE username = :u"),
                {"h": new_hash, "u": username}
            )
            # 修改密码后踢出所有设备
            conn.execute(
                text("DELETE FROM user_sessions WHERE username = :u"),
                {"u": username}
            )

            return True, "密码修改成功，请重新登录"

    except Exception as e:
        print(f"修改密码失败: {e}")
        return False, "修改密码失败"


def reset_password_with_email(email: str, email_code: str, new_password: str):
    """通过邮箱验证码重置密码"""
    # 1. 验证邮箱验证码
    code_valid, code_msg = verify_reset_password_code(email, email_code)
    if not code_valid:
        return False, code_msg

    if len(new_password) < 6:
        return False, "新密码长度不能少于6位"

    try:
        with engine.begin() as conn:
            check = conn.execute(
                text("SELECT username FROM users WHERE email = :e"),
                {"e": email}
            ).fetchone()

            if not check:
                return False, "该邮箱未注册"

            new_hash = hash_password(new_password)
            conn.execute(
                text("UPDATE users SET password_hash = :h WHERE email = :e"),
                {"h": new_hash, "e": email}
            )
            # 重置密码后踢出所有设备
            conn.execute(
                text("DELETE FROM user_sessions WHERE username = :u"),
                {"u": check[0]}
            )

            return True, "密码重置成功，请使用新密码登录"

    except Exception as e:
        print(f"重置密码失败: {e}")
        return False, "重置密码失败"


# ============================================
# 🔥 绑定邮箱（老用户使用）
# ============================================

def bind_email(username: str, email: str, email_code: str):
    """
    为已有账号绑定/换绑邮箱

    Args:
        username: 用户名
        email: 新邮箱
        email_code: 邮箱验证码
    """
    # 1. 验证邮箱验证码
    code_valid, code_msg = verify_bind_email_code(email, email_code)
    if not code_valid:
        return False, code_msg

    try:
        with engine.begin() as conn:
            # 🔥 检查邮箱是否已被其他账号绑定
            check = conn.execute(
                text("SELECT username FROM users WHERE email = :e AND username != :u"),
                {"e": email, "u": username}
            ).fetchone()

            if check:
                return False, "该邮箱已被其他账号绑定"

            # 绑定邮箱
            update_sql = text("""
                              UPDATE users
                              SET email          = :e,
                                  email_verified = 1
                              WHERE username = :u
                              """)
            conn.execute(update_sql, {"e": email, "u": username})

            return True, "邮箱绑定成功"

    except Exception as e:
        print(f"绑定邮箱失败: {e}")
        return False, "绑定失败"


# ============================================
# Token 验证
# ============================================

def logout_user(username: str, token: str = None):
    """
    登出用户。
    - 传入 token：只删除当前设备的会话，其他设备不受影响
    - 不传 token：删除该用户所有会话（踢出所有设备）
    """
    try:
        with engine.begin() as conn:
            if token:
                conn.execute(
                    text("DELETE FROM user_sessions WHERE username = :u AND session_token = :t"),
                    {"u": username, "t": token}
                )
            else:
                conn.execute(
                    text("DELETE FROM user_sessions WHERE username = :u"),
                    {"u": username}
                )
            return True
    except Exception as e:
        print(f"登出失败: {e}")
        return False


def check_token(username, token):
    """验证Token有效性（查 user_sessions，支持多设备）"""
    if not username or not token:
        return False

    try:
        with engine.connect() as conn:
            sql = text("""
                SELECT s.token_expire, u.is_active
                FROM user_sessions s
                JOIN users u ON u.username = s.username
                WHERE s.username = :u AND s.session_token = :t
            """)
            result = conn.execute(sql, {"u": username, "t": token}).fetchone()

            if not result:
                return False

            expire_time, is_active = result

            if not is_active:
                return False

            if expire_time and expire_time > datetime.now():
                return True
            else:
                # 清理当前过期 token
                with engine.begin() as clean_conn:
                    clean_conn.execute(
                        text("DELETE FROM user_sessions WHERE session_token = :t"),
                        {"t": token}
                    )
                return False

    except Exception as e:
        print(f"Token验证错误: {e}")
        return False


def restore_login_from_cookies(cookies: dict) -> bool:
    """
    从浏览器 Cookie 恢复登录态。
    仅在 session_state 未登录时尝试恢复。
    """
    if st.session_state.get("is_logged_in") and st.session_state.get("user_id"):
        return True

    cookies = cookies or {}
    c_user = cookies.get("username")
    c_token = cookies.get("token")

    if not c_user or not c_token or not str(c_user).strip():
        return False

    if not check_token(str(c_user), c_token):
        return False

    st.session_state["is_logged_in"] = True
    st.session_state["user_id"] = str(c_user)
    st.session_state["token"] = c_token
    return True


# ============================================
# 用户信息查询
# ============================================

def get_user_info(username):
    """获取用户信息"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT username,
                              email,
                              email_verified,
                              level,
                              capital,
                              is_active,
                              last_login,
                              created_at
                       FROM users
                       WHERE username = :u
                       """)
            result = conn.execute(sql, {"u": username}).fetchone()

            if result:
                return {
                    "username": result[0],
                    "email": result[1],
                    "email_verified": result[2],
                    "level": result[3],
                    "capital": result[4],
                    "is_active": result[5],
                    "last_login": result[6],
                    "created_at": result[7]
                }
            return None
    except Exception as e:
        print(f"获取用户信息失败: {e}")
        return None


def get_masked_email(username):
    """获取脱敏邮箱（用于前端显示）"""
    try:
        with engine.connect() as conn:
            sql = text("SELECT email FROM users WHERE username = :u")
            result = conn.execute(sql, {"u": username}).fetchone()

            if result and result[0]:
                email = result[0]
                parts = email.split('@')
                if len(parts) == 2:
                    name = parts[0]
                    domain = parts[1]
                    if len(name) > 2:
                        masked_name = name[:2] + '***'
                    else:
                        masked_name = name[0] + '***'
                    return f"{masked_name}@{domain}"
            return None
    except:
        return None


# ============================================
# 旧版注册（兼容，但建议弃用）
# ============================================

def register_user(username, password):
    """
    用户名+密码注册（不带邮箱）
    """
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
            check = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u"),
                {"u": username}
            ).fetchone()

            if check:
                return False, "用户名已存在"

            sql_user = text("""
                            INSERT INTO users (username, password_hash, level, experience, capital, is_active,
                                               created_at)
                            VALUES (:u, :p, 1, 0, 1000000, 1, :now)
                            """)
            conn.execute(sql_user, {"u": username, "p": hashed, "now": datetime.now()})

            sql_profile = text("""
                               INSERT INTO user_profile (user_id, risk_preference, focus_assets, current_mood)
                               VALUES (:uid, '未知', '暂无', '平静')
                               """)
            conn.execute(sql_profile, {"uid": username})

            registered_username = username

        trial_ok, trial_msg = sub_svc.grant_new_user_trial(registered_username)
        if not trial_ok:
            print(
                f"[auth][trial_grant] register_user user={registered_username} "
                f"status=failed reason={trial_msg}"
            )
        return True, "注册成功！请登录"

    except Exception as e:
        print(f"注册失败: {e}")
        return False, "注册失败"

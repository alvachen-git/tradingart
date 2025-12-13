import pandas as pd
import bcrypt
from sqlalchemy import text
import streamlit as st
import time
import uuid
from datetime import datetime, timedelta

# 导入数据库引擎
from data_engine import engine


# --- 密码处理 ---
def hash_password(password):
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password, hashed):
    return bcrypt.checkpw(password.encode(), hashed.encode())


def generate_token():
    return str(uuid.uuid4())


# --- 【改进】登录逻辑 ---
def login_user(username, password):
    """
    改进点：
    1. 添加更详细的错误日志
    2. 使用事务确保数据一致性
    3. 返回更多调试信息
    """
    if not username or not password:
        return False, "请输入用户名和密码", None

    try:
        with engine.begin() as conn:  # 使用 begin() 自动管理事务
            # 查询用户
            sql = text("SELECT password_hash, is_active FROM users WHERE username = :u")
            result = conn.execute(sql, {"u": username}).fetchone()

            if not result:
                return False, "用户不存在", None

            stored_hash, is_active = result

            # 检查账号是否被禁用
            if not is_active:
                return False, "账号已被禁用", None

            # 验证密码
            if not verify_password(password, stored_hash):
                return False, "密码错误", None

            # 生成新Token（30天有效期）
            token = generate_token()
            expire_time = datetime.now() + timedelta(days=30)

            # 更新Token到数据库
            update_sql = text("""
                              UPDATE users
                              SET session_token = :t,
                                  token_expire  = :e,
                                  last_login    = :now
                              WHERE username = :u
                              """)
            conn.execute(update_sql, {
                "t": token,
                "e": expire_time,
                "now": datetime.now(),
                "u": username
            })

            # 事务自动提交
            return True, "登录成功", token

    except Exception as e:
        # 添加详细错误日志
        error_msg = f"登录系统错误: {str(e)}"
        print(error_msg)  # 后台日志
        return False, "登录失败，请稍后重试", None


# --- 【改进】Token 验证 ---
def check_token(username, token):
    """
    改进点：
    1. 添加更详细的验证逻辑
    2. 自动清理过期Token
    3. 返回验证详情（可选）
    """
    if not username or not token:
        return False

    try:
        with engine.connect() as conn:
            # 查询Token和过期时间
            sql = text("""
                       SELECT token_expire, is_active
                       FROM users
                       WHERE username = :u
                         AND session_token = :t
                       """)
            result = conn.execute(sql, {"u": username, "t": token}).fetchone()

            if not result:
                # Token不匹配或用户不存在
                return False

            expire_time, is_active = result

            # 检查账号状态
            if not is_active:
                return False

            # 检查Token是否过期
            if expire_time and expire_time > datetime.now():
                return True
            else:
                # Token已过期，清理数据库中的过期Token
                with engine.begin() as clean_conn:
                    clean_sql = text("""
                                     UPDATE users
                                     SET session_token = NULL,
                                         token_expire  = NULL
                                     WHERE username = :u
                                     """)
                    clean_conn.execute(clean_sql, {"u": username})
                return False

    except Exception as e:
        print(f"Token验证错误: {e}")
        return False


# --- 【改进】注册逻辑 ---
def register_user(username, password):
    """
    改进点：
    1. 添加用户名格式验证
    2. 使用事务确保数据一致性
    3. 更详细的错误提示
    """
    # 1. 基础验证
    if not username or not password:
        return False, "用户名和密码不能为空"

    if len(username) < 3:
        return False, "用户名长度不能少于3位"

    if len(password) < 6:
        return False, "密码长度不能少于6位"

    # 2. 用户名格式验证（可选，根据需求调整）
    if not username.replace("_", "").isalnum():
        return False, "用户名只能包含字母、数字和下划线"

    # 3. 密码加密
    hashed = hash_password(password)

    try:
        with engine.begin() as conn:  # 使用事务
            # 检查用户名是否已存在
            check = conn.execute(
                text("SELECT 1 FROM users WHERE username = :u"),
                {"u": username}
            ).fetchone()

            if check:
                return False, "用户名已存在，请换一个"

            # 插入用户表
            sql_user = text("""
                            INSERT INTO users (username,
                                               password_hash,
                                               level,
                                               experience,
                                               capital,
                                               is_active,
                                               created_at)
                            VALUES (:u, :p, 1, 0, 1000000, 1, :now)
                            """)
            conn.execute(sql_user, {
                "u": username,
                "p": hashed,
                "now": datetime.now()
            })

            # 初始化用户画像
            sql_profile = text("""
                               INSERT INTO user_profile (user_id,
                                                         risk_preference,
                                                         focus_assets,
                                                         current_mood)
                               VALUES (:uid, '未知', '暂无', '平静')
                               """)
            conn.execute(sql_profile, {"uid": username})

            # 事务自动提交
            return True, "注册成功！请登录"

    except Exception as e:
        error_msg = str(e)
        print(f"注册失败: {error_msg}")

        # 根据错误类型返回友好提示
        if "Duplicate entry" in error_msg or "UNIQUE" in error_msg:
            return False, "用户名已存在"
        else:
            return False, f"注册失败: {error_msg}"


# --- 【新增】Token刷新功能 ---
def refresh_token(username, old_token):
    """
    刷新用户Token（延长登录时间）
    当用户活跃时可以自动调用此函数
    """
    if not check_token(username, old_token):
        return False, None

    try:
        with engine.begin() as conn:
            new_token = generate_token()
            new_expire = datetime.now() + timedelta(days=30)

            sql = text("""
                       UPDATE users
                       SET session_token = :t,
                           token_expire  = :e
                       WHERE username = :u
                       """)
            conn.execute(sql, {"t": new_token, "e": new_expire, "u": username})

            return True, new_token
    except:
        return False, None


# --- 【新增】批量清理过期Token ---
def cleanup_expired_tokens():
    """
    定期清理数据库中的过期Token
    可以在应用启动时调用
    """
    try:
        with engine.begin() as conn:
            sql = text("""
                       UPDATE users
                       SET session_token = NULL,
                           token_expire  = NULL
                       WHERE token_expire < :now
                       """)
            result = conn.execute(sql, {"now": datetime.now()})
            return result.rowcount
    except Exception as e:
        print(f"清理过期Token失败: {e}")
        return 0


# --- 【新增】获取用户详细信息 ---
def get_user_info(username):
    """
    获取用户完整信息（用于调试）
    """
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT username,
                              level,
                              capital,
                              is_active,
                              token_expire,
                              last_login,
                              created_at
                       FROM users
                       WHERE username = :u
                       """)
            result = conn.execute(sql, {"u": username}).fetchone()

            if result:
                return {
                    "username": result[0],
                    "level": result[1],
                    "capital": result[2],
                    "is_active": result[3],
                    "token_expire": result[4],
                    "last_login": result[5],
                    "created_at": result[6]
                }
            return None
    except Exception as e:
        print(f"获取用户信息失败: {e}")
        return None
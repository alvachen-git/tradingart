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


# --- 登录逻辑 ---
def login_user(username, password):
    if not username or not password:
        return False, "请输入用户名和密码", None

    try:
        with engine.connect() as conn:
            # 查询用户
            sql = text("SELECT password_hash, is_active FROM users WHERE username = :u")
            result = conn.execute(sql, {"u": username}).fetchone()

            if not result:
                return False, "用户不存在", None

            stored_hash, is_active = result

            if not is_active:
                return False, "账号已被禁用", None

            if verify_password(password, stored_hash):
                # 生成 Token
                token = generate_token()
                expire_time = datetime.now() + timedelta(days=7)

                update_sql = text("UPDATE users SET session_token=:t, token_expire=:e WHERE username=:u")
                conn.execute(update_sql, {"t": token, "e": expire_time, "u": username})
                conn.commit()

                return True, "登录成功", token
            else:
                return False, "密码错误", None
    except Exception as e:
        return False, f"登录系统错误: {e}", None


# --- 注册逻辑 (核心修正) ---
def register_user(username, password):
    """
    注册新用户：统一使用 username 作为唯一标识，不使用自增 ID
    """
    if len(password) < 6:
        return False, "密码长度不能少于6位"

    hashed = hash_password(password)

    try:
        with engine.connect() as conn:
            # 1. 检查是否存在
            check = conn.execute(text("SELECT 1 FROM users WHERE username=:u"), {"u": username}).fetchone()
            if check:
                return False, "用户名已存在"

            # 2. 插入用户表 (users)
            # 注意：这里不需要插入 user_id，因为 username 本身就是主键
            sql_user = text("""
                            INSERT INTO users (username, password_hash, level, experience, capital, is_active)
                            VALUES (:u, :p, 1, 0, 1000000, 1)
                            """)
            conn.execute(sql_user, {"u": username, "p": hashed})

            # 3. 初始化用户画像 (user_profile)
            # 【关键修正】直接把 username 存入 user_id 字段
            sql_profile = text("""
                               INSERT INTO user_profile (user_id, risk_preference, focus_assets, current_mood)
                               VALUES (:uid, '未知', '暂无', '平静')
                               """)
            conn.execute(sql_profile, {"uid": username})

            conn.commit()
            return True, "注册成功！请登录"

    except Exception as e:
        return False, f"注册失败: {e}"


# --- Token 验证 ---
def check_token(username, token):
    if not username or not token: return False
    try:
        with engine.connect() as conn:
            sql = text("SELECT token_expire FROM users WHERE username=:u AND session_token=:t")
            result = conn.execute(sql, {"u": username, "t": token}).fetchone()

            if result:
                expire_time = result[0]
                if expire_time > datetime.now():
                    return True
        return False
    except:
        return False
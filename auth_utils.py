import pandas as pd
import bcrypt
from sqlalchemy import text

import uuid
from datetime import datetime, timedelta

# 导入您的数据库引擎 (复用 data_engine 的连接)
# 确保 data_engine.py 里有 engine 对象
from data_engine import engine


def hash_password(password):
    """加密密码"""
    return bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()


def verify_password(password, hashed):
    """验证密码"""
    return bcrypt.checkpw(password.encode(), hashed.encode())

def generate_token():
    return str(uuid.uuid4())

def login_user(username, password):
    """
    处理登录逻辑
    返回: (success: bool, message: str)
    """
    if not username or not password:
        return False, "请输入用户名和密码"

    try:
        with engine.connect() as conn:
            # 查询用户
            sql = text(f"SELECT password_hash, is_active FROM users WHERE username = :u")
            result = conn.execute(sql, {"u": username}).fetchone()
            stored_hash, is_active = result
            if verify_password(password, stored_hash):
                # --- 【新增】登录成功后，生成 Token 并存库 ---
                token = generate_token()
                # 设置过期时间为 7 天后
                expire_time = datetime.now() + timedelta(days=7)

                update_sql = text("UPDATE users SET session_token=:t, token_expire=:e WHERE username=:u")
                conn.execute(update_sql, {"t": token, "e": expire_time, "u": username})
                conn.commit()

                # 返回 token
                return True, "登录成功", token

            if not result:
                return False, "用户不存在"

            stored_hash, is_active = result

            if not is_active:
                return False, "账号已被禁用"

            if verify_password(password, stored_hash):
                return True, "登录成功"
            else:
                return False, "密码错误"
    except Exception as e:
        return False, f"系统错误: {e}"


def check_token(username, token):
    """检查 Token 是否有效"""
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

def register_user(username, password):
    """
    处理注册逻辑 (修复版)
    """
    if len(password) < 6:
        return False, "密码长度不能少于6位"

    hashed = hash_password(password)

    try:
        with engine.connect() as conn:
            # 1. 检查是否存在
            # (这里稍微优化了一下写法，原来的也没问题)
            check = conn.execute(text("SELECT 1 FROM users WHERE username=:u"), {"u": username}).fetchone()
            if check:
                return False, "用户名已存在"

            # 2. 插入用户表 (users)
            # 注意：不需要插入 user_id，数据库会自动生成
            sql_user = text("""
                INSERT INTO users (username, password_hash, level, experience, capital, is_active)
                VALUES (:u, :p, 1, 0, 1000000, 1)
            """)
            conn.execute(sql_user, {"u": username, "p": hashed})

            # -----------------------------------------------------------
            # 👇👇👇 关键修改：获取刚才生成的数字 ID 👇👇👇
            # -----------------------------------------------------------
            # 因为 username 是唯一的，查它就能拿到最新的 user_id (数字)
            sql_get_id = text("SELECT user_id FROM users WHERE username = :u")
            result = conn.execute(sql_get_id, {"u": username})
            new_user_id = result.scalar()  # 例如：拿到数字 1

            # 3. 初始化用户画像 (user_profile)
            # 🔴 注意：这里 VALUES 变成了 :uid (数字)，而不是原来的 :u (名字)
            sql_profile = text("""
                INSERT INTO user_profile (user_id, risk_preference, focus_assets)
                VALUES (:uid, '未知', '暂无')
            """)
            # 🔴 传入刚才拿到的数字 new_user_id
            conn.execute(sql_profile, {"uid": new_user_id})

            conn.commit()
            return True, "注册成功！请登录"

    except Exception as e:
        return False, f"注册失败: {e}"


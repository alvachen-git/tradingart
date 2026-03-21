"""
订阅中心服务模块
- 频道管理
- 订阅权限控制
- 内容发布与获取
- 站内消息
"""

from datetime import datetime, timedelta
import os
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from typing import Optional, List, Dict, Any
import pandas as pd

# 导入数据库引擎
from data_engine import engine


_SUB_SOURCE_COLUMNS = {"source_type", "source_ref", "source_note", "granted_at", "operator"}
_HAS_SUB_SOURCE_COLUMNS: Optional[bool] = None
_TRIAL_TABLE_READY: bool = False


def _has_subscription_source_columns(conn) -> bool:
    global _HAS_SUB_SOURCE_COLUMNS
    if _HAS_SUB_SOURCE_COLUMNS is not None:
        return _HAS_SUB_SOURCE_COLUMNS

    try:
        if conn.dialect.name == "sqlite":
            rows = conn.execute(text("PRAGMA table_info(user_subscriptions)")).fetchall()
            cols = {str(row[1]).lower() for row in rows}
        else:
            rows = conn.execute(
                text(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = DATABASE()
                      AND table_name = 'user_subscriptions'
                    """
                )
            ).fetchall()
            cols = {str(row[0]).lower() for row in rows}
        _HAS_SUB_SOURCE_COLUMNS = _SUB_SOURCE_COLUMNS.issubset(cols)
    except Exception as exc:
        print(f"[subscription] detect_source_columns_failed err={exc}")
        _HAS_SUB_SOURCE_COLUMNS = False
    return bool(_HAS_SUB_SOURCE_COLUMNS)


def _ensure_trial_grants_table(conn) -> None:
    global _TRIAL_TABLE_READY
    if _TRIAL_TABLE_READY:
        return

    if conn.dialect.name == "sqlite":
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_trial_grants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    trial_code TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    days INTEGER NOT NULL,
                    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source_note TEXT DEFAULT NULL,
                    operator TEXT DEFAULT NULL,
                    UNIQUE(user_id, trial_code)
                )
                """
            )
        )
    else:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_trial_grants (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(50) NOT NULL,
                    trial_code VARCHAR(100) NOT NULL,
                    channel_id INT NOT NULL,
                    days INT NOT NULL,
                    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source_note VARCHAR(255) DEFAULT NULL,
                    operator VARCHAR(100) DEFAULT NULL,
                    UNIQUE KEY uq_trial_user_code (user_id, trial_code),
                    INDEX idx_trial_user (user_id),
                    INDEX idx_trial_channel (channel_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
    _TRIAL_TABLE_READY = True


def _add_subscription_core(
    conn,
    user_id: str,
    channel_id: int,
    days: int = 30,
    source_type: Optional[str] = None,
    source_ref: Optional[str] = None,
    source_note: Optional[str] = None,
    operator: Optional[str] = None,
) -> tuple[bool, str]:
    expire_at = None
    if days > 0:
        expire_at = datetime.now() + timedelta(days=days)

    has_source_cols = _has_subscription_source_columns(conn)

    check_sql = text(
        """
        SELECT id, expire_at
        FROM user_subscriptions
        WHERE user_id = :uid
          AND channel_id = :cid
        """
    )
    existing = conn.execute(check_sql, {"uid": user_id, "cid": channel_id}).fetchone()

    if existing:
        old_expire = existing[1]
        if old_expire and old_expire > datetime.now() and days > 0:
            new_expire = old_expire + timedelta(days=days)
        else:
            new_expire = expire_at

        if has_source_cols:
            update_sql = text(
                """
                UPDATE user_subscriptions
                SET is_active = 1,
                    expire_at = :expire,
                    updated_at = CURRENT_TIMESTAMP,
                    source_type = COALESCE(:source_type, source_type),
                    source_ref = COALESCE(:source_ref, source_ref),
                    source_note = COALESCE(:source_note, source_note),
                    operator = COALESCE(:operator, operator),
                    granted_at = COALESCE(granted_at, CURRENT_TIMESTAMP)
                WHERE user_id = :uid
                  AND channel_id = :cid
                """
            )
            conn.execute(
                update_sql,
                {
                    "expire": new_expire,
                    "uid": user_id,
                    "cid": channel_id,
                    "source_type": source_type,
                    "source_ref": source_ref,
                    "source_note": source_note,
                    "operator": operator,
                },
            )
        else:
            update_sql = text(
                """
                UPDATE user_subscriptions
                SET is_active = 1,
                    expire_at = :expire,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = :uid
                  AND channel_id = :cid
                """
            )
            conn.execute(update_sql, {"expire": new_expire, "uid": user_id, "cid": channel_id})
        return True, f"订阅续期成功，有效期至 {new_expire.strftime('%Y-%m-%d') if new_expire else '永久'}"

    if has_source_cols:
        insert_sql = text(
            """
            INSERT INTO user_subscriptions
            (user_id, channel_id, is_active, expire_at, source_type, source_ref, source_note, granted_at, operator)
            VALUES (:uid, :cid, 1, :expire, :source_type, :source_ref, :source_note, CURRENT_TIMESTAMP, :operator)
            """
        )
        conn.execute(
            insert_sql,
            {
                "uid": user_id,
                "cid": channel_id,
                "expire": expire_at,
                "source_type": source_type or "unknown",
                "source_ref": source_ref,
                "source_note": source_note,
                "operator": operator,
            },
        )
    else:
        insert_sql = text(
            """
            INSERT INTO user_subscriptions (user_id, channel_id, is_active, expire_at)
            VALUES (:uid, :cid, 1, :expire)
            """
        )
        conn.execute(insert_sql, {"uid": user_id, "cid": channel_id, "expire": expire_at})

    return True, "订阅成功"


# =============================================
# 频道管理
# =============================================

def get_all_channels(only_active: bool = True) -> List[Dict]:
    """获取所有频道"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT id,
                              code,
                              name,
                              icon,
                              description,
                              is_premium,
                              price_monthly,
                              sort_order
                       FROM content_channels
                       WHERE is_active = 1
                          OR :only_active = 0
                       ORDER BY sort_order, id
                       """)
            result = conn.execute(sql, {"only_active": only_active}).fetchall()

            channels = []
            for row in result:
                channels.append({
                    "id": row[0],
                    "code": row[1],
                    "name": row[2],
                    "icon": row[3],
                    "description": row[4],
                    "is_premium": bool(row[5]),
                    "price_monthly": float(row[6]) if row[6] else None,
                    "sort_order": row[7]
                })
            return channels
    except Exception as e:
        print(f"获取频道列表失败: {e}")
        return []


def get_channel_by_code(code: str) -> Optional[Dict]:
    """根据code获取频道信息"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT id, code, name, icon, description, is_premium
                       FROM content_channels
                       WHERE code = :code
                         AND is_active = 1
                       """)
            row = conn.execute(sql, {"code": code}).fetchone()

            if row:
                return {
                    "id": row[0],
                    "code": row[1],
                    "name": row[2],
                    "icon": row[3],
                    "description": row[4],
                    "is_premium": bool(row[5])
                }
            return None
    except Exception as e:
        print(f"获取频道失败: {e}")
        return None


# =============================================
# 订阅权限控制
# =============================================

def check_subscription_access(user_id: str, channel_id: int) -> Dict:
    """
    检查用户是否有权访问某频道

    Returns:
        {
            "has_access": bool,
            "reason": "ok" | "not_subscribed" | "expired",
            "expire_at": datetime | None
        }
    """
    if not user_id:
        return {"has_access": False, "reason": "not_logged_in", "expire_at": None}

    try:
        with engine.connect() as conn:
            # 先检查频道是否免费
            channel_sql = text("SELECT is_premium FROM content_channels WHERE id = :cid")
            channel = conn.execute(channel_sql, {"cid": channel_id}).fetchone()

            if channel and not channel[0]:
                # 免费频道，直接放行
                return {"has_access": True, "reason": "free_channel", "expire_at": None}

            # 付费频道，检查订阅
            sql = text("""
                       SELECT is_active, expire_at
                       FROM user_subscriptions
                       WHERE user_id = :uid
                         AND channel_id = :cid
                       """)
            result = conn.execute(sql, {"uid": user_id, "cid": channel_id}).fetchone()

            if not result:
                return {"has_access": False, "reason": "not_subscribed", "expire_at": None}

            is_active, expire_at = result

            if not is_active:
                return {"has_access": False, "reason": "subscription_inactive", "expire_at": expire_at}

            # 检查是否过期
            if expire_at and expire_at < datetime.now():
                return {"has_access": False, "reason": "expired", "expire_at": expire_at}

            return {"has_access": True, "reason": "ok", "expire_at": expire_at}

    except Exception as e:
        print(f"检查订阅权限失败: {e}")
        return {"has_access": False, "reason": "error", "expire_at": None}


def get_user_subscriptions(user_id: str) -> List[Dict]:
    """获取用户的所有订阅"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT us.channel_id,
                              c.code,
                              c.name,
                              c.icon,
                              us.is_active,
                              us.expire_at,
                              us.notify_email,
                              us.notify_site
                       FROM user_subscriptions us
                                JOIN content_channels c ON us.channel_id = c.id
                       WHERE us.user_id = :uid
                       ORDER BY c.sort_order
                       """)
            result = conn.execute(sql, {"uid": user_id}).fetchall()

            subs = []
            for row in result:
                expire_at = row[5]
                is_expired = expire_at and expire_at < datetime.now() if expire_at else False

                subs.append({
                    "channel_id": row[0],
                    "code": row[1],
                    "name": row[2],
                    "icon": row[3],
                    "is_active": bool(row[4]) and not is_expired,
                    "expire_at": expire_at,
                    "is_expired": is_expired,
                    "notify_email": bool(row[6]),
                    "notify_site": bool(row[7])
                })
            return subs
    except Exception as e:
        print(f"获取用户订阅失败: {e}")
        return []


def get_channel_email_subscribers(channel_code: str) -> List[Dict]:
    """
    获取某频道开启邮件通知且订阅有效的用户邮箱。

    Returns:
        [{"user_id": "...", "email": "..."}, ...]
    """
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT us.user_id, u.email
                       FROM user_subscriptions us
                                JOIN content_channels c ON us.channel_id = c.id
                                JOIN users u ON u.username = us.user_id
                       WHERE c.code = :code
                         AND us.is_active = 1
                         AND us.notify_email = 1
                         AND (us.expire_at IS NULL OR us.expire_at > NOW())
                         AND u.email IS NOT NULL
                         AND u.email != ''
                       ORDER BY us.user_id
                       """)
            rows = conn.execute(sql, {"code": channel_code}).fetchall()

        results = []
        for row in rows:
            user_id = str(row[0] or "").strip()
            email = str(row[1] or "").strip()
            if not user_id or not email or "@" not in email:
                continue
            results.append({"user_id": user_id, "email": email})
        return results
    except Exception as e:
        print(f"获取频道邮件订阅用户失败: {e}")
        return []


def _legacy_add_subscription(user_id: str, channel_id: int, days: int = 30) -> tuple:
    """
    为用户添加订阅

    Args:
        user_id: 用户名
        channel_id: 频道ID
        days: 订阅天数，0表示永久

    Returns:
        (success, message)
    """
    try:
        expire_at = None
        if days > 0:
            expire_at = datetime.now() + timedelta(days=days)

        with engine.begin() as conn:
            # 检查是否已存在订阅
            check_sql = text("""
                             SELECT id, expire_at
                             FROM user_subscriptions
                             WHERE user_id = :uid
                               AND channel_id = :cid
                             """)
            existing = conn.execute(check_sql, {"uid": user_id, "cid": channel_id}).fetchone()

            if existing:
                # 已存在，续期
                old_expire = existing[1]
                if old_expire and old_expire > datetime.now() and days > 0:
                    # 在原有效期基础上续期
                    new_expire = old_expire + timedelta(days=days)
                else:
                    new_expire = expire_at

                update_sql = text("""
                                  UPDATE user_subscriptions
                                  SET is_active  = 1,
                                      expire_at  = :expire,
                                      updated_at = NOW()
                                  WHERE user_id = :uid
                                    AND channel_id = :cid
                                  """)
                conn.execute(update_sql, {"expire": new_expire, "uid": user_id, "cid": channel_id})
                return True, f"订阅续期成功，有效期至 {new_expire.strftime('%Y-%m-%d') if new_expire else '永久'}"
            else:
                # 新增订阅
                insert_sql = text("""
                                  INSERT INTO user_subscriptions (user_id, channel_id, is_active, expire_at)
                                  VALUES (:uid, :cid, 1, :expire)
                                  """)
                conn.execute(insert_sql, {"uid": user_id, "cid": channel_id, "expire": expire_at})
                return True, "订阅成功"

    except Exception as e:
        print(f"添加订阅失败: {e}")
        return False, "订阅失败，请稍后重试"


def add_subscription_in_tx(
    conn,
    user_id: str,
    channel_id: int,
    days: int = 30,
    source_type: Optional[str] = None,
    source_ref: Optional[str] = None,
    source_note: Optional[str] = None,
    operator: Optional[str] = None,
) -> tuple[bool, str]:
    """
    在外部事务中开通/续期订阅（不创建新事务）。
    """
    return _add_subscription_core(
        conn=conn,
        user_id=user_id,
        channel_id=channel_id,
        days=days,
        source_type=source_type,
        source_ref=source_ref,
        source_note=source_note,
        operator=operator,
    )


def add_subscription(
    user_id: str,
    channel_id: int,
    days: int = 30,
    source_type: Optional[str] = None,
    source_ref: Optional[str] = None,
    source_note: Optional[str] = None,
    operator: Optional[str] = None,
) -> tuple:
    """
    为用户开通/续期订阅（独立事务）。
    """
    try:
        with engine.begin() as conn:
            return _add_subscription_core(
                conn=conn,
                user_id=user_id,
                channel_id=channel_id,
                days=days,
                source_type=source_type,
                source_ref=source_ref,
                source_note=source_note,
                operator=operator,
            )
    except Exception as e:
        print(f"[subscription] add_failed user={user_id} channel={channel_id} err={e}")
        return False, "订阅失败，请稍后重试"


def grant_new_user_trial(user_id: str, channel_code: str = "daily_report", days: int = 5) -> tuple[bool, str]:
    """
    注册成功后发放 5 天复盘试用（幂等，每账号仅一次）。
    """
    if not user_id:
        return False, "user_id_empty"
    if days <= 0:
        return False, "days_invalid"

    trial_code = f"new_user_{channel_code}_{days}d"
    source_note = f"new_user_trial:{channel_code}:{days}d"
    operator = os.getenv("TRIAL_GRANT_OPERATOR", "system_register")

    try:
        with engine.begin() as conn:
            _ensure_trial_grants_table(conn)
            exists = conn.execute(
                text(
                    """
                    SELECT id
                    FROM user_trial_grants
                    WHERE user_id = :uid AND trial_code = :trial_code
                    LIMIT 1
                    """
                ),
                {"uid": user_id, "trial_code": trial_code},
            ).fetchone()
            if exists:
                return True, "already_granted"

            channel = conn.execute(
                text(
                    """
                    SELECT id
                    FROM content_channels
                    WHERE code = :code AND is_active = 1
                    LIMIT 1
                    """
                ),
                {"code": channel_code},
            ).fetchone()
            if not channel:
                return False, f"channel_not_found:{channel_code}"

            channel_id = int(channel[0])
            ok, msg = _add_subscription_core(
                conn=conn,
                user_id=user_id,
                channel_id=channel_id,
                days=days,
                source_type="trial",
                source_ref=trial_code,
                source_note=source_note,
                operator=operator,
            )
            if not ok:
                return False, msg

            conn.execute(
                text(
                    """
                    INSERT INTO user_trial_grants
                    (user_id, trial_code, channel_id, days, granted_at, source_note, operator)
                    VALUES (:uid, :trial_code, :channel_id, :days, CURRENT_TIMESTAMP, :source_note, :operator)
                    """
                ),
                {
                    "uid": user_id,
                    "trial_code": trial_code,
                    "channel_id": channel_id,
                    "days": days,
                    "source_note": source_note,
                    "operator": operator,
                },
            )
            return True, "trial_granted"
    except IntegrityError:
        return True, "already_granted"
    except Exception as exc:
        print(f"[subscription] grant_trial_failed user={user_id} trial={trial_code} err={exc}")
        return False, "trial_grant_failed"


def update_notification_settings(user_id: str, channel_id: int,
                                 notify_email: bool = None,
                                 notify_site: bool = None) -> bool:
    """更新通知设置"""
    try:
        updates = []
        params = {"uid": user_id, "cid": channel_id}

        if notify_email is not None:
            updates.append("notify_email = :email")
            params["email"] = notify_email
        if notify_site is not None:
            updates.append("notify_site = :site")
            params["site"] = notify_site

        if not updates:
            return True

        with engine.begin() as conn:
            sql = text(f"""
                UPDATE user_subscriptions 
                SET {', '.join(updates)}, updated_at = NOW()
                WHERE user_id = :uid AND channel_id = :cid
            """)
            conn.execute(sql, params)
            return True
    except Exception as e:
        print(f"更新通知设置失败: {e}")
        return False


# =============================================
# 内容管理
# =============================================

def publish_content(channel_code: str, title: str, content: str,
                    summary: str = None) -> tuple:
    """
    发布内容到指定频道

    Returns:
        (success, content_id or error_message)
    """
    try:
        channel = get_channel_by_code(channel_code)
        if not channel:
            return False, f"频道 {channel_code} 不存在"

        with engine.begin() as conn:
            # 插入内容
            sql = text("""
                       INSERT INTO content_items (channel_id, title, summary, content, publish_time, is_published)
                       VALUES (:cid, :title, :summary, :content, NOW(), 1)
                       """)
            result = conn.execute(sql, {
                "cid": channel["id"],
                "title": title,
                "summary": summary or title[:100],
                "content": content
            })
            content_id = result.lastrowid

            # 为所有订阅该频道的用户创建站内消息
            notify_sql = text("""
                              INSERT INTO site_notifications (user_id, channel_id, content_id, title, message, link)
                              SELECT us.user_id,
                                     :cid,
                                     :content_id,
                                     :title,
                                     :summary,
                                     :link
                              FROM user_subscriptions us
                              WHERE us.channel_id = :cid
                                AND us.is_active = 1
                                AND us.notify_site = 1
                                AND (us.expire_at IS NULL OR us.expire_at > NOW())
                              """)
            conn.execute(notify_sql, {
                "cid": channel["id"],
                "content_id": content_id,
                "title": f"{channel['icon']} {title}",
                "summary": summary[:100] if summary else title[:100],
                "link": f"/Subscriptions?content_id={content_id}"
            })

            return True, content_id

    except Exception as e:
        print(f"发布内容失败: {e}")
        return False, str(e)


def get_channel_contents(channel_id: int = None, channel_code: str = None,
                         days: int = 7, limit: int = 50) -> List[Dict]:
    """
    获取频道内容列表

    Args:
        channel_id: 频道ID
        channel_code: 频道code（二选一）
        days: 获取最近N天的内容
        limit: 最大条数
    """
    try:
        with engine.connect() as conn:
            # 构建查询条件
            where_clause = "ci.is_published = 1"
            params = {"days": days, "limit": limit}

            if channel_id:
                where_clause += " AND ci.channel_id = :cid"
                params["cid"] = channel_id
            elif channel_code:
                where_clause += " AND c.code = :code"
                params["code"] = channel_code

            sql = text(f"""
                SELECT 
                    ci.id,
                    ci.channel_id,
                    c.code as channel_code,
                    c.name as channel_name,
                    c.icon as channel_icon,
                    ci.title,
                    ci.summary,
                    ci.content,
                    ci.publish_time
                FROM content_items ci
                JOIN content_channels c ON ci.channel_id = c.id
                WHERE {where_clause}
                  AND ci.publish_time >= DATE_SUB(NOW(), INTERVAL :days DAY)
                ORDER BY ci.publish_time DESC
                LIMIT :limit
            """)

            result = conn.execute(sql, params).fetchall()

            contents = []
            for row in result:
                contents.append({
                    "id": row[0],
                    "channel_id": row[1],
                    "channel_code": row[2],
                    "channel_name": row[3],
                    "channel_icon": row[4],
                    "title": row[5],
                    "summary": row[6],
                    "content": row[7],
                    "publish_time": row[8]
                })
            return contents

    except Exception as e:
        print(f"获取内容列表失败: {e}")
        return []


def get_content_by_id(content_id: int) -> Optional[Dict]:
    """根据ID获取单条内容"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT ci.id,
                              ci.channel_id,
                              c.code as channel_code,
                              c.name as channel_name,
                              c.icon as channel_icon,
                              c.is_premium,
                              ci.title,
                              ci.summary,
                              ci.content,
                              ci.publish_time
                       FROM content_items ci
                                JOIN content_channels c ON ci.channel_id = c.id
                       WHERE ci.id = :id
                         AND ci.is_published = 1
                       """)
            row = conn.execute(sql, {"id": content_id}).fetchone()

            if row:
                return {
                    "id": row[0],
                    "channel_id": row[1],
                    "channel_code": row[2],
                    "channel_name": row[3],
                    "channel_icon": row[4],
                    "is_premium": bool(row[5]),
                    "title": row[6],
                    "summary": row[7],
                    "content": row[8],
                    "publish_time": row[9]
                }
            return None
    except Exception as e:
        print(f"获取内容失败: {e}")
        return None


# =============================================
# 站内消息
# =============================================

def get_unread_count(user_id: str) -> int:
    """获取未读消息数量"""
    try:
        with engine.connect() as conn:
            sql = text("""
                       SELECT COUNT(*)
                       FROM site_notifications
                       WHERE user_id = :uid
                         AND is_read = 0
                       """)
            result = conn.execute(sql, {"uid": user_id}).fetchone()
            return result[0] if result else 0
    except:
        return 0


def get_notifications(user_id: str, limit: int = 20, only_unread: bool = False) -> List[Dict]:
    """获取用户的站内消息"""
    try:
        with engine.connect() as conn:
            where = "user_id = :uid"
            if only_unread:
                where += " AND is_read = 0"

            sql = text(f"""
                SELECT id, channel_id, content_id, title, message, link, is_read, created_at
                FROM site_notifications
                WHERE {where}
                ORDER BY created_at DESC
                LIMIT :limit
            """)
            result = conn.execute(sql, {"uid": user_id, "limit": limit}).fetchall()

            notifications = []
            for row in result:
                notifications.append({
                    "id": row[0],
                    "channel_id": row[1],
                    "content_id": row[2],
                    "title": row[3],
                    "message": row[4],
                    "link": row[5],
                    "is_read": bool(row[6]),
                    "created_at": row[7]
                })
            return notifications
    except Exception as e:
        print(f"获取消息失败: {e}")
        return []


def mark_notification_read(notification_id: int = None, user_id: str = None,
                           mark_all: bool = False) -> bool:
    """标记消息为已读"""
    try:
        with engine.begin() as conn:
            if mark_all and user_id:
                sql = text("UPDATE site_notifications SET is_read = 1 WHERE user_id = :uid")
                conn.execute(sql, {"uid": user_id})
            elif notification_id:
                sql = text("UPDATE site_notifications SET is_read = 1 WHERE id = :id")
                conn.execute(sql, {"id": notification_id})
            return True
    except:
        return False


# =============================================
# 辅助函数
# =============================================

def format_expire_time(expire_at: datetime) -> str:
    """格式化到期时间显示"""
    if not expire_at:
        return "永久有效"

    now = datetime.now()
    if expire_at < now:
        return "已过期"

    delta = expire_at - now
    if delta.days > 30:
        return f"剩余 {delta.days} 天"
    elif delta.days > 0:
        return f"剩余 {delta.days} 天"
    else:
        hours = delta.seconds // 3600
        return f"剩余 {hours} 小时"


def cancel_subscription(user_id: str, channel_id: int) -> bool:
    """取消订阅（将状态设为不活跃）"""
    try:
        with engine.begin() as conn:
            # 将 is_active 设为 0
            sql = text("""
                UPDATE user_subscriptions 
                SET is_active = 0, updated_at = NOW() 
                WHERE user_id = :uid AND channel_id = :cid
            """)
            conn.execute(sql, {"uid": user_id, "cid": channel_id})
            return True
    except Exception as e:
        print(f"取消订阅失败: {e}")
        return False

import redis
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from celery.result import AsyncResult
from chat_routing import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    default_progress_for_chat_mode,
)

load_dotenv(override=True)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


def _read_redis_timeout(env_name: str, default: float) -> float:
    try:
        value = float(str(os.getenv(env_name, default)).strip())
        if value <= 0:
            return float(default)
        return value
    except (TypeError, ValueError):
        return float(default)


REDIS_CONNECT_TIMEOUT_SEC = _read_redis_timeout("TASK_REDIS_CONNECT_TIMEOUT_SEC", 0.15)
REDIS_SOCKET_TIMEOUT_SEC = _read_redis_timeout("TASK_REDIS_SOCKET_TIMEOUT_SEC", 0.20)

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True,
    socket_connect_timeout=REDIS_CONNECT_TIMEOUT_SEC,
    socket_timeout=REDIS_SOCKET_TIMEOUT_SEC,
)

TASK_META_PREFIX = "task_meta:"
USER_TASK_PREFIX = "user_pending_task:"  # 🔥 [新增] 用于存储用户待处理任务
USER_PORTFOLIO_TASK_PREFIX = "user_pending_portfolio_task:"


class TaskManager:

    @staticmethod
    def _store_task_meta(task_meta):
        if not isinstance(task_meta, dict):
            return
        task_id = str(task_meta.get("task_id") or "").strip()
        user_id = str(task_meta.get("user_id") or "").strip()
        if not task_id or not user_id:
            return
        try:
            redis_client.setex(
                f"{TASK_META_PREFIX}{task_id}",
                7200,
                json.dumps(task_meta, ensure_ascii=False),
            )
            redis_client.setex(
                f"{USER_TASK_PREFIX}{user_id}",
                7200,
                json.dumps(task_meta, ensure_ascii=False),
            )
        except Exception as e:
            print(f"⚠️ 保存任务元数据失败: {e}")

    @staticmethod
    def get_task_meta(task_id):
        if not task_id:
            return {}
        try:
            task_data = redis_client.get(f"{TASK_META_PREFIX}{task_id}")
            if not task_data:
                return {}
            meta = json.loads(task_data)
            return meta if isinstance(meta, dict) else {}
        except Exception as e:
            print(f"⚠️ 读取任务元数据失败: {e}")
            return {}

    @staticmethod
    def create_task(
        user_id,
        prompt,
        image_context="",
        risk_preference="稳健型",
        history_messages=None,
        context_payload=None,
        has_portfolio=False,
    ):
        """创建后台任务"""
        # 延迟导入避免循环依赖
        from tasks import process_ai_query

        task = process_ai_query.delay(
            user_id=user_id,
            prompt=prompt,
            image_context=image_context,
            risk_preference=risk_preference,
            history_messages=history_messages or [],
            context_payload=context_payload or {},
            has_portfolio=has_portfolio,
        )

        chat_mode = str((context_payload or {}).get("chat_mode") or CHAT_MODE_ANALYSIS)
        task_meta = {
            "task_id": task.id,
            "user_id": user_id,
            "prompt": prompt,
            "image_context": image_context,
            "risk_preference": risk_preference,
            "context_payload": context_payload or {},
            "status": "pending",
            "chat_mode": chat_mode,
            "delivery_mode": "task",
            "task_type": "analysis",
            "created_at": datetime.now().isoformat(),
            "start_time": datetime.now().timestamp(),
            "progress": default_progress_for_chat_mode(chat_mode, status="pending"),
        }

        TaskManager._store_task_meta(task_meta)

        print(f"✅ 任务已创建: {task.id} | 用户: {user_id}")
        return task.id

    @staticmethod
    def create_knowledge_task(
        user_id,
        prompt,
        risk_preference="稳健型",
        history_messages=None,
        context_payload=None,
    ):
        """创建知识问答后台任务。"""
        from tasks import process_knowledge_chat

        task = process_knowledge_chat.delay(
            user_id=user_id,
            prompt=prompt,
            risk_preference=risk_preference,
            history_messages=history_messages or [],
            context_payload=context_payload or {},
        )

        task_meta = {
            "task_id": task.id,
            "user_id": user_id,
            "prompt": prompt,
            "image_context": "",
            "risk_preference": risk_preference,
            "context_payload": context_payload or {},
            "status": "pending",
            "chat_mode": CHAT_MODE_KNOWLEDGE,
            "delivery_mode": "task",
            "task_type": "knowledge",
            "created_at": datetime.now().isoformat(),
            "start_time": datetime.now().timestamp(),
            "progress": default_progress_for_chat_mode(CHAT_MODE_KNOWLEDGE, status="pending"),
        }

        TaskManager._store_task_meta(task_meta)
        print(f"✅ 知识问答任务已创建: {task.id} | 用户: {user_id}")
        return task.id

    @staticmethod
    def get_task_status(task_id):
        """查询任务状态"""
        # 延迟导入避免循环依赖
        from tasks import process_ai_query

        task = AsyncResult(task_id, app=process_ai_query.app)
        task_meta = TaskManager.get_task_meta(task_id)
        chat_mode = str(task_meta.get("chat_mode") or CHAT_MODE_ANALYSIS)

        if task.state == 'PENDING':
            return {
                "status": "pending",
                "progress": str(task_meta.get("progress") or default_progress_for_chat_mode(chat_mode, status="pending")),
                "result": None,
                "error": None,
                "chat_mode": chat_mode,
                "delivery_mode": str(task_meta.get("delivery_mode") or "task"),
            }
        elif task.state == 'PROCESSING':
            meta = task.info or {}
            return {
                "status": "processing",
                "progress": meta.get('progress', default_progress_for_chat_mode(chat_mode, status="processing")),
                "result": None,
                "error": None,
                "chat_mode": chat_mode,
                "delivery_mode": str(task_meta.get("delivery_mode") or "task"),
            }
        elif task.state == 'SUCCESS':
            return {
                "status": "success",
                "progress": "已完成",
                "result": task.result,
                "error": None,
                "chat_mode": chat_mode,
                "delivery_mode": str(task_meta.get("delivery_mode") or "task"),
            }
        elif task.state == 'FAILURE':
            return {
                "status": "error",
                "progress": "任务失败",
                "result": None,
                "error": str(task.info),
                "chat_mode": chat_mode,
                "delivery_mode": str(task_meta.get("delivery_mode") or "task"),
            }
        else:
            return {
                "status": "unknown",
                "progress": f"未知状态: {task.state}",
                "result": None,
                "error": None,
                "chat_mode": chat_mode,
                "delivery_mode": str(task_meta.get("delivery_mode") or "task"),
            }

    @staticmethod
    def get_user_pending_task(user_id):
        """
        🔥 [新增] 获取用户的待处理任务
        用于在 Session State 丢失后恢复任务
        """
        key = f"{USER_TASK_PREFIX}{user_id}"
        try:
            task_data = redis_client.get(key)
            if task_data:
                task_meta = json.loads(task_data)
                print(f"✅ 从 Redis 恢复任务: {task_meta['task_id']} | 用户: {user_id}")
                return task_meta
        except Exception as e:
            print(f"❌ 恢复任务失败: {e}")
            return None
        return None

    @staticmethod
    def clear_user_pending_task(user_id):
        """
        🔥 [新增] 清除用户的待处理任务
        任务完成或失败后调用
        """
        key = f"{USER_TASK_PREFIX}{user_id}"
        try:
            redis_client.delete(key)
            print(f"🗑️ 已清除用户待处理任务: {user_id}")
        except Exception as e:
            print(f"⚠️ 清理用户待处理任务失败: {e}")

    @staticmethod
    def create_portfolio_task(
        user_id,
        positions,
        screenshot_hash="",
        source_text="",
    ):
        """创建持仓分析后台任务。"""
        from tasks import process_portfolio_snapshot_task

        task = process_portfolio_snapshot_task.delay(
            user_id=user_id,
            positions=positions or [],
            screenshot_hash=screenshot_hash or "",
            source_text=source_text or "",
        )

        task_meta = {
            "task_id": task.id,
            "task_type": "portfolio",
            "user_id": user_id,
            "positions_count": len(positions or []),
            "screenshot_hash": screenshot_hash or "",
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "start_time": datetime.now().timestamp(),
        }

        redis_client.setex(
            f"{TASK_META_PREFIX}{task.id}",
            7200,
            json.dumps(task_meta, ensure_ascii=False),
        )
        redis_client.setex(
            f"{USER_PORTFOLIO_TASK_PREFIX}{user_id}",
            7200,
            json.dumps(task_meta, ensure_ascii=False),
        )

        print(f"✅ 持仓任务已创建: {task.id} | 用户: {user_id}")
        return task.id

    @staticmethod
    def get_user_pending_portfolio_task(user_id):
        """获取用户待处理持仓任务。"""
        key = f"{USER_PORTFOLIO_TASK_PREFIX}{user_id}"
        task_data = redis_client.get(key)
        if task_data:
            try:
                task_meta = json.loads(task_data)
                print(f"✅ 从 Redis 恢复持仓任务: {task_meta['task_id']} | 用户: {user_id}")
                return task_meta
            except Exception as e:
                print(f"❌ 恢复持仓任务失败: {e}")
                return None
        return None

    @staticmethod
    def clear_user_pending_portfolio_task(user_id):
        """清除用户待处理持仓任务。"""
        key = f"{USER_PORTFOLIO_TASK_PREFIX}{user_id}"
        redis_client.delete(key)
        print(f"🗑️ 已清除用户待处理持仓任务: {user_id}")

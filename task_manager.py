import redis
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4
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
USER_ACTIVE_TASK_PREFIX = "user_active_task:"
USER_TASK_QUEUE_PREFIX = "user_task_queue:"
TASK_META_TTL_SEC = 7200
USER_MAX_QUEUED_TASKS = 2


class UserTaskQueueFullError(RuntimeError):
    """Raised when a user already has too many queued chat tasks."""

    def __init__(self, active_count: int, queued_count: int, max_queued: int):
        self.active_count = int(active_count)
        self.queued_count = int(queued_count)
        self.max_queued = int(max_queued)
        super().__init__(f"user queue full: active={active_count} queued={queued_count} max_queued={max_queued}")


class TaskManager:

    @staticmethod
    def _active_task_key(user_id: str) -> str:
        return f"{USER_ACTIVE_TASK_PREFIX}{user_id}"

    @staticmethod
    def _queue_key(user_id: str) -> str:
        return f"{USER_TASK_QUEUE_PREFIX}{user_id}"

    @staticmethod
    def _read_json(key: str, default: Any):
        try:
            raw = redis_client.get(key)
            if not raw:
                return default
            parsed = json.loads(raw)
            return parsed
        except Exception as e:
            print(f"⚠️ 读取 Redis JSON 失败 key={key}: {e}")
            return default

    @staticmethod
    def _write_json(key: str, payload: Any):
        try:
            redis_client.setex(
                key,
                TASK_META_TTL_SEC,
                json.dumps(payload, ensure_ascii=False),
            )
        except Exception as e:
            print(f"⚠️ 写入 Redis JSON 失败 key={key}: {e}")

    @staticmethod
    def _delete_key(key: str):
        try:
            redis_client.delete(key)
        except Exception as e:
            print(f"⚠️ 删除 Redis key 失败 key={key}: {e}")

    @staticmethod
    def _load_queue_ids(user_id: str) -> List[str]:
        queue_ids = TaskManager._read_json(TaskManager._queue_key(user_id), [])
        if not isinstance(queue_ids, list):
            return []
        return [str(task_id).strip() for task_id in queue_ids if str(task_id).strip()]

    @staticmethod
    def _save_queue_ids(user_id: str, queue_ids: List[str]):
        clean_ids = [str(task_id).strip() for task_id in queue_ids if str(task_id).strip()]
        if clean_ids:
            TaskManager._write_json(TaskManager._queue_key(user_id), clean_ids)
        else:
            TaskManager._delete_key(TaskManager._queue_key(user_id))

    @staticmethod
    def _get_active_task_id(user_id: str) -> str:
        try:
            return str(redis_client.get(TaskManager._active_task_key(user_id)) or "").strip()
        except Exception as e:
            print(f"⚠️ 读取活跃任务失败 user={user_id}: {e}")
            return ""

    @staticmethod
    def _set_active_task_id(user_id: str, task_id: str):
        try:
            redis_client.setex(TaskManager._active_task_key(user_id), TASK_META_TTL_SEC, str(task_id).strip())
        except Exception as e:
            print(f"⚠️ 写入活跃任务失败 user={user_id}: {e}")

    @staticmethod
    def _refresh_user_pending_alias(user_id: str):
        active_task_id = TaskManager._get_active_task_id(user_id)
        active_meta = TaskManager.get_task_meta(active_task_id) if active_task_id else {}
        if active_meta:
            TaskManager._write_json(f"{USER_TASK_PREFIX}{user_id}", active_meta)
            return

        queue_ids = TaskManager._load_queue_ids(user_id)
        for queue_task_id in queue_ids:
            queue_meta = TaskManager.get_task_meta(queue_task_id)
            if queue_meta:
                TaskManager._write_json(f"{USER_TASK_PREFIX}{user_id}", queue_meta)
                return

        TaskManager._delete_key(f"{USER_TASK_PREFIX}{user_id}")

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
                TASK_META_TTL_SEC,
                json.dumps(task_meta, ensure_ascii=False),
            )
        except Exception as e:
            print(f"⚠️ 保存任务元数据失败: {e}")
            return
        TaskManager._refresh_user_pending_alias(user_id)

    @staticmethod
    def _build_analysis_dispatch_payload(
        user_id,
        prompt,
        image_context="",
        risk_preference="稳健型",
        history_messages=None,
        context_payload=None,
        has_portfolio=False,
    ) -> Dict[str, Any]:
        return {
            "user_id": user_id,
            "prompt": prompt,
            "image_context": image_context,
            "risk_preference": risk_preference,
            "history_messages": history_messages or [],
            "context_payload": context_payload or {},
            "has_portfolio": has_portfolio,
        }

    @staticmethod
    def _build_knowledge_dispatch_payload(
        user_id,
        prompt,
        risk_preference="稳健型",
        history_messages=None,
        context_payload=None,
    ) -> Dict[str, Any]:
        return {
            "user_id": user_id,
            "prompt": prompt,
            "risk_preference": risk_preference,
            "history_messages": history_messages or [],
            "context_payload": context_payload or {},
        }

    @staticmethod
    def _dispatch_task(task_meta: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(task_meta, dict):
            return {}
        task_type = str(task_meta.get("task_type") or "analysis").strip().lower()
        task_id = str(task_meta.get("task_id") or "").strip()
        user_id = str(task_meta.get("user_id") or "").strip()
        payload = task_meta.get("dispatch_payload") or {}
        if not task_id or not user_id or not isinstance(payload, dict):
            return task_meta

        if task_type == "knowledge":
            from tasks import process_knowledge_chat

            process_knowledge_chat.apply_async(kwargs=payload, task_id=task_id)
        else:
            from tasks import process_ai_query

            process_ai_query.apply_async(kwargs=payload, task_id=task_id)

        task_meta = dict(task_meta)
        task_meta["status"] = "pending"
        task_meta["start_time"] = datetime.now().timestamp()
        task_meta["dispatched_at"] = datetime.now().isoformat()
        TaskManager._set_active_task_id(user_id, task_id)
        TaskManager._store_task_meta(task_meta)
        print(f"🚀 任务已下发执行: {task_id} | 用户: {user_id} | 类型: {task_type}")
        return task_meta

    @staticmethod
    def _create_or_enqueue_task(task_meta: Dict[str, Any]) -> str:
        user_id = str(task_meta.get("user_id") or "").strip()
        if not user_id:
            return str(task_meta.get("task_id") or "").strip()

        active_task_id = TaskManager._get_active_task_id(user_id)
        queue_ids = TaskManager._load_queue_ids(user_id)

        if not active_task_id and queue_ids:
            next_task_id = queue_ids.pop(0)
            TaskManager._save_queue_ids(user_id, queue_ids)
            next_meta = TaskManager.get_task_meta(next_task_id)
            if next_meta:
                TaskManager._dispatch_task(next_meta)
                active_task_id = next_task_id
            queue_ids = TaskManager._load_queue_ids(user_id)

        if active_task_id:
            if len(queue_ids) >= USER_MAX_QUEUED_TASKS:
                raise UserTaskQueueFullError(active_count=1, queued_count=len(queue_ids), max_queued=USER_MAX_QUEUED_TASKS)

            task_meta = dict(task_meta)
            task_meta["status"] = "queued"
            task_meta["queued_at"] = datetime.now().isoformat()
            TaskManager._store_task_meta(task_meta)
            queue_ids.append(str(task_meta["task_id"]))
            TaskManager._save_queue_ids(user_id, queue_ids)
            TaskManager._refresh_user_pending_alias(user_id)
            print(f"🕒 任务已入队: {task_meta['task_id']} | 用户: {user_id} | 前方任务数: {len(queue_ids)}")
            return str(task_meta["task_id"])

        task_meta = dict(task_meta)
        TaskManager._dispatch_task(task_meta)
        return str(task_meta["task_id"])

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
        """创建后台任务；若当前用户已有 active task，则先进入用户级队列。"""
        chat_mode = str((context_payload or {}).get("chat_mode") or CHAT_MODE_ANALYSIS)
        task_id = str(uuid4())
        task_meta = {
            "task_id": task_id,
            "user_id": user_id,
            "prompt": prompt,
            "image_context": image_context,
            "risk_preference": risk_preference,
            "context_payload": context_payload or {},
            "status": "queued",
            "chat_mode": chat_mode,
            "delivery_mode": "task",
            "task_type": "analysis",
            "created_at": datetime.now().isoformat(),
            "start_time": 0.0,
            "progress": default_progress_for_chat_mode(chat_mode, status="pending"),
            "dispatch_payload": TaskManager._build_analysis_dispatch_payload(
                user_id=user_id,
                prompt=prompt,
                image_context=image_context,
                risk_preference=risk_preference,
                history_messages=history_messages,
                context_payload=context_payload,
                has_portfolio=has_portfolio,
            ),
        }
        task_id = TaskManager._create_or_enqueue_task(task_meta)
        print(f"✅ 任务已创建: {task_id} | 用户: {user_id}")
        return task_id

    @staticmethod
    def create_knowledge_task(
        user_id,
        prompt,
        risk_preference="稳健型",
        history_messages=None,
        context_payload=None,
    ):
        """创建知识问答后台任务；若当前用户已有 active task，则先进入用户级队列。"""
        task_id = str(uuid4())
        task_meta = {
            "task_id": task_id,
            "user_id": user_id,
            "prompt": prompt,
            "image_context": "",
            "risk_preference": risk_preference,
            "context_payload": context_payload or {},
            "status": "queued",
            "chat_mode": CHAT_MODE_KNOWLEDGE,
            "delivery_mode": "task",
            "task_type": "knowledge",
            "created_at": datetime.now().isoformat(),
            "start_time": 0.0,
            "progress": default_progress_for_chat_mode(CHAT_MODE_KNOWLEDGE, status="pending"),
            "dispatch_payload": TaskManager._build_knowledge_dispatch_payload(
                user_id=user_id,
                prompt=prompt,
                risk_preference=risk_preference,
                history_messages=history_messages,
                context_payload=context_payload,
            ),
        }
        task_id = TaskManager._create_or_enqueue_task(task_meta)
        print(f"✅ 知识问答任务已创建: {task_id} | 用户: {user_id}")
        return task_id

    @staticmethod
    def get_task_status(task_id):
        """查询任务状态"""
        task_meta = TaskManager.get_task_meta(task_id)
        chat_mode = str(task_meta.get("chat_mode") or CHAT_MODE_ANALYSIS)
        task_status = str(task_meta.get("status") or "").strip().lower()
        if task_status == "queued":
            user_id = str(task_meta.get("user_id") or "").strip()
            active_task_id = TaskManager._get_active_task_id(user_id)
            queue_ids = TaskManager._load_queue_ids(user_id)
            queue_ahead = 0
            if task_id in queue_ids:
                queue_ahead = queue_ids.index(task_id) + (1 if active_task_id else 0)
            progress_msg = f"排队中，前面还有 {queue_ahead} 个问题" if queue_ahead > 0 else "排队中，等待开始处理..."
            return {
                "status": "queued",
                "progress": progress_msg,
                "result": None,
                "error": None,
                "chat_mode": chat_mode,
                "delivery_mode": str(task_meta.get("delivery_mode") or "task"),
                "queue_ahead": queue_ahead,
            }

        # 延迟导入避免循环依赖
        from tasks import process_ai_query

        task = AsyncResult(task_id, app=process_ai_query.app)

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
        TaskManager._delete_key(f"{USER_TASK_PREFIX}{user_id}")
        TaskManager._delete_key(TaskManager._active_task_key(user_id))
        TaskManager._delete_key(TaskManager._queue_key(user_id))
        print(f"🗑️ 已清除用户待处理任务: {user_id}")

    @staticmethod
    def get_user_task_queue(user_id: str) -> List[Dict[str, Any]]:
        active_task_id = TaskManager._get_active_task_id(user_id)
        queue_ids = TaskManager._load_queue_ids(user_id)
        queue_snapshot: List[Dict[str, Any]] = []

        if active_task_id:
            active_meta = TaskManager.get_task_meta(active_task_id)
            if active_meta:
                active_meta = dict(active_meta)
                active_meta["queue_state"] = "active"
                active_meta["queue_ahead"] = 0
                queue_snapshot.append(active_meta)

        for idx, queued_task_id in enumerate(queue_ids):
            queue_meta = TaskManager.get_task_meta(queued_task_id)
            if not queue_meta:
                continue
            queue_meta = dict(queue_meta)
            queue_meta["queue_state"] = "queued"
            queue_meta["queue_ahead"] = idx + (1 if active_task_id else 0)
            queue_snapshot.append(queue_meta)

        return queue_snapshot

    @staticmethod
    def remove_user_task(user_id: str, task_id: str) -> Optional[Dict[str, Any]]:
        active_task_id = TaskManager._get_active_task_id(user_id)
        removed_task_id = str(task_id).strip()
        removed_active = False
        if active_task_id and active_task_id == removed_task_id:
            TaskManager._delete_key(TaskManager._active_task_key(user_id))
            removed_active = True

        queue_ids = TaskManager._load_queue_ids(user_id)
        if removed_task_id in queue_ids:
            queue_ids = [queued_task_id for queued_task_id in queue_ids if queued_task_id != removed_task_id]
            TaskManager._save_queue_ids(user_id, queue_ids)

        promoted_meta = None
        if removed_active and not TaskManager._get_active_task_id(user_id):
            queue_ids = TaskManager._load_queue_ids(user_id)
            if queue_ids:
                next_task_id = queue_ids.pop(0)
                TaskManager._save_queue_ids(user_id, queue_ids)
                next_meta = TaskManager.get_task_meta(next_task_id)
                if next_meta:
                    promoted_meta = TaskManager._dispatch_task(next_meta)

        TaskManager._refresh_user_pending_alias(user_id)
        return promoted_meta

    @staticmethod
    def complete_user_task(user_id: str, task_id: str) -> Optional[Dict[str, Any]]:
        return TaskManager.remove_user_task(user_id, task_id)

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

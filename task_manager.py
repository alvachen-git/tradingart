import redis
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from celery.result import AsyncResult

load_dotenv(override=True)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.from_url(REDIS_URL, decode_responses=True)

TASK_META_PREFIX = "task_meta:"
USER_TASK_PREFIX = "user_pending_task:"  # 🔥 [新增] 用于存储用户待处理任务


class TaskManager:

    @staticmethod
    def create_task(
        user_id,
        prompt,
        image_context="",
        risk_preference="稳健型",
        history_messages=None,
        context_payload=None,
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
        )

        task_meta = {
            "task_id": task.id,
            "user_id": user_id,
            "prompt": prompt,
            "image_context": image_context,
            "risk_preference": risk_preference,
            "context_payload": context_payload or {},
            "status": "pending",
            "created_at": datetime.now().isoformat(),
            "start_time": datetime.now().timestamp()  # 🔥 [新增] 用于超时检查
        }

        # 保存任务元数据
        redis_client.setex(
            f"{TASK_META_PREFIX}{task.id}",
            7200,  # 1小时过期
            json.dumps(task_meta, ensure_ascii=False)
        )

        # 🔥 [新增] 保存用户的待处理任务（用于恢复）
        redis_client.setex(
            f"{USER_TASK_PREFIX}{user_id}",
            7200,  # 1小时过期
            json.dumps(task_meta, ensure_ascii=False)
        )

        print(f"✅ 任务已创建: {task.id} | 用户: {user_id}")
        return task.id

    @staticmethod
    def get_task_status(task_id):
        """查询任务状态"""
        # 延迟导入避免循环依赖
        from tasks import process_ai_query

        task = AsyncResult(task_id, app=process_ai_query.app)

        if task.state == 'PENDING':
            return {
                "status": "pending",
                "progress": "任务排队中...",
                "result": None,
                "error": None
            }
        elif task.state == 'PROCESSING':
            meta = task.info or {}
            return {
                "status": "processing",
                "progress": meta.get('progress', '正在处理...'),
                "result": None,
                "error": None
            }
        elif task.state == 'SUCCESS':
            return {
                "status": "success",
                "progress": "已完成",
                "result": task.result,
                "error": None
            }
        elif task.state == 'FAILURE':
            return {
                "status": "error",
                "progress": "任务失败",
                "result": None,
                "error": str(task.info)
            }
        else:
            return {
                "status": "unknown",
                "progress": f"未知状态: {task.state}",
                "result": None,
                "error": None
            }

    @staticmethod
    def get_user_pending_task(user_id):
        """
        🔥 [新增] 获取用户的待处理任务
        用于在 Session State 丢失后恢复任务
        """
        key = f"{USER_TASK_PREFIX}{user_id}"
        task_data = redis_client.get(key)

        if task_data:
            try:
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
        redis_client.delete(key)
        print(f"🗑️ 已清除用户待处理任务: {user_id}")

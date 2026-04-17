# celery_config.py
import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv(override=True)


def _configure_langsmith_tracing() -> None:
    """
    Default to disable LangSmith tracing for the main Celery worker.
    Set ENABLE_LANGSMITH_TRACING=1 to opt in explicitly.
    """
    enabled = str(os.getenv("ENABLE_LANGSMITH_TRACING", "0")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if enabled:
        return
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"
    os.environ["LANGCHAIN_CALLBACKS_BACKGROUND"] = "false"


_configure_langsmith_tracing()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

celery_app = Celery(
    'trading_ai_tasks',
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=['tasks']
)

celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Shanghai',
    enable_utc=True,
    result_expires=3600,
    task_time_limit=1800,
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=10,
)

print(f"✅ Celery 配置完成: {REDIS_URL}")

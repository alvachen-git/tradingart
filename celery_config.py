# celery_config.py
import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv(override=True)

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
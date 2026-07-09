"""
mobile_api.py — 爱波塔手机端专用 FastAPI 后端

启动命令:
    uvicorn mobile_api:app --host 0.0.0.0 --port 8001

注意：不要加 --workers N，本服务含后台线程（实时价格刷新）和全局缓存，
      多进程会导致每个 worker 各跑一份线程且状态不共享，Windows 下 Ctrl+C 也无法正常退出。

端点汇总:
  GET    /api/health                    健康检查
  POST   /api/auth/login                密码登录
  POST   /api/auth/login/email          邮箱验证码登录
  POST   /api/auth/register/send-phone-code    注册短信验证码
  POST   /api/auth/register/verify-phone-code  校验注册短信验证码
  POST   /api/auth/register                    账号注册（手机号验证）
  POST   /api/auth/password-reset/send-phone-code  忘记密码短信验证码
  POST   /api/auth/password-reset                  手机号重置密码
  POST   /api/auth/logout               登出当前设备
  GET    /api/auth/verify               验证 Token

  GET    /api/device/ping               设备链路健康检查
  GET    /api/device/config             设备端行为配置
  GET    /api/device/briefing           设备简报（StackChan v1）
  GET    /api/device/contracts/menu      设备合约菜单（StackChan v1.1）
  GET    /api/device/contracts/briefing  设备单合约 IV 看板（StackChan v1.1）
  POST   /api/device/voice/query       设备语音问答（StackChan v2）
  WS     /api/device/voice/realtime    设备实时语音协议原型（StackChan v3 research）
  GET    /api/device/voice/audio/{voice_id} 设备语音回答音频
  GET    /api/device/voice/audio-prompt/{prompt_key} 设备固定提示音频
  GET    /api/device/voice/task/{task_id} 设备深度分析任务状态

  POST   /api/chat/submit               提交 AI 问答任务
  GET    /api/chat/status/{task_id}     轮询 AI 任务状态
  GET    /api/chat/pending              获取最近聊天任务恢复态
  POST   /api/chat/cancel               取消聊天任务

  GET    /api/intel/reports             获取情报站晚报列表（支持分页/频道筛选）
  GET    /api/intel/ai/overview         获取 AI炒股总览（KPI/曲线/持仓/交易/复盘）
  GET    /api/intel/ai/review           获取 AI炒股单日复盘（支持 trade_date）
  GET    /api/intel/report/{id}         获取单篇晚报完整内容
  POST   /api/intel/subscribe           订阅白名单频道
  POST   /api/alipay/notify             支付宝异步回调通知（验签+到账）

  GET    /api/pay/wallet                点数钱包信息
  GET    /api/pay/packages              充值套餐列表
  GET    /api/pay/products              付费产品列表
  POST   /api/pay/purchase              使用点数购买权限
  GET    /api/pay/config                充值中心配置（外部链接）

  GET    /api/market/snapshot           综合行情快照
  GET    /api/market/term-structure/products  期限结构品种与窗口
  GET    /api/market/term-structure     期限结构数据（含股指升贴水）
  GET    /api/us-options/products       美股期权标的池
  GET    /api/us-options/overview       美股期权单标的总览
  GET    /api/us-options/surface        美股期权波动率曲面
  GET    /api/us-options/defense        美股期权持仓防线
  GET    /api/us-options/anomalies      美股期权异动观察

  POST   /api/position/upload           上传持仓截图 → 自动分流(股票体检/期权分析)
  POST   /api/portfolio/upload          上传股票持仓截图 → 识别 → 提交体检
  GET    /api/portfolio/status/{id}     轮询持仓分析进度
  GET    /api/portfolio/result          获取最新持仓体检结果

  GET    /api/user/profile              获取用户资料与订阅状态
"""

import base64
import difflib
import hashlib
import html as html_lib
import io
import json
import math
import os
import platform
import re
import shutil
import struct
import subprocess
import sys
import threading
import time
import uuid
import wave
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from typing import Optional, List, Any, Dict, Tuple, Mapping

import redis
from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Form, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from dotenv import load_dotenv

try:
    _env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(_env_path):
        load_dotenv(dotenv_path=_env_path, override=True)
    else:
        load_dotenv(override=True)
except Exception as exc:
    print(f"[mobile_api] load_dotenv_failed err={exc}", flush=True)

if str(os.getenv("ENABLE_LANGSMITH_TRACING", "")).strip().lower() not in {"1", "true", "yes", "on"}:
    os.environ["LANGCHAIN_TRACING_V2"] = "false"
    os.environ["LANGSMITH_TRACING"] = "false"

from llm_compat import ChatTongyiCompat as ChatTongyi, build_deepseek_flash_llm

# 确保同目录模块可以 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import auth_utils as auth
from task_manager import TaskManager, UserTaskQueueFullError
from agent_core import simple_chatter_reply
from chat_routing import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    CHAT_MODE_SIMPLE,
    classify_chat_mode,
    default_progress_for_chat_mode,
    is_broker_signal_analysis_query,
)
from chat_context_utils import (
    build_followup_action_context as _build_followup_action_context,
    build_topic_anchors as _build_topic_anchors,
    extract_focus_aspect as _shared_extract_focus_aspect,
    extract_focus_entity as _shared_extract_focus_entity,
    infer_correction_intent as _infer_correction_intent,
    infer_conversation_memory_window as _infer_conversation_memory_window,
    infer_followup_intent as _infer_followup_intent,
    infer_followup_goal as _infer_followup_goal,
    infer_focus_topic as _infer_focus_topic,
    infer_lookup_followup_intent as _infer_lookup_followup_intent,
    is_semantically_related as _shared_is_semantically_related,
    select_target_anchor as _select_target_anchor,
    should_preserve_recent_context as _should_preserve_recent_context,
)
from chat_context_layers import attach_context_layers
from followup_task_policy import (
    build_followup_route_context as _build_followup_route_context,
    classify_followup_task_policy as _classify_followup_task_policy,
)
import subscription_service as sub_svc
import payment_service as pay_svc
import data_engine as de
from user_profile_memory import build_profile_memory_context
from ai_simulation_service import (
    OFFICIAL_PORTFOLIO_ID,
    OFFICIAL_PORTFOLIO_3_ID,
    compute_sharpe_ratio_from_nav as ai_compute_sharpe_ratio_from_nav,
    get_closed_trade_extremes as ai_get_closed_trade_extremes,
    get_daily_review as ai_get_daily_review,
    get_latest_snapshot as ai_get_latest_snapshot,
    get_nav_series as ai_get_nav_series,
    get_positions as ai_get_positions,
    get_review_dates as ai_get_review_dates,
    get_trades as ai_get_trades,
)
from term_structure_service import (
    WINDOW_LABELS as TERM_WINDOW_LABELS,
    build_index_basis_longterm_payload,
    build_index_basis_term_structure_payload,
    build_term_structure_payload,
)
from us_market_dashboard_data import (
    DEFAULT_DASHBOARD_UNDERLYINGS as US_OPTION_DEFAULT_UNDERLYINGS,
    UNDERLYING_DISPLAY_NAMES as US_OPTION_DISPLAY_NAMES,
    build_underlying_profile_card as us_build_underlying_profile_card,
    calculate_atm_iv_pct as us_calculate_atm_iv_pct,
    calculate_overview_metrics_from_market_history as us_calculate_overview_metrics_from_market_history,
    calculate_volatility_positioning_metrics as us_calculate_volatility_positioning_metrics,
    get_underlying_profile as us_get_underlying_profile,
    load_available_option_trade_dates as us_load_available_option_trade_dates,
    load_iv_history as us_load_iv_history,
    load_latest_option_trade_date as us_load_latest_option_trade_date,
    load_market_metrics_history as us_load_market_metrics_history,
    load_oi_defense_history as us_load_oi_defense_history,
    load_option_anomaly_scan as us_load_option_anomaly_scan,
    load_option_chain_daily as us_load_option_chain_daily,
    load_option_chain_summary as us_load_option_chain_summary,
    load_otm_volatility_curve_snapshot as us_load_otm_volatility_curve_snapshot,
    load_stock_daily as us_load_stock_daily,
    load_volatility_cone_history as us_load_volatility_cone_history,
    load_volatility_cone_line_snapshot as us_load_volatility_cone_line_snapshot,
    selected_underlying_price as us_selected_underlying_price,
    summarize_option_chain as us_summarize_option_chain,
)
from us_options_ai_tools import normalize_us_option_underlying
from chat_feedback_service import (
    CHAT_FEEDBACK_ALLOWED_TYPES as _CHAT_FEEDBACK_ALLOWED_TYPES,
    CHAT_FEEDBACK_REASON_CODES as _CHAT_FEEDBACK_REASON_CODES,
    CHAT_FEEDBACK_SAMPLE_OPTIMIZATION_TYPES as _CHAT_FEEDBACK_SAMPLE_OPTIMIZATION_TYPES,
    CHAT_FEEDBACK_SAMPLE_STATUSES as _CHAT_FEEDBACK_SAMPLE_STATUSES,
    ensure_chat_feedback_tables,
    get_chat_feedback_sample,
    generate_chat_answer_id,
    generate_chat_trace_id,
    get_chat_answer_event,
    list_chat_feedback_events,
    list_chat_feedback_failure_candidates,
    list_chat_feedback_samples,
    save_chat_feedback_event,
    save_chat_answer_event,
    submit_chat_feedback,
    update_chat_feedback_sample,
    upsert_chat_feedback_sample,
)
from vision_tools import analyze_portfolio_image, analyze_position_image
from mobile_trading_day import enrich_prices_payload_with_trading_day
from simple_chat_runtime import build_simple_runtime_context

# ── Redis ─────────────────────────────────────────────────────
_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
_redis = redis.from_url(_REDIS_URL, decode_responses=True)
_MOBILE_CHAT_PROMPT_KEY_PREFIX = "mobile:chat:raw_prompt:"
_MOBILE_CHAT_PROMPT_TTL = 7200
_MOBILE_CHAT_MEMORY_QUEUE_KEY_PREFIX = "mobile:chat:memory:queued:"
_MOBILE_CHAT_MEMORY_QUEUE_TTL = 7200
_MOBILE_CHAT_STATE_KEY_PREFIX = "mobile:chat:state:"
_MOBILE_CHAT_RESULT_KEY_PREFIX = "mobile:chat:result:"
_MOBILE_CHAT_LAST_TASK_KEY_PREFIX = "mobile:user:last_task:"
_MOBILE_CHAT_RESULT_TTL_SECONDS = int(str(os.getenv("MOBILE_CHAT_RESULT_TTL_SECONDS", "86400")).strip() or 86400)
_MOBILE_CHAT_MAX_PENDING_SECONDS = int(str(os.getenv("MOBILE_CHAT_MAX_PENDING_SECONDS", "400")).strip() or 400)
_MOBILE_CHAT_BACKGROUND_MAX_PENDING_SECONDS = int(
    str(os.getenv("MOBILE_CHAT_BACKGROUND_MAX_PENDING_SECONDS", "3600")).strip() or 3600
)
_CHAT_FEEDBACK_SCHEMA_LOCK = threading.Lock()
_CHAT_FEEDBACK_SCHEMA_READY = False
_CHAT_FEEDBACK_SCHEMA_ENGINE_ID = ""
_CHAT_FEEDBACK_DEFAULT_ADMIN_USERS = {"mike0919"}
_CHAT_FEEDBACK_ENV_ADMIN_USERS = {
    item.strip() for item in str(os.getenv("AI_FEEDBACK_ADMIN_USERS", "")).split(",") if item.strip()
}
_CHAT_FEEDBACK_ADMIN_USERS = _CHAT_FEEDBACK_DEFAULT_ADMIN_USERS | _CHAT_FEEDBACK_ENV_ADMIN_USERS
_DEVICE_AUTO_POLL_ENABLED_DEFAULT = (
    str(os.getenv("DEVICE_AUTO_POLL_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
)
_DEVICE_AUTO_POLL_SECONDS_DEFAULT = int(str(os.getenv("DEVICE_AUTO_POLL_SECONDS", "180")).strip() or 180)
_DEVICE_AUTO_POLL_MIN_SECONDS = 60
_DEVICE_VOICE_ENABLED_DEFAULT = (
    str(os.getenv("DEVICE_VOICE_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
)
_DEVICE_VOICE_MODE_DEFAULT = "tap_to_wake"
_DEVICE_VOICE_RECORD_MAX_SECONDS = 8
_DEVICE_VOICE_SAMPLE_RATE = 16000
_DEVICE_VOICE_CHANNELS = 1
_DEVICE_VOICE_BITS_PER_SAMPLE = 16
_DEVICE_VOICE_MAX_WAV_BYTES = 44 + (_DEVICE_VOICE_RECORD_MAX_SECONDS * _DEVICE_VOICE_SAMPLE_RATE * 2)
_DEVICE_VOICE_REALTIME_MAX_PCM_BYTES = _DEVICE_VOICE_RECORD_MAX_SECONDS * _DEVICE_VOICE_SAMPLE_RATE * 2
_DEVICE_VOICE_REALTIME_FOLLOWUP_WINDOW_SECONDS = 8
_DEVICE_VOICE_REALTIME_TTS_CHUNK_BYTES = 4096
_DEVICE_VOICE_REALTIME_TTS_AUDIO_DELTA_ENABLED = (
    str(os.getenv("DEVICE_REALTIME_TTS_AUDIO_DELTA_ENABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}
)
_DEVICE_VOICE_MIN_CLEAR_AUDIO_MS = 500
_DEVICE_VOICE_LOW_PEAK_THRESHOLD = 600
_DEVICE_VOICE_LOW_RMS_THRESHOLD = 60.0
_DEVICE_VOICE_AUDIO_CACHE_TTL_SECONDS = 600
_DEVICE_VOICE_AUDIO_DISK_CACHE_TTL_SECONDS = max(
    600,
    int(str(os.getenv("DEVICE_VOICE_AUDIO_DISK_CACHE_TTL_SECONDS", "3600")).strip() or 3600),
)
_DEVICE_VOICE_AUDIO_DISK_CACHE_DIR = os.getenv(
    "DEVICE_VOICE_AUDIO_DISK_CACHE_DIR",
    "/tmp/tradingart_device_voice_cache",
)
_DEVICE_VOICE_AUDIO_CACHE: Dict[str, Dict[str, Any]] = {}
_DEVICE_VOICE_TEXT_AUDIO_CACHE: Dict[str, Dict[str, Any]] = {}
_DEVICE_VOICE_AUDIO_CACHE_LOCK = threading.Lock()
_DEVICE_TTS_VOLUME_GAIN_DEFAULT = 1.5
_DEVICE_TTS_TARGET_PEAK = 22000
_DEVICE_TTS_HARD_PEAK = 28000
_DEVICE_VOICE_TASK_POLL_SECONDS = max(
    1,
    int(str(os.getenv("DEVICE_VOICE_TASK_POLL_SECONDS", "2")).strip() or 2),
)
_DEVICE_VOICE_TASK_MAX_WAIT_SECONDS = max(
    300,
    _DEVICE_VOICE_TASK_POLL_SECONDS,
    int(str(os.getenv("DEVICE_VOICE_TASK_MAX_WAIT_SECONDS", "900")).strip() or 900),
)
_DEVICE_VOICE_TASK_WORKER_GRACE_SECONDS = max(
    20,
    int(str(os.getenv("DEVICE_VOICE_TASK_WORKER_GRACE_SECONDS", "35")).strip() or 35),
)
_DEVICE_VOICE_TASK_LOST_GRACE_SECONDS = max(
    45,
    int(str(os.getenv("DEVICE_VOICE_TASK_LOST_GRACE_SECONDS", "60")).strip() or 60),
)
_DEVICE_VOICE_LAST_TASK_TTL_SECONDS = 7200
_DEVICE_VOICE_LAST_TASK_PREFIX = "device_voice:last_task:"
_DEVICE_VOICE_LATENCY_OBSERVATION_ENABLED = (
    str(os.getenv("DEVICE_VOICE_LATENCY_OBSERVATION_ENABLED", "1")).strip().lower() in {"1", "true", "yes", "on"}
)

# ════════════════════════════════════════════════════════════
#  实时行情后台刷新 — 直连新浪行情接口（绕过 akshare 封装层）
# ════════════════════════════════════════════════════════════

_PRICES_KEY = "mobile:futures:prices"
_PRICES_TTL = 30  # seconds
_PRICES_REFRESH_LOCK_KEY = "mobile:futures:prices:refresh:lock"
_PRICES_CONSUMER_HEARTBEAT_KEY = "mobile:futures:prices:consumer:alive"
_PRICES_REFRESH_INTERVAL_TRADING_SEC = int(
    str(os.getenv("MOBILE_PRICES_REFRESH_INTERVAL_TRADING_SEC", "8")).strip() or 8
)
_PRICES_REFRESH_INTERVAL_IDLE_SEC = int(
    str(os.getenv("MOBILE_PRICES_REFRESH_INTERVAL_IDLE_SEC", "30")).strip() or 30
)
_PRICES_CONSUMER_TTL_SEC = int(
    str(os.getenv("MOBILE_PRICES_CONSUMER_TTL_SEC", "90")).strip() or 90
)
_PRICES_REQUIRE_REDIS_LOCK = (
    str(os.getenv("MOBILE_PRICES_REQUIRE_REDIS_LOCK", "1")).strip().lower() not in {"0", "false", "no", "off"}
)
_PRICES_FETCH_CONNECT_TIMEOUT_SEC = float(
    str(os.getenv("MOBILE_PRICES_FETCH_CONNECT_TIMEOUT_SEC", "2")).strip() or 2
)
_PRICES_FETCH_READ_TIMEOUT_SEC = float(
    str(os.getenv("MOBILE_PRICES_FETCH_READ_TIMEOUT_SEC", "5")).strip() or 5
)
_PRICES_LIVE_OVERRIDE_MAX_AGE_SEC = int(
    str(os.getenv("MOBILE_PRICES_LIVE_OVERRIDE_MAX_AGE_SEC", "1800")).strip() or 1800
)
_PRICES_POST_CLOSE_CAPTURE_MINUTES = int(
    str(os.getenv("MOBILE_PRICES_POST_CLOSE_CAPTURE_MINUTES", "90")).strip() or 90
)
_PRICES_REFRESH_LOCK_TTL = max(
    3,
    min(
        int(str(os.getenv("MOBILE_PRICES_REFRESH_LOCK_TTL", "7")).strip() or 7),
        max(3, _PRICES_REFRESH_INTERVAL_TRADING_SEC - 1),
    ),
)
_PRICES_METRICS_LOG_INTERVAL_SEC = 60
_MARKET_CHART_CACHE_PREFIX = "mobile:market:chart"
_MARKET_CHART_CACHE_TTL = int(str(os.getenv("MOBILE_MARKET_CHART_CACHE_TTL", "120")).strip() or 120)
_INSTANCE_ID = f"{os.getenv('HOSTNAME', 'local')}:{os.getpid()}"

# 品种代码 → 交易所（决定新浪行情代码前缀和月份格式）
_PRODUCT_EXCHANGE: dict[str, str] = {
    # SHFE 上海期货交易所 → nf_{CODE}
    "cu":"SHFE","al":"SHFE","zn":"SHFE","pb":"SHFE","ni":"SHFE","sn":"SHFE",
    "au":"SHFE","ag":"SHFE","rb":"SHFE","hc":"SHFE","ss":"SHFE","bu":"SHFE",
    "ru":"SHFE","fu":"SHFE","lu":"SHFE","sp":"SHFE","bc":"SHFE","ao":"SHFE",
    "sc":"SHFE","sh":"SHFE","ad":"SHFE","br":"SHFE","ec":"SHFE","pt":"SHFE",
    # 原油(INE)、烧碱、铝合金、BR橡胶、集运欧线、铂金
    # DCE 大连商品交易所 → nf_{CODE}
    "m":"DCE","y":"DCE","a":"DCE","b":"DCE","c":"DCE","cs":"DCE","jd":"DCE",
    "l":"DCE","pp":"DCE","v":"DCE","eb":"DCE","eg":"DCE","j":"DCE","jm":"DCE",
    "i":"DCE","rr":"DCE","lh":"DCE","pg":"DCE","p":"DCE","lg":"DCE",
    # CZCE 郑商所 → nf_{CODE}（实测与SHFE/DCE格式相同）
    "sr":"CZCE","cf":"CZCE","ta":"CZCE","ma":"CZCE","rm":"CZCE","oi":"CZCE",
    "zc":"CZCE","fg":"CZCE","sa":"CZCE","ur":"CZCE","ap":"CZCE","cj":"CZCE",
    "lc":"CZCE","bz":"CZCE","pr":"CZCE","si":"CZCE","ps":"CZCE","nr":"CZCE",
    "sf":"CZCE","sm":"CZCE","wt":"CZCE","pm":"CZCE","pf":"CZCE","cy":"CZCE",
    "pl":"CZCE","op":"CZCE","fb":"CZCE","pk":"CZCE","px":"CZCE",   # 短纤、棉纱、丙烯、双胶纸、纤维板、花生、PX
    # CFFEX 中金所 → nf_{CODE}（字段布局与商品期货不同，见 _fetch_sina_prices）
    "if":"CFFEX","ic":"CFFEX","ih":"CFFEX","im":"CFFEX",
    "ts":"CFFEX","tf":"CFFEX","t":"CFFEX","tl":"CFFEX",   # 补充30年国债
}

# 期权相关品种代码集合（只抓这些品种的合约）
_OPTION_PRODUCTS: set[str] = set(_PRODUCT_EXCHANGE.keys())


def _is_trading_hours() -> bool:
    """判断当前是否在期货交易时段（上海时间）。"""
    import pytz
    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    t = now.hour * 60 + now.minute
    return (
        (9 * 60 <= t <= 11 * 60 + 30) or
        (13 * 60 + 30 <= t <= 15 * 60) or
        (21 * 60 <= t <= 23 * 60 + 59) or
        (0 <= t <= 2 * 60 + 30)
    )


def _is_post_close_capture_window() -> bool:
    """
    收盘后短窗口继续低频抓取，确保拿到最终收盘口径。
    默认覆盖：
    - 日盘收盘后：15:00 ~ 15:00+N分钟
    - 夜盘收盘后：02:30 ~ 02:30+N分钟
    """
    import pytz

    now = datetime.now(pytz.timezone("Asia/Shanghai"))
    t = now.hour * 60 + now.minute
    day_close = 15 * 60
    night_close = 2 * 60 + 30
    window = max(1, _PRICES_POST_CLOSE_CAPTURE_MINUTES)
    return (day_close <= t <= day_close + window) or (night_close <= t <= night_close + window)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if f == f and math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


def _safe_text(v, default: str = "") -> str:
    try:
        text = str(v or "").strip()
    except Exception:
        text = ""
    return text or default


def _build_mobile_simple_runtime_context(current_user: str) -> Dict[str, str]:
    user_label = str(current_user or "").strip() or "访客"
    return build_simple_runtime_context(current_user_label=user_label)


_CHAOS_BAND_LABELS = {
    "nothing_happens": "局势偏稳",
    "something_might_happen": "混乱升温",
    "something_is_brewing": "全球失序",
    "things_are_happening": "世界大战",
}
_CHAOS_REGION_LABELS = {
    "middle_east": "中东",
    "east_asia": "东亚",
    "korean_peninsula": "朝鲜半岛",
    "europe": "欧洲",
    "global": "全球",
    "north_america": "北美",
    "balkans": "巴尔干",
}
_CHAOS_CATEGORY_LABELS = {
    "military_conflict": "军事冲突",
    "nuclear_escalation": "核升级",
    "political_instability": "政治失稳",
    "economic_crisis": "经济危机",
    "public_health": "公共卫生",
}


def _chaos_market_history_key(item: Dict[str, Any]) -> str:
    return _safe_text(
        item.get("event_slug")
        or item.get("market_slug")
        or item.get("event_key")
        or item.get("pair_tag")
        or item.get("display_title")
    ).lower()


def _build_chaos_market_trend_map(snapshots: List[Dict[str, Any]], threshold: float = 0.005) -> Dict[str, Dict[str, Any]]:
    series_by_key: Dict[str, List[float]] = {}
    for snap in snapshots or []:
        if not isinstance(snap, dict):
            continue
        source_status = snap.get("source_status") if isinstance(snap.get("source_status"), dict) else {}
        items = source_status.get("monitored_markets") or snap.get("top_markets") or []
        seen: set[str] = set()
        for item in items:
            if not isinstance(item, dict):
                continue
            key = _chaos_market_history_key(item)
            if not key or key in seen:
                continue
            seen.add(key)
            series_by_key.setdefault(key, []).append(_safe_float(item.get("probability"), 0.0))

    trend_map: Dict[str, Dict[str, Any]] = {}
    for key, values in series_by_key.items():
        recent_values = values[-4:]
        if len(recent_values) < 2:
            continue
        deltas = [recent_values[i] - recent_values[i - 1] for i in range(1, len(recent_values))]
        latest_delta = deltas[-1]
        directions: List[int] = []
        for delta in deltas:
            if delta >= threshold:
                directions.append(1)
            elif delta <= -threshold:
                directions.append(-1)
            else:
                directions.append(0)

        strength = 0
        direction = 0
        for expected in (1, -1):
            run = 0
            for value in reversed(directions):
                if value == expected:
                    run += 1
                else:
                    break
            if run > 0:
                strength = min(3, run)
                direction = expected
                break

        arrows = ""
        trend_direction = ""
        if strength > 0:
            arrows = ("▲" if direction > 0 else "▼") * strength
            trend_direction = "up" if direction > 0 else "down"
        flames = "🔥" if latest_delta >= 0.05 else ""

        if not arrows and not flames:
            continue
        trend_map[key] = {
            "trend_arrows": arrows,
            "trend_direction": trend_direction,
            "trend_flames": flames,
            "trend_latest_delta": latest_delta,
        }
    return trend_map


def _chaos_region_label(region_tag: str) -> str:
    key = _safe_text(region_tag)
    if not key:
        return "全球"
    return _CHAOS_REGION_LABELS.get(key, key.replace("_", " "))


def _format_chaos_updated_time(value: str) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    iso = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        dt = datetime.fromisoformat(iso)
        return dt.strftime("%H:%M:%S")
    except Exception:
        return text


def _empty_chaos_payload() -> Dict[str, Any]:
    return {
        "has_data": False,
        "score_raw": 0.0,
        "score_display": 0.0,
        "band": "nothing_happens",
        "band_label": _CHAOS_BAND_LABELS["nothing_happens"],
        "updated_at": "",
        "updated_time_text": "",
        "methodology_version": "",
        "components": {
            "ongoing_baseline": 0.0,
            "escalation_pressure": 0.0,
            "contagion_bonus": 0.0,
        },
        "monitored_markets": [],
        "top_drivers": [],
        "category_breakdown": [],
    }


def _build_chaos_payload(snapshot: Dict[str, Any], trend_map: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    if not isinstance(snapshot, dict) or not snapshot:
        return _empty_chaos_payload()

    source_status = snapshot.get("source_status") if isinstance(snapshot.get("source_status"), dict) else {}
    score_components = source_status.get("score_components") if isinstance(source_status.get("score_components"), dict) else {}
    monitored_src = source_status.get("monitored_markets") or snapshot.get("top_markets") or []
    top_markets = snapshot.get("top_markets") or []
    category_src = snapshot.get("category_breakdown") or []

    trend_map = trend_map or {}
    monitored_markets: List[Dict[str, Any]] = []
    for idx, item in enumerate(monitored_src[:12]):
        if not isinstance(item, dict):
            continue
        trend = trend_map.get(_chaos_market_history_key(item), {})
        monitored_markets.append(
            {
                "rank": idx + 1,
                "display_title": _safe_text(item.get("display_title"), "-"),
                "region_label": _chaos_region_label(_safe_text(item.get("region_tag"))),
                "pair_tag": _safe_text(item.get("pair_tag"), "-"),
                "probability": _safe_float(item.get("probability"), 0.0),
                "delta_24h": _safe_float(item.get("delta_24h"), 0.0),
                "event_raw": _safe_float(item.get("event_raw"), 0.0),
                "trend_arrows": _safe_text(trend.get("trend_arrows")),
                "trend_direction": _safe_text(trend.get("trend_direction")),
                "trend_flames": _safe_text(trend.get("trend_flames")),
                "trend_latest_delta": _safe_float(trend.get("trend_latest_delta"), 0.0),
            }
        )

    top_drivers: List[Dict[str, Any]] = []
    for item in top_markets[:5]:
        if not isinstance(item, dict):
            continue
        top_drivers.append(
            {
                "display_title": _safe_text(item.get("display_title"), "-"),
                "region_label": _chaos_region_label(_safe_text(item.get("region_tag"))),
                "probability": _safe_float(item.get("probability"), 0.0),
                "delta_24h": _safe_float(item.get("delta_24h"), 0.0),
                "event_raw": _safe_float(item.get("event_raw"), 0.0),
            }
        )

    category_breakdown: List[Dict[str, Any]] = []
    for item in category_src:
        if not isinstance(item, dict):
            continue
        key = _safe_text(item.get("key"))
        label = _safe_text(item.get("label")) or _CHAOS_CATEGORY_LABELS.get(key, key or "未分类")
        baseline = _safe_float(item.get("baseline"), 0.0)
        escalation = _safe_float(item.get("escalation"), 0.0)
        category_breakdown.append(
            {
                "key": key,
                "label": label,
                "baseline": baseline,
                "escalation": escalation,
                "total": baseline + escalation,
            }
        )

    band = _safe_text(snapshot.get("band"), "nothing_happens")
    updated_at = _safe_text(snapshot.get("updated_at"))
    return {
        "has_data": True,
        "score_raw": _safe_float(snapshot.get("score_raw"), 0.0),
        "score_display": _safe_float(snapshot.get("score_display"), 0.0),
        "band": band,
        "band_label": _CHAOS_BAND_LABELS.get(band, _CHAOS_BAND_LABELS["nothing_happens"]),
        "updated_at": updated_at,
        "updated_time_text": _format_chaos_updated_time(updated_at),
        "methodology_version": _safe_text(snapshot.get("methodology_version")),
        "components": {
            "ongoing_baseline": _safe_float(score_components.get("ongoing_baseline"), 0.0),
            "escalation_pressure": _safe_float(score_components.get("escalation_pressure"), 0.0),
            "contagion_bonus": _safe_float(score_components.get("contagion_bonus"), 0.0),
        },
        "monitored_markets": monitored_markets,
        "top_drivers": top_drivers,
        "category_breakdown": category_breakdown,
    }


def _market_chart_cache_key(product: str, contract: str) -> str:
    p = str(product or "").strip().lower()
    c = str(contract or "").strip().upper() or "_"
    return f"{_MARKET_CHART_CACHE_PREFIX}:{p}:{c}"


def _is_missing_value(v) -> bool:
    if v is None:
        return True
    if isinstance(v, str) and v.strip() in {"", "-", "—", "--", "nan", "NaN", "None"}:
        return True
    try:
        return bool(v != v)  # NaN
    except Exception:
        return False


def _compute_iv_chg_fallback_map(df):
    """
    从最近数日 IV 历史中计算“最近两个交易日”的日变动回退值。
    入参 df 列: code, td, iv
    返回: {code_lower: iv_chg_1d}
    """
    if df is None or getattr(df, "empty", True):
        return {}

    import pandas as pd

    req_cols = {"code", "td", "iv"}
    if not req_cols.issubset(set(df.columns)):
        return {}

    work = df.copy()
    work["code"] = work["code"].astype(str).str.lower()
    work["td"] = work["td"].astype(str)
    work["iv"] = pd.to_numeric(work["iv"], errors="coerce")
    work = work.dropna(subset=["iv"])
    if work.empty:
        return {}

    work = work.sort_values(["code", "td"], ascending=[True, False])
    out = {}
    for code, grp in work.groupby("code"):
        vals = []
        seen_td = set()
        for _, r in grp.iterrows():
            td = str(r["td"])
            if td in seen_td:
                continue
            seen_td.add(td)
            vals.append(float(r["iv"]))
            if len(vals) >= 2:
                break
        if len(vals) >= 2 and vals[1] > 0:
            out[code] = round(vals[0] - vals[1], 2)
    return out


IV_RANK_EXPIRING = -1
IV_RANK_NO_OPTION = -2
IV_RANK_MISSING = -3

_OPTION_PRODUCT_CODES_CACHE: dict[str, Any] = {
    "ts": 0.0,
    "values": set(),
}


def _normalize_product_code(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    m = re.match(r"^([a-z]+)", s)
    return m.group(1) if m else ""


def _extract_product_code_from_contract(name: str) -> str:
    s = str(name or "").strip()
    m = re.match(r"([a-zA-Z]+)", s)
    return m.group(1).lower() if m else ""


def _row_implies_has_option(raw_iv: Any, iv_rank_raw: Any) -> bool:
    """
    从单行综合数据推断“该品种/合约有期权”。
    用于补齐 commodity_option_basic 未覆盖的品类（如股指相关）。
    """
    iv_val = _safe_float(raw_iv, 0.0)
    if iv_val > 0:
        return True

    rank_text = str(iv_rank_raw or "").strip()
    if not rank_text:
        return False
    if rank_text == "快到期":
        return True
    try:
        return float(rank_text) > 0
    except Exception:
        return False


def _get_option_product_codes(cache_ttl_sec: int = 600) -> set[str]:
    """读取“有期权品种”清单（带短缓存）。"""
    now_ts = time.time()
    cache_vals = _OPTION_PRODUCT_CODES_CACHE.get("values", set())
    cache_ts = float(_OPTION_PRODUCT_CODES_CACHE.get("ts", 0.0) or 0.0)
    if cache_vals and (now_ts - cache_ts) < cache_ttl_sec:
        return set(cache_vals)

    import pandas as pd

    values: set[str] = set()
    try:
        # 优先用 underlying（最稳定）
        df = pd.read_sql(
            """
            SELECT DISTINCT LOWER(TRIM(underlying)) AS product
            FROM commodity_option_basic
            WHERE underlying IS NOT NULL
              AND TRIM(underlying) <> ''
            """,
            de.engine,
        )
        if not df.empty:
            values = {
                _normalize_product_code(x)
                for x in df["product"].tolist()
                if _normalize_product_code(x)
            }
    except Exception:
        values = set()

    if not values:
        try:
            # 兜底：从 ts_code 前缀提取（避免 underlying 为空时失效）
            df2 = pd.read_sql(
                """
                SELECT DISTINCT LOWER(REGEXP_SUBSTR(SUBSTRING_INDEX(ts_code,'.',1),'^[a-zA-Z]+')) AS product
                FROM commodity_option_basic
                """,
                de.engine,
            )
            if not df2.empty:
                values = {
                    _normalize_product_code(x)
                    for x in df2["product"].tolist()
                    if _normalize_product_code(x)
                }
        except Exception:
            values = set()

    if values:
        _OPTION_PRODUCT_CODES_CACHE["values"] = set(values)
        _OPTION_PRODUCT_CODES_CACHE["ts"] = now_ts
        return values

    return set(cache_vals) if cache_vals else set()


def _to_sina_code(contract: str) -> str:
    """将大写合约代码转换为新浪行情请求码。
    所有交易所（SHFE/DCE/CZCE/CFFEX）统一格式：nf_{CODE}
    RB2604 → nf_RB2604
    SR2605 → nf_SR2605
    """
    m = re.match(r'^([A-Za-z]+)(\d+)$', contract)
    if not m:
        return ""
    prod = m.group(1).lower()
    if prod not in _PRODUCT_EXCHANGE:
        return ""
    return f"nf_{contract.upper()}"


def _get_active_contracts() -> list[str]:
    """从 futures_price 表获取最近 5 天内有数据的活跃合约代码（大写）。"""
    try:
        import pandas as pd
        df = pd.read_sql(
            """
            SELECT DISTINCT UPPER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code
            FROM futures_price
            WHERE trade_date >= DATE_SUB(CURDATE(), INTERVAL 5 DAY)
              AND ts_code NOT LIKE '%%TAS%%'
            """,
            de.engine,
        )
        result = []
        for code in df["code"].tolist():
            m = re.match(r'^([A-Z]+)\d+$', code)
            if m and m.group(1).lower() in _OPTION_PRODUCTS:
                result.append(code)
        return result
    except Exception:
        return []


def _fetch_sina_prices(
    contracts: list[str],
    session=None,
    timeout: Optional[tuple[float, float]] = None,
) -> dict:
    """直接调用新浪行情接口，绕过 akshare。
    返回 {大写合约代码: {open,high,low,price,pct,volume}}
    """
    import requests as _req
    # 建立 sina_code → contract_code 映射
    sina_map: dict[str, str] = {}
    for c in contracts:
        sc = _to_sina_code(c)
        if sc:
            sina_map[sc] = c

    if not sina_map:
        return {}

    result: dict = {}
    batch_size = 80
    keys = list(sina_map.keys())
    # 不继承系统代理环境，避免 ALL_PROXY/HTTP_PROXY 指向 socks 时触发依赖错误。
    own_session = session is None
    if own_session:
        session = _req.Session()
        session.trust_env = False
    req_timeout = timeout or (_PRICES_FETCH_CONNECT_TIMEOUT_SEC, _PRICES_FETCH_READ_TIMEOUT_SEC)

    try:
        for i in range(0, len(keys), batch_size):
            batch = keys[i:i + batch_size]
            try:
                url = "https://hq.sinajs.cn/list=" + ",".join(batch)
                headers = {
                    "Referer": "https://finance.sina.com.cn",
                    "User-Agent": "Mozilla/5.0",
                }
                resp = session.get(url, headers=headers, timeout=req_timeout)
                text = resp.content.decode("gbk", errors="replace")
                for line in text.split("\n"):
                    line = line.strip()
                    mat = re.match(r'var hq_str_(.+?)="(.*)";', line)
                    if not mat:
                        continue
                    sc = mat.group(1)
                    contract = sina_map.get(sc, "")
                    if not contract:
                        continue
                    fields = mat.group(2).split(",")
                    try:
                        # 判断交易所，CFFEX 字段布局与商品期货完全不同
                        m_prod = re.match(r'^([A-Z]+)\d+$', contract)
                        exchange = _PRODUCT_EXCHANGE.get(m_prod.group(1).lower(), "") if m_prod else ""

                        if exchange == "CFFEX":
                            # CFFEX 金融期货字段布局（股指/国债）：
                            # [0]=开 [1]=高 [2]=低 [3]=最新价 [4]=成交量
                            # [9]=涨停价(≠昨结算!) [10]=跌停价 [13]=昨结算价
                            if len(fields) < 14:
                                continue
                            cur  = _safe_float(fields[3])
                            prev = _safe_float(fields[13])  # 昨结算价，非fields[9](涨停价)
                            if cur <= 0:
                                continue
                            result[contract] = {
                                "open":   _safe_float(fields[0]),
                                "high":   _safe_float(fields[1]),
                                "low":    _safe_float(fields[2]),
                                "price":  cur,
                                "pct":    round((cur - prev) / prev * 100, 2) if prev > 0 else 0.0,
                                "volume": int(_safe_float(fields[4])),
                                "updated_at": "",
                            }
                        else:
                            # 商品期货字段布局（SHFE/DCE/CZCE）：
                            # [0]=合约名 [1]=昨持仓量 [2]=开 [3]=高 [4]=低
                            # [5]=最新价(盘后=0) [6]=买价 [8]=成交价 [10]=昨结算 [14]=成交量
                            if len(fields) < 11:
                                continue
                            prev = _safe_float(fields[10])
                            cur  = _safe_float(fields[5])
                            if cur <= 0:
                                cur = _safe_float(fields[8])
                            if cur <= 0:
                                cur = _safe_float(fields[6])
                            if cur <= 0:
                                continue
                            result[contract] = {
                                "open":   _safe_float(fields[2]),
                                "high":   _safe_float(fields[3]),
                                "low":    _safe_float(fields[4]),
                                "price":  cur,
                                "pct":    round((cur - prev) / prev * 100, 2) if prev > 0 else 0.0,
                                "volume": int(_safe_float(fields[14])) if len(fields) > 14 else 0,
                                "updated_at": "",
                            }
                    except Exception:
                        continue
            except Exception as e:
                print(f"[sina_batch] ERROR: {e}", flush=True)
                continue
    finally:
        if own_session and session is not None:
            session.close()

    return result


# 活跃合约列表缓存（每小时从 DB 刷新一次）
_contract_cache: list[str] = []
_contract_cache_ts: float = 0.0

# last snapshot fallback when Redis read fails
_last_prices_payload: dict = {
    "items": [],
    "is_trading": False,
    "refreshed_at": "",
    "refreshed_ts": 0,
    "contracts": {},
}
_last_prices_lock = threading.Lock()


def _save_last_prices_payload(payload: dict) -> None:
    global _last_prices_payload
    with _last_prices_lock:
        # JSON round-trip copy to avoid shared mutable references across threads
        _last_prices_payload = json.loads(json.dumps(payload, ensure_ascii=False))


def _load_last_prices_payload() -> dict:
    with _last_prices_lock:
        return json.loads(json.dumps(_last_prices_payload, ensure_ascii=False))


def _load_shared_prices_payload() -> dict:
    """
    优先读取 Redis 共享行情快照，失败时回退进程内最后快照。
    """
    try:
        raw = _redis.get(_PRICES_KEY)
    except Exception:
        raw = None
    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                _save_last_prices_payload(payload)
                return payload
        except Exception:
            pass
    return _load_last_prices_payload()


def _get_fresh_live_contracts_map(payload: Optional[dict] = None) -> dict:
    """
    返回可用于覆盖展示的“新鲜实时合约映射”。
    仅当 refreshed_ts 在阈值内时生效，避免收盘后长期使用陈旧盘中价。
    """
    p = payload if isinstance(payload, dict) else _load_shared_prices_payload()
    if not isinstance(p, dict):
        return {}
    try:
        refreshed_ts = int(p.get("refreshed_ts") or 0)
    except Exception:
        refreshed_ts = 0
    if refreshed_ts <= 0:
        return {}
    if (time.time() - refreshed_ts) > _PRICES_LIVE_OVERRIDE_MAX_AGE_SEC:
        return {}
    contracts = p.get("contracts", {})
    return contracts if isinstance(contracts, dict) else {}


def _extract_contract_code(raw_name: str) -> str:
    """
    从 'MA2605 (甲醇)' / 'MA2605' 中提取标准合约代码（大写）。
    """
    text = str(raw_name or "").strip().upper()
    if not text:
        return ""
    text = text.split("(")[0].strip()
    m = re.match(r"^([A-Z]+[0-9]{3,4})$", text)
    return m.group(1) if m else ""


def _should_use_live_contract_for_display(
    live_row: Optional[dict],
    db_trade_day: str,
    *,
    fresh: bool,
) -> bool:
    """
    展示层是否采用实时合约价覆盖。
    规则：
    1) 若实时交易日 > DB交易日，则无条件使用实时（直到DB追平）。
    2) 若交易日相同，仅在实时快照“新鲜”时使用实时覆盖。
    """
    if not isinstance(live_row, dict):
        return False
    live_price = _safe_float(live_row.get("price"), 0.0)
    if live_price <= 0:
        return False
    live_td = str(live_row.get("trading_day") or "").strip()
    db_td = str(db_trade_day or "").strip()
    if live_td and (not db_td or live_td > db_td):
        return True
    return bool(fresh)


def _try_acquire_prices_refresh_lock() -> tuple[bool, str]:
    """Singleflight lock across multi-worker or multi-instance deployment."""
    try:
        ok = _redis.set(_PRICES_REFRESH_LOCK_KEY, _INSTANCE_ID, nx=True, ex=_PRICES_REFRESH_LOCK_TTL)
        return bool(ok), ("acquired" if ok else "miss")
    except Exception as e:
        if _PRICES_REQUIRE_REDIS_LOCK:
            print(f"[prices_loop] REDIS_LOCK_FAIL strict=1 err={e}", flush=True)
            return False, "redis_error_strict"
        print(f"[prices_loop] REDIS_LOCK_FAIL strict=0 fallback_local=1 err={e}", flush=True)
        return True, "redis_error_fallback"


def _touch_prices_consumer_heartbeat() -> None:
    try:
        _redis.setex(_PRICES_CONSUMER_HEARTBEAT_KEY, _PRICES_CONSUMER_TTL_SEC, str(int(time.time())))
    except Exception:
        # 心跳写失败只影响节流策略，不影响接口返回
        pass


def _has_active_prices_consumer() -> bool:
    try:
        return bool(_redis.get(_PRICES_CONSUMER_HEARTBEAT_KEY))
    except Exception:
        return False


def _run_prices_refresh_once(session) -> tuple[float, str]:
    """
    执行一轮价格刷新逻辑。
    返回：(下一轮sleep秒数, 结果标签)。
    """
    global _contract_cache, _contract_cache_ts

    is_trading = _is_trading_hours()
    has_consumer = _has_active_prices_consumer()
    should_capture_post_close = (not is_trading) and has_consumer and _is_post_close_capture_window()

    if not (is_trading and has_consumer) and not should_capture_post_close:
        # 无消费者/非交易时段：不抓上游，但维持最后快照续期，避免读者回退到DB午盘价。
        last_payload = _load_last_prices_payload()
        if last_payload.get("items") or last_payload.get("contracts"):
            last_payload["is_trading"] = is_trading
            try:
                _redis.setex(_PRICES_KEY, _PRICES_TTL, json.dumps(last_payload, ensure_ascii=False))
            except Exception:
                pass
        return float(_PRICES_REFRESH_INTERVAL_IDLE_SEC), "refresh_skip_no_consumer"

    interval_sec = float(_PRICES_REFRESH_INTERVAL_TRADING_SEC if is_trading else _PRICES_REFRESH_INTERVAL_IDLE_SEC)
    acquired, lock_status = _try_acquire_prices_refresh_lock()
    if not acquired:
        if lock_status == "miss":
            return interval_sec, "refresh_skip_lock_miss"
        return interval_sec, "refresh_skip_lock_error"

    # 收盘后保留最后一笔实时快照，避免前端回退到午盘 DB 价。
    last_payload = _load_last_prices_payload()
    contracts: dict = last_payload.get("contracts", {}) or {}
    items: list = last_payload.get("items", []) or []

    # refresh active contracts from DB hourly
    if time.time() - _contract_cache_ts > 3600 or not _contract_cache:
        _contract_cache = _get_active_contracts()
        _contract_cache_ts = time.time()

    if _contract_cache:
        contracts = _fetch_sina_prices(
            _contract_cache,
            session=session,
            timeout=(_PRICES_FETCH_CONNECT_TIMEOUT_SEC, _PRICES_FETCH_READ_TIMEOUT_SEC),
        )

        # choose major contract per product by max volume
        prod_best: dict = {}
        for code, data in contracts.items():
            m = re.match(r"^([A-Z]+)\d+$", code)
            if not m:
                continue
            prod = m.group(1).lower()
            if prod not in prod_best or data["volume"] > prod_best[prod]["volume"]:
                prod_best[prod] = {
                    "code": prod,
                    "name": code,
                    "price": data["price"],
                    "pct": data["pct"],
                    "volume": data["volume"],
                    "updated_at": "",
                }
        items = sorted(prod_best.values(), key=lambda x: x["code"])

    payload_obj = {
        "items": items,
        "is_trading": is_trading,
        "refreshed_at": datetime.now().strftime("%H:%M:%S"),
        "refreshed_ts": int(time.time()),
        "contracts": contracts,
    }
    payload_obj = enrich_prices_payload_with_trading_day(payload_obj)
    payload_raw = json.dumps(payload_obj, ensure_ascii=False)

    try:
        _redis.setex(_PRICES_KEY, _PRICES_TTL, payload_raw)
    except Exception as e:
        print(f"[prices_loop] REDIS_WRITE_FAIL: {e}", flush=True)

    _save_last_prices_payload(payload_obj)
    return interval_sec, "refresh_ok"


def _prices_refresh_loop():
    """Background thread: adaptive refresh with consumer-aware throttling."""
    import requests as _req

    metrics = {
        "refresh_ok": 0,
        "refresh_skip_no_consumer": 0,
        "refresh_skip_lock_miss": 0,
        "refresh_skip_lock_error": 0,
        "refresh_err": 0,
    }
    last_metrics_log_at = time.time()
    session = _req.Session()
    session.trust_env = False

    try:
        while True:
            started = time.time()
            interval_sec = float(_PRICES_REFRESH_INTERVAL_IDLE_SEC)
            outcome = "refresh_err"
            try:
                interval_sec, outcome = _run_prices_refresh_once(session)
            except Exception as e:
                print(f"[prices_loop] CRASH: {e}", flush=True)
                outcome = "refresh_err"

            metrics[outcome] = metrics.get(outcome, 0) + 1
            now = time.time()
            if now - last_metrics_log_at >= _PRICES_METRICS_LOG_INTERVAL_SEC:
                print(
                    "[prices_loop] metrics "
                    f"ok={metrics.get('refresh_ok', 0)} "
                    f"skip_no_consumer={metrics.get('refresh_skip_no_consumer', 0)} "
                    f"skip_lock_miss={metrics.get('refresh_skip_lock_miss', 0)} "
                    f"skip_lock_err={metrics.get('refresh_skip_lock_error', 0)} "
                    f"err={metrics.get('refresh_err', 0)}",
                    flush=True,
                )
                for k in list(metrics.keys()):
                    metrics[k] = 0
                last_metrics_log_at = now

            elapsed = time.time() - started
            sleep_sec = max(0.2, float(interval_sec) - elapsed)
            time.sleep(sleep_sec)
    finally:
        session.close()


# ════════════════════════════════════════════════════════════
#  工具函数
# ════════════════════════════════════════════════════════════

def _html_to_plain(html: str, max_len: int = 120) -> str:
    """剥除 HTML 标签，返回纯文字摘要，用于列表卡片预览。"""
    if not html:
        return ""
    text = re.sub(r"<style[\s\S]*?</style>", "", html, flags=re.IGNORECASE)
    text = re.sub(r"<script[\s\S]*?</script>", "", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text).strip()
    return text[:max_len] + ("…" if len(text) > max_len else "")


def _fmt_expire_at(expire_at: Any) -> str:
    if hasattr(expire_at, "strftime"):
        return expire_at.strftime("%Y-%m-%d")
    text = str(expire_at or "").strip()
    return text[:10] if len(text) >= 10 else text


# ════════════════════════════════════════════════════════════
#  App & CORS
# ════════════════════════════════════════════════════════════

app = FastAPI(
    title="爱波塔 Mobile API",
    version="1.0.0",
    description="uni-app 手机端专用后端接口",
)


@app.on_event("startup")
def _start_prices_thread():
    t = threading.Thread(target=_prices_refresh_loop, daemon=True)
    t.start()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],          # 上线后改为小程序/域名白名单
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ════════════════════════════════════════════════════════════
#  Bearer Token 认证
#
#  客户端 token 格式（兼容）:
#  - 新版: "raw_uuid_token"（推荐，纯 ASCII）
#  - 旧版: "username:raw_uuid_token"
# ════════════════════════════════════════════════════════════

_bearer = HTTPBearer()


def _unpack_token(token_str: str):
    """拆分 token，返回 (username_hint, raw_token)。兼容旧版 username:token 与新版纯 token。"""
    raw = str(token_str or "").strip()
    if not raw:
        raise HTTPException(status_code=401, detail="Token 格式错误")
    if ":" in raw:
        username, raw_token = raw.split(":", 1)
        if not username or not raw_token:
            raise HTTPException(status_code=401, detail="Token 格式错误")
        return username, raw_token
    return "", raw


def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> str:
    """验证 Bearer Token，返回 username；失败则抛出 401。"""
    username_hint, raw_token = _unpack_token(credentials.credentials)
    return _resolve_username_from_raw_token(username_hint, raw_token)


def _resolve_username_from_raw_token(username_hint: str, raw_token: str) -> str:
    try:
        if username_hint:
            is_valid = auth.check_token(username_hint, raw_token, strict=True)
            username = username_hint if is_valid else ""
        else:
            username = auth.get_username_by_token(raw_token, strict=True)
            is_valid = bool(username)
    except Exception as exc:
        print(f"[auth_guard] check_token error username_hint={username_hint}: {exc}")
        raise HTTPException(status_code=503, detail="认证服务繁忙，请稍后重试")
    if not is_valid:
        raise HTTPException(status_code=401, detail="Token 无效或已过期，请重新登录")
    return username


def _resolve_websocket_user(websocket: WebSocket) -> str:
    auth_header = _safe_textv(websocket.headers.get("authorization"))
    token = ""
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
    if not token:
        token = _safe_textv(websocket.query_params.get("token"))
    username_hint, raw_token = _unpack_token(token)
    return _resolve_username_from_raw_token(username_hint, raw_token)


def _request_from_websocket(websocket: WebSocket) -> Request:
    raw_headers = []
    try:
        raw_headers = list(websocket.headers.raw)
    except Exception:
        raw_headers = []
    return Request({"type": "http", "headers": raw_headers})


def _pack_token(username: str, raw_token: str) -> str:
    _ = username
    return str(raw_token or "")


def _device_now_text() -> str:
    import pytz

    return datetime.now(pytz.timezone("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")


def _perf_ms_since(started_at: float) -> int:
    return int(round((time.perf_counter() - started_at) * 1000))


def _record_timing(timings: Dict[str, Any], key: str, started_at: float) -> None:
    if _DEVICE_VOICE_LATENCY_OBSERVATION_ENABLED:
        timings[key] = _perf_ms_since(started_at)


def _safe_floatv(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    if value is None:
        return default
    try:
        if isinstance(value, float) and math.isnan(value):
            return default
        return float(value)
    except Exception:
        return default


def _safe_textv(value: Any, default: str = "") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text if text else default


def _clamp_device_score(value: Any) -> Optional[int]:
    parsed = _safe_floatv(value, default=None)
    if parsed is None:
        return None
    return max(0, min(100, int(round(parsed))))


def _normalize_device_auto_poll_seconds(raw_value: Any) -> int:
    parsed = _safe_floatv(raw_value, default=float(_DEVICE_AUTO_POLL_SECONDS_DEFAULT))
    if parsed is None:
        parsed = float(_DEVICE_AUTO_POLL_SECONDS_DEFAULT)
    return max(_DEVICE_AUTO_POLL_MIN_SECONDS, int(round(parsed)))


def _extract_device_context(request: Request) -> Dict[str, str]:
    headers = request.headers
    return {
        "device_id": _safe_textv(headers.get("X-Device-Id")),
        "device_model": _safe_textv(headers.get("X-Device-Model")),
        "device_version": _safe_textv(headers.get("X-Device-Version")),
    }


def _log_device_request(
    *,
    endpoint: str,
    username: str,
    request: Request,
    status: str,
    detail: str = "",
) -> None:
    device_ctx = _extract_device_context(request)
    print(
        "[device_api] "
        f"endpoint={endpoint} status={status} user={username} "
        f"device_id={device_ctx.get('device_id') or '-'} "
        f"model={device_ctx.get('device_model') or '-'} "
        f"version={device_ctx.get('device_version') or '-'} "
        f"detail={detail or '-'}",
        flush=True,
    )


def _build_device_config_payload() -> Dict[str, Any]:
    auto_poll_enabled = bool(_DEVICE_AUTO_POLL_ENABLED_DEFAULT)
    auto_poll_seconds = None
    briefing_mode = "manual"
    if auto_poll_enabled:
        auto_poll_seconds = _normalize_device_auto_poll_seconds(_DEVICE_AUTO_POLL_SECONDS_DEFAULT)
        briefing_mode = "hybrid"
    return {
        "auto_poll_enabled": auto_poll_enabled,
        "auto_poll_seconds": auto_poll_seconds,
        "voice_enabled": bool(_DEVICE_VOICE_ENABLED_DEFAULT),
        "voice_mode": _DEVICE_VOICE_MODE_DEFAULT,
        "record_max_seconds": _DEVICE_VOICE_RECORD_MAX_SECONDS,
        "audio_format": "wav_pcm_16k_mono",
        "voice_task_poll_seconds": _DEVICE_VOICE_TASK_POLL_SECONDS,
        "voice_task_max_wait_seconds": _DEVICE_VOICE_TASK_MAX_WAIT_SECONDS,
        "voice_latency_observation_enabled": _DEVICE_VOICE_LATENCY_OBSERVATION_ENABLED,
        "voice_realtime_endpoint": "/api/device/voice/realtime",
        "briefing_mode": briefing_mode,
    }


def _pick_market_brief_alert(market_df: Any) -> str:
    if market_df is None or getattr(market_df, "empty", True):
        return ""

    iv_col = "IV变动(日)"
    pct_col = "涨跌%(日)"
    name_col = "合约"
    best_msg = ""
    best_score = 0.0

    try:
        for _, row in market_df.iterrows():
            name = _safe_textv(row.get(name_col), "市场")
            iv_delta = _safe_floatv(row.get(iv_col), default=None)
            if iv_delta is not None:
                score = abs(iv_delta)
                if score >= 1.0 and score > best_score:
                    direction = "升温" if iv_delta > 0 else "回落"
                    best_msg = f"{name}IV{direction}{abs(iv_delta):.1f}点"
                    best_score = score
            pct_1d = _safe_floatv(row.get(pct_col), default=None)
            if pct_1d is not None:
                score = abs(pct_1d) * 0.8
                if score >= 2.0 and score > best_score:
                    direction = "上涨" if pct_1d > 0 else "回落"
                    best_msg = f"{name}{direction}{abs(pct_1d):.1f}%"
                    best_score = score
    except Exception as exc:
        print(f"[device_api] market_alert_build_failed err={exc}", flush=True)
        return ""

    return best_msg


def _derive_device_market_state(
    *,
    iv_temperature: Optional[int],
    chaos_index: Optional[int],
) -> tuple[str, str]:
    if (chaos_index is not None and chaos_index >= 65) or (iv_temperature is not None and iv_temperature >= 68):
        return "risk_off", "high"
    if (chaos_index is not None and chaos_index <= 35) and (iv_temperature is not None and iv_temperature <= 40):
        return "risk_on", "low"
    return "neutral", "medium"


def _build_device_texts(
    *,
    market_state: str,
    latest_alert: str,
    iv_temperature: Optional[int],
    chaos_index: Optional[int],
    data_freshness: str,
) -> tuple[str, str]:
    state_label = {
        "risk_off": "市场偏谨慎",
        "neutral": "市场中性观察",
        "risk_on": "市场风险偏好回暖",
    }.get(market_state, "市场中性观察")
    alert_text = _safe_textv(latest_alert)

    if alert_text:
        headline = f"{state_label}，{alert_text}"
    elif data_freshness == "degraded":
        headline = "设备简报暂时降级，请稍后重试"
    else:
        headline = f"{state_label}，当前无突出预警"

    metrics_bits = []
    if chaos_index is not None:
        metrics_bits.append(f"混乱指数{chaos_index}")
    if iv_temperature is not None:
        metrics_bits.append(f"IV温度{iv_temperature}")
    metrics_text = "，".join(metrics_bits)

    if data_freshness == "degraded":
        speak_text = "当前市场简报暂时降级，请稍后再查询。"
    elif alert_text and metrics_text:
        speak_text = f"{state_label}。{alert_text}。{metrics_text}。"
    elif alert_text:
        speak_text = f"{state_label}。{alert_text}。"
    elif metrics_text:
        speak_text = f"{state_label}。{metrics_text}。"
    else:
        speak_text = f"{state_label}。当前没有新的设备预警。"

    return headline, speak_text


def _build_device_briefing_payload(username: str, request: Request) -> Dict[str, Any]:
    device_ctx = _extract_device_context(request)
    market_df = None
    chaos_snapshot: Dict[str, Any] = {}
    iv_snapshot: Dict[str, Any] = {}
    source_errors: list[str] = []

    try:
        market_df = de.get_comprehensive_market_data()
    except Exception as exc:
        source_errors.append(f"market:{exc}")

    try:
        raw_chaos = de.get_latest_geopolitical_risk_snapshot()
        if isinstance(raw_chaos, dict):
            chaos_snapshot = raw_chaos
    except Exception as exc:
        source_errors.append(f"chaos:{exc}")

    try:
        raw_iv = de.get_cross_asset_iv_index(auto_compute=False)
        if isinstance(raw_iv, dict):
            iv_snapshot = raw_iv
    except Exception as exc:
        source_errors.append(f"iv:{exc}")

    chaos_index = _clamp_device_score(
        chaos_snapshot.get("score_display", chaos_snapshot.get("score_raw"))
    )
    iv_temperature = _clamp_device_score(
        iv_snapshot.get("index_ewma5", iv_snapshot.get("index_raw"))
    )
    latest_alert = _pick_market_brief_alert(market_df)
    if not latest_alert and iv_temperature is not None and iv_temperature >= 68:
        latest_alert = "跨资产IV温度偏高"
    if not latest_alert and chaos_index is not None and chaos_index >= 65:
        latest_alert = "全球风险扰动偏强"

    if not source_errors and chaos_index is not None and iv_temperature is not None:
        data_freshness = "fresh"
    elif chaos_index is not None or iv_temperature is not None or latest_alert:
        data_freshness = "stale"
    else:
        data_freshness = "degraded"

    market_state, risk_level = _derive_device_market_state(
        iv_temperature=iv_temperature,
        chaos_index=chaos_index,
    )
    headline, speak_text = _build_device_texts(
        market_state=market_state,
        latest_alert=latest_alert,
        iv_temperature=iv_temperature,
        chaos_index=chaos_index,
        data_freshness=data_freshness,
    )
    updated_at = (
        _safe_textv(chaos_snapshot.get("updated_at"))
        or _safe_textv(iv_snapshot.get("trade_date"))
        or _device_now_text()
    )

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "market_state": market_state,
        "risk_level": risk_level,
        "headline": headline,
        "speak_text": speak_text,
        "iv_temperature": iv_temperature,
        "chaos_index": chaos_index,
        "latest_alert": latest_alert,
        "updated_at": updated_at,
        "data_freshness": data_freshness,
    }


def _normalize_device_contract_code(value: Any) -> str:
    raw = _safe_textv(value).upper()
    return raw if re.match(r"^[A-Z]+[0-9]{3,4}$", raw) else ""


def _device_product_name(product_code: str) -> str:
    code = _safe_textv(product_code).upper()
    return _safe_textv(getattr(de, "PRODUCT_MAP", {}).get(code), code)


def _device_contract_product_code(contract: str) -> str:
    m = re.match(r"^([A-Z]+)[0-9]{3,4}$", _safe_textv(contract).upper())
    return m.group(1).lower() if m else ""


def _device_contract_display(contract: str, product_name: str = "") -> str:
    product_name = _safe_textv(product_name)
    return f"{contract} {product_name}" if product_name else contract


def _format_device_metric(value: Any, suffix: str = "") -> str:
    parsed = _safe_floatv(value, default=None)
    if parsed is None:
        return "--"
    text = f"{parsed:.1f}".rstrip("0").rstrip(".")
    return f"{text}{suffix}"


def _format_device_price(value: Any) -> str:
    parsed = _safe_floatv(value, default=None)
    if parsed is None:
        return "--"
    abs_value = abs(parsed)
    if abs_value < 10:
        text = f"{parsed:.3f}"
    elif abs_value < 100:
        text = f"{parsed:.2f}"
    else:
        text = f"{parsed:.1f}"
    return text.rstrip("0").rstrip(".")


_DEVICE_MENU_CATEGORIES = {"futures", "etf", "favorites", "stock"}
_DEVICE_FAVORITE_PRODUCTS = ["pp", "ag", "au", "cu", "m", "rm", "sr", "ta", "sc", "if", "im", "io"]
_DEVICE_ETF_PRODUCTS = [
    {"product_code": "510050.SH", "product_name": "上证50ETF"},
    {"product_code": "510300.SH", "product_name": "沪深300ETF"},
    {"product_code": "510500.SH", "product_name": "中证500ETF"},
    {"product_code": "159915.SZ", "product_name": "创业板ETF"},
    {"product_code": "588000.SH", "product_name": "科创50ETF"},
    {"product_code": "159901.SZ", "product_name": "深证100ETF"},
]
_DEVICE_VOICE_ETF_ALIASES = {
    "上证50ETF": "510050.SH",
    "上证50": "510050.SH",
    "50ETF": "510050.SH",
    "沪深300ETF": "510300.SH",
    "沪深300": "510300.SH",
    "300ETF": "510300.SH",
    "中证500ETF": "510500.SH",
    "中证500": "510500.SH",
    "500ETF": "510500.SH",
    "创业板ETF": "159915.SZ",
    "创业板etf": "159915.SZ",
    "创业板": "159915.SZ",
    "创业ETF": "159915.SZ",
    "科创50ETF": "588000.SH",
    "科创50": "588000.SH",
    "深证100ETF": "159901.SZ",
    "深证100": "159901.SZ",
}
_DEVICE_VOICE_STOCK_NAME_ALIASES = {
    "澜起科技": "澜起科技",
    "蓝起科技": "澜起科技",
    "蓝启科技": "澜起科技",
    "兰起科技": "澜起科技",
    "澜启科技": "澜起科技",
}
_DEVICE_VOICE_STOCK_FAST_ALIASES: Dict[str, Tuple[str, str]] = {
    "澜起科技": ("688008.SH", "澜起科技"),
    "蓝起科技": ("688008.SH", "澜起科技"),
    "蓝启科技": ("688008.SH", "澜起科技"),
    "兰起科技": ("688008.SH", "澜起科技"),
    "澜启科技": ("688008.SH", "澜起科技"),
}
_DEVICE_VOICE_STOCK_NAME_CONFUSIONS = str.maketrans(
    {
        "蓝": "澜",
        "兰": "澜",
        "啟": "起",
        "启": "起",
    }
)
_DEVICE_VOICE_STOCK_NAME_CACHE_TTL_SECONDS = 600
_DEVICE_VOICE_STOCK_NAME_CACHE: Dict[str, Any] = {"loaded_at": 0.0, "items": []}
_DEVICE_VOICE_STOCK_NAME_CACHE_LOCK = threading.RLock()
_DEVICE_VOICE_STOCK_NAME_PREWARMING = False
_DEVICE_VOICE_PROMPT_TEXTS = {
    "voice_timeout": "服务响应超时，请再问一次。",
    "voice_network_error": "网络连接不稳定，请稍后再问一次。",
    "voice_listening": "我在听。",
    "voice_received": "收到，我看一下。",
    "voice_thinking": "我正在思考。",
    "voice_deep_confirm": "这个问题需要深度分析，我先帮你看。",
    "voice_deep_busy": "我还在分析上一个复杂问题，先问我行情也可以。",
    "voice_task_processing": "分析团队还在看技术面和波动率。",
    "voice_task_timeout": "分析还没完成，我先不占着你，你可以继续问行情，稍后问我刚才结果。",
    "voice_stt_empty": "没听清楚，请再说一次哦。",
    "voice_hello": "你好，我在。",
    "voice_help": "你可以问价格、涨跌、IV，或让我做深度分析。",
}
_DEVICE_VOICE_PROMPT_PREWARMING = False
_DEVICE_VOICE_MARKET_FACT_KEYWORDS = (
    "价格",
    "最新价",
    "现价",
    "报价",
    "收盘价",
    "多少钱",
    "多少点",
    "涨跌",
    "涨幅",
    "跌幅",
    "IV",
    "iv",
    "波动率",
    "Rank",
    "rank",
    "技术面",
    "做多",
    "做空",
)
_DEVICE_VOICE_DEEP_ANALYSIS_KEYWORDS = (
    "能做吗",
    "能不能做",
    "能不能买",
    "可以买",
    "该不该买",
    "该不该卖",
    "适合买",
    "适合做",
    "怎么做",
    "怎么交易",
    "策略",
    "风险",
    "仓位",
    "止损",
    "止盈",
    "入场",
    "出场",
    "为什么",
    "原因",
    "分析",
    "看法",
    "建议",
    "机会",
    "做多",
    "做空",
    "买入",
    "卖出",
    "追高",
    "抄底",
    "趋势",
    "突破",
    "回调",
    "综合",
)
_DEVICE_VOICE_QUICK_AI_KEYWORDS = (
    "你是谁",
    "你叫什么",
    "你好",
    "hello",
    "在吗",
    "怎么用",
    "可以做什么",
)
_DEVICE_VOICE_INSTANT_REPLY_KEYWORDS = (
    "你是谁",
    "你叫什么",
    "你好",
    "hello",
    "在吗",
    "怎么用",
    "可以做什么",
    "现在几点",
    "几点了",
    "几点",
    "当前时间",
    "什么时间",
    "还在分析",
    "分析好了",
    "结果好了",
    "刚才结果",
    "上一个结果",
)


def _normalize_device_menu_category(category: Optional[str]) -> str:
    value = _safe_textv(category, "futures").lower()
    return value if value in _DEVICE_MENU_CATEGORIES else "futures"


def _normalize_device_etf_code(raw: Any) -> str:
    value = _safe_textv(raw).upper()
    m = re.search(r"(510\d{3}|588\d{3}|159\d{3})(?:\.(SH|SZ))?", value)
    if not m:
        return ""
    base = m.group(1)
    suffix = m.group(2)
    if suffix:
        return f"{base}.{suffix}"
    return f"{base}.SZ" if base.startswith("159") else f"{base}.SH"


def _normalize_device_stock_code(raw: Any) -> str:
    value = _safe_textv(raw).upper()
    if not value:
        return ""
    m = re.search(r"\b([0-9]{6})(?:\.(SH|SZ|BJ))?\b", value)
    if m:
        base = m.group(1)
        suffix = m.group(2)
        if suffix:
            return f"{base}.{suffix}"
        if base.startswith(("6", "5", "9")):
            return f"{base}.SH"
        if base.startswith(("0", "1", "2", "3")):
            return f"{base}.SZ"
        if base.startswith(("4", "8")):
            return f"{base}.BJ"
        return f"{base}.SH"
    m = re.search(r"\b([0-9]{5})(?:\.HK)?\b", value)
    if m:
        return f"{m.group(1)}.HK"
    m = re.search(r"\b([A-Z][A-Z0-9]{0,9})\.US\b", value)
    if m:
        return f"{m.group(1)}.US"
    return ""


def _device_etf_name(etf_code: str) -> str:
    normalized = _normalize_device_etf_code(etf_code)
    for item in _DEVICE_ETF_PRODUCTS:
        if item["product_code"] == normalized:
            return item["product_name"]
    return normalized or _safe_textv(etf_code)


def _normalize_device_voice_stock_name(value: str) -> str:
    text = _safe_textv(value).lower()
    text = re.sub(r"[\s\-_.,，。:：;；!?！？、/\\|（）()【】\[\]{}<>《》\"'“”‘’]", "", text)
    return text.translate(_DEVICE_VOICE_STOCK_NAME_CONFUSIONS)


def _device_voice_fast_stock_alias_match(transcript: str) -> Tuple[str, str]:
    text = _safe_textv(transcript)
    normalized_text = _normalize_device_voice_stock_name(text)
    if not text and not normalized_text:
        return "", ""
    for alias, target in sorted(_DEVICE_VOICE_STOCK_FAST_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        alias_norm = _normalize_device_voice_stock_name(alias)
        if alias in text or (alias_norm and alias_norm in normalized_text):
            return target
    return "", ""


def _device_voice_stock_candidate_parts(candidate: Any) -> Tuple[str, str, str]:
    if not isinstance(candidate, (list, tuple)) or len(candidate) < 2:
        return "", "", ""
    code = _safe_textv(candidate[0]).upper()
    name = _safe_textv(candidate[1])
    name_norm = _safe_textv(candidate[2]) if len(candidate) >= 3 else _normalize_device_voice_stock_name(name)
    return code, name, name_norm


def _device_voice_stock_name_cache_fresh(now: Optional[float] = None) -> bool:
    current = time.time() if now is None else now
    cached_at = float(_DEVICE_VOICE_STOCK_NAME_CACHE.get("loaded_at") or 0.0)
    cached_items = _DEVICE_VOICE_STOCK_NAME_CACHE.get("items") or []
    return bool(cached_items) and current - cached_at < _DEVICE_VOICE_STOCK_NAME_CACHE_TTL_SECONDS


def _device_voice_stock_name_candidates() -> List[Tuple[str, str, str]]:
    now = time.time()
    with _DEVICE_VOICE_STOCK_NAME_CACHE_LOCK:
        cached_items = _DEVICE_VOICE_STOCK_NAME_CACHE.get("items") or []
        if _device_voice_stock_name_cache_fresh(now):
            return list(cached_items)

    try:
        import pandas as pd
        from sqlalchemy import text as _text

        df = pd.read_sql(
            _text("""
            SELECT ts_code, name
            FROM stock_price
            WHERE name IS NOT NULL
              AND name <> ''
            GROUP BY ts_code, name
            LIMIT 8000
            """),
            de.engine,
        )
        items: List[Tuple[str, str, str]] = []
        for _, row in df.iterrows():
            code = _safe_textv(row.get("ts_code")).upper()
            name = _safe_textv(row.get("name"))
            if code and name:
                items.append((code, name, _normalize_device_voice_stock_name(name)))
        with _DEVICE_VOICE_STOCK_NAME_CACHE_LOCK:
            _DEVICE_VOICE_STOCK_NAME_CACHE.update({"loaded_at": time.time(), "items": items})
        return list(items)
    except Exception as exc:
        print(f"[device_api] stock_symbol_db_resolve_failed err={exc}", flush=True)
        return []


def _ensure_device_voice_stock_name_cache_async() -> None:
    global _DEVICE_VOICE_STOCK_NAME_PREWARMING
    with _DEVICE_VOICE_STOCK_NAME_CACHE_LOCK:
        if _device_voice_stock_name_cache_fresh() or _DEVICE_VOICE_STOCK_NAME_PREWARMING:
            return
        _DEVICE_VOICE_STOCK_NAME_PREWARMING = True

    def _runner() -> None:
        global _DEVICE_VOICE_STOCK_NAME_PREWARMING
        try:
            _device_voice_stock_name_candidates()
        finally:
            with _DEVICE_VOICE_STOCK_NAME_CACHE_LOCK:
                _DEVICE_VOICE_STOCK_NAME_PREWARMING = False

    threading.Thread(target=_runner, name="device-stock-name-prewarm", daemon=True).start()


def _ensure_device_voice_prompt_audio_cache_async() -> None:
    global _DEVICE_VOICE_PROMPT_PREWARMING
    with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
        if _DEVICE_VOICE_PROMPT_PREWARMING:
            return
        _DEVICE_VOICE_PROMPT_PREWARMING = True

    def _runner() -> None:
        global _DEVICE_VOICE_PROMPT_PREWARMING
        try:
            for prompt_text in _DEVICE_VOICE_PROMPT_TEXTS.values():
                _device_voice_audio_url_for_text(prompt_text)
        finally:
            with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
                _DEVICE_VOICE_PROMPT_PREWARMING = False

    threading.Thread(target=_runner, name="device-voice-prompt-prewarm", daemon=True).start()


def _best_device_voice_stock_name_match(transcript: str, candidates: List[Tuple[str, ...]]) -> Tuple[str, str]:
    text = _safe_textv(transcript)
    normalized_text = _normalize_device_voice_stock_name(text)
    if not normalized_text:
        return "", ""

    fast_code, fast_name = _device_voice_fast_stock_alias_match(text)
    if fast_code:
        return fast_code, fast_name

    for alias, canonical_name in sorted(_DEVICE_VOICE_STOCK_NAME_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
        if alias in text or _normalize_device_voice_stock_name(alias) in normalized_text:
            canonical_norm = _normalize_device_voice_stock_name(canonical_name)
            for candidate in candidates:
                code, name, name_norm = _device_voice_stock_candidate_parts(candidate)
                if name_norm == canonical_norm:
                    return code, name

    exact_matches = []
    for candidate in candidates:
        code, name, name_norm = _device_voice_stock_candidate_parts(candidate)
        if not name_norm:
            continue
        if name in text or name_norm in normalized_text:
            exact_matches.append((len(name_norm), code, name))
    if exact_matches:
        _, code, name = max(exact_matches, key=lambda item: item[0])
        return code, name

    best_score = 0.0
    second_score = 0.0
    best_code = ""
    best_name = ""
    best_name_norm = ""
    for candidate in candidates:
        code, name, name_norm = _device_voice_stock_candidate_parts(candidate)
        if len(name_norm) < 3:
            continue
        window_lengths = {len(name_norm)}
        if len(name_norm) >= 4:
            window_lengths.update({len(name_norm) - 1, len(name_norm) + 1})
        local_best = 0.0
        for window_len in sorted(window_lengths):
            if window_len <= 0 or window_len > len(normalized_text):
                continue
            for start in range(0, len(normalized_text) - window_len + 1):
                window = normalized_text[start : start + window_len]
                score = difflib.SequenceMatcher(None, name_norm, window).ratio()
                if score > local_best:
                    local_best = score
        if local_best > best_score:
            second_score = best_score
            best_score = local_best
            best_code = code
            best_name = name
            best_name_norm = name_norm
        elif local_best > second_score:
            second_score = local_best

    if best_name:
        threshold = 0.86 if len(best_name_norm) <= 3 else 0.78
        if best_score >= threshold and (best_score >= 0.92 or best_score - second_score >= 0.08):
            return best_code, best_name
    return "", ""


def _resolve_device_voice_stock_symbol(transcript: str) -> Tuple[str, str]:
    text = _safe_textv(transcript)
    if not text:
        return "", ""

    direct_code = _normalize_device_stock_code(text)
    if direct_code:
        return direct_code, ""

    fast_code, fast_name = _device_voice_fast_stock_alias_match(text)
    if fast_code:
        return fast_code, fast_name

    code, name = _best_device_voice_stock_name_match(text, _device_voice_stock_name_candidates())
    if code:
        return code, name

    try:
        from symbol_map import COMMON_ALIASES, resolve_symbol

        normalized_text = text.upper()
        for alias, code in sorted(COMMON_ALIASES.items(), key=lambda item: len(str(item[0])), reverse=True):
            alias_text = _safe_textv(alias).upper()
            code_text = _safe_textv(code).upper()
            if not alias_text or "." not in code_text:
                continue
            if alias_text in normalized_text:
                return code_text, _safe_textv(alias)

        resolved_code, asset_type = resolve_symbol(text)
        if asset_type == "stock":
            normalized_code = _normalize_device_stock_code(resolved_code) or _safe_textv(resolved_code).upper()
            return normalized_code, ""
    except Exception as exc:
        print(f"[device_api] stock_symbol_resolve_failed err={exc}", flush=True)

    return "", ""


def _resolve_device_voice_futures_product(transcript: str) -> str:
    text = _safe_textv(transcript)
    if not text:
        return ""
    upper_text = text.upper()
    product_map = getattr(de, "PRODUCT_MAP", {}) or {}

    for code, name in sorted(product_map.items(), key=lambda item: len(str(item[1])), reverse=True):
        name_text = _safe_textv(name)
        if name_text and name_text in text:
            return _safe_textv(code).lower()

    try:
        from symbol_map import COMMON_ALIASES, resolve_symbol

        for alias, code in sorted(COMMON_ALIASES.items(), key=lambda item: len(str(item[0])), reverse=True):
            alias_text = _safe_textv(alias).upper()
            code_text = _safe_textv(code).upper()
            if not alias_text or "." in code_text:
                continue
            if alias_text in upper_text and code_text in product_map:
                return code_text.lower()

        resolved_code, asset_type = resolve_symbol(text)
        if asset_type == "future":
            resolved = _safe_textv(resolved_code).upper()
            if resolved in product_map:
                return resolved.lower()
    except Exception as exc:
        print(f"[device_api] futures_product_resolve_failed err={exc}", flush=True)

    for code in sorted(product_map.keys(), key=len, reverse=True):
        code_text = _safe_textv(code).upper()
        if len(code_text) < 2:
            continue
        if re.search(rf"(?<![A-Z0-9]){re.escape(code_text)}(?![A-Z0-9])", upper_text):
            return code_text.lower()
    return ""


def _resolve_device_voice_market_target_detail(transcript: str, fallback_contract: str = "") -> Tuple[str, str, str]:
    text = _safe_textv(transcript)
    upper_text = text.upper()

    explicit_etf = _normalize_device_etf_code(upper_text)
    if explicit_etf:
        return explicit_etf, "etf", ""

    for alias, etf_code in _DEVICE_VOICE_ETF_ALIASES.items():
        if alias in text:
            return etf_code, "etf", ""

    m = re.search(r"\b([A-Z]{1,4}[0-9]{3,4})\b", upper_text)
    if m:
        contract = _normalize_device_contract_code(m.group(1))
        if contract:
            return contract, "futures", ""

    stock_code, stock_name = _resolve_device_voice_stock_symbol(text)
    if stock_code:
        return stock_code, "stock", stock_name

    product_code = _resolve_device_voice_futures_product(text)
    if product_code:
        return product_code, "futures_product", ""

    fallback_etf = _normalize_device_etf_code(fallback_contract)
    if fallback_etf:
        return fallback_etf, "etf", ""
    fallback_futures = _normalize_device_contract_code(fallback_contract)
    if fallback_futures:
        return fallback_futures, "futures", ""
    return "", "", ""


def _resolve_device_voice_market_target(transcript: str, fallback_contract: str = "") -> Tuple[str, str]:
    target, target_category, _ = _resolve_device_voice_market_target_detail(transcript, fallback_contract)
    return target, target_category


def _device_voice_asks_market_fact(transcript: str) -> bool:
    text = _safe_textv(transcript)
    return any(keyword in text for keyword in _DEVICE_VOICE_MARKET_FACT_KEYWORDS)


def _device_voice_asks_time(transcript: str) -> bool:
    text = _safe_textv(transcript)
    return any(marker in text for marker in ("现在几点", "几点了", "几点", "当前时间", "什么时间"))


def _device_voice_asks_stop_listening(transcript: str) -> bool:
    text = _safe_textv(transcript)
    if not text:
        return False
    return any(
        marker in text
        for marker in (
            "结束语音",
            "停止语音",
            "退出语音",
            "关闭语音",
            "别听了",
            "不用听了",
            "先这样",
            "不用了",
            "结束对话",
            "停止对话",
            "退出对话",
            "结束吧",
            "停止吧",
        )
    ) or text in {"结束", "停止", "退出", "再见", "拜拜"}


def _device_voice_asks_deep_analysis(transcript: str) -> bool:
    text = _safe_textv(transcript)
    if not text:
        return False
    price_only_markers = ("价格", "最新价", "现价", "报价", "多少钱", "多少点", "涨跌", "涨幅", "跌幅", "IV", "iv", "波动率", "Rank", "rank")
    if any(marker in text for marker in price_only_markers) and not any(
        marker in text
        for marker in (
            "能做",
            "能不能",
            "可以买",
            "该不该",
            "适合",
            "策略",
            "风险",
            "仓位",
            "止损",
            "止盈",
            "为什么",
            "分析",
            "怎么看",
            "建议",
            "机会",
            "追高",
            "抄底",
            "突破",
            "回调",
        )
    ):
        return False
    return any(keyword in text for keyword in _DEVICE_VOICE_DEEP_ANALYSIS_KEYWORDS)


def _classify_device_voice_route(transcript: str) -> str:
    text = _safe_textv(transcript)
    if not text or text in {"语音识别失败", "语音识别暂未配置"}:
        return "error"
    if _device_voice_asks_stop_listening(text):
        return "stop_listening"
    if _device_voice_asks_deep_analysis(text):
        return "deep_analysis"
    if _device_voice_asks_market_fact(text):
        return "market_fact"
    if any(keyword in text for keyword in _DEVICE_VOICE_INSTANT_REPLY_KEYWORDS):
        return "instant_reply"
    if any(keyword in text for keyword in _DEVICE_VOICE_QUICK_AI_KEYWORDS):
        return "quick_ai"
    return "quick_ai"


def _device_voice_fact_answer(contract_context: Dict[str, Any]) -> str:
    if not contract_context:
        return ""

    product_name = _safe_textv(contract_context.get("product_name"))
    contract = _safe_textv(contract_context.get("contract") or contract_context.get("product_code"))
    label = product_name or contract or "当前标的"
    latest_price = contract_context.get("latest_price")
    price_pct = contract_context.get("price_pct")
    iv = contract_context.get("iv")
    iv_rank = contract_context.get("iv_rank")
    technical_label = _safe_textv(contract_context.get("technical_label")) or "待生成"

    has_price = _safe_floatv(latest_price, default=None) is not None
    has_iv = _safe_floatv(iv, default=None) is not None
    if not has_price and not has_iv:
        return f"{label}当前行情数据还没有稳定取到，我先不乱报价格。"

    parts = [label]
    if has_price:
        parts.append(f"最新价格{_format_device_price(latest_price)}")
        if _safe_floatv(price_pct, default=None) is not None:
            parts.append(f"涨跌{_format_device_metric(price_pct, '%')}")
    if has_iv:
        parts.append(f"IV{_format_device_metric(iv)}")
    if _safe_floatv(iv_rank, default=None) is not None:
        parts.append(f"IV Rank{_format_device_metric(iv_rank)}")
    parts.append(f"技术面{technical_label}")
    return "，".join(parts) + "。"


def _query_device_stock_snapshot(stock_code: str) -> Dict[str, Any]:
    normalized = _normalize_device_stock_code(stock_code) or _safe_textv(stock_code).upper()
    if not normalized:
        return {}

    snapshot: Dict[str, Any] = {
        "name": "",
        "latest_price": None,
        "price_pct": None,
        "updated_at": "",
    }
    try:
        import pandas as pd
        from sqlalchemy import text as _text

        price_df = pd.read_sql(
            _text("""
            SELECT ts_code, name, REPLACE(trade_date, '-', '') AS trade_date, close_price
            FROM stock_price
            WHERE UPPER(ts_code) = :stock_code
              AND close_price IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 2
            """),
            de.engine,
            params={"stock_code": normalized},
        )
        if price_df.empty and normalized.endswith(".US"):
            ticker = normalized.split(".", 1)[0]
            price_df = pd.read_sql(
                _text("""
                SELECT UPPER(symbol) AS ts_code, '' AS name, REPLACE(date, '-', '') AS trade_date, close AS close_price
                FROM stock_prices
                WHERE UPPER(symbol) = :ticker
                  AND close IS NOT NULL
                ORDER BY date DESC
                LIMIT 2
                """),
                de.engine,
                params={"ticker": ticker},
            )
        if price_df.empty:
            return snapshot

        latest = price_df.iloc[0]
        latest_price = _safe_floatv(latest.get("close_price"), default=None)
        snapshot["name"] = _safe_textv(latest.get("name"))
        snapshot["latest_price"] = latest_price
        snapshot["updated_at"] = _safe_textv(latest.get("trade_date"))
        if len(price_df) > 1 and latest_price is not None:
            prev_price = _safe_floatv(price_df.iloc[1].get("close_price"), default=None)
            if prev_price and prev_price > 0:
                snapshot["price_pct"] = round((latest_price / prev_price - 1.0) * 100.0, 2)
    except Exception as exc:
        print(f"[device_api] stock_snapshot_failed stock={normalized} err={exc}", flush=True)
    return snapshot


def _build_device_stock_briefing_payload(
    *,
    username: str,
    request: Request,
    stock_code: str,
    name_hint: str = "",
) -> Dict[str, Any]:
    device_ctx = _extract_device_context(request)
    normalized = _normalize_device_stock_code(stock_code) or _safe_textv(stock_code).upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="股票代码格式错误")

    snap = _query_device_stock_snapshot(normalized)
    latest_price = _safe_floatv(snap.get("latest_price"), default=None)
    price_pct = _safe_floatv(snap.get("price_pct"), default=None)
    name = _safe_textv(snap.get("name")) or _safe_textv(name_hint) or normalized
    updated_at = _safe_textv(snap.get("updated_at")) or _device_now_text()
    data_freshness = "fresh" if latest_price is not None else "degraded"
    technical_state = "pending"
    technical_label = "待生成"
    headline = f"{name} 价格{_format_device_price(latest_price)}，涨跌{_format_device_metric(price_pct, '%')}"
    speak_text = (
        f"{name}。最新价格{_format_device_price(latest_price)}，"
        f"涨跌{_format_device_metric(price_pct, '%')}。"
        f"技术面状态{technical_label}。"
    )

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "category": "stock",
        "contract": normalized,
        "product_code": normalized,
        "product_name": name,
        "latest_price": latest_price,
        "price_pct": price_pct,
        "iv": None,
        "iv_rank": None,
        "technical_state": technical_state,
        "technical_label": technical_label,
        "headline": headline,
        "speak_text": speak_text,
        "updated_at": updated_at,
        "data_freshness": data_freshness,
    }


def _build_device_futures_product_briefing_payload(
    *,
    username: str,
    request: Request,
    product_code: str,
) -> Dict[str, Any]:
    product = _safe_textv(product_code).lower()
    if not re.match(r"^[a-z]{1,4}$", product):
        return {}
    try:
        raw = market_contracts(product=product, username=username)
        for item in raw.get("items", []) if isinstance(raw, dict) else []:
            contract = _normalize_device_contract_code(_extract_contract_code(item.get("name", "")))
            if contract:
                return _build_device_contract_briefing_payload(
                    username=username,
                    request=request,
                    contract=contract,
                    category="futures",
                )
    except Exception as exc:
        print(f"[device_api] futures_product_context_failed product={product} err={exc}", flush=True)
    return {}


def _load_device_voice_market_context(
    *,
    username: str,
    request: Request,
    transcript: str,
    contract: str,
    category: str,
) -> Dict[str, Any]:
    target, target_category, name_hint = _resolve_device_voice_market_target_detail(transcript, contract)
    if not target:
        return {}
    try:
        if target_category == "stock":
            return _build_device_stock_briefing_payload(
                username=username,
                request=request,
                stock_code=target,
                name_hint=name_hint,
            )
        if target_category == "futures_product":
            return _build_device_futures_product_briefing_payload(
                username=username,
                request=request,
                product_code=target,
            )
        return _build_device_contract_briefing_payload(
            username=username,
            request=request,
            contract=target,
            category=target_category or category,
        )
    except Exception as exc:
        print(f"[device_api] voice_market_context_failed target={target} err={exc}", flush=True)
        return {}


def _query_device_etf_snapshot(etf_code: str) -> Dict[str, Any]:
    normalized = _normalize_device_etf_code(etf_code)
    if not normalized:
        return {}

    snapshot: Dict[str, Any] = {
        "latest_price": None,
        "price_pct": None,
        "iv": None,
        "iv_rank": None,
        "updated_at": "",
    }
    try:
        import pandas as pd
        from sqlalchemy import text as _text

        iv_df = pd.read_sql(
            _text("""
            SELECT REPLACE(trade_date, '-', '') AS trade_date, iv
            FROM etf_iv_history
            WHERE etf_code = :etf_code
              AND iv IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 252
            """),
            de.engine,
            params={"etf_code": normalized},
        )
        if not iv_df.empty:
            current_iv = _safe_floatv(iv_df.iloc[0].get("iv"), default=None)
            snapshot["iv"] = current_iv
            snapshot["updated_at"] = _safe_textv(iv_df.iloc[0].get("trade_date"))
            if current_iv is not None:
                iv_values = [
                    _safe_floatv(value, default=None)
                    for value in iv_df["iv"].tolist()
                ]
                iv_values = [value for value in iv_values if value is not None]
                if iv_values:
                    rank = sum(1 for value in iv_values if value <= current_iv) / len(iv_values) * 100.0
                    snapshot["iv_rank"] = round(rank, 1)

        price_df = pd.read_sql(
            _text("""
            SELECT REPLACE(trade_date, '-', '') AS trade_date, close_price
            FROM stock_price
            WHERE ts_code = :etf_code
              AND close_price IS NOT NULL
            ORDER BY trade_date DESC
            LIMIT 2
            """),
            de.engine,
            params={"etf_code": normalized},
        )
        if not price_df.empty:
            latest_price = _safe_floatv(price_df.iloc[0].get("close_price"), default=None)
            snapshot["latest_price"] = latest_price
            snapshot["updated_at"] = snapshot["updated_at"] or _safe_textv(price_df.iloc[0].get("trade_date"))
            if len(price_df) > 1 and latest_price is not None:
                prev_price = _safe_floatv(price_df.iloc[1].get("close_price"), default=None)
                if prev_price and prev_price > 0:
                    snapshot["price_pct"] = round((latest_price / prev_price - 1.0) * 100.0, 2)
    except Exception as exc:
        print(f"[device_api] etf_snapshot_failed etf={normalized} err={exc}", flush=True)
    return snapshot


def _build_device_etf_menu_payload(
    *,
    username: str,
    request: Request,
    max_products: int,
    product: Optional[str] = None,
) -> Dict[str, Any]:
    _ = username
    device_ctx = _extract_device_context(request)
    product_filter = _normalize_device_etf_code(product)
    catalog = [item for item in _DEVICE_ETF_PRODUCTS if not product_filter or item["product_code"] == product_filter]
    products: List[Dict[str, Any]] = []

    for item in catalog[:max_products]:
        code = item["product_code"]
        name = item["product_name"]
        contracts: List[Dict[str, Any]] = []
        if product_filter:
            snap = _query_device_etf_snapshot(code)
            contracts.append(
                {
                    "contract": code,
                    "label": name,
                    "latest_price": _safe_floatv(snap.get("latest_price"), default=None),
                    "price_pct": _safe_floatv(snap.get("price_pct"), default=None),
                    "iv": _safe_floatv(snap.get("iv"), default=None),
                    "iv_rank": _safe_floatv(snap.get("iv_rank"), default=None),
                }
            )
        products.append(
            {
                "category": "etf",
                "product_code": code,
                "product_name": name,
                "contracts": contracts,
            }
        )

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "category": "etf",
        "products": products,
        "updated_at": _device_now_text(),
        "data_freshness": "fresh" if products else "degraded",
    }


def _build_device_etf_briefing_payload(
    *,
    username: str,
    request: Request,
    etf_code: str,
) -> Dict[str, Any]:
    device_ctx = _extract_device_context(request)
    normalized = _normalize_device_etf_code(etf_code)
    if not normalized:
        raise HTTPException(status_code=400, detail="ETF代码格式错误")

    name = _device_etf_name(normalized)
    snap = _query_device_etf_snapshot(normalized)
    latest_price = _safe_floatv(snap.get("latest_price"), default=None)
    price_pct = _safe_floatv(snap.get("price_pct"), default=None)
    iv = _safe_floatv(snap.get("iv"), default=None)
    iv_rank = _safe_floatv(snap.get("iv_rank"), default=None)
    updated_at = _safe_textv(snap.get("updated_at")) or _device_now_text()
    data_freshness = "fresh" if iv is not None or latest_price is not None else "degraded"

    technical_state = "pending"
    technical_label = "待生成"
    headline = (
        f"{name} 价格{_format_device_price(latest_price)}，"
        f"IV{_format_device_metric(iv)}，Rank{_format_device_metric(iv_rank)}"
    )
    speak_text = (
        f"{name}。最新价格{_format_device_price(latest_price)}，"
        f"涨跌{_format_device_metric(price_pct, '%')}，"
        f"IV{_format_device_metric(iv)}，"
        f"IV Rank{_format_device_metric(iv_rank)}。"
        f"技术面状态{technical_label}。"
    )

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "category": "etf",
        "contract": normalized,
        "product_code": normalized,
        "product_name": name,
        "latest_price": latest_price,
        "price_pct": price_pct,
        "iv": iv,
        "iv_rank": iv_rank,
        "technical_state": technical_state,
        "technical_label": technical_label,
        "headline": headline,
        "speak_text": speak_text,
        "updated_at": updated_at,
        "data_freshness": data_freshness,
    }


def _build_device_contract_menu_payload(
    *,
    username: str,
    request: Request,
    max_products: int = 12,
    max_contracts: int = 6,
    product: Optional[str] = None,
    category: Optional[str] = None,
) -> Dict[str, Any]:
    device_ctx = _extract_device_context(request)
    category_value = _normalize_device_menu_category(category)
    max_products = max(1, min(int(max_products or 12), 60))
    max_contracts = max(1, min(int(max_contracts or 6), 12))
    if category_value == "etf":
        return _build_device_etf_menu_payload(
            username=username,
            request=request,
            max_products=max_products,
            product=product,
        )

    product_filter = _safe_textv(product).lower()
    product_filter = product_filter if re.match(r"^[a-z]{1,4}$", product_filter) else ""

    grouped: Dict[str, Dict[str, Any]] = {}

    if product_filter:
        grouped[product_filter] = {
            "category": category_value,
            "product_code": product_filter,
            "product_name": _device_product_name(product_filter),
            "contracts": [],
        }
        raw: Dict[str, Any] = {"items": [], "updated_at": _device_now_text()}
    else:
        try:
            raw = market_options(username=username)
        except Exception as exc:
            print(f"[device_api] contract_menu_options_failed err={exc}", flush=True)
            raw = {"items": []}

        for item in raw.get("items", []) if isinstance(raw, dict) else []:
            contract = _normalize_device_contract_code(_extract_contract_code(item.get("name", "")))
            product_code = _safe_textv(item.get("product_code")).lower() or _device_contract_product_code(contract)
            if not contract or not product_code:
                continue
            if category_value == "favorites" and product_code not in _DEVICE_FAVORITE_PRODUCTS:
                continue
            product_item = grouped.setdefault(
                product_code,
                {
                    "category": category_value,
                    "product_code": product_code,
                    "product_name": _device_product_name(product_code),
                    "contracts": [],
                },
            )
            product_item["contracts"].append(
                {
                    "contract": contract,
                    "label": _device_contract_display(contract, product_item["product_name"]),
                    "latest_price": _safe_floatv(item.get("cur_price"), default=None),
                    "price_pct": _safe_floatv(item.get("pct_1d"), default=None),
                    "iv": _safe_floatv(item.get("iv"), default=None),
                    "iv_rank": _safe_floatv(item.get("iv_rank"), default=None),
                }
            )

    products = []
    for product in grouped.values():
        if product_filter:
            try:
                expanded = market_contracts(product=product["product_code"], username=username)
            except Exception as exc:
                print(
                    f"[device_api] contract_menu_expand_failed product={product['product_code']} err={exc}",
                    flush=True,
                )
                expanded = {"items": []}
            expanded_contracts = []
            for item in expanded.get("items", []) if isinstance(expanded, dict) else []:
                contract = _normalize_device_contract_code(_extract_contract_code(item.get("name", "")))
                if not contract:
                    continue
                expanded_contracts.append(
                    {
                        "contract": contract,
                        "label": _device_contract_display(contract, product["product_name"]),
                        "latest_price": _safe_floatv(item.get("cur_price"), default=None),
                        "price_pct": _safe_floatv(item.get("pct_1d"), default=None),
                        "iv": _safe_floatv(item.get("iv"), default=None),
                        "iv_rank": _safe_floatv(item.get("iv_rank"), default=None),
                    }
                )
            product["contracts"] = expanded_contracts

        contracts = product["contracts"]
        contracts.sort(
            key=lambda x: (
                0 if _safe_floatv(x.get("iv"), default=0.0) and _safe_floatv(x.get("iv"), default=0.0) > 0 else 1,
                -(_safe_floatv(x.get("iv_rank"), default=-999.0) or -999.0),
                x.get("contract") or "",
            )
        )
        product["contracts"] = contracts[:max_contracts]
        products.append(product)

    if category_value == "favorites":
        order = {code: idx for idx, code in enumerate(_DEVICE_FAVORITE_PRODUCTS)}
        products.sort(key=lambda x: order.get(x.get("product_code", ""), 999))
    else:
        products.sort(key=lambda x: x.get("product_code", ""))
    products = products[:max_products]

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "category": category_value,
        "products": products,
        "updated_at": _safe_textv(raw.get("updated_at")) if isinstance(raw, dict) else _device_now_text(),
        "data_freshness": "fresh" if products else "degraded",
    }


def _find_device_contract_item(username: str, contract: str) -> Dict[str, Any]:
    product_code = _device_contract_product_code(contract)
    if not product_code:
        return {}
    try:
        raw = market_contracts(product=product_code, username=username)
    except Exception as exc:
        print(f"[device_api] contract_lookup_failed contract={contract} err={exc}", flush=True)
        raw = {"items": []}

    for item in raw.get("items", []) if isinstance(raw, dict) else []:
        item_contract = _normalize_device_contract_code(_extract_contract_code(item.get("name", "")))
        if item_contract == contract:
            return dict(item)
    return {}


def _build_device_contract_briefing_payload(
    *,
    username: str,
    request: Request,
    contract: str,
    category: Optional[str] = None,
) -> Dict[str, Any]:
    category_value = _normalize_device_menu_category(category)
    if category_value == "etf" or _normalize_device_etf_code(contract):
        return _build_device_etf_briefing_payload(username=username, request=request, etf_code=contract)

    device_ctx = _extract_device_context(request)
    normalized_contract = _normalize_device_contract_code(contract)
    if not normalized_contract:
        raise HTTPException(status_code=400, detail="合约格式错误")

    product_code = _device_contract_product_code(normalized_contract)
    product_name = _device_product_name(product_code)
    item = _find_device_contract_item(username=username, contract=normalized_contract)

    chart_payload: Dict[str, Any] = {}
    try:
        raw_chart = market_chart(product=product_code, contract=normalized_contract, username=username)
        if isinstance(raw_chart, dict):
            chart_payload = raw_chart
    except Exception as exc:
        print(f"[device_api] contract_chart_failed contract={normalized_contract} err={exc}", flush=True)

    latest_price = _safe_floatv(chart_payload.get("cur_price"), default=None)
    if latest_price is None:
        latest_price = _safe_floatv(item.get("cur_price"), default=None)
    price_pct = _safe_floatv(chart_payload.get("cur_pct"), default=None)
    if price_pct is None:
        price_pct = _safe_floatv(item.get("pct_1d"), default=None)
    iv = _safe_floatv(chart_payload.get("cur_iv"), default=None)
    if iv is None:
        iv = _safe_floatv(item.get("iv"), default=None)
    iv_rank = _safe_floatv(item.get("iv_rank"), default=None)

    has_price = latest_price is not None and latest_price > 0
    has_iv = iv is not None and iv > 0
    data_freshness = "fresh" if has_price or has_iv else "degraded"

    technical_state = "pending"
    technical_label = "待生成"
    headline = (
        f"{normalized_contract} 价格{_format_device_price(latest_price)}，"
        f"IV{_format_device_metric(iv)}，Rank{_format_device_metric(iv_rank)}"
    )
    speak_text = (
        f"{product_name}{normalized_contract}。"
        f"最新价格{_format_device_price(latest_price)}，"
        f"涨跌{_format_device_metric(price_pct, '%')}，"
        f"IV{_format_device_metric(iv)}，"
        f"IV Rank{_format_device_metric(iv_rank)}。"
        f"技术面状态{technical_label}。"
    )
    updated_at = _safe_textv(chart_payload.get("db_cur_td")) or _device_now_text()

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "contract": normalized_contract,
        "product_code": product_code,
        "product_name": product_name,
        "latest_price": latest_price,
        "price_pct": price_pct,
        "iv": iv,
        "iv_rank": iv_rank,
        "technical_state": technical_state,
        "technical_label": technical_label,
        "headline": headline,
        "speak_text": speak_text,
        "updated_at": updated_at,
        "data_freshness": data_freshness,
    }


def _read_device_wav_info(audio_bytes: bytes, *, enforce_device_recording_limits: bool = True) -> Dict[str, int]:
    if len(audio_bytes) < 44:
        raise HTTPException(status_code=400, detail="音频太短，请上传 WAV")
    if enforce_device_recording_limits and len(audio_bytes) > _DEVICE_VOICE_MAX_WAV_BYTES:
        raise HTTPException(status_code=413, detail="音频超过 8 秒限制")
    if audio_bytes[:4] != b"RIFF" or audio_bytes[8:12] != b"WAVE":
        raise HTTPException(status_code=400, detail="仅支持 WAV 音频")

    offset = 12
    fmt_info: Dict[str, int] = {}
    data_size = 0
    while offset + 8 <= len(audio_bytes):
        chunk_id = audio_bytes[offset : offset + 4]
        chunk_size = struct.unpack_from("<I", audio_bytes, offset + 4)[0]
        chunk_start = offset + 8
        chunk_end = chunk_start + chunk_size
        if chunk_end > len(audio_bytes):
            raise HTTPException(status_code=400, detail="WAV 数据不完整")
        if chunk_id == b"fmt ":
            if chunk_size < 16:
                raise HTTPException(status_code=400, detail="WAV fmt chunk 无效")
            audio_format, channels, sample_rate, _, _, bits_per_sample = struct.unpack_from(
                "<HHIIHH", audio_bytes, chunk_start
            )
            fmt_info = {
                "audio_format": int(audio_format),
                "channels": int(channels),
                "sample_rate": int(sample_rate),
                "bits_per_sample": int(bits_per_sample),
            }
        elif chunk_id == b"data":
            data_size = int(chunk_size)
            break
        offset = chunk_end + (chunk_size % 2)

    if not fmt_info or data_size <= 0:
        raise HTTPException(status_code=400, detail="WAV 缺少 fmt 或 data chunk")
    if fmt_info["audio_format"] != 1:
        raise HTTPException(status_code=400, detail="仅支持 PCM WAV")
    if fmt_info["channels"] != _DEVICE_VOICE_CHANNELS:
        raise HTTPException(status_code=400, detail="仅支持单声道音频")
    if fmt_info["sample_rate"] != _DEVICE_VOICE_SAMPLE_RATE:
        raise HTTPException(status_code=400, detail="仅支持 16kHz 音频")
    if fmt_info["bits_per_sample"] != _DEVICE_VOICE_BITS_PER_SAMPLE:
        raise HTTPException(status_code=400, detail="仅支持 16-bit 音频")
    fmt_info["data_size"] = data_size
    fmt_info["data_offset"] = chunk_start
    return fmt_info


def _device_wav_signal_stats(audio_bytes: bytes, wav_info: Dict[str, int]) -> Dict[str, Any]:
    data_offset = int(wav_info.get("data_offset") or 0)
    data_size = int(wav_info.get("data_size") or 0)
    sample_rate = int(wav_info.get("sample_rate") or _DEVICE_VOICE_SAMPLE_RATE)
    bits_per_sample = int(wav_info.get("bits_per_sample") or _DEVICE_VOICE_BITS_PER_SAMPLE)
    if bits_per_sample != 16 or data_offset <= 0 or data_size <= 0:
        return {"duration_ms": 0, "samples": 0, "peak": 0, "rms": 0.0}

    data_end = min(len(audio_bytes), data_offset + data_size)
    sample_count = max(0, (data_end - data_offset) // 2)
    if sample_count <= 0:
        return {"duration_ms": 0, "samples": 0, "peak": 0, "rms": 0.0}

    peak = 0
    square_sum = 0.0
    for idx in range(sample_count):
        sample = struct.unpack_from("<h", audio_bytes, data_offset + idx * 2)[0]
        abs_sample = abs(int(sample))
        if abs_sample > peak:
            peak = abs_sample
        square_sum += float(sample) * float(sample)

    rms = math.sqrt(square_sum / sample_count) if sample_count else 0.0
    duration_ms = int(round(sample_count * 1000.0 / max(1, sample_rate)))
    return {
        "duration_ms": duration_ms,
        "samples": sample_count,
        "peak": int(peak),
        "rms": round(float(rms), 1),
    }


def _build_pcm_wav(pcm_bytes: bytes, *, sample_rate: int = 16000, channels: int = 1, bits_per_sample: int = 16) -> bytes:
    byte_rate = sample_rate * channels * bits_per_sample // 8
    block_align = channels * bits_per_sample // 8
    data_size = len(pcm_bytes)
    header = struct.pack(
        "<4sI4s4sIHHIIHH4sI",
        b"RIFF",
        36 + data_size,
        b"WAVE",
        b"fmt ",
        16,
        1,
        channels,
        sample_rate,
        byte_rate,
        block_align,
        bits_per_sample,
        b"data",
        data_size,
    )
    return header + pcm_bytes


def _extract_dashscope_text(response: Any) -> str:
    if response is None:
        return ""
    data = response
    try:
        if hasattr(response, "output"):
            data = response.output
        if isinstance(data, dict):
            choices = data.get("choices") or []
            if choices:
                message = choices[0].get("message") or {}
                content = message.get("content")
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    parts = []
                    for item in content:
                        if isinstance(item, dict):
                            parts.append(str(item.get("text") or item.get("transcript") or "").strip())
                    return " ".join([part for part in parts if part]).strip()
            text = data.get("text") or data.get("transcript") or ""
            return str(text).strip()
    except Exception:
        return ""
    return str(data).strip() if isinstance(data, str) else ""


def _extract_dashscope_asr_text(response: Any) -> str:
    if response is None:
        return ""
    try:
        sentences = response.get_sentence() if hasattr(response, "get_sentence") else None
        if isinstance(sentences, dict):
            sentences = [sentences]
        parts = []
        if isinstance(sentences, list):
            for sentence in sentences:
                if isinstance(sentence, dict):
                    text = sentence.get("text") or sentence.get("transcript") or ""
                    if text:
                        parts.append(str(text).strip())
                elif isinstance(sentence, str):
                    parts.append(sentence.strip())
        if parts:
            return "".join(parts).strip()
    except Exception:
        return ""
    return _extract_dashscope_text(response)


def _device_transcribe_wav(audio_bytes: bytes) -> str:
    override = _safe_textv(os.getenv("DEVICE_STT_TEXT_OVERRIDE"))
    if override:
        return override
    api_key = _safe_textv(os.getenv("DASHSCOPE_API_KEY"))
    if not api_key:
        raise RuntimeError("DASHSCOPE_API_KEY not configured")

    import tempfile
    import dashscope

    path = ""
    try:
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
            tmp.write(audio_bytes)
            path = tmp.name
        model = os.getenv("DEVICE_STT_MODEL", "paraformer-realtime-v2")
        if model.startswith("qwen-audio"):
            response = dashscope.MultiModalConversation.call(
                model=model,
                api_key=api_key,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"audio": f"file://{path}"},
                            {"text": "请把这段普通话语音转写成纯文本，只输出转写内容。"},
                        ],
                    }
                ],
            )
            transcript = _extract_dashscope_text(response)
        else:
            from dashscope.audio.asr import Recognition

            dashscope.api_key = api_key
            recognition = Recognition(
                model=model,
                format=os.getenv("DEVICE_STT_AUDIO_FORMAT", "wav"),
                sample_rate=int(os.getenv("DEVICE_STT_SAMPLE_RATE", "16000")),
                language_hints=["zh", "en"],
                callback=None,
            )
            response = recognition.call(path)
            transcript = _extract_dashscope_asr_text(response)
        if not transcript:
            status_code = getattr(response, "status_code", "")
            code = getattr(response, "code", "")
            message = getattr(response, "message", "")
            request_id = getattr(response, "request_id", "")
            raise RuntimeError(
                f"empty transcript status={status_code} code={code} "
                f"message={message} request_id={request_id}"
            )
        return transcript
    finally:
        if path:
            try:
                os.unlink(path)
            except Exception:
                pass


def _build_device_voice_prompt(
    *,
    transcript: str,
    contract_context: Dict[str, Any],
    screen_context: str,
) -> str:
    context_lines = []
    if contract_context:
        context_lines.append(
            "当前设备看板："
            f"{contract_context.get('product_name') or ''}{contract_context.get('contract') or ''}，"
            f"最新价{_format_device_price(contract_context.get('latest_price'))}，"
            f"涨跌{_format_device_metric(contract_context.get('price_pct'), '%')}，"
            f"IV{_format_device_metric(contract_context.get('iv'))}，"
            f"IV Rank{_format_device_metric(contract_context.get('iv_rank'))}。"
        )
    if screen_context:
        context_lines.append(f"设备屏幕上下文：{screen_context[:300]}")
    context_text = "\n".join(context_lines) if context_lines else "当前没有明确合约上下文。"
    return (
        "你正在通过桌面机器人 StackChan 回答交易者的问题。"
        "回答必须适合语音播报，最多 80 个中文字，直接给结论和理由，不要输出 Markdown。\n"
        "如果用户问价格、涨跌、IV、IV Rank、技术面，只能使用当前设备看板里的数字；"
        "没有数字就说数据暂缺，禁止编造行情。\n"
        f"{context_text}\n"
        f"用户语音转写：{transcript}"
    )


def _device_generate_voice_answer(
    *,
    username: str,
    transcript: str,
    contract_context: Dict[str, Any],
    screen_context: str = "",
) -> str:
    if not transcript or transcript in {"语音识别失败", "语音识别暂未配置"}:
        return "我没有听清楚，你可以再短按一次，用普通话问我。"
    prompt = _build_device_voice_prompt(
        transcript=transcript,
        contract_context=contract_context,
        screen_context=screen_context,
    )
    try:
        llm = build_deepseek_flash_llm(
            model=os.getenv("DEVICE_VOICE_LLM_MODEL") or None,
            streaming=False,
            temperature=0.2,
        )
        answer = simple_chatter_reply(prompt, llm, runtime_context={"current_user": username, "device": "StackChan"})
        answer = _safe_textv(answer)
        return answer[:180] if answer else "我暂时没有拿到稳定回答，请稍后再试。"
    except Exception as exc:
        print(f"[device_api] voice_llm_failed user={username} err={exc}", flush=True)
        if contract_context:
            return (
                f"{contract_context.get('contract') or '当前合约'}最新价"
                f"{_format_device_price(contract_context.get('latest_price'))}，"
                f"IV{_format_device_metric(contract_context.get('iv'))}，"
                f"IV Rank{_format_device_metric(contract_context.get('iv_rank'))}。"
            )
    return "我暂时无法连接大模型，但设备和后端链路是通的。"


def _device_voice_observed_audio_stats(
    audio_stats: Dict[str, Any],
    *,
    client_audio_peak: Any = None,
    client_audio_rms: Any = None,
) -> Dict[str, Any]:
    peak = _safe_floatv(audio_stats.get("peak"), default=0.0) or 0.0
    rms = _safe_floatv(audio_stats.get("rms"), default=0.0) or 0.0
    client_peak = _safe_floatv(client_audio_peak, default=None)
    client_rms = _safe_floatv(client_audio_rms, default=None)
    if client_peak is not None:
        peak = max(peak, client_peak)
    if client_rms is not None:
        rms = max(rms, client_rms)
    return {
        "duration_ms": int(_safe_floatv(audio_stats.get("duration_ms"), default=0.0) or 0),
        "peak": int(round(peak)),
        "rms": round(float(rms), 1),
    }


def _classify_device_voice_stt_failure(
    stt_error: str,
    audio_stats: Dict[str, Any],
    *,
    client_audio_peak: Any = None,
    client_audio_rms: Any = None,
) -> str:
    text = _safe_textv(stt_error)
    lower = text.lower()
    if "allocationquota" in lower or "quota" in lower or "throttling" in lower:
        return "stt_quota"
    if "api_key" in lower or "unauthorized" in lower or "forbidden" in lower or "status=401" in lower:
        return "stt_auth"
    if "timeout" in lower:
        return "stt_timeout"

    observed = _device_voice_observed_audio_stats(
        audio_stats,
        client_audio_peak=client_audio_peak,
        client_audio_rms=client_audio_rms,
    )
    duration_ms = int(observed.get("duration_ms") or 0)
    peak = int(observed.get("peak") or 0)
    rms = float(observed.get("rms") or 0.0)
    if duration_ms > 0 and duration_ms < _DEVICE_VOICE_MIN_CLEAR_AUDIO_MS:
        return "recording_too_short"
    if peak <= _DEVICE_VOICE_LOW_PEAK_THRESHOLD and rms <= _DEVICE_VOICE_LOW_RMS_THRESHOLD:
        return "audio_too_quiet"
    if "empty transcript" in lower:
        return "stt_empty"
    return "stt_unavailable"


def _device_voice_stt_error_answer(stt_error: str, failure_reason: str = "") -> str:
    reason = _safe_textv(failure_reason)
    if reason == "recording_too_short":
        return "没听清楚，请再说一次哦。"
    if reason == "audio_too_quiet":
        return "没听清楚，请再说一次哦。"
    text = _safe_textv(stt_error)
    lower = text.lower()
    if "allocationquota" in lower or "quota" in lower or "throttling" in lower:
        return "语音识别额度暂时用完了，我现在无法把你的语音转成文字。请稍后再试，或先用文字问我。"
    if "api_key" in lower or "unauthorized" in lower or "forbidden" in lower or "status=401" in lower:
        return "语音识别服务的密钥配置有问题，我现在无法识别语音。请检查后端配置。"
    if "timeout" in lower:
        return "没听清楚，请再说一次哦。"
    if "empty transcript" in lower:
        return "没听清楚，请再说一次哦。"
    return "语音识别服务暂时不可用，我现在无法把你的语音转成文字。请稍后再试，或先用文字问我。"


def _device_synthesize_macos_say_wav(text: str) -> Optional[bytes]:
    if platform.system() != "Darwin":
        return None
    if str(os.getenv("DEVICE_TTS_MACOS_FALLBACK", "1")).strip().lower() in {"0", "false", "no", "off"}:
        return None
    say_bin = shutil.which("say")
    afconvert_bin = shutil.which("afconvert")
    if not say_bin or not afconvert_bin:
        return None

    import tempfile

    aiff_path = ""
    wav_path = ""
    try:
        with tempfile.NamedTemporaryFile(suffix=".aiff", delete=False) as aiff_tmp:
            aiff_path = aiff_tmp.name
        with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as wav_tmp:
            wav_path = wav_tmp.name

        voice = os.getenv("DEVICE_TTS_MACOS_VOICE", "Tingting")
        subprocess.run(
            [say_bin, "-v", voice, "-o", aiff_path, text[:120]],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=8,
        )
        subprocess.run(
            [afconvert_bin, "-f", "WAVE", "-d", "LEI16@16000", aiff_path, wav_path],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=8,
        )
        with open(wav_path, "rb") as f:
            wav_bytes = f.read()
        return wav_bytes if wav_bytes.startswith(b"RIFF") else None
    except Exception as exc:
        print(f"[device_api] voice_macos_tts_failed err={exc}", flush=True)
        return None
    finally:
        for path in (aiff_path, wav_path):
            if path:
                try:
                    os.unlink(path)
                except Exception:
                    pass


class _DeviceVoiceRealtimeAsrSession:
    def __init__(self) -> None:
        self._recognition = None
        self._events: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._final_text = ""
        self._partial_text = ""
        self.enabled = False
        self.error = ""

    def start(self) -> bool:
        if str(os.getenv("DEVICE_REALTIME_ASR_ENABLED", "1")).strip().lower() in {"0", "false", "no", "off"}:
            self.error = "disabled"
            return False
        api_key = _safe_textv(os.getenv("DASHSCOPE_API_KEY"))
        if not api_key:
            self.error = "DASHSCOPE_API_KEY not configured"
            return False
        try:
            import dashscope
            from dashscope.audio.asr import Recognition, RecognitionCallback

            parent = self

            class Callback(RecognitionCallback):
                def on_event(self, result) -> None:  # type: ignore[override]
                    sentences = result.get_sentence() if hasattr(result, "get_sentence") else None
                    items = sentences if isinstance(sentences, list) else [sentences]
                    for sentence in items:
                        if not isinstance(sentence, dict):
                            continue
                        text = _safe_textv(sentence.get("text") or sentence.get("transcript"))
                        if not text:
                            continue
                        is_final = False
                        try:
                            is_final = bool(result.is_sentence_end(sentence))
                        except Exception:
                            is_final = bool(sentence.get("sentence_end") or sentence.get("is_final"))
                        parent._push_text(text, is_final=is_final)

                def on_error(self, result) -> None:  # type: ignore[override]
                    message = _safe_textv(getattr(result, "message", "")) or _safe_textv(result)
                    with parent._lock:
                        parent.error = message or "realtime asr error"
                        parent._events.append({"type": "asr_error", "message": parent.error})

            dashscope.api_key = api_key
            self._recognition = Recognition(
                model=os.getenv("DEVICE_REALTIME_STT_MODEL", os.getenv("DEVICE_STT_MODEL", "paraformer-realtime-v2")),
                format="pcm",
                sample_rate=_DEVICE_VOICE_SAMPLE_RATE,
                language_hints=["zh", "en"],
                callback=Callback(),
            )
            self._recognition.start()
            self.enabled = True
            return True
        except Exception as exc:
            self.error = str(exc)
            print(f"[device_api] realtime_asr_start_failed err={exc}", flush=True)
            return False

    def _push_text(self, text: str, *, is_final: bool) -> None:
        with self._lock:
            if is_final:
                self._final_text = text
                event_type = "final_transcript"
            else:
                self._partial_text = text
                event_type = "partial_transcript"
            self._events.append({"type": event_type, "text": text})

    def send_audio_frame(self, frame: bytes) -> None:
        if not self.enabled or not self._recognition or not frame:
            return
        try:
            self._recognition.send_audio_frame(frame)
        except Exception as exc:
            with self._lock:
                self.error = str(exc)
                self._events.append({"type": "asr_error", "message": self.error})
            self.enabled = False

    def stop(self) -> None:
        if not self._recognition:
            return
        try:
            self._recognition.stop()
        except Exception as exc:
            print(f"[device_api] realtime_asr_stop_failed err={exc}", flush=True)

    def drain_events(self) -> List[Dict[str, Any]]:
        with self._lock:
            events = list(self._events)
            self._events.clear()
        return events

    def best_text(self) -> str:
        with self._lock:
            return self._final_text or self._partial_text


def _device_audio_gain() -> float:
    try:
        value = float(os.getenv("DEVICE_TTS_VOLUME_GAIN", str(_DEVICE_TTS_VOLUME_GAIN_DEFAULT)))
        return max(0.2, min(2.0, value))
    except Exception:
        return _DEVICE_TTS_VOLUME_GAIN_DEFAULT


def _clip_int16(value: float) -> int:
    if value > 32767:
        return 32767
    if value < -32768:
        return -32768
    return int(round(value))


def _decode_wav_to_mono_i16(audio_bytes: bytes) -> tuple[List[int], int]:
    with wave.open(io.BytesIO(audio_bytes), "rb") as wav_file:
        channels = wav_file.getnchannels()
        sample_width = wav_file.getsampwidth()
        sample_rate = wav_file.getframerate()
        frame_count = wav_file.getnframes()
        raw = wav_file.readframes(frame_count)

    if channels <= 0 or sample_rate <= 0 or sample_width not in {1, 2, 3, 4}:
        return [], 0

    frame_size = channels * sample_width
    if frame_size <= 0:
        return [], 0

    samples: List[int] = []
    for offset in range(0, len(raw) - frame_size + 1, frame_size):
        total = 0
        for channel in range(channels):
            start = offset + channel * sample_width
            chunk = raw[start : start + sample_width]
            if sample_width == 1:
                value = (int(chunk[0]) - 128) << 8
            elif sample_width == 2:
                value = int.from_bytes(chunk, "little", signed=True)
            elif sample_width == 3:
                sign = b"\xff" if chunk[2] & 0x80 else b"\x00"
                value = int.from_bytes(chunk + sign, "little", signed=True) >> 8
            else:
                value = int.from_bytes(chunk, "little", signed=True) >> 16
            total += value
        samples.append(_clip_int16(total / channels))
    return samples, sample_rate


def _resample_i16(samples: List[int], source_rate: int, target_rate: int) -> List[int]:
    if not samples or source_rate <= 0 or source_rate == target_rate:
        return samples
    target_count = max(1, int(round(len(samples) * float(target_rate) / float(source_rate))))
    if target_count == len(samples):
        return samples
    if target_count == 1 or len(samples) == 1:
        return [samples[0]]

    result: List[int] = []
    scale = float(len(samples) - 1) / float(target_count - 1)
    for idx in range(target_count):
        pos = idx * scale
        left = int(pos)
        right = min(left + 1, len(samples) - 1)
        frac = pos - left
        value = samples[left] * (1.0 - frac) + samples[right] * frac
        result.append(_clip_int16(value))
    return result


def _soft_limit_i16(samples: List[int]) -> List[int]:
    if not samples:
        return samples
    peak = max(abs(int(sample)) for sample in samples)
    if peak <= 0 or peak <= _DEVICE_TTS_HARD_PEAK:
        return samples
    scale = float(_DEVICE_TTS_TARGET_PEAK) / float(peak)
    return [_clip_int16(int(sample) * scale) for sample in samples]


def _normalize_device_tts_wav(audio_bytes: Optional[bytes]) -> Optional[bytes]:
    if not audio_bytes or not bytes(audio_bytes).startswith(b"RIFF"):
        return audio_bytes
    try:
        samples, sample_rate = _decode_wav_to_mono_i16(bytes(audio_bytes))
        if not samples:
            return audio_bytes
        samples = _resample_i16(samples, sample_rate, _DEVICE_VOICE_SAMPLE_RATE)
        gain = _device_audio_gain()
        if gain != 1.0:
            samples = [_clip_int16(sample * gain) for sample in samples]
        samples = _soft_limit_i16(samples)
        pcm = bytearray()
        for sample in samples:
            pcm.extend(int(sample).to_bytes(2, "little", signed=True))
        return _build_pcm_wav(bytes(pcm), sample_rate=_DEVICE_VOICE_SAMPLE_RATE)
    except Exception as exc:
        print(f"[device_api] voice_tts_normalize_failed err={exc}", flush=True)
        return audio_bytes


def _extract_device_qwen_tts_audio_url(response: Any) -> str:
    data = getattr(response, "output", response)
    if not isinstance(data, dict):
        data = getattr(data, "__dict__", {}) if data is not None else {}
    audio = data.get("audio") if isinstance(data, dict) else None
    if isinstance(audio, dict):
        return _safe_textv(audio.get("url"))
    if audio is not None:
        return _safe_textv(getattr(audio, "url", ""))
    return _safe_textv(data.get("audio_url") if isinstance(data, dict) else "")


def _extract_device_qwen_tts_audio_data(response: Any) -> bytes:
    data = getattr(response, "output", response)
    if not isinstance(data, dict):
        data = getattr(data, "__dict__", {}) if data is not None else {}
    audio = data.get("audio") if isinstance(data, dict) else None
    raw = ""
    if isinstance(audio, dict):
        raw = _safe_textv(audio.get("data") or audio.get("audio"))
    elif audio is not None:
        raw = _safe_textv(getattr(audio, "data", "") or getattr(audio, "audio", ""))
    if not raw:
        raw = _safe_textv(data.get("audio_data") if isinstance(data, dict) else "")
    if not raw:
        return b""
    try:
        return base64.b64decode(raw)
    except Exception:
        return b""


def _device_synthesize_qwen_tts_wav(text: str, api_key: str) -> Optional[bytes]:
    try:
        import dashscope
        import requests as _req

        model = os.getenv("DEVICE_TTS_MODEL", "qwen3-tts-instruct-flash")
        voice = os.getenv("DEVICE_TTS_VOICE", "Cherry")
        instructions = _safe_textv(
            os.getenv("DEVICE_TTS_INSTRUCTIONS"),
            "用清晰、沉稳、简短的中文交易助理语气播报。",
        )
        response = dashscope.MultiModalConversation.call(
            model=model,
            api_key=api_key,
            text=text[:180],
            voice=voice,
            language_type=os.getenv("DEVICE_TTS_LANGUAGE_TYPE", "Chinese"),
            instructions=instructions,
            optimize_instructions=True,
            stream=False,
        )
        audio_bytes = _extract_device_qwen_tts_audio_data(response)
        if audio_bytes:
            return audio_bytes if audio_bytes.startswith(b"RIFF") else None
        audio_url = _extract_device_qwen_tts_audio_url(response)
        if audio_url:
            resp = _req.get(audio_url, timeout=10)
            resp.raise_for_status()
            downloaded = bytes(resp.content or b"")
            return downloaded if downloaded.startswith(b"RIFF") else None
        status_code = getattr(response, "status_code", "")
        code = getattr(response, "code", "")
        message = getattr(response, "message", "")
        raise RuntimeError(f"empty qwen tts audio status={status_code} code={code} message={message}")
    except Exception as exc:
        print(f"[device_api] voice_qwen_tts_failed err={exc}", flush=True)
        return None


def _device_synthesize_cosyvoice_wav(text: str, api_key: str) -> Optional[bytes]:
    try:
        import dashscope
        from dashscope.audio.tts_v2 import AudioFormat, SpeechSynthesizer

        dashscope.api_key = api_key
        synthesizer = SpeechSynthesizer(
            model=os.getenv("DEVICE_TTS_MODEL", "cosyvoice-v2"),
            voice=os.getenv("DEVICE_TTS_VOICE", "longxiaochun"),
            format=AudioFormat.PCM_16000HZ_MONO_16BIT,
        )
        pcm = synthesizer.call(text[:180])
        if not pcm:
            return None
        return _build_pcm_wav(bytes(pcm))
    except Exception as exc:
        print(f"[device_api] voice_cosyvoice_tts_failed err={exc}", flush=True)
        return None


def _device_synthesize_speech_wav(text: str) -> Optional[bytes]:
    if not _safe_textv(text):
        return None
    if str(os.getenv("DEVICE_TTS_DISABLED", "0")).strip().lower() in {"1", "true", "yes", "on"}:
        return None
    api_key = _safe_textv(os.getenv("DASHSCOPE_API_KEY"))
    if not api_key:
        return _normalize_device_tts_wav(_device_synthesize_macos_say_wav(text))
    model = os.getenv("DEVICE_TTS_MODEL", "qwen3-tts-instruct-flash")
    if model.startswith("qwen"):
        audio = _device_synthesize_qwen_tts_wav(text, api_key)
    else:
        audio = _device_synthesize_cosyvoice_wav(text, api_key)
    return _normalize_device_tts_wav(audio or _device_synthesize_macos_say_wav(text))


def _device_voice_audio_playable(audio_bytes: Optional[bytes], *, min_duration_ms: int = 350) -> bool:
    if not audio_bytes or not bytes(audio_bytes).startswith(b"RIFF"):
        return False
    try:
        wav_info = _read_device_wav_info(bytes(audio_bytes), enforce_device_recording_limits=False)
        stats = _device_wav_signal_stats(bytes(audio_bytes), wav_info)
        return int(stats.get("duration_ms") or 0) >= min_duration_ms and int(stats.get("samples") or 0) > 0
    except Exception:
        return False


def _cleanup_device_voice_audio_cache(now: Optional[float] = None) -> None:
    now_value = now or time.time()
    stale_ids = [
        voice_id
        for voice_id, item in _DEVICE_VOICE_AUDIO_CACHE.items()
        if now_value - float(item.get("created_at", 0)) > _DEVICE_VOICE_AUDIO_CACHE_TTL_SECONDS
    ]
    for voice_id in stale_ids:
        _DEVICE_VOICE_AUDIO_CACHE.pop(voice_id, None)


def _store_device_voice_audio(audio_bytes: bytes) -> str:
    voice_id = uuid.uuid4().hex
    with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
        _cleanup_device_voice_audio_cache()
        _DEVICE_VOICE_AUDIO_CACHE[voice_id] = {"created_at": time.time(), "audio": bytes(audio_bytes)}
    return voice_id


def _get_device_voice_audio(voice_id: str) -> Optional[bytes]:
    normalized = re.sub(r"[^a-fA-F0-9]", "", str(voice_id or ""))[:64]
    if not normalized:
        return None
    with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
        _cleanup_device_voice_audio_cache()
        item = _DEVICE_VOICE_AUDIO_CACHE.get(normalized)
        if not item:
            return None
        return bytes(item.get("audio") or b"")


def _device_voice_disk_cache_enabled() -> bool:
    return str(os.getenv("DEVICE_VOICE_AUDIO_DISK_CACHE_DISABLED", "0")).strip().lower() not in {"1", "true", "yes", "on"}


def _device_voice_disk_cache_path(cache_key: str) -> str:
    safe_key = re.sub(r"[^a-fA-F0-9]", "", cache_key)[:64]
    return os.path.join(_DEVICE_VOICE_AUDIO_DISK_CACHE_DIR, f"{safe_key}.wav")


def _read_device_voice_disk_audio(cache_key: str) -> Optional[bytes]:
    if not _device_voice_disk_cache_enabled():
        return None
    path = _device_voice_disk_cache_path(cache_key)
    try:
        stat = os.stat(path)
        if time.time() - float(stat.st_mtime) > _DEVICE_VOICE_AUDIO_DISK_CACHE_TTL_SECONDS:
            return None
        with open(path, "rb") as f:
            audio = f.read()
        return audio if audio.startswith(b"RIFF") else None
    except FileNotFoundError:
        return None
    except Exception as exc:
        print(f"[device_api] voice_disk_cache_read_failed err={exc}", flush=True)
        return None


def _write_device_voice_disk_audio(cache_key: str, audio_bytes: bytes) -> None:
    if not _device_voice_disk_cache_enabled() or not audio_bytes or not audio_bytes.startswith(b"RIFF"):
        return
    try:
        os.makedirs(_DEVICE_VOICE_AUDIO_DISK_CACHE_DIR, exist_ok=True)
        path = _device_voice_disk_cache_path(cache_key)
        tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        with open(tmp_path, "wb") as f:
            f.write(audio_bytes)
        os.replace(tmp_path, path)
    except Exception as exc:
        print(f"[device_api] voice_disk_cache_write_failed err={exc}", flush=True)


def _device_voice_last_task_key(username: str, device_id: str) -> str:
    user = re.sub(r"[^a-zA-Z0-9_.@-]", "_", _safe_textv(username) or "anonymous")[:80]
    device = re.sub(r"[^a-zA-Z0-9_.@-]", "_", _safe_textv(device_id) or "default")[:80]
    return f"{_DEVICE_VOICE_LAST_TASK_PREFIX}{user}:{device}"


def _read_device_voice_last_task(username: str, device_id: str) -> Dict[str, Any]:
    try:
        raw = _redis.get(_device_voice_last_task_key(username, device_id))
        data = _safe_json_loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        print(f"[device_api] voice_last_task_read_failed user={username} err={exc}", flush=True)
        return {}


def _write_device_voice_last_task(username: str, device_id: str, payload: Dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        return
    try:
        stored = dict(payload)
        stored["updated_at"] = datetime.now().isoformat()
        _redis.setex(
            _device_voice_last_task_key(username, device_id),
            _DEVICE_VOICE_LAST_TASK_TTL_SECONDS,
            json.dumps(stored, ensure_ascii=False),
        )
    except Exception as exc:
        print(f"[device_api] voice_last_task_write_failed user={username} err={exc}", flush=True)


def _device_voice_task_elapsed_seconds(task_id: str) -> float:
    state = _read_mobile_chat_state(task_id)
    created_ts = _parse_iso_ts(state.get("created_at")) if state else 0.0
    if created_ts <= 0:
        meta = TaskManager.get_task_meta(task_id) or {}
        start_ts = float(meta.get("start_time") or 0.0)
        if start_ts > 0:
            created_ts = start_ts
        else:
            created_ts = _parse_iso_ts(meta.get("created_at")) if meta else 0.0
    if created_ts <= 0:
        return 0.0
    return max(0.0, time.time() - created_ts)


def _device_voice_next_poll_seconds(elapsed_seconds: float) -> int:
    if elapsed_seconds < 20:
        return 2
    if elapsed_seconds < 120:
        return 5
    return 10


def _device_voice_active_task_id(username: str, device_id: str) -> str:
    last_task = _read_device_voice_last_task(username, device_id)
    task_id = _safe_textv(last_task.get("task_id"))
    if not task_id:
        return ""
    status = TaskManager.get_task_status(task_id) or {}
    status_name = _safe_textv(status.get("status") or status.get("state")).lower()
    if status_name in {"queued", "pending", "processing"} and _device_voice_task_elapsed_seconds(task_id) < _DEVICE_VOICE_TASK_MAX_WAIT_SECONDS:
        return task_id
    return ""


def _device_voice_last_task_answer(username: str, device_id: str) -> str:
    last_task = _read_device_voice_last_task(username, device_id)
    task_id = _safe_textv(last_task.get("task_id"))
    if not task_id:
        return "我这边没有正在分析的深度任务。你可以直接问我行情、IV，或者让我做一次深度分析。"
    try:
        status_payload = chat_status(task_id=task_id, username=username)
    except Exception as exc:
        print(f"[device_api] voice_last_task_status_failed user={username} task_id={task_id} err={exc}", flush=True)
        return "我刚才的分析状态暂时读取失败，你可以稍后再问我一次结果。"

    status_name = _safe_textv(status_payload.get("status") or status_payload.get("state")).lower()
    if status_name in {"queued", "pending", "processing"}:
        return "还在分析，我会继续盯着。你现在也可以先问我价格、IV 或者切换合约。"
    if status_name == "success":
        answer = _extract_mobile_chat_response_text(status_payload)
        return _summarize_device_voice_text(answer, max_chars=150) or "刚才的分析已经完成，但我没有拿到可播报的摘要。"
    err_text = _safe_textv(status_payload.get("error") or status_payload.get("detail"))
    return err_text or "刚才的分析没有成功完成，我没有拿到可靠结论。"


def _device_voice_instant_answer(transcript: str, *, username: str, device_id: str) -> str:
    text = _safe_textv(transcript).lower()
    if any(marker in text for marker in ("还在分析", "分析好了", "结果好了", "刚才结果", "上一个结果")):
        return _device_voice_last_task_answer(username, device_id)
    if _device_voice_asks_time(text):
        return f"现在是北京时间 {datetime.now().strftime('%H点%M分')}。"
    if "怎么用" in text or "可以做什么" in text:
        return _DEVICE_VOICE_PROMPT_TEXTS.get("voice_help", "你可以问价格、涨跌、IV，或让我做深度分析。")
    if "你是谁" in text or "你叫什么" in text:
        return "我是 TradingArt 助手。"
    return _DEVICE_VOICE_PROMPT_TEXTS.get("voice_hello", "你好，我在。")


def _summarize_device_voice_text(text: str, *, max_chars: int = 120) -> str:
    value = re.sub(r"\s+", " ", _safe_textv(text)).strip()
    if not value:
        return ""
    sentence_parts = [part.strip() for part in re.split(r"[。！？!?]\s*", value) if part.strip()]
    if sentence_parts:
        value = "。".join(sentence_parts[:3])
        if value and not value.endswith("。"):
            value += "。"
    if len(value) <= max_chars:
        return value
    return value[: max_chars - 1].rstrip("，。；;、 ") + "。"


def _device_voice_audio_url_for_text(text: str) -> str:
    normalized_text = re.sub(r"\s+", " ", _safe_textv(text)).strip()
    if not normalized_text:
        return ""
    cache_basis = "|".join(
        [
            normalized_text,
            os.getenv("DEVICE_TTS_MODEL", "qwen3-tts-instruct-flash"),
            os.getenv("DEVICE_TTS_VOICE", "Cherry"),
            os.getenv("DEVICE_TTS_LANGUAGE_TYPE", "Chinese"),
            str(_device_audio_gain()),
        ]
    )
    cache_key = hashlib.sha256(cache_basis.encode("utf-8")).hexdigest()
    now_value = time.time()
    with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
        item = _DEVICE_VOICE_TEXT_AUDIO_CACHE.get(cache_key)
        if item and now_value - float(item.get("created_at", 0)) <= _DEVICE_VOICE_AUDIO_CACHE_TTL_SECONDS:
            cached_voice_id = _safe_textv(item.get("voice_id"))
            if cached_voice_id and cached_voice_id in _DEVICE_VOICE_AUDIO_CACHE:
                return f"/api/device/voice/audio/{cached_voice_id}"

    disk_audio = _read_device_voice_disk_audio(cache_key)
    if _device_voice_audio_playable(disk_audio):
        voice_id = _store_device_voice_audio(disk_audio)
        with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
            _DEVICE_VOICE_TEXT_AUDIO_CACHE[cache_key] = {"created_at": now_value, "voice_id": voice_id}
        return f"/api/device/voice/audio/{voice_id}"

    tts_bytes = _device_synthesize_speech_wav(normalized_text)
    if not _device_voice_audio_playable(tts_bytes):
        macos_fallback = _normalize_device_tts_wav(_device_synthesize_macos_say_wav(normalized_text))
        if _device_voice_audio_playable(macos_fallback):
            tts_bytes = macos_fallback
        else:
            print(
                f"[device_api] voice_tts_audio_too_short text={normalized_text[:30]} "
                f"bytes={len(tts_bytes or b'')}",
                flush=True,
            )
            return ""
    if not tts_bytes:
        return ""
    _write_device_voice_disk_audio(cache_key, tts_bytes)
    voice_id = _store_device_voice_audio(tts_bytes)
    with _DEVICE_VOICE_AUDIO_CACHE_LOCK:
        _DEVICE_VOICE_TEXT_AUDIO_CACHE[cache_key] = {"created_at": now_value, "voice_id": voice_id}
    return f"/api/device/voice/audio/{voice_id}"


def _device_voice_audio_id_from_url(audio_url: str) -> str:
    text = _safe_textv(audio_url)
    match = re.search(r"/api/device/voice/audio/([a-fA-F0-9_-]+)", text)
    return match.group(1) if match else ""


def _device_voice_wav_pcm_payload(audio_bytes: bytes) -> Tuple[bytes, Dict[str, Any]]:
    if not audio_bytes or not audio_bytes.startswith(b"RIFF"):
        return b"", {"encoding": "unknown"}
    try:
        with wave.open(io.BytesIO(audio_bytes), "rb") as wf:
            sample_rate = wf.getframerate()
            channels = wf.getnchannels()
            sample_width = wf.getsampwidth()
            frames = wf.getnframes()
            pcm = wf.readframes(frames)
        if channels == 1 and sample_width == 2 and sample_rate == _DEVICE_VOICE_SAMPLE_RATE:
            return pcm, {
                "encoding": "pcm16",
                "sample_rate": sample_rate,
                "channels": channels,
                "sample_width": sample_width,
                "samples": frames,
            }
    except Exception:
        pass
    return audio_bytes, {"encoding": "wav"}


def _build_device_voice_deep_prompt(
    *,
    transcript: str,
    contract_context: Dict[str, Any],
    screen_context: str,
) -> str:
    context_lines = []
    if contract_context:
        context_lines.append(
            "设备当前看板："
            f"标的={contract_context.get('product_name') or contract_context.get('contract') or contract_context.get('product_code') or ''}；"
            f"代码={contract_context.get('contract') or contract_context.get('product_code') or ''}；"
            f"最新价={_format_device_price(contract_context.get('latest_price'))}；"
            f"涨跌={_format_device_metric(contract_context.get('price_pct'), '%')}；"
            f"IV={_format_device_metric(contract_context.get('iv'))}；"
            f"IV Rank={_format_device_metric(contract_context.get('iv_rank'))}；"
            f"技术面={_safe_textv(contract_context.get('technical_label')) or '待生成'}。"
        )
    if screen_context:
        context_lines.append(f"设备屏幕上下文：{_safe_textv(screen_context)[:300]}")
    context_text = "\n".join(context_lines) if context_lines else "设备没有明确标的上下文。"
    return (
        "这是来自 StackChan 桌面机器人的语音提问。请复用 TradingArt 的分析框架，"
        "结合确定性行情、波动率、技术面、风险和仓位做交易者可理解的分析。"
        "不要编造缺失行情数字；若数据不足，请明确说明。\n"
        f"{context_text}\n"
        f"用户问题：{_safe_textv(transcript)}"
    )


def _submit_device_voice_deep_task(
    *,
    username: str,
    transcript: str,
    contract_context: Dict[str, Any],
    screen_context: str,
    conversation_id: str,
) -> str:
    prompt = _build_device_voice_deep_prompt(
        transcript=transcript,
        contract_context=contract_context,
        screen_context=screen_context,
    )
    try:
        profile = de.get_user_profile(username) or {}
    except Exception:
        profile = {}
    risk = str(profile.get("risk_preference") or "稳健型")
    context_payload = _build_mobile_context_payload(
        prompt_text=prompt,
        current_user=username,
        history=[],
        profile=profile,
    )
    context_payload.update(
        {
            "chat_mode": CHAT_MODE_ANALYSIS,
            "delivery_mode": "device_voice_task",
            "device_voice": True,
            "screen_context": _safe_textv(screen_context)[:500],
            "contract_context": contract_context or {},
            "conversation_id": _safe_textv(conversation_id) or f"stackchan-{username}-{uuid.uuid4()}",
        }
    )
    task_id = TaskManager.create_task(
        user_id=username,
        prompt=prompt,
        risk_preference=risk,
        history_messages=[],
        context_payload=context_payload,
        has_portfolio=_detect_mobile_has_portfolio(username),
    )
    trace_id = _generate_chat_trace_id()
    answer_id = _generate_chat_answer_id()
    progress_text = "分析团队还在看技术面和波动率"
    existing_state = _read_mobile_chat_state(task_id)
    existing_status = _safe_textv(existing_state.get("status")).lower()
    if existing_status not in {"success", "error", "timeout", "canceled"}:
        _write_mobile_chat_state(
            task_id=task_id,
            user_id=username,
            status=existing_status if existing_status in {"processing"} else "pending",
            error="",
            finished=False,
            extra_fields={
                "trace_id": trace_id,
                "answer_id": answer_id,
                "prompt_text": prompt,
                "intent_domain": str(context_payload.get("intent_domain") or "option"),
                "feedback_allowed": False,
                "chat_mode": CHAT_MODE_ANALYSIS,
                "progress": _safe_textv(existing_state.get("progress")) or progress_text,
                "delivery_mode": "device_voice_task",
            },
        )
    _set_mobile_chat_last_task(username, task_id)
    try:
        _redis.setex(_mobile_chat_prompt_key(task_id), _MOBILE_CHAT_PROMPT_TTL, prompt)
    except Exception as exc:
        print(f"[device_api] voice_prompt_cache_failed task_id={task_id} err={exc}", flush=True)
    return task_id


def _build_device_voice_query_payload(
    *,
    username: str,
    request: Request,
    audio_bytes: bytes,
    contract: str = "",
    category: str = "futures",
    screen_context: str = "",
    conversation_id: str = "",
    client_audio_peak: Any = None,
    client_audio_rms: Any = None,
    initial_timings_ms: Optional[Dict[str, Any]] = None,
    transcript_override: str = "",
) -> Dict[str, Any]:
    total_started_at = time.perf_counter()
    timings_ms: Dict[str, Any] = dict(initial_timings_ms or {})
    stage_started_at = time.perf_counter()
    wav_info = _read_device_wav_info(audio_bytes)
    audio_stats = _device_wav_signal_stats(audio_bytes, wav_info)
    _record_timing(timings_ms, "audio_parse_ms", stage_started_at)
    stage_started_at = time.perf_counter()
    category_value = _normalize_device_menu_category(category)
    device_ctx = _extract_device_context(request)
    device_id = _safe_textv(device_ctx.get("device_id"))
    _record_timing(timings_ms, "context_parse_ms", stage_started_at)

    freshness = "fresh"
    emotion = "thinking"
    action = "display"
    route_type = "error"
    task_id = ""
    poll_after_seconds: Optional[int] = None
    stt_status = "ok"
    stt_error = ""
    stt_failure_reason = ""
    contract_context: Dict[str, Any] = {}
    transcript_override = _safe_textv(transcript_override)
    if transcript_override:
        transcript = transcript_override
        timings_ms.setdefault("stt_ms", 0)
    else:
        try:
            stage_started_at = time.perf_counter()
            transcript = _safe_textv(_device_transcribe_wav(audio_bytes))
            _record_timing(timings_ms, "stt_ms", stage_started_at)
        except Exception as exc:
            _record_timing(timings_ms, "stt_ms", stage_started_at)
            print(
                "[device_api] voice_stt_failed "
                f"user={username} duration_ms={audio_stats.get('duration_ms', 0)} "
                f"peak={audio_stats.get('peak', 0)} rms={audio_stats.get('rms', 0.0)} "
                f"err={exc}",
                flush=True,
            )
            transcript = "语音识别失败"
            stt_status = "failed"
            stt_error = _safe_textv(f"{type(exc).__name__}: {exc}")[:180]
            stt_failure_reason = _classify_device_voice_stt_failure(
                stt_error,
                audio_stats,
                client_audio_peak=client_audio_peak,
                client_audio_rms=client_audio_rms,
            )
            freshness = "degraded"
            emotion = "error"

    if stt_status == "failed":
        route_type = "error"
        stage_started_at = time.perf_counter()
        answer_text = _device_voice_stt_error_answer(stt_error, stt_failure_reason)
        _record_timing(timings_ms, "answer_ms", stage_started_at)
    else:
        stage_started_at = time.perf_counter()
        route_type = _classify_device_voice_route(transcript)
        _record_timing(timings_ms, "route_ms", stage_started_at)
        active_task_id = ""
        if route_type == "stop_listening":
            stage_started_at = time.perf_counter()
            answer_text = "好的，我先不听了。需要时再点我。"
            action = "stop_listening"
            emotion = "happy"
            _record_timing(timings_ms, "answer_ms", stage_started_at)
        elif route_type == "instant_reply":
            stage_started_at = time.perf_counter()
            answer_text = _device_voice_instant_answer(transcript, username=username, device_id=device_id)
            _record_timing(timings_ms, "answer_ms", stage_started_at)
            emotion = "happy"
        elif route_type == "deep_analysis":
            active_task_id = _device_voice_active_task_id(username, device_id)
            if active_task_id:
                stage_started_at = time.perf_counter()
                answer_text = "我还在分析上一个复杂问题，先问我价格、IV 或者等我播报结果。"
                route_type = "deep_analysis_busy"
                freshness = "degraded"
                emotion = "thinking"
                _record_timing(timings_ms, "answer_ms", stage_started_at)
            else:
                stage_started_at = time.perf_counter()
                contract_context = _load_device_voice_market_context(
                    username=username,
                    request=request,
                    transcript=transcript,
                    contract=contract,
                    category=category_value,
                )
                _record_timing(timings_ms, "market_context_ms", stage_started_at)
        elif route_type == "market_fact":
            stage_started_at = time.perf_counter()
            contract_context = _load_device_voice_market_context(
                username=username,
                request=request,
                transcript=transcript,
                contract=contract,
                category=category_value,
            )
            _record_timing(timings_ms, "market_context_ms", stage_started_at)

        if route_type == "market_fact":
            stage_started_at = time.perf_counter()
            answer_text = _device_voice_fact_answer(contract_context)
            if not answer_text:
                answer_text = "我还没有取到这个标的的实时看板数据，不能直接报行情数字。"
                freshness = "degraded"
            if _device_voice_asks_time(transcript) and "北京时间" not in answer_text:
                answer_text = f"现在是北京时间 {datetime.now().strftime('%H点%M分')}。{answer_text}"
            _record_timing(timings_ms, "answer_ms", stage_started_at)
        elif route_type == "deep_analysis":
            answer_text = _DEVICE_VOICE_PROMPT_TEXTS.get("voice_deep_confirm", "这个问题需要深度分析，我先帮你看。")
            try:
                stage_started_at = time.perf_counter()
                task_id = _submit_device_voice_deep_task(
                    username=username,
                    transcript=transcript,
                    contract_context=contract_context,
                    screen_context=screen_context,
                    conversation_id=conversation_id,
                )
                _write_device_voice_last_task(
                    username,
                    device_id,
                    {
                        "task_id": task_id,
                        "transcript": transcript,
                        "status": "processing",
                        "contract": contract_context.get("contract") or contract_context.get("product_code") or contract,
                        "category": category_value,
                        "created_at": datetime.now().isoformat(),
                    },
                )
                _record_timing(timings_ms, "deep_submit_ms", stage_started_at)
                action = "thinking"
                poll_after_seconds = _DEVICE_VOICE_TASK_POLL_SECONDS
                emotion = "thinking"
            except UserTaskQueueFullError as exc:
                _record_timing(timings_ms, "deep_submit_ms", stage_started_at)
                answer_text = (
                    f"你前面已有 {exc.active_count} 个问题处理中、{exc.queued_count} 个排队，"
                    "我先不继续塞任务，稍后再帮你分析。"
                )
                freshness = "degraded"
                emotion = "error"
            except Exception as exc:
                _record_timing(timings_ms, "deep_submit_ms", stage_started_at)
                print(f"[device_api] voice_deep_task_failed user={username} err={exc}", flush=True)
                answer_text = "深度分析任务提交失败了，我先不乱给交易结论。请稍后再试。"
                freshness = "degraded"
                emotion = "error"
        elif route_type not in {"instant_reply", "deep_analysis_busy", "stop_listening"}:
            stage_started_at = time.perf_counter()
            answer_text = _device_generate_voice_answer(
                username=username,
                transcript=transcript,
                contract_context=contract_context,
                screen_context=screen_context,
            )
            _record_timing(timings_ms, "llm_ms", stage_started_at)

    audio_url = ""
    stage_started_at = time.perf_counter()
    if route_type in {"instant_reply", "market_fact", "deep_analysis", "deep_analysis_busy", "stop_listening", "error"}:
        audio_url = _device_voice_audio_url_for_text(answer_text)
    else:
        tts_bytes = _device_synthesize_speech_wav(answer_text)
        if tts_bytes:
            voice_id = _store_device_voice_audio(tts_bytes)
            audio_url = f"/api/device/voice/audio/{voice_id}"
        else:
            freshness = "degraded"
    _record_timing(timings_ms, "tts_ms", stage_started_at)
    if not audio_url and action != "thinking":
        freshness = "degraded"
    if audio_url:
        if action != "thinking":
            emotion = "speaking" if emotion != "error" else emotion
    if action not in {"thinking", "stop_listening"}:
        action = "speak" if audio_url else "display"
    _record_timing(timings_ms, "server_total_ms", total_started_at)
    if _DEVICE_VOICE_LATENCY_OBSERVATION_ENABLED:
        print(
            "[device_api] voice_timings "
            f"user={username} device_id={device_id} route={route_type} "
            f"total_ms={timings_ms.get('server_total_ms')} timings={timings_ms}",
            flush=True,
        )

    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "conversation_id": _safe_textv(conversation_id) or uuid.uuid4().hex,
        "route_type": route_type,
        "task_id": task_id,
        "poll_after_seconds": poll_after_seconds,
        "task_max_wait_seconds": _DEVICE_VOICE_TASK_MAX_WAIT_SECONDS,
        "transcript": transcript,
        "answer_text": answer_text,
        "speak_text": answer_text,
        "emotion": emotion,
        "action": action,
        "audio_url": audio_url,
        "audio_duration_ms": audio_stats.get("duration_ms", 0),
        "audio_peak": audio_stats.get("peak", 0),
        "audio_rms": audio_stats.get("rms", 0.0),
        "client_audio_peak": _safe_floatv(client_audio_peak, default=None),
        "client_audio_rms": _safe_floatv(client_audio_rms, default=None),
        "stt_status": stt_status,
        "stt_error": stt_error,
        "stt_failure_reason": stt_failure_reason,
        "stt_user_message": answer_text if stt_status == "failed" else "",
        "timings_ms": timings_ms,
        "updated_at": _device_now_text(),
        "data_freshness": freshness,
    }


def _extract_mobile_chat_response_text(status_payload: Dict[str, Any]) -> str:
    result = status_payload.get("result")
    if isinstance(result, dict):
        for key in ("response", "answer", "answer_text", "message"):
            value = _safe_textv(result.get(key))
            if value:
                return value
    for key in ("response", "answer", "answer_text", "message"):
        value = _safe_textv(status_payload.get(key))
        if value:
            return value
    return ""


def _read_device_voice_chat_status(task_id: str, username: str) -> Dict[str, Any]:
    """Read chat task status for device voice without mobile pending timeout side effects."""
    state = _read_mobile_chat_state(task_id)
    if state and state.get("user_id") and str(state.get("user_id")) != str(username):
        raise HTTPException(status_code=403, detail="无权限访问该任务")

    status_name = _safe_textv(state.get("status") or state.get("state")).lower() if state else ""
    result_wrapper = _read_mobile_chat_result(task_id)
    result_payload = result_wrapper.get("result") if isinstance(result_wrapper, dict) else None
    if status_name == "success" and isinstance(result_payload, dict):
        return {"status": "success", "result": result_payload}
    if status_name in {"error", "canceled", "timeout"}:
        return {
            "status": status_name,
            "error": _safe_textv(state.get("error")) or ("AI思考太久，请稍后问我刚才结果。" if status_name == "timeout" else "分析失败，请稍后重试。"),
        }
    if status_name in {"pending", "queued", "processing"}:
        chat_mode = _safe_textv(state.get("chat_mode")) or CHAT_MODE_ANALYSIS
        return {
            "status": status_name,
            "progress": _safe_textv(state.get("progress")) or default_progress_for_chat_mode(chat_mode, status=status_name),
            "result": None,
            "error": None,
            "chat_mode": chat_mode,
        }

    status = TaskManager.get_task_status(task_id) or {}
    status_name = _safe_textv(status.get("status") or status.get("state")).lower()
    if status_name == "success":
        result = status.get("result")
        if isinstance(result, dict):
            _write_mobile_chat_result(task_id=task_id, user_id=username, result_payload=result)
            existing_state = _read_mobile_chat_state(task_id)
            _write_mobile_chat_state(
                task_id=task_id,
                user_id=username,
                status="success",
                error="",
                finished=True,
                extra_fields={
                    "feedback_allowed": True,
                    "trace_id": _safe_textv(existing_state.get("trace_id")),
                    "answer_id": _safe_textv(existing_state.get("answer_id")),
                    "prompt_text": _safe_textv(existing_state.get("prompt_text")),
                    "intent_domain": _safe_textv(existing_state.get("intent_domain")) or "general",
                    "chat_mode": _safe_textv(existing_state.get("chat_mode") or status.get("chat_mode")) or CHAT_MODE_ANALYSIS,
                },
            )
            try:
                TaskManager.complete_user_task(username, task_id)
            except Exception:
                pass
        return status
    if status_name == "error":
        err_msg = _safe_textv(status.get("error")) or "分析失败，请稍后重试。"
        _write_mobile_chat_state(task_id=task_id, user_id=username, status="error", error=err_msg, finished=True)
        try:
            TaskManager.complete_user_task(username, task_id)
        except Exception:
            pass
        return status
    if status_name in {"pending", "queued", "processing"}:
        return status
    return status if isinstance(status, dict) else {}


def _build_device_voice_task_payload(
    *,
    username: str,
    request: Request,
    task_id: str,
) -> Dict[str, Any]:
    total_started_at = time.perf_counter()
    timings_ms: Dict[str, Any] = {}
    normalized_task_id = re.sub(r"[^a-zA-Z0-9_-]", "", _safe_textv(task_id))[:80]
    if not normalized_task_id:
        raise HTTPException(status_code=400, detail="无效任务 ID")

    freshness = "fresh"
    elapsed_seconds = _device_voice_task_elapsed_seconds(normalized_task_id)
    status_payload: Dict[str, Any]
    try:
        stage_started_at = time.perf_counter()
        raw_status = _read_device_voice_chat_status(task_id=normalized_task_id, username=username)
        status_payload = raw_status if isinstance(raw_status, dict) else {}
        _record_timing(timings_ms, "status_read_ms", stage_started_at)
    except HTTPException:
        raise
    except Exception as exc:
        _record_timing(timings_ms, "status_read_ms", stage_started_at)
        print(f"[device_api] voice_task_status_failed user={username} task_id={normalized_task_id} err={exc}", flush=True)
        status_payload = {"status": "error", "error": "分析任务状态读取失败。"}
        freshness = "degraded"

    status_name = _safe_textv(status_payload.get("status") or status_payload.get("state")).lower()
    task_status_source = "runtime"
    state_status = ""
    worker_status_name = ""
    if status_name in {"pending", "queued", "processing"} and elapsed_seconds >= _DEVICE_VOICE_TASK_MAX_WAIT_SECONDS:
        task_status_source = "timeout"
        status_name = "timeout"
        answer_text = "分析还没完成，我先不占着你，你可以继续问行情，稍后问我刚才结果。"
        speak_text = answer_text
        stage_started_at = time.perf_counter()
        audio_url = _device_voice_audio_url_for_text(speak_text)
        _record_timing(timings_ms, "tts_ms", stage_started_at)
        action = "speak" if audio_url else "display"
        emotion = "error"
        freshness = "degraded"
    elif status_name in {"pending", "processing"} and elapsed_seconds >= _DEVICE_VOICE_TASK_WORKER_GRACE_SECONDS:
        state_snapshot = _read_mobile_chat_state(normalized_task_id)
        state_status = _safe_textv(state_snapshot.get("status") or state_snapshot.get("state")).lower()
        try:
            worker_status = TaskManager.get_task_status(normalized_task_id) or {}
        except Exception as exc:
            print(
                f"[device_api] voice_task_worker_status_failed user={username} task_id={normalized_task_id} err={exc}",
                flush=True,
            )
            worker_status = {}
        worker_status_name = _safe_textv(worker_status.get("status") or worker_status.get("state")).lower()
        print(
            "[device_api] voice_task_status_source "
            f"user={username} task_id={normalized_task_id} status={status_name} "
            f"elapsed={int(elapsed_seconds)} state={state_status or '-'} "
            f"worker={worker_status_name or '-'}",
            flush=True,
        )
        if not state_status and worker_status_name == "pending" and elapsed_seconds >= _DEVICE_VOICE_TASK_LOST_GRACE_SECONDS:
            task_status_source = "lost"
            status_name = "error"
            status_payload = {
                "status": "error",
                "error": "后台分析任务状态丢失了。我先不占着你，请稍后重新让我分析。",
            }
            freshness = "degraded"

    if status_name in {"pending", "queued"}:
        progress_text = _safe_textv(status_payload.get("progress")) or "分析团队还在排队看这个问题"
        answer_text = progress_text
        speak_text = ""
        action = "thinking"
        emotion = "thinking"
        audio_url = ""
    elif status_name == "processing":
        progress_text = _safe_textv(status_payload.get("progress")) or "分析团队还在看技术面和波动率"
        answer_text = progress_text
        speak_text = ""
        action = "thinking"
        emotion = "thinking"
        audio_url = ""
    elif status_name == "success":
        answer_text = _extract_mobile_chat_response_text(status_payload)
        if not answer_text:
            answer_text = "分析已经完成，但我没有拿到可播报的摘要。"
            freshness = "degraded"
        speak_text = _summarize_device_voice_text(answer_text, max_chars=90)
        stage_started_at = time.perf_counter()
        audio_url = _device_voice_audio_url_for_text(speak_text)
        _record_timing(timings_ms, "tts_ms", stage_started_at)
        action = "speak" if audio_url else "display"
        emotion = "speaking" if audio_url else "happy"
    elif status_name == "timeout":
        answer_text = "分析还没完成，我先不占着你，你可以继续问行情，稍后问我刚才结果。"
        speak_text = answer_text
        stage_started_at = time.perf_counter()
        audio_url = _device_voice_audio_url_for_text(speak_text)
        _record_timing(timings_ms, "tts_ms", stage_started_at)
        action = "speak" if audio_url else "display"
        emotion = "error"
        freshness = "degraded"
    else:
        status_name = status_name or "error"
        err_text = _safe_textv(status_payload.get("error") or status_payload.get("detail"))
        answer_text = err_text or "分析服务刚刚出了点问题，我没有拿到可靠结论。"
        speak_text = _summarize_device_voice_text(answer_text, max_chars=120)
        stage_started_at = time.perf_counter()
        audio_url = _device_voice_audio_url_for_text(speak_text)
        _record_timing(timings_ms, "tts_ms", stage_started_at)
        action = "speak" if audio_url else "display"
        emotion = "error"
        freshness = "degraded"

    device_ctx = _extract_device_context(request)
    if status_name in {"success", "error", "timeout"}:
        _write_device_voice_last_task(
            username,
            _safe_textv(device_ctx.get("device_id")),
            {
                "task_id": normalized_task_id,
                "status": status_name,
                "answer_text": answer_text,
                "speak_text": speak_text,
            },
        )
    _record_timing(timings_ms, "server_total_ms", total_started_at)
    return {
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "task_id": normalized_task_id,
        "route_type": "deep_analysis",
        "status": status_name,
        "action": action,
        "emotion": emotion,
        "answer_text": answer_text,
        "speak_text": speak_text,
        "audio_url": audio_url,
        "poll_after_seconds": _device_voice_next_poll_seconds(elapsed_seconds) if action == "thinking" else None,
        "task_max_wait_seconds": _DEVICE_VOICE_TASK_MAX_WAIT_SECONDS,
        "elapsed_seconds": int(elapsed_seconds),
        "task_status_source": task_status_source,
        "state_status": state_status,
        "worker_status": worker_status_name,
        "timings_ms": timings_ms,
        "updated_at": _device_now_text(),
        "data_freshness": freshness,
    }


# ════════════════════════════════════════════════════════════
#  Request Schemas
# ════════════════════════════════════════════════════════════

class LoginRequest(BaseModel):
    account: str        # 用户名 或 邮箱
    password: str


class EmailLoginRequest(BaseModel):
    email: str
    code: str


class SendCodeRequest(BaseModel):
    email: str


class RegisterSendPhoneCodeRequest(BaseModel):
    phone: str


class RegisterVerifyPhoneCodeRequest(BaseModel):
    phone: str
    code: str


class RegisterRequest(BaseModel):
    username: str
    password: str
    password_confirm: str
    phone: str
    sms_code: str


class PasswordResetSendPhoneCodeRequest(BaseModel):
    phone: str


class PasswordResetRequest(BaseModel):
    phone: str
    sms_code: str
    new_password: str
    new_password_confirm: str


class ChatSubmitRequest(BaseModel):
    prompt: str
    history: List[dict] = []    # [{role: "user"/"assistant", content: "..."}]


class ChatCancelRequest(BaseModel):
    task_id: Optional[str] = None
    reason: Optional[str] = "manual"  # clear | timeout | manual


class ChatFeedbackRequest(BaseModel):
    trace_id: str
    answer_id: str
    feedback_type: str
    reason_code: Optional[str] = None
    feedback_text: Optional[str] = ""


class ChatFeedbackSampleCreateRequest(BaseModel):
    prompt_text: str
    reason_code: str
    intent_domain: Optional[str] = "general"
    occurrence_count: Optional[int] = 1
    latest_feedback_at: Optional[str] = ""
    latest_feedback_text: Optional[str] = ""
    sample_answer_id: Optional[str] = ""
    sample_trace_id: Optional[str] = ""
    sample_response_text: Optional[str] = ""
    sample_status: Optional[str] = "new"
    optimization_type: Optional[str] = ""
    review_notes: Optional[str] = ""


class ChatFeedbackSampleUpdateRequest(BaseModel):
    sample_key: str
    sample_status: Optional[str] = None
    optimization_type: Optional[str] = None
    review_notes: Optional[str] = None


class SubscribeRequest(BaseModel):
    channel_code: str


class PayPurchaseRequest(BaseModel):
    product_type: str   # channel | package
    code: str
    months: int = 1


_MOBILE_FOLLOWUP_KEYWORDS = (
    "刚刚", "刚才", "上一个", "上一条", "上次", "前面",
    "继续", "接着", "承接", "基于刚才", "刚聊到", "上一轮",
    "详细说明", "详细说", "展开说", "再展开", "再详细", "那为什么", "为什么呢", "补充一下",
)

_MOBILE_OPTION_KEYWORDS = (
    "期权", "认购", "认沽", "行权价", "牛市价差", "熊市价差", "跨式", "宽跨", "勒式",
    "call", "put", "delta", "gamma", "vega", "theta", "iv", "波动率", "权利金",
)

_MOBILE_STOCK_PORTFOLIO_KEYWORDS = (
    "持仓体检", "我的持仓", "我的股票", "股票持仓", "持仓分析", "仓位", "调仓", "加仓", "减仓",
    "股票组合", "股票账户", "前3大持仓", "行业分布",
)

_MOBILE_FOCUS_ENTITY_SUFFIXES = (
    "股份", "集团", "科技", "技术", "控股", "电子", "电气", "机械", "汽车", "能源",
    "药业", "银行", "证券", "制造", "动力", "材料", "智能", "软件", "通信", "航空",
    "医药", "生物", "实业", "新材",
)

_MOBILE_FOCUS_ENTITY_PATTERN = re.compile(
    r"[一-龥]{2,10}(?:%s)" % "|".join(_MOBILE_FOCUS_ENTITY_SUFFIXES)
)

_MOBILE_FOCUS_ASPECT_KEYWORDS = (
    "机器人业务", "汽车业务", "机器人", "汽车", "工业自动化", "工业软件",
    "协作机器人", "服务机器人", "工业机器人", "业务线", "这块业务", "这个业务",
)

_MOBILE_COMPANY_NEWS_TOPIC_KEYWORDS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近动态", "最近进展",
    "最近催化", "最近有没有催化", "最近消息", "最新消息", "近期动态", "近期进展",
    "近期催化", "最近怎么样", "业务最近怎么样", "业务最近如何",
)

_MOBILE_FOCUS_ENTITY_BAD_SUBSTRINGS = ("的", "业务", "或")
_MOBILE_FOCUS_PRONOUN_HINTS = ("他", "她", "它", "他的", "她的", "它的", "这家公司", "这个公司")


def _classify_mobile_intent_domain(text: str) -> str:
    text_norm = str(text or "").strip().lower()
    if not text_norm:
        return "general"
    if any(kw in text_norm for kw in _MOBILE_OPTION_KEYWORDS):
        return "option"
    if is_broker_signal_analysis_query(text_norm):
        return "general"
    if any(kw in text_norm for kw in _MOBILE_STOCK_PORTFOLIO_KEYWORDS):
        return "stock_portfolio"
    return "general"


def _extract_similarity_tokens(text: str) -> set[str]:
    if not text:
        return set()
    normalized = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", str(text).lower())
    tokens: set[str] = set()
    for word in normalized.split():
        if len(word) >= 2:
            tokens.add(word)
        if re.search(r"[\u4e00-\u9fff]", word) and len(word) >= 2:
            for i in range(len(word) - 1):
                tokens.add(word[i:i + 2])
    return tokens


def _is_semantically_related(prompt_text: str, recent_turns: List[dict], threshold: float = 0.18) -> bool:
    return _shared_is_semantically_related(prompt_text, recent_turns, threshold=threshold)


def _extract_mobile_focus_entity(text: str) -> str:
    return _shared_extract_focus_entity(text)


def _extract_mobile_focus_aspect(text: str) -> str:
    return _shared_extract_focus_aspect(text)


def _looks_like_mobile_company_news_topic(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return any(keyword in raw for keyword in _MOBILE_COMPANY_NEWS_TOPIC_KEYWORDS)


def _normalize_mobile_history(history: Optional[List[dict]], max_turns: int = 4) -> List[dict]:
    out: List[dict] = []
    for turn in history or []:
        if not isinstance(turn, dict):
            continue
        role = str(turn.get("role", "")).strip()
        content = str(turn.get("content", "")).strip()
        if role not in {"user", "assistant", "ai"} or not content:
            continue
        out.append({"role": role, "content": content})
    return out[-max_turns:]


def _build_recent_context_text(recent_turns: List[dict], max_chars: int = 1200) -> str:
    role_map = {"user": "用户", "assistant": "AI", "ai": "AI"}
    lines = []
    for turn in recent_turns:
        role = role_map.get(turn.get("role", ""), turn.get("role", ""))
        content = str(turn.get("content", "")).strip()
        if not content:
            continue
        lines.append(f"{role}: {content[:260]}")
    return "\n".join(lines)[:max_chars]


def _get_latest_mobile_user_turn_content(recent_turns: List[dict]) -> str:
    for turn in reversed(recent_turns):
        if str(turn.get("role", "")).strip() == "user":
            return str(turn.get("content", "")).strip()
    return ""


def _filter_mobile_memory_context_by_domain(memory_context: str, intent_domain: str, max_chars: int = 1500) -> str:
    if not memory_context:
        return ""
    if intent_domain != "option":
        return str(memory_context)[:max_chars]

    chunks: List[str] = []
    current: List[str] = []
    for line in str(memory_context).splitlines():
        if line.startswith("- "):
            if current:
                chunks.append("\n".join(current))
            current = [line]
        elif current:
            current.append(line)
    if current:
        chunks.append("\n".join(current))

    if not chunks:
        chunks = [str(memory_context)]

    option_chunks = [chunk for chunk in chunks if _classify_mobile_intent_domain(chunk) == "option"]
    return "\n".join(option_chunks)[:max_chars] if option_chunks else ""


def _build_mobile_context_payload(
    prompt_text: str,
    current_user: str,
    history: Optional[List[dict]],
    profile: Optional[dict] = None,
) -> dict:
    recent_turns = _normalize_mobile_history(history)
    intent_domain = _classify_mobile_intent_domain(prompt_text)
    latest_user_content = _get_latest_mobile_user_turn_content(recent_turns)
    recent_domain = _classify_mobile_intent_domain(latest_user_content)
    recent_context_full = _build_recent_context_text(recent_turns)
    recent_context = recent_context_full
    is_followup = _infer_followup_intent(prompt_text)
    lookup_followup = _infer_lookup_followup_intent(prompt_text)
    semantic_related = _is_semantically_related(prompt_text, recent_turns)
    is_same_domain = intent_domain == recent_domain
    initial_followup_goal = _infer_followup_goal(
        prompt_text,
        recent_context=recent_context_full,
    )
    topic_anchors = _build_topic_anchors(history or [], max_anchors=3)
    anchor_info = _select_target_anchor(
        prompt_text,
        topic_anchors,
        followup_goal=initial_followup_goal,
        is_followup=bool(is_followup),
    )
    target_anchor = anchor_info.get("target_anchor") or {}
    recent_topic_anchor = anchor_info.get("recent_topic_anchor") or {}
    candidate_topic_anchors = anchor_info.get("candidate_anchors") or []
    recent_context_for_focus = str(target_anchor.get("context_text") or recent_context_full)
    recent_focus_entity = (
        str(target_anchor.get("focus_entity") or "")
        or str(recent_topic_anchor.get("focus_entity") or "")
        or _extract_mobile_focus_entity(recent_context_for_focus)
        or _extract_mobile_focus_entity(latest_user_content)
    )
    recent_focus_topic = str(target_anchor.get("focus_topic") or recent_topic_anchor.get("focus_topic") or "")
    recent_focus_mode_hint = str(
        target_anchor.get("focus_mode_hint") or recent_topic_anchor.get("focus_mode_hint") or ""
    )
    if not recent_focus_topic:
        recent_focus_topic, recent_focus_mode_hint = _infer_focus_topic(recent_context_for_focus)
    should_include_recent_context = _should_preserve_recent_context(
        prompt_text,
        is_followup=is_followup,
        semantic_related=semantic_related,
        is_same_domain=is_same_domain,
        recent_turns=recent_turns,
        recent_focus_entity=recent_focus_entity,
        recent_focus_topic=recent_focus_topic,
    )
    conversation_memory_window = _infer_conversation_memory_window(prompt_text)
    conversation_memory_query = bool(conversation_memory_window.get("is_query"))
    conversation_memory_source = str(conversation_memory_window.get("source") or "")
    if conversation_memory_query and conversation_memory_source == "recent":
        should_include_recent_context = True
    should_load_long_memory = should_include_recent_context or (
        conversation_memory_query and conversation_memory_source == "long"
    )
    account_total_capital = None

    if current_user and current_user != "访客":
        try:
            parsed_capital = de.parse_account_total_capital(prompt_text)
            if parsed_capital:
                account_total_capital = float(parsed_capital)
                de.upsert_user_account_total_capital(
                    user_id=current_user,
                    total_capital=account_total_capital,
                    source_text=prompt_text,
                )
            else:
                profile_capital = (profile or {}).get("account_total_capital")
                normalized = de.normalize_account_total_capital(profile_capital)
                if normalized:
                    account_total_capital = float(normalized)
        except Exception as e:
            print(f"[mobile-chat] account capital profile read/update failed user={current_user} err={e}")

    if not should_include_recent_context:
        recent_context = ""
        recent_context_for_focus = ""
        recent_focus_entity = ""
        recent_focus_topic = ""
        recent_focus_mode_hint = ""
        target_anchor = {}
    else:
        recent_context = recent_context_for_focus

    pronoun_followup = any(hint in str(prompt_text or "") for hint in _MOBILE_FOCUS_PRONOUN_HINTS)
    explicit_focus_entity = _extract_mobile_focus_entity(prompt_text)
    explicit_focus_aspect = _extract_mobile_focus_aspect(prompt_text)
    recent_focus_aspect = str(target_anchor.get("focus_aspect") or "") or _extract_mobile_focus_aspect(recent_context_for_focus)
    should_inherit_focus = (
        should_include_recent_context
        or lookup_followup
        or pronoun_followup
        or bool(explicit_focus_aspect)
        or bool(recent_focus_entity)
    )
    focus_entity = explicit_focus_entity or (recent_focus_entity if should_inherit_focus else "")
    focus_aspect = explicit_focus_aspect or (recent_focus_aspect if should_inherit_focus else "")
    focus_topic, focus_mode_hint = _infer_focus_topic(prompt_text)
    if not focus_topic and should_inherit_focus:
        focus_topic, focus_mode_hint = recent_focus_topic, recent_focus_mode_hint
    followup_goal = _infer_followup_goal(
        prompt_text,
        recent_context=recent_context_for_focus,
        recent_focus_topic=focus_topic,
    )
    followup_action_context = _build_followup_action_context(
        target_anchor if followup_goal == "execute_suggested_action" else {}
    )
    correction_intent = _infer_correction_intent(
        prompt_text,
        recent_context=recent_context_for_focus,
        recent_focus_topic=focus_topic,
    )
    policy_followup_goal = followup_goal if (is_followup or should_include_recent_context or lookup_followup or pronoun_followup) else ""
    followup_task_policy = _classify_followup_task_policy(
        prompt_text,
        is_followup=bool(is_followup),
        followup_goal=policy_followup_goal,
        recent_context=recent_context_for_focus,
        target_anchor=target_anchor,
        focus_topic=focus_topic,
        focus_mode_hint=focus_mode_hint,
        correction_intent=bool(correction_intent),
    ).to_dict()
    followup_route_context = _build_followup_route_context(followup_task_policy)

    memory_context = ""
    if current_user and current_user != "访客" and should_load_long_memory:
        try:
            import memory_utils as mem

            if conversation_memory_query and conversation_memory_source == "long":
                found = mem.retrieve_recent_conversation_memory(
                    user_id=current_user,
                    limit=int(conversation_memory_window.get("limit") or 6),
                    since=str(conversation_memory_window.get("since") or ""),
                    until=str(conversation_memory_window.get("until") or ""),
                )
                if not found:
                    label = str(conversation_memory_window.get("label") or "相关时间")
                    found = f"【未检索到历史对话记录】没有查到{label}可用的历史对话记录。"
            else:
                found = mem.retrieve_relevant_memory(
                    user_id=current_user,
                    query=prompt_text,
                    k=2,
                    query_topic=intent_domain,
                    strict_topic=(intent_domain == "option"),
                )
            if found:
                memory_context = _filter_mobile_memory_context_by_domain(found, intent_domain=intent_domain)
        except Exception as e:
            print(f"[mobile-chat] memory retrieval failed user={current_user} err={e}")

    profile_memory_payload = {
        "profile_context": "",
        "memory_action": "guest_skip" if not current_user or current_user == "访客" else "context",
        "confirmation": "",
        "should_short_circuit": False,
        "temporary_overrides": {},
    }
    if current_user and current_user != "访客":
        try:
            profile_memory_payload = build_profile_memory_context(
                de.engine,
                user_id=current_user,
                prompt_text=prompt_text,
                portfolio_snapshot_loader=de.get_user_portfolio_snapshot,
            )
        except Exception as e:
            print(f"[mobile-chat] profile memory context failed user={current_user} err={e}")

    return {
        "is_followup": bool(is_followup),
        "intent_domain": intent_domain,
        "recent_domain": recent_domain,
        "recent_turns": recent_turns,
        "recent_context": recent_context,
        "memory_context": memory_context,
        "conversation_memory_query": conversation_memory_query,
        "conversation_memory_label": str(conversation_memory_window.get("label") or ""),
        "conversation_memory_source": conversation_memory_source,
        "profile_context": str(profile_memory_payload.get("profile_context") or ""),
        "profile_memory_action": str(profile_memory_payload.get("memory_action") or ""),
        "profile_memory_confirmation": str(profile_memory_payload.get("confirmation") or ""),
        "profile_memory_should_short_circuit": bool(profile_memory_payload.get("should_short_circuit", False)),
        "profile_memory_temporary_overrides": profile_memory_payload.get("temporary_overrides") or {},
        "focus_entity": focus_entity,
        "focus_topic": focus_topic,
        "focus_aspect": focus_aspect,
        "focus_mode_hint": focus_mode_hint,
        "followup_goal": followup_goal,
        "followup_action_context": followup_action_context,
        "followup_task_policy": followup_task_policy,
        "followup_route_context": followup_route_context,
        "correction_intent": bool(correction_intent),
        "recent_topic_anchor": recent_topic_anchor,
        "candidate_topic_anchors": candidate_topic_anchors,
        "target_anchor_id": str(target_anchor.get("anchor_id") or ""),
        "anchor_confidence": float(anchor_info.get("anchor_confidence") or 0.0),
        "followup_anchor_ambiguous": bool(anchor_info.get("followup_anchor_ambiguous")),
        "followup_anchor_clarify": str(anchor_info.get("followup_anchor_clarify") or ""),
        "semantic_related": bool(semantic_related),
        "conversation_id": f"mobile-{current_user}-{uuid.uuid4()}",
        "account_total_capital": account_total_capital,
        "vision_position_payload": None,
        "vision_position_domain": "",
    }


def _detect_mobile_has_portfolio(current_user: str) -> bool:
    if not current_user or current_user == "访客":
        return False
    try:
        from portfolio_analysis_service import get_user_portfolio_snapshot

        snapshot = get_user_portfolio_snapshot(current_user)
        return bool(snapshot and snapshot.get("recognized_count", 0) > 0)
    except Exception as e:
        print(f"[mobile-chat] portfolio check failed user={current_user} err={e}")
        return False


def _build_mobile_chat_prompt(user_prompt: str) -> str:
    """移动端与网页端保持同一意图路由输入，不再改写用户原问题。"""
    return str(user_prompt or "").strip()


def _mobile_chat_prompt_key(task_id: str) -> str:
    return f"{_MOBILE_CHAT_PROMPT_KEY_PREFIX}{task_id}"


def _mobile_chat_memory_queue_key(task_id: str) -> str:
    return f"{_MOBILE_CHAT_MEMORY_QUEUE_KEY_PREFIX}{task_id}"


def _dispatch_mobile_chat_memory_task(
    task_id: str,
    username: str,
    user_prompt: str,
    ai_response: str,
) -> None:
    from tasks import persist_mobile_chat_memory_task

    persist_mobile_chat_memory_task.delay(
        user_id=username,
        user_prompt=user_prompt,
        ai_response=ai_response,
        source="mobile",
        task_id=task_id,
    )


def _queue_mobile_chat_memory_persist(
    task_id: str,
    username: str,
    user_prompt: str,
    ai_response: str,
) -> str:
    """返回 queued / already_queued / invalid_payload / failed。"""
    prompt = str(user_prompt or "").strip()
    response = str(ai_response or "").strip()
    if not prompt or not response:
        return "invalid_payload"

    queue_key = _mobile_chat_memory_queue_key(task_id)
    try:
        queued = _redis.set(queue_key, "1", nx=True, ex=_MOBILE_CHAT_MEMORY_QUEUE_TTL)
    except Exception as e:
        print(f"[mobile-memory] queue lock failed task_id={task_id} err={e}")
        return "failed"

    if not queued:
        return "already_queued"

    try:
        _dispatch_mobile_chat_memory_task(
            task_id=task_id,
            username=username,
            user_prompt=prompt,
            ai_response=response,
        )
        return "queued"
    except Exception as e:
        print(f"[mobile-memory] dispatch failed task_id={task_id} err={e}")
        try:
            _redis.delete(queue_key)
        except Exception:
            pass
        return "failed"


def _mobile_chat_state_key(task_id: str) -> str:
    return f"{_MOBILE_CHAT_STATE_KEY_PREFIX}{task_id}"


def _mobile_chat_result_key(task_id: str) -> str:
    return f"{_MOBILE_CHAT_RESULT_KEY_PREFIX}{task_id}"


def _mobile_chat_last_task_key(username: str) -> str:
    return f"{_MOBILE_CHAT_LAST_TASK_KEY_PREFIX}{username}"


def _parse_iso_ts(value: Any) -> float:
    txt = str(value or "").strip()
    if not txt:
        return 0.0
    try:
        return datetime.fromisoformat(txt).timestamp()
    except Exception:
        return 0.0


def _safe_json_loads(raw: Any) -> dict:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    txt = str(raw).strip()
    if not txt:
        return {}
    try:
        data = json.loads(txt)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


_MARKET_MOVE_EXPLAIN_KEYWORDS = (
    "为什么", "原因", "怎么回事", "咋回事", "为何", "为啥", "怎么看",
    "什么情况", "发生了什么", "异动原因",
)
_MARKET_MOVE_KEYWORDS = (
    "大跌", "暴跌", "下跌", "跌", "跳水", "回落", "杀跌", "走弱",
    "大涨", "暴涨", "上涨", "涨", "拉升", "反弹", "走强", "冲高",
)
_MARKET_MOVE_UP_KEYWORDS = ("大涨", "暴涨", "上涨", "涨", "拉升", "反弹", "走强", "冲高")
_MARKET_MOVE_DOWN_KEYWORDS = ("大跌", "暴跌", "下跌", "跌", "跳水", "回落", "杀跌", "走弱")
_MARKET_MOVE_PERSONAL_EXCLUDE_KEYWORDS = (
    "我的", "我持有", "我买", "我卖", "持仓", "仓位", "账户", "净值", "收益", "亏损",
    "浮盈", "浮亏", "调仓", "加仓", "减仓", "买入", "卖出", "开仓", "平仓", "止损",
    "止盈", "要不要", "该不该", "能买吗", "能不能买", "可以买吗", "适合买",
    "目标价", "止损位", "止盈位", "怎么调", "如何调", "调一下", "具体操作",
    "该买", "该卖", "买吗", "卖吗", "做多", "做空",
)
_HYBRID_OPTION_EXCLUDE_KEYWORDS = (
    "期权", "认购", "认沽", "行权价", "牛市价差", "熊市价差", "跨式", "宽跨", "勒式",
    "蝶式", "铁鹰", "卖方", "买方", "IV", "iv", "波动率", "Delta", "delta",
    "Gamma", "gamma", "Vega", "vega", "Theta", "theta", "权利金",
)
_TECHNICAL_ANALYSIS_KEYWORDS = (
    "技术面", "技术分析", "走势", "趋势", "形态", "K线", "k线", "均线", "支撑",
    "压力", "阻力", "突破", "跌破", "回踩", "放量", "缩量", "量能", "MACD",
    "macd", "RSI", "rsi", "偏多", "偏空", "强弱", "走强", "走弱",
)
_MARKET_MOVE_SUBJECT_KEYWORDS = (
    "美股", "A股", "a股", "港股", "日股", "欧股", "纳指", "纳斯达克", "标普", "道指",
    "沪深300", "中证500", "中证1000", "上证", "创业板", "科创", "恒生", "恒科",
    "黄金", "白银", "原油", "铜", "沪铜", "螺纹", "铁矿", "焦煤", "焦炭", "商品",
    "螺纹钢", "热卷", "玻璃", "纯碱", "甲醇", "豆粕", "菜粕", "棕榈油", "豆油",
    "橡胶", "PTA", "pta", "PVC", "pvc", "沪银", "沪金", "沪铝", "沪锌", "沪镍",
    "期货", "指数", "ETF", "etf", "股票", "美元", "美债", "人民币", "英伟达", "特斯拉",
    "苹果", "微软", "谷歌", "亚马逊", "Meta", "meta", "英特尔",
)
_MARKET_MOVE_TARGET_STOPWORDS = (
    "为什么", "为何", "为啥", "怎么回事", "咋回事", "怎么看", "什么情况", "发生了什么",
    "今天", "今日", "今晚", "昨天", "最近", "近期", "现在", "当前", "这两天", "这几天",
    "这么", "那么", "突然", "一直", "又", "会", "还", "呢", "吗", "啊", "了", "的",
    "技术面", "技术分析", "走势", "趋势", "形态", "K线", "k线", "均线", "支撑",
    "压力", "阻力", "突破", "跌破", "回踩", "放量", "缩量", "量能", "MACD",
    "macd", "RSI", "rsi", "偏多", "偏空", "强弱",
)


def _extract_market_move_direction(prompt_text: str) -> str:
    text = str(prompt_text or "")
    if any(keyword in text for keyword in _MARKET_MOVE_DOWN_KEYWORDS):
        return "下跌"
    if any(keyword in text for keyword in _MARKET_MOVE_UP_KEYWORDS):
        return "上涨"
    return "异动"


def _detect_hybrid_quick_scenario(prompt_text: str) -> str:
    text = str(prompt_text or "")
    if any(keyword in text for keyword in _TECHNICAL_ANALYSIS_KEYWORDS):
        return "technical"
    return "market_move"


_FRESHNESS_TIME_KEYWORDS = (
    "今天", "今日", "现在", "当前", "此刻", "刚刚", "刚才", "最新", "最近", "近期",
    "本周", "本月", "今晚", "明天", "昨天",
)
_FRESHNESS_FACT_KEYWORDS = (
    "上市", "IPO", "ipo", "公告", "新闻", "消息", "快讯", "财报", "业绩", "传闻",
    "政策", "利率", "发布", "披露", "确认", "官宣", "宣布", "批准",
)
_FRESHNESS_CONCEPT_PREFIXES = (
    "什么是", "什么叫", "解释", "科普", "介绍一下", "介绍下", "定义", "概念",
)
_FRESHNESS_MARKET_DATA_KEYWORDS = (
    "最新价", "现价", "报价", "行情", "涨跌", "涨幅", "跌幅", "K线", "k线",
    "技术面", "选股", "做空", "IV", "iv", "波动率", "期权", "合约",
)
_FRESHNESS_QUERY_STOPWORDS = (
    "今天", "今日", "现在", "当前", "此刻", "刚刚", "刚才", "最新", "最近", "近期",
    "是不是", "是否", "有没有", "要不要", "会不会", "能不能", "是否会", "是不是要",
    "上市", "IPO", "ipo", "公告", "新闻", "消息", "快讯", "传闻", "确认", "官宣",
    "宣布", "发布", "披露", "吗", "呢", "么", "呀", "啊", "请问", "帮我", "查一下",
)


def _mobile_freshness_quick_max_workers() -> int:
    try:
        value = int(os.getenv("MOBILE_FRESHNESS_QUICK_MAX_WORKERS", "2") or "2")
    except Exception:
        value = 2
    return min(max(value, 1), 8)


_FRESHNESS_QUICK_EXECUTOR = ThreadPoolExecutor(max_workers=_mobile_freshness_quick_max_workers())


def _mobile_freshness_as_of() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _mobile_freshness_timeout_seconds() -> float:
    try:
        value = float(os.getenv("MOBILE_FRESHNESS_QUICK_TIMEOUT_SECONDS", "1.5") or "1.5")
    except Exception:
        value = 1.5
    return min(max(value, 0.2), 5.0)


def _contains_any(text: str, keywords: Tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _is_freshness_concept_query(prompt_text: str) -> bool:
    text = str(prompt_text or "").strip()
    lower_text = text.lower()
    if not text:
        return False
    if any(text.startswith(prefix) or lower_text.startswith(prefix.lower()) for prefix in _FRESHNESS_CONCEPT_PREFIXES):
        return True
    return bool(re.search(r"(是什么|是什么意思|怎么理解|概念|定义)$", text))


def _is_freshness_market_data_or_trading_query(prompt_text: str) -> bool:
    text = str(prompt_text or "")
    lower_text = text.lower()
    if _contains_any(text, _HYBRID_OPTION_EXCLUDE_KEYWORDS):
        return True
    if _contains_any(text, _TECHNICAL_ANALYSIS_KEYWORDS):
        return True
    return any(keyword.lower() in lower_text for keyword in _FRESHNESS_MARKET_DATA_KEYWORDS)


def _is_freshness_sensitive_query(
    prompt_text: str,
    chat_mode: str = "",
    context_payload: Mapping[str, Any] | None = None,
) -> bool:
    text = str(prompt_text or "").strip()
    if not text:
        return False
    if _is_freshness_market_data_or_trading_query(text):
        return False

    lower_text = text.lower()
    has_time_word = _contains_any(text, _FRESHNESS_TIME_KEYWORDS)
    has_fact_word = any(keyword.lower() in lower_text for keyword in _FRESHNESS_FACT_KEYWORDS)
    asks_listing_or_ipo = "上市" in text or "ipo" in lower_text

    if _is_freshness_concept_query(text) and not has_time_word:
        return False
    if asks_listing_or_ipo and not _is_freshness_concept_query(text):
        return True
    if has_time_word and has_fact_word:
        return True
    if any(phrase in text for phrase in ("最新消息", "最新新闻", "最近公告", "最新公告", "刚刚发布")):
        return True
    return False


def _extract_freshness_query_target(
    prompt_text: str,
    context_payload: Mapping[str, Any] | None = None,
) -> str:
    payload = context_payload or {}
    focus_entity = str(payload.get("focus_entity") or "").strip()
    if focus_entity:
        return focus_entity[:32]

    text = str(prompt_text or "").strip()
    for keyword in sorted(_FRESHNESS_QUERY_STOPWORDS, key=len, reverse=True):
        text = re.sub(re.escape(keyword), " ", text, flags=re.I)
    text = re.sub(r"[，。！？、,.!?；;：:\s]+", " ", text).strip()
    chunks = [chunk.strip(" -_/（）()[]【】") for chunk in text.split(" ") if chunk.strip()]
    for chunk in chunks:
        if 2 <= len(chunk) <= 32:
            return chunk
    return str(prompt_text or "").strip()[:32]


def _build_freshness_lookup_query(prompt_text: str, target: str = "") -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    if target:
        return f"{target} 最新 官方 公告 新闻 IPO 上市 {today}"
    return f"{prompt_text} 最新 官方 公告 新闻 {today}"


def _format_freshness_as_of(as_of: str) -> str:
    return str(as_of or "").replace("T", " ")[:16]


def _freshness_placeholder_answer(status: str, as_of: str) -> str:
    display_as_of = _format_freshness_as_of(as_of)
    if status == "not_found":
        return (
            f"快速核验：截至{display_as_of}，限时检索还没有拿到明确公开确认。"
            "这个问题需要实时核验，我先不凭模型记忆下结论；后台正在继续查证公告和新闻源。"
        )
    return (
        f"快速核验：截至{display_as_of}，这个问题需要实时核验。"
        "我先不凭模型记忆下结论；后台正在继续查证公告和新闻源，稍后补充结果。"
    )


def _is_freshness_lookup_empty(lookup_text: str) -> bool:
    text = str(lookup_text or "").strip()
    if not text:
        return True
    empty_hints = (
        "未搜索到", "没有搜索到", "没有找到", "暂无", "未找到", "不可用",
        "搜索失败", "搜索出错", "无法访问", "未配置", "timeout",
    )
    return any(hint in text for hint in empty_hints)


def _build_freshness_lookup_answer(
    prompt_text: str,
    target: str,
    lookup_text: str,
    as_of: str,
) -> str:
    display_as_of = _format_freshness_as_of(as_of)
    snippet = re.sub(r"\s+", " ", str(lookup_text or "")).strip()
    snippet = snippet[:220]
    subject = target or "这个问题"
    return (
        f"快速核验：截至{display_as_of}，我先查到的公开检索摘要是：{snippet}"
        f"。这只是对{subject}的限时检索，不作为最终结论；后台会继续核验公告和新闻源后补充。"
    )


def _invoke_freshness_lookup(query: str) -> str:
    from search_tools import search_web

    if hasattr(search_web, "invoke"):
        return str(search_web.invoke({"query": query}) or "")
    return str(search_web(query) or "")


def _generate_freshness_quick_answer(
    prompt_text: str,
    context_payload: Mapping[str, Any] | None = None,
) -> Tuple[str, str, int, str, str]:
    started_at = time.time()
    as_of = _mobile_freshness_as_of()
    target = _extract_freshness_query_target(prompt_text, context_payload)
    query = _build_freshness_lookup_query(prompt_text, target)
    future = _FRESHNESS_QUICK_EXECUTOR.submit(_invoke_freshness_lookup, query)
    try:
        lookup_text = str(future.result(timeout=_mobile_freshness_timeout_seconds()) or "").strip()
    except TimeoutError:
        future.cancel()
        elapsed_ms = int((time.time() - started_at) * 1000)
        return _freshness_placeholder_answer("timeout", as_of), "fresh_lookup_timeout", elapsed_ms, "timeout", as_of
    except Exception as exc:
        print(f"[mobile-chat] freshness quick lookup failed err={exc}", flush=True)
        elapsed_ms = int((time.time() - started_at) * 1000)
        return _freshness_placeholder_answer("unavailable", as_of), "fresh_lookup_empty", elapsed_ms, "unavailable", as_of

    elapsed_ms = int((time.time() - started_at) * 1000)
    if _is_freshness_lookup_empty(lookup_text):
        return _freshness_placeholder_answer("not_found", as_of), "fresh_lookup_empty", elapsed_ms, "not_found", as_of
    return _build_freshness_lookup_answer(prompt_text, target, lookup_text, as_of), "fresh_lookup", elapsed_ms, "verified", as_of


def _extract_hybrid_quick_bias(prompt_text: str) -> str:
    text = str(prompt_text or "")
    if any(keyword in text for keyword in ("跌破", "跳水", "杀跌", "走弱", "回落", "下跌", "大跌", "暴跌")):
        return "短线偏弱"
    if any(keyword in text for keyword in ("突破", "放量", "拉升", "走强", "反弹", "上涨", "大涨", "暴涨")):
        return "短线偏强"
    if any(keyword in text for keyword in ("偏空", "压力", "阻力")):
        return "偏谨慎"
    if any(keyword in text for keyword in ("偏多", "支撑")):
        return "偏观察反弹"
    return "先按震荡观察"


def _extract_market_move_target(prompt_text: str, context_payload: Mapping[str, Any] | None = None) -> str:
    payload = context_payload or {}
    focus_entity = str(payload.get("focus_entity") or "").strip()
    if focus_entity:
        return focus_entity[:24]

    text = str(prompt_text or "").strip()
    for keyword in sorted(_MARKET_MOVE_EXPLAIN_KEYWORDS + _MARKET_MOVE_KEYWORDS, key=len, reverse=True):
        text = text.replace(keyword, " ")
    for keyword in _MARKET_MOVE_TARGET_STOPWORDS:
        text = text.replace(keyword, " ")
    text = re.sub(r"[，。！？、,.!?；;：:\s]+", " ", text).strip()
    chunks = [chunk.strip(" -_/（）()[]【】") for chunk in text.split(" ") if chunk.strip()]
    for chunk in chunks:
        if 2 <= len(chunk) <= 24:
            return chunk
    return ""


def _has_market_move_subject(prompt_text: str, context_payload: Mapping[str, Any] | None = None) -> bool:
    text = str(prompt_text or "")
    if any(keyword in text for keyword in _MARKET_MOVE_SUBJECT_KEYWORDS):
        return True
    if re.search(r"\b[A-Z]{1,6}(?:\.[A-Z]{1,4})?\b", text):
        return True
    if re.search(r"\b\d{6}(?:\.(?:SH|SZ|BJ))?\b", text, flags=re.I):
        return True
    return bool(_extract_market_move_target(text, context_payload))


def _is_market_move_quick_answer_candidate(
    prompt_text: str,
    chat_mode: str,
    context_payload: Mapping[str, Any] | None = None,
) -> bool:
    if chat_mode != CHAT_MODE_ANALYSIS:
        return False
    text = str(prompt_text or "").strip()
    if not text:
        return False
    if any(keyword in text for keyword in _MARKET_MOVE_PERSONAL_EXCLUDE_KEYWORDS):
        return False
    if any(keyword in text for keyword in _HYBRID_OPTION_EXCLUDE_KEYWORDS):
        return False
    has_subject = _has_market_move_subject(text, context_payload)
    has_explain_intent = any(keyword in text for keyword in _MARKET_MOVE_EXPLAIN_KEYWORDS)
    has_move_word = any(keyword in text for keyword in _MARKET_MOVE_KEYWORDS)
    has_technical_intent = any(keyword in text for keyword in _TECHNICAL_ANALYSIS_KEYWORDS)
    if has_explain_intent and has_move_word and has_subject:
        return True
    if has_technical_intent and has_subject:
        return True
    return False


def _build_market_move_quick_template(
    prompt_text: str,
    context_payload: Mapping[str, Any] | None = None,
) -> str:
    target = _extract_market_move_target(prompt_text, context_payload) or "这个市场"
    direction = _extract_market_move_direction(prompt_text)
    scenario = _detect_hybrid_quick_scenario(prompt_text)
    bias = _extract_hybrid_quick_bias(prompt_text)
    if scenario == "technical":
        return (
            f"快速判断：{target}先按技术面看，初步倾向是{bias}。"
            "需要重点核对三点：趋势是否仍在关键均线或前高前低附近延续；"
            "量能是否配合突破、回踩或跌破；"
            "支撑压力位附近有没有重新放量确认。"
            "这只是快速框架，后台会继续核实最新盘面后补充完整分析。"
        )
    return (
        f"快速判断：关于{target}{direction}，初步倾向是{bias}，先看三条线："
        "一是利率、美元和流动性是否压缩风险偏好；"
        "二是权重资产、行业主线或财报预期有没有被重新定价；"
        "三是新闻、政策或资金流是否触发了短线集中反应。"
        "我先给这个初步框架，后台会继续核实最新盘面和消息，稍后补充更完整分析。"
    )


def _extract_llm_response_text(response: Any) -> str:
    if hasattr(response, "content"):
        return str(getattr(response, "content") or "").strip()
    if isinstance(response, dict):
        return str(response.get("content") or response.get("text") or response.get("response") or "").strip()
    return str(response or "").strip()


def _generate_market_move_quick_answer(
    prompt_text: str,
    context_payload: Mapping[str, Any] | None = None,
) -> Tuple[str, str, int]:
    started_at = time.time()
    fallback = _build_market_move_quick_template(prompt_text, context_payload)
    target = _extract_market_move_target(prompt_text, context_payload) or "这个市场"
    direction = _extract_market_move_direction(prompt_text)
    scenario = _detect_hybrid_quick_scenario(prompt_text)
    bias = _extract_hybrid_quick_bias(prompt_text)
    recent_context = str((context_payload or {}).get("recent_context") or "").strip()
    focus_topic = str((context_payload or {}).get("focus_topic") or "").strip()
    prompt = (
        "你是爱波塔小程序的交易问答助手。请给用户一个很短的股票、期货或技术分析快速判断。\n"
        "硬性要求：\n"
        "1. 只输出 120-220 字中文，不分点超过 3 条。\n"
        "2. 可以给保守的初步倾向，但必须说明待后台核实；不要把快答写成最终结论。\n"
        "3. 不编造实时行情、具体新闻、具体数字；没有数据就说先按框架判断。\n"
        "4. 不给具体买卖、仓位、止损、目标价或开仓建议。\n"
        "5. 期权策略不在本轮快答范围内；如果出现期权语境，只提示后台深度分析。\n"
        "6. 结尾说明后台会继续做深度分析。\n\n"
        f"用户问题：{prompt_text}\n"
        f"识别对象：{target}\n"
        f"识别方向：{direction}\n"
        f"快答场景：{scenario}\n"
        f"初步倾向参考：{bias}\n"
        f"当前焦点：{focus_topic or '无'}\n"
        f"近期上下文：{recent_context[:500] if recent_context else '无'}"
    )
    try:
        llm = build_deepseek_flash_llm(
            streaming=False,
            temperature=0.2,
            timeout=8,
            max_retries=0,
            max_tokens=320,
        )
        response_text = _extract_llm_response_text(llm.invoke(prompt)).strip()
        elapsed_ms = int((time.time() - started_at) * 1000)
        if response_text:
            return response_text, "llm", elapsed_ms
    except Exception as exc:
        print(f"[mobile-chat] hybrid quick llm fallback err={exc}", flush=True)
    elapsed_ms = int((time.time() - started_at) * 1000)
    return fallback, "template", elapsed_ms


def _is_hybrid_delivery_state(state: Mapping[str, Any] | None) -> bool:
    return str((state or {}).get("delivery_mode") or "").strip().lower() == "hybrid"


def _mobile_chat_timeout_seconds_for_state(state: Mapping[str, Any] | None) -> int:
    return _MOBILE_CHAT_BACKGROUND_MAX_PENDING_SECONDS if _is_hybrid_delivery_state(state) else _MOBILE_CHAT_MAX_PENDING_SECONDS


def _mobile_chat_timeout_error_for_state(state: Mapping[str, Any] | None) -> str:
    if _is_hybrid_delivery_state(state):
        return "深度分析暂时还没完成，我先保留上面的快速判断；你可以继续提问，稍后也可以再回来查看。"
    return "AI思考太久，请重新提问。"


def _read_mobile_chat_state(task_id: str) -> dict:
    try:
        return _safe_json_loads(_redis.get(_mobile_chat_state_key(task_id)))
    except Exception:
        return {}


def _read_mobile_chat_result(task_id: str) -> dict:
    try:
        return _safe_json_loads(_redis.get(_mobile_chat_result_key(task_id)))
    except Exception:
        return {}


def _write_mobile_chat_state(
    task_id: str,
    user_id: str,
    status: str,
    error: str = "",
    finished: bool = False,
    extra_fields: Optional[Dict[str, Any]] = None,
):
    existing = _read_mobile_chat_state(task_id)
    now_iso = datetime.now().isoformat()
    payload = dict(existing) if isinstance(existing, dict) else {}
    payload.update(
        {
            "task_id": task_id,
            "user_id": str(user_id or ""),
            "status": str(status or "").strip() or "pending",
            "error": str(error or "").strip(),
            "created_at": str(existing.get("created_at") or now_iso),
            "updated_at": now_iso,
            "finished_at": now_iso if finished else str(existing.get("finished_at") or ""),
        }
    )
    if isinstance(extra_fields, dict):
        for key, value in extra_fields.items():
            if value is None:
                continue
            payload[str(key)] = value
    try:
        _redis.setex(
            _mobile_chat_state_key(task_id),
            _MOBILE_CHAT_RESULT_TTL_SECONDS,
            json.dumps(payload, ensure_ascii=False),
        )
    except Exception as e:
        print(f"[mobile-chat] state write failed task_id={task_id} err={e}")


def _write_mobile_chat_result(task_id: str, user_id: str, result_payload: dict):
    payload = {
        "task_id": task_id,
        "user_id": str(user_id or ""),
        "result": result_payload if isinstance(result_payload, dict) else {},
        "updated_at": datetime.now().isoformat(),
    }
    try:
        _redis.setex(
            _mobile_chat_result_key(task_id),
            _MOBILE_CHAT_RESULT_TTL_SECONDS,
            json.dumps(payload, ensure_ascii=False),
        )
    except Exception as e:
        print(f"[mobile-chat] result write failed task_id={task_id} err={e}")


def _set_mobile_chat_last_task(username: str, task_id: str):
    if not username or not task_id:
        return
    try:
        _redis.setex(_mobile_chat_last_task_key(username), _MOBILE_CHAT_RESULT_TTL_SECONDS, task_id)
    except Exception as e:
        print(f"[mobile-chat] set last_task failed user={username} err={e}")


def _get_mobile_chat_last_task(username: str) -> str:
    if not username:
        return ""
    try:
        return str(_redis.get(_mobile_chat_last_task_key(username)) or "").strip()
    except Exception:
        return ""


def _clear_mobile_chat_last_task_if_matches(username: str, task_id: str):
    if not username or not task_id:
        return
    key = _mobile_chat_last_task_key(username)
    try:
        current = str(_redis.get(key) or "").strip()
        if current == task_id:
            _redis.delete(key)
    except Exception:
        pass


def _build_mobile_chat_success_response(result_payload: dict, state: Optional[dict] = None) -> dict:
    payload = {
        "status": "success",
        "progress": "已完成",
        "result": result_payload if isinstance(result_payload, dict) else {},
        "error": None,
    }
    state = state if isinstance(state, dict) else {}
    trace_id = str(state.get("trace_id") or "").strip()
    answer_id = str(state.get("answer_id") or "").strip()
    if trace_id:
        payload["trace_id"] = trace_id
    if answer_id:
        payload["answer_id"] = answer_id
    payload["feedback_allowed"] = bool(state.get("feedback_allowed", bool(answer_id)))
    chat_mode = str(state.get("chat_mode") or "").strip()
    if chat_mode:
        payload["chat_mode"] = chat_mode
    delivery_mode = str(state.get("delivery_mode") or "").strip()
    if delivery_mode:
        payload["delivery_mode"] = delivery_mode
    return payload


def _build_mobile_chat_error_response(err_msg: str, code: str = "", state: Optional[dict] = None) -> dict:
    payload = {
        "status": "error",
        "progress": "任务失败",
        "result": None,
        "error": str(err_msg or "分析失败，请稍后重试"),
    }
    if code:
        payload["code"] = code
    state = state if isinstance(state, dict) else {}
    delivery_mode = str(state.get("delivery_mode") or "").strip()
    if delivery_mode:
        payload["delivery_mode"] = delivery_mode
    chat_mode = str(state.get("chat_mode") or "").strip()
    if chat_mode:
        payload["chat_mode"] = chat_mode
    return payload


def _generate_chat_trace_id() -> str:
    return generate_chat_trace_id()


def _generate_chat_answer_id() -> str:
    return generate_chat_answer_id()


def _get_chat_feedback_engine():
    return getattr(de, "engine", None)


def _ensure_chat_feedback_tables() -> bool:
    return ensure_chat_feedback_tables(_get_chat_feedback_engine())


def _ensure_chat_feedback_admin(username: str):
    if str(username or "").strip() not in _CHAT_FEEDBACK_ADMIN_USERS:
        raise HTTPException(status_code=403, detail="no permission to view feedback pool")


def _save_chat_answer_event(
    *,
    task_id: str,
    user_id: str,
    trace_id: str,
    answer_id: str,
    prompt_text: str,
    response_text: str,
    intent_domain: str = "general",
    feedback_allowed: bool = True,
) -> bool:
    return save_chat_answer_event(
        _get_chat_feedback_engine(),
        task_id=task_id,
        user_id=user_id,
        trace_id=trace_id,
        answer_id=answer_id,
        prompt_text=prompt_text,
        response_text=response_text,
        intent_domain=intent_domain,
        feedback_allowed=feedback_allowed,
    )


def _get_chat_answer_event(answer_id: str) -> dict:
    return get_chat_answer_event(_get_chat_feedback_engine(), answer_id)


def _save_chat_feedback_event(**kwargs) -> bool:
    return save_chat_feedback_event(_get_chat_feedback_engine(), **kwargs)


def _list_chat_feedback_failure_candidates(
    limit: int = 20,
    *,
    intent_domain: str = "",
    reason_code: str = "",
    keyword: str = "",
    start_at: str = "",
    end_at: str = "",
    min_occurrence: int = 1,
) -> List[dict]:
    return list_chat_feedback_failure_candidates(
        _get_chat_feedback_engine(),
        limit=limit,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
        start_at=start_at,
        end_at=end_at,
        min_occurrence=min_occurrence,
    )


def _list_chat_feedback_events(
    limit: int = 100,
    feedback_type: str = "",
    answer_id: str = "",
    user_id: str = "",
    intent_domain: str = "",
    reason_code: str = "",
    keyword: str = "",
    start_at: str = "",
    end_at: str = "",
) -> List[dict]:
    return list_chat_feedback_events(
        _get_chat_feedback_engine(),
        limit=limit,
        feedback_type=feedback_type,
        answer_id=answer_id,
        user_id=user_id,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
        start_at=start_at,
        end_at=end_at,
    )


def _list_chat_feedback_samples(
    limit: int = 100,
    sample_status: str = "",
    optimization_type: str = "",
    intent_domain: str = "",
    reason_code: str = "",
    keyword: str = "",
) -> List[dict]:
    return list_chat_feedback_samples(
        _get_chat_feedback_engine(),
        limit=limit,
        sample_status=sample_status,
        optimization_type=optimization_type,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
    )


def _upsert_chat_feedback_sample(
    *,
    prompt_text: str,
    reason_code: str,
    intent_domain: str = "general",
    occurrence_count: int = 1,
    latest_feedback_at: str = "",
    latest_feedback_text: str = "",
    sample_answer_id: str = "",
    sample_trace_id: str = "",
    sample_response_text: str = "",
    created_by: str = "",
    sample_status: str = "new",
    optimization_type: str = "",
    review_notes: str = "",
) -> dict:
    return upsert_chat_feedback_sample(
        _get_chat_feedback_engine(),
        prompt_text=prompt_text,
        reason_code=reason_code,
        intent_domain=intent_domain,
        occurrence_count=occurrence_count,
        latest_feedback_at=latest_feedback_at,
        latest_feedback_text=latest_feedback_text,
        sample_answer_id=sample_answer_id,
        sample_trace_id=sample_trace_id,
        sample_response_text=sample_response_text,
        created_by=created_by,
        sample_status=sample_status,
        optimization_type=optimization_type,
        review_notes=review_notes,
    )


def _update_chat_feedback_sample(
    *,
    sample_key: str,
    sample_status: Optional[str] = None,
    optimization_type: Optional[str] = None,
    review_notes: Optional[str] = None,
    reviewed_by: str = "",
) -> dict:
    return update_chat_feedback_sample(
        _get_chat_feedback_engine(),
        sample_key=sample_key,
        sample_status=sample_status,
        optimization_type=optimization_type,
        review_notes=review_notes,
        reviewed_by=reviewed_by,
    )


def _build_mobile_chat_runtime_snapshot(task_id: str, username: str) -> dict:
    state = _read_mobile_chat_state(task_id)
    if state and state.get("user_id") and str(state.get("user_id")) != str(username):
        raise HTTPException(status_code=403, detail="无权限访问该任务")

    result_wrapper = _read_mobile_chat_result(task_id)
    result_payload = result_wrapper.get("result") if isinstance(result_wrapper, dict) else None

    status_name = str(state.get("status") or "").strip().lower()
    if status_name == "success" and isinstance(result_payload, dict):
        trace_id = str(state.get("trace_id") or "").strip()
        answer_id = str(state.get("answer_id") or "").strip()
        prompt_text = str(state.get("prompt_text") or "").strip()
        response_text = str((result_payload or {}).get("response") or "").strip()
        if trace_id and answer_id and response_text:
            _save_chat_answer_event(
                task_id=task_id,
                user_id=str(state.get("user_id") or username),
                trace_id=trace_id,
                answer_id=answer_id,
                prompt_text=prompt_text,
                response_text=response_text,
                intent_domain=str(state.get("intent_domain") or "general"),
                feedback_allowed=bool(state.get("feedback_allowed", True)),
            )
        return _build_mobile_chat_success_response(result_payload, state=state)

    if status_name in {"error", "canceled", "timeout"}:
        msg = str(state.get("error") or "").strip()
        if not msg:
            if status_name == "timeout":
                msg = _mobile_chat_timeout_error_for_state(state)
            elif status_name == "canceled":
                msg = "任务已取消。"
            else:
                msg = "分析失败，请稍后重试。"
        return _build_mobile_chat_error_response(msg, code=f"task_{status_name}", state=state)

    created_ts = _parse_iso_ts(state.get("created_at")) if state else 0.0
    if created_ts > 0 and status_name in {"pending", "processing"}:
        elapsed = time.time() - created_ts
        timeout_seconds = _mobile_chat_timeout_seconds_for_state(state)
        if elapsed >= timeout_seconds:
            timeout_msg = _mobile_chat_timeout_error_for_state(state)
            _write_mobile_chat_state(
                task_id=task_id,
                user_id=str(state.get("user_id") or username),
                status="timeout",
                error=timeout_msg,
                finished=True,
            )
            TaskManager.complete_user_task(username, task_id)
            _clear_mobile_chat_last_task_if_matches(username, task_id)
            return _build_mobile_chat_error_response(timeout_msg, code="task_timeout", state=state)

    if status_name in {"pending", "processing"} and state:
        chat_mode = str(state.get("chat_mode") or CHAT_MODE_ANALYSIS).strip() or CHAT_MODE_ANALYSIS
        payload = {
            "status": status_name,
            "progress": str(state.get("progress") or default_progress_for_chat_mode(chat_mode, status=status_name)),
            "result": None,
            "error": None,
            "chat_mode": chat_mode,
        }
        trace_id = str(state.get("trace_id") or "").strip()
        answer_id = str(state.get("answer_id") or "").strip()
        if trace_id:
            payload["trace_id"] = trace_id
        if answer_id:
            payload["answer_id"] = answer_id
        delivery_mode = str(state.get("delivery_mode") or "").strip()
        if delivery_mode:
            payload["delivery_mode"] = delivery_mode
        return payload

    return {}


# ════════════════════════════════════════════════════════════
#  Health Check
# ════════════════════════════════════════════════════════════

@app.get("/api/health", tags=["系统"])
def health():
    return {"status": "ok", "service": "爱波塔 Mobile API v1.0"}


# ════════════════════════════════════════════════════════════
#  AUTH
# ════════════════════════════════════════════════════════════

@app.post("/api/auth/send-code", tags=["认证"])
def send_code(body: SendCodeRequest):
    """发送邮箱登录验证码（60 秒内限发一次）。"""
    from email_utils import send_login_code
    try:
        result = send_login_code(body.email)
        success, msg = (result[0], result[1]) if isinstance(result, tuple) else (bool(result), "验证码已发送")
        if not success:
            raise HTTPException(status_code=400, detail=msg)
        return {"message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"发送失败: {e}")


@app.post("/api/auth/register/send-phone-code", tags=["认证"])
def register_send_phone_code(body: RegisterSendPhoneCodeRequest, request: Request):
    """注册流程：发送手机号短信验证码。"""
    client_ip = ""
    try:
        client_ip = (request.client.host if request and request.client else "") or ""
    except Exception:
        client_ip = ""
    try:
        success, msg = auth.send_register_phone_code(body.phone, client_ip=client_ip)
        if not success:
            raise HTTPException(status_code=400, detail=msg)
        return {"message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"验证码发送失败: {e}")


@app.post("/api/auth/register/verify-phone-code", tags=["认证"])
def register_verify_phone_code(body: RegisterVerifyPhoneCodeRequest):
    """注册流程：校验手机号验证码。"""
    try:
        success, msg, normalized_phone = auth.verify_register_phone_code(body.phone, body.code)
        if not success:
            raise HTTPException(status_code=400, detail=msg)
        return {"message": msg, "phone": normalized_phone}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"验证码校验失败: {e}")


@app.post("/api/auth/register", tags=["认证"])
def register_account(body: RegisterRequest):
    """
    账号注册（移动端）：
    1) 校验账号密码
    2) 校验手机号短信验证码
    3) 创建账号并自动签发 token
    """
    ok_step1, step1_msg, normalized_username = auth.validate_register_step1(
        body.username, body.password, body.password_confirm
    )
    if not ok_step1:
        raise HTTPException(status_code=400, detail=step1_msg)

    ok_phone, phone_msg, normalized_phone = auth.verify_register_phone_code(body.phone, body.sms_code)
    if not ok_phone:
        raise HTTPException(status_code=400, detail=phone_msg)

    reg_ok, reg_msg = auth.register_with_username_phone(
        normalized_username,
        body.password,
        normalized_phone,
    )
    if not reg_ok:
        raise HTTPException(status_code=400, detail=reg_msg)

    sess_ok, sess_msg, raw_token = auth.create_user_session(normalized_username)
    if not sess_ok or not raw_token:
        # 账号已创建但会话创建失败，返回 500 让前端提示用户用登录入口重试
        raise HTTPException(status_code=500, detail=sess_msg or "注册成功但登录失败，请使用登录功能进入")

    return {
        "token": _pack_token(normalized_username, raw_token),
        "username": normalized_username,
        "message": reg_msg or "注册成功",
    }


@app.post("/api/auth/password-reset/send-phone-code", tags=["认证"])
def password_reset_send_phone_code(body: PasswordResetSendPhoneCodeRequest, request: Request):
    """忘记密码：发送手机号短信验证码，复用登录验证码模板。"""
    client_ip = ""
    try:
        client_ip = (request.client.host if request and request.client else "") or ""
    except Exception:
        client_ip = ""
    try:
        success, msg = auth.send_reset_password_phone_code(body.phone, client_ip=client_ip)
        if not success:
            raise HTTPException(status_code=400, detail=msg)
        return {"message": msg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"验证码发送失败: {e}")


@app.post("/api/auth/password-reset", tags=["认证"])
def password_reset(body: PasswordResetRequest):
    """忘记密码：通过手机号验证码重置密码。"""
    if body.new_password != body.new_password_confirm:
        raise HTTPException(status_code=400, detail="两次密码不一致")

    success, msg, username = auth.reset_password_with_phone(
        body.phone,
        body.sms_code,
        body.new_password,
    )
    if not success:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg, "username": username}


@app.post("/api/auth/login", tags=["认证"])
def login(body: LoginRequest):
    """用户名或邮箱 + 密码登录，返回 token 和 username。"""
    success, msg, raw_token, username = auth.login_user(body.account, body.password)
    if not success:
        raise HTTPException(status_code=401, detail=msg)
    return {
        "token": _pack_token(username, raw_token),
        "username": username,
        "message": msg,
    }


@app.post("/api/auth/login/email", tags=["认证"])
def login_email(body: EmailLoginRequest):
    """邮箱验证码登录，返回 token 和 username。"""
    success, msg, raw_token, username = auth.login_with_email_code(body.email, body.code)
    if not success:
        raise HTTPException(status_code=401, detail=msg)
    return {
        "token": _pack_token(username, raw_token),
        "username": username,
        "message": msg,
    }


@app.post("/api/auth/logout", tags=["认证"])
def logout(credentials: HTTPAuthorizationCredentials = Depends(_bearer)):
    """登出当前设备，其他设备不受影响。"""
    username_hint, raw_token = _unpack_token(credentials.credentials)
    username = username_hint or auth.get_username_by_token(raw_token)
    auth.logout_user(username=username, token=raw_token)
    return {"message": "已登出"}


@app.get("/api/auth/verify", tags=["认证"])
def verify_token(username: str = Depends(get_current_user)):
    """验证 Token 是否仍然有效。"""
    return {"valid": True, "username": username}


@app.get("/api/device/ping", tags=["设备"])
def device_ping(request: Request, username: str = Depends(get_current_user)):
    device_ctx = _extract_device_context(request)
    payload = {
        "ok": True,
        "user_id": username,
        "device_id": device_ctx.get("device_id", ""),
        "server_time": _device_now_text(),
        "device_headers_echo": device_ctx,
    }
    _log_device_request(endpoint="/api/device/ping", username=username, request=request, status="ok")
    return payload


@app.get("/api/device/config", tags=["设备"])
def device_config(request: Request, username: str = Depends(get_current_user)):
    _ensure_device_voice_stock_name_cache_async()
    _ensure_device_voice_prompt_audio_cache_async()
    payload = _build_device_config_payload()
    _log_device_request(
        endpoint="/api/device/config",
        username=username,
        request=request,
        status="ok",
        detail=f"auto_poll={payload['auto_poll_enabled']}",
    )
    return payload


@app.get("/api/device/briefing", tags=["设备"])
def device_briefing(request: Request, username: str = Depends(get_current_user)):
    payload = _build_device_briefing_payload(username=username, request=request)
    _log_device_request(
        endpoint="/api/device/briefing",
        username=username,
        request=request,
        status=payload.get("data_freshness", "unknown"),
        detail=payload.get("market_state", ""),
    )
    return payload


@app.get("/api/device/contracts/menu", tags=["设备"])
def device_contracts_menu(
    request: Request,
    max_products: int = Query(default=12, ge=1, le=60),
    max_contracts: int = Query(default=6, ge=1, le=12),
    product: Optional[str] = Query(default=None, description="可选品种代码，如 pp；为空时只返回轻量商品菜单"),
    category: str = Query(default="futures", description="菜单分类：futures / etf / favorites"),
    username: str = Depends(get_current_user),
):
    payload = _build_device_contract_menu_payload(
        username=username,
        request=request,
        max_products=max_products,
        max_contracts=max_contracts,
        product=product,
        category=category,
    )
    _log_device_request(
        endpoint="/api/device/contracts/menu",
        username=username,
        request=request,
        status=payload.get("data_freshness", "unknown"),
        detail=f"products={len(payload.get('products') or [])}",
    )
    return payload


@app.get("/api/device/contracts/briefing", tags=["设备"])
def device_contract_briefing(
    request: Request,
    contract: str = Query(..., description="具体合约，如 PP2609"),
    category: str = Query(default="futures", description="看板分类：futures / etf / favorites"),
    username: str = Depends(get_current_user),
):
    payload = _build_device_contract_briefing_payload(
        username=username,
        request=request,
        contract=contract,
        category=category,
    )
    _log_device_request(
        endpoint="/api/device/contracts/briefing",
        username=username,
        request=request,
        status=payload.get("data_freshness", "unknown"),
        detail=payload.get("contract", ""),
    )
    return payload


@app.post("/api/device/voice/query", tags=["设备"])
def device_voice_query(
    request: Request,
    audio: UploadFile = File(..., description="16kHz mono 16-bit WAV，最长 8 秒"),
    contract: str = Form(default="", description="当前合约，如 PP2609 或 510300.SH"),
    category: str = Form(default="futures", description="看板分类：futures / etf / favorites"),
    screen_context: str = Form(default="", description="设备屏幕上下文"),
    conversation_id: str = Form(default="", description="设备端会话 ID"),
    client_audio_peak: str = Form(default="", description="设备端本地录音峰值，用于没听清原因分类"),
    client_audio_rms: str = Form(default="", description="设备端本地录音 RMS，用于没听清原因分类"),
    username: str = Depends(get_current_user),
):
    upload_started_at = time.perf_counter()
    audio_bytes = audio.file.read()
    initial_timings_ms: Dict[str, Any] = {}
    _record_timing(initial_timings_ms, "upload_read_ms", upload_started_at)
    payload = _build_device_voice_query_payload(
        username=username,
        request=request,
        audio_bytes=audio_bytes,
        contract=contract,
        category=category,
        screen_context=screen_context,
        conversation_id=conversation_id,
        client_audio_peak=client_audio_peak,
        client_audio_rms=client_audio_rms,
        initial_timings_ms=initial_timings_ms,
    )
    _log_device_request(
        endpoint="/api/device/voice/query",
        username=username,
        request=request,
        status=payload.get("data_freshness", "unknown"),
        detail=f"{payload.get('route_type', '')}:{payload.get('timings_ms', {}).get('server_total_ms', '-')}",
    )
    return payload


@app.get("/api/device/voice/audio/{voice_id}", tags=["设备"])
def device_voice_audio(
    voice_id: str,
    request: Request,
    username: str = Depends(get_current_user),
):
    audio_bytes = _get_device_voice_audio(voice_id)
    if not audio_bytes:
        _log_device_request(
            endpoint="/api/device/voice/audio",
            username=username,
            request=request,
            status="missing",
            detail=voice_id,
        )
        raise HTTPException(status_code=404, detail="音频已过期或不存在")
    _log_device_request(
        endpoint="/api/device/voice/audio",
        username=username,
        request=request,
        status="ok",
        detail=voice_id,
    )
    return Response(content=audio_bytes, media_type="audio/wav")


@app.get("/api/device/voice/audio-prompt/{prompt_key}", tags=["设备"])
def device_voice_audio_prompt(
    prompt_key: str,
    request: Request,
    username: str = Depends(get_current_user),
):
    prompt_text = _DEVICE_VOICE_PROMPT_TEXTS.get(_safe_textv(prompt_key))
    if not prompt_text:
        _log_device_request(
            endpoint="/api/device/voice/audio-prompt",
            username=username,
            request=request,
            status="missing",
            detail=prompt_key,
        )
        raise HTTPException(status_code=404, detail="提示音不存在")
    audio_url = _device_voice_audio_url_for_text(prompt_text)
    voice_id = audio_url.rsplit("/", 1)[-1] if audio_url else ""
    audio_bytes = _get_device_voice_audio(voice_id) if voice_id else b""
    if not audio_bytes:
        _log_device_request(
            endpoint="/api/device/voice/audio-prompt",
            username=username,
            request=request,
            status="tts_failed",
            detail=prompt_key,
        )
        raise HTTPException(status_code=503, detail="提示音生成失败")
    _log_device_request(
        endpoint="/api/device/voice/audio-prompt",
        username=username,
        request=request,
        status="ok",
        detail=prompt_key,
    )
    return Response(content=audio_bytes, media_type="audio/wav")


@app.get("/api/device/voice/task/{task_id}", tags=["设备"])
def device_voice_task(
    task_id: str,
    request: Request,
    username: str = Depends(get_current_user),
):
    payload = _build_device_voice_task_payload(
        username=username,
        request=request,
        task_id=task_id,
    )
    _log_device_request(
        endpoint="/api/device/voice/task",
        username=username,
        request=request,
        status=payload.get("status", "unknown"),
        detail=payload.get("task_id", ""),
    )
    return payload


@app.websocket("/api/device/voice/realtime")
async def device_voice_realtime(websocket: WebSocket):
    """StackChan V3 realtime bridge.

    v3-alpha accepts 16k mono pcm16 chunks over WebSocket and reuses the
    existing STT/routing/TTS pipeline when the client sends a stop event.
    Streaming STT/TTS can replace the finalization step later without changing
    the client-side event envelope.
    """
    try:
        username = _resolve_websocket_user(websocket)
    except HTTPException:
        await websocket.close(code=1008)
        return

    device_ctx = {
        "device_id": _safe_textv(websocket.headers.get("X-Device-Id")),
        "device_model": _safe_textv(websocket.headers.get("X-Device-Model")),
        "device_version": _safe_textv(websocket.headers.get("X-Device-Version")),
    }
    await websocket.accept()
    await websocket.send_json(
        {
            "type": "hello",
            "protocol": "tradingart.stackchan.voice.realtime",
            "version": "research-v1",
            "mode": "prototype",
            "user_id": username,
            "device_id": device_ctx.get("device_id", ""),
            "audio_format": {
                "codec": "pcm16",
                "sample_rate": _DEVICE_VOICE_SAMPLE_RATE,
                "channels": _DEVICE_VOICE_CHANNELS,
                "frame_ms": 60,
            },
            "capabilities": {
                "status_events": True,
                "binary_audio_frames": True,
                "json_audio_frames": True,
                "answer_delta": True,
                "tts_audio_delta": _DEVICE_VOICE_REALTIME_TTS_AUDIO_DELTA_ENABLED,
                "streaming_tts": _DEVICE_VOICE_REALTIME_TTS_AUDIO_DELTA_ENABLED,
                "barge_in": True,
                "followup_window_seconds": _DEVICE_VOICE_REALTIME_FOLLOWUP_WINDOW_SECONDS,
                "finalize_event": "stop",
                "http_fallback": "/api/device/voice/query",
            },
            "message": "实时语音通道已连接。",
        }
    )
    print(
        "[device_api] endpoint=/api/device/voice/realtime status=connected "
        f"user={username} device_id={device_ctx.get('device_id') or '-'} "
        f"model={device_ctx.get('device_model') or '-'} version={device_ctx.get('device_version') or '-'}",
        flush=True,
    )

    realtime_request = _request_from_websocket(websocket)
    session: Dict[str, Any] = {
        "pcm": bytearray(),
        "contract": "",
        "category": "futures",
        "screen_context": "",
        "conversation_id": "",
        "last_seq": 0,
        "started_at": time.perf_counter(),
        "asr": None,
    }

    def reset_session(payload: Optional[Dict[str, Any]] = None) -> None:
        payload = payload or {}
        previous_asr = session.get("asr")
        if previous_asr:
            try:
                previous_asr.stop()
            except Exception:
                pass
        session["pcm"] = bytearray()
        session["contract"] = _safe_textv(payload.get("contract"))
        session["category"] = _normalize_device_menu_category(payload.get("category"))
        session["screen_context"] = _safe_textv(payload.get("screen_context"))
        session["conversation_id"] = _safe_textv(payload.get("conversation_id"))
        session["last_seq"] = 0
        session["started_at"] = time.perf_counter()
        asr_session = _DeviceVoiceRealtimeAsrSession()
        session["asr"] = asr_session if asr_session.start() else None

    async def drain_realtime_asr_events() -> None:
        asr_session = session.get("asr")
        if not asr_session:
            return
        for event in asr_session.drain_events():
            if event.get("type") == "asr_error":
                print(f"[device_api] realtime_asr_error user={username} err={event.get('message')}", flush=True)
                continue
            await websocket.send_json(event)

    async def append_pcm_frame(frame: bytes, seq: int = 0) -> None:
        if not frame:
            await websocket.send_json({"type": "error", "code": "empty_audio_frame", "message": "空音频帧已忽略。"})
            return
        pcm: bytearray = session["pcm"]
        if len(pcm) + len(frame) > _DEVICE_VOICE_REALTIME_MAX_PCM_BYTES:
            await websocket.send_json(
                {
                    "type": "error",
                    "code": "audio_too_long",
                    "message": "音频超过设备实时通道最大长度，请重新开始。",
                }
            )
            return
        pcm.extend(frame)
        asr_session = session.get("asr")
        if asr_session:
            asr_session.send_audio_frame(frame)
            await drain_realtime_asr_events()
        if seq:
            session["last_seq"] = seq
        await websocket.send_json({"type": "audio_ack", "seq": seq or session["last_seq"], "bytes": len(pcm)})

    async def finalize_realtime_turn() -> None:
        pcm = bytes(session["pcm"])
        if not pcm:
            await websocket.send_json({"type": "error", "code": "empty_audio", "message": "没有收到音频。"})
            return
        await websocket.send_json({"type": "status", "state": "transcribing", "bytes": len(pcm)})
        asr_session = session.get("asr")
        transcript_override = ""
        if asr_session:
            asr_session.stop()
            await drain_realtime_asr_events()
            transcript_override = _safe_textv(asr_session.best_text())
        audio_bytes = _build_pcm_wav(pcm, sample_rate=_DEVICE_VOICE_SAMPLE_RATE)
        try:
            wav_info = _read_device_wav_info(audio_bytes)
            audio_stats = _device_wav_signal_stats(audio_bytes, wav_info)
            print(
                "[device_api] realtime_audio_stats "
                f"user={username} bytes={len(pcm)} duration_ms={audio_stats.get('duration_ms')} "
                f"peak={audio_stats.get('peak')} rms={audio_stats.get('rms')} "
                f"rt_asr={'yes' if transcript_override else 'no'}",
                flush=True,
            )
        except Exception as exc:
            print(f"[device_api] realtime_audio_stats_failed user={username} err={exc}", flush=True)
        await websocket.send_json({"type": "status", "state": "thinking"})
        payload = _build_device_voice_query_payload(
            username=username,
            request=realtime_request,
            audio_bytes=audio_bytes,
            contract=_safe_textv(session.get("contract")),
            category=_safe_textv(session.get("category"), "futures"),
            screen_context=_safe_textv(session.get("screen_context")),
            conversation_id=_safe_textv(session.get("conversation_id")),
            initial_timings_ms={
                "ws_buffer_ms": _perf_ms_since(float(session.get("started_at") or time.perf_counter())),
            },
            transcript_override=transcript_override,
        )
        await websocket.send_json(
            {
                "type": "result",
                "route_type": payload.get("route_type", ""),
                "action": payload.get("action", ""),
                "emotion": payload.get("emotion", ""),
                "transcript": payload.get("transcript", ""),
                "answer_text": payload.get("answer_text", ""),
                "task_id": payload.get("task_id", ""),
                "poll_after_seconds": payload.get("poll_after_seconds"),
                "timings_ms": payload.get("timings_ms", {}),
                "stt_status": payload.get("stt_status", ""),
                "stt_failure_reason": payload.get("stt_failure_reason", ""),
            }
        )
        answer_text = _safe_textv(payload.get("answer_text"))
        if payload.get("transcript"):
            await websocket.send_json({"type": "final_transcript", "text": payload.get("transcript", "")})
        if answer_text:
            await websocket.send_json({"type": "answer_delta", "text": answer_text, "is_final": True})
        audio_url = _safe_textv(payload.get("audio_url"))
        if audio_url:
            await websocket.send_json({"type": "status", "state": "speaking"})
            await websocket.send_json({"type": "audio_url", "url": audio_url})
            if _DEVICE_VOICE_REALTIME_TTS_AUDIO_DELTA_ENABLED:
                audio_id = _device_voice_audio_id_from_url(audio_url)
                cached_audio = _get_device_voice_audio(audio_id) if audio_id else b""
                pcm_payload, audio_meta = _device_voice_wav_pcm_payload(cached_audio or b"")
            else:
                pcm_payload, audio_meta = b"", {}
            if pcm_payload:
                chunk_size = _DEVICE_VOICE_REALTIME_TTS_CHUNK_BYTES
                chunk_total = int(math.ceil(len(pcm_payload) / float(chunk_size)))
                for chunk_index in range(chunk_total):
                    start = chunk_index * chunk_size
                    chunk = pcm_payload[start : start + chunk_size]
                    await websocket.send_json(
                        {
                            "type": "tts_audio_delta",
                            "seq": chunk_index + 1,
                            "total": chunk_total,
                            "audio_b64": base64.b64encode(chunk).decode("ascii"),
                            "is_final": chunk_index + 1 == chunk_total,
                            **audio_meta,
                        }
                    )
                    await websocket.send_bytes(chunk)
        if payload.get("action") == "thinking" and payload.get("task_id"):
            await websocket.send_json(
                {
                    "type": "task",
                    "task_id": payload.get("task_id"),
                    "poll_after_seconds": payload.get("poll_after_seconds"),
                    "task_max_wait_seconds": payload.get("task_max_wait_seconds"),
                }
            )
        await websocket.send_json(
            {
                "type": "done",
                "conversation_id": payload.get("conversation_id", ""),
                "followup_window_seconds": _DEVICE_VOICE_REALTIME_FOLLOWUP_WINDOW_SECONDS,
            }
        )
        await websocket.send_json(
            {
                "type": "status",
                "state": "followup_listening",
                "timeout_seconds": _DEVICE_VOICE_REALTIME_FOLLOWUP_WINDOW_SECONDS,
            }
        )
        session["pcm"] = bytearray()

    try:
        while True:
            message = await websocket.receive()
            if message.get("type") == "websocket.disconnect":
                break
            if "text" in message and message["text"] is not None:
                raw_text = _safe_textv(message.get("text"))
                try:
                    payload = json.loads(raw_text) if raw_text else {}
                except Exception:
                    payload = {"type": "text", "text": raw_text}
                message_type = _safe_textv(payload.get("type")).lower()
                if message_type == "ping":
                    await websocket.send_json({"type": "pong", "server_time": _device_now_text()})
                elif message_type == "start":
                    reset_session(payload)
                    await websocket.send_json({"type": "status", "state": "listening"})
                elif message_type in {"user_speaking", "speech_start"}:
                    await websocket.send_json({"type": "status", "state": "user_speaking"})
                elif message_type in {"cancel", "barge_in", "interrupt"}:
                    reset_session(payload)
                    await websocket.send_json({"type": "status", "state": "listening", "interrupted": True})
                elif message_type in {"audio", "audio_frame"}:
                    raw_b64 = _safe_textv(payload.get("pcm_b64") or payload.get("audio_b64") or payload.get("data_b64"))
                    try:
                        frame = base64.b64decode(raw_b64, validate=True) if raw_b64 else b""
                    except Exception:
                        await websocket.send_json({"type": "error", "code": "bad_audio_frame", "message": "音频帧不是有效 base64。"})
                        continue
                    await append_pcm_frame(frame, int(_safe_floatv(payload.get("seq"), default=0) or 0))
                elif message_type in {"stop", "finalize"}:
                    await finalize_realtime_turn()
                elif message_type in {"capabilities", "hello"}:
                    await websocket.send_json(
                        {
                            "type": "capabilities",
                            "status_events": True,
                            "binary_audio_frames": True,
                            "json_audio_frames": True,
                            "answer_delta": True,
                            "tts_audio_delta": _DEVICE_VOICE_REALTIME_TTS_AUDIO_DELTA_ENABLED,
                            "streaming_tts": _DEVICE_VOICE_REALTIME_TTS_AUDIO_DELTA_ENABLED,
                            "barge_in": True,
                            "followup_window_seconds": _DEVICE_VOICE_REALTIME_FOLLOWUP_WINDOW_SECONDS,
                            "finalize_event": "stop",
                            "http_fallback": "/api/device/voice/query",
                        }
                    )
                else:
                    await websocket.send_json(
                        {
                            "type": "status",
                            "status": "prototype",
                            "message": "当前实时通道只用于协议研究，请继续使用 HTTP 语音接口。",
                        }
                    )
            elif "bytes" in message and message["bytes"] is not None:
                await append_pcm_frame(bytes(message["bytes"]), int(session.get("last_seq") or 0) + 1)
    except WebSocketDisconnect:
        print(
            "[device_api] endpoint=/api/device/voice/realtime status=disconnected "
            f"user={username} device_id={device_ctx.get('device_id') or '-'}",
            flush=True,
        )


# ════════════════════════════════════════════════════════════
#  CHAT — AI 问答
# ════════════════════════════════════════════════════════════

@app.post("/api/chat/submit", tags=["AI问答"])
def chat_submit(
    body: ChatSubmitRequest,
    username: str = Depends(get_current_user),
):
    """
    提交 AI 分析问答任务。
    返回 task_id，客户端每隔 2-3 秒轮询 /api/chat/status/{task_id}。
    """
    profile = de.get_user_profile(username) or {}
    risk = profile.get("risk_preference", "稳健型")

    raw_prompt = str(body.prompt or "").strip()
    normalized_prompt = _build_mobile_chat_prompt(raw_prompt)
    history_for_task = _normalize_mobile_history(body.history, max_turns=4)
    context_payload = _build_mobile_context_payload(
        prompt_text=normalized_prompt,
        current_user=username,
        history=history_for_task,
        profile=profile,
    )
    trace_id = _generate_chat_trace_id()
    answer_id = _generate_chat_answer_id()
    intent_domain = str(context_payload.get("intent_domain") or "general")
    if bool(context_payload.get("profile_memory_should_short_circuit", False)):
        chat_mode = CHAT_MODE_SIMPLE
    elif bool(context_payload.get("conversation_memory_query", False)):
        chat_mode = CHAT_MODE_SIMPLE
    elif context_payload.get("followup_anchor_ambiguous") and context_payload.get("followup_anchor_clarify"):
        response_text = str(context_payload.get("followup_anchor_clarify") or "").strip()
        feedback_allowed = _save_chat_answer_event(
            task_id=f"immediate-{uuid.uuid4()}",
            user_id=username,
            trace_id=trace_id,
            answer_id=answer_id,
            prompt_text=raw_prompt or normalized_prompt,
            response_text=response_text,
            intent_domain=intent_domain,
            feedback_allowed=True,
        )
        _queue_mobile_chat_memory_persist(
            task_id=f"immediate-memory-{uuid.uuid4()}",
            username=username,
            user_prompt=raw_prompt or normalized_prompt,
            ai_response=response_text,
        )
        return {
            "delivery_mode": "immediate",
            "task_id": "",
            "message": "需要确认追问对象",
            "chat_mode": CHAT_MODE_SIMPLE,
            "trace_id": trace_id,
            "answer_id": answer_id,
            "feedback_allowed": feedback_allowed,
            "result": {
                "status": "success",
                "response": response_text,
            },
        }
    else:
        chat_mode = classify_chat_mode(
            normalized_prompt,
            is_followup=bool(context_payload.get("is_followup", False)),
            recent_context=str(context_payload.get("recent_context") or ""),
            focus_entity=str(context_payload.get("focus_entity") or ""),
            focus_topic=str(context_payload.get("focus_topic") or ""),
            focus_aspect=str(context_payload.get("focus_aspect") or ""),
            focus_mode_hint=str(context_payload.get("focus_mode_hint") or ""),
            followup_goal=str(context_payload.get("followup_goal") or ""),
            followup_task_policy=context_payload.get("followup_task_policy") or {},
            correction_intent=bool(context_payload.get("correction_intent", False)),
        )
    context_payload["chat_mode"] = chat_mode
    context_payload = attach_context_layers(context_payload, prompt_text=normalized_prompt, channel="mobile")

    if bool(context_payload.get("profile_memory_should_short_circuit", False)):
        response_text = str(context_payload.get("profile_memory_confirmation") or "好，我记住了。")
        feedback_allowed = _save_chat_answer_event(
            task_id=f"immediate-{uuid.uuid4()}",
            user_id=username,
            trace_id=trace_id,
            answer_id=answer_id,
            prompt_text=raw_prompt or normalized_prompt,
            response_text=response_text,
            intent_domain=intent_domain,
            feedback_allowed=False,
        )
        return {
            "delivery_mode": "immediate",
            "task_id": "",
            "message": "已更新记忆",
            "chat_mode": CHAT_MODE_SIMPLE,
            "trace_id": trace_id,
            "answer_id": answer_id,
            "feedback_allowed": feedback_allowed,
            "result": {
                "status": "success",
                "response": response_text,
                "chart": None,
                "attachments": [],
                "error": None,
            },
        }

    freshness_candidate = _is_freshness_sensitive_query(
        normalized_prompt,
        chat_mode=chat_mode,
        context_payload=context_payload,
    )
    if freshness_candidate:
        context_payload["freshness_required"] = True
        context_payload["freshness_query_target"] = _extract_freshness_query_target(
            normalized_prompt,
            context_payload,
        )
        context_payload["freshness_quick_status"] = "pending"
        if chat_mode == CHAT_MODE_SIMPLE:
            chat_mode = CHAT_MODE_KNOWLEDGE
            context_payload["chat_mode"] = chat_mode

    has_portfolio = _detect_mobile_has_portfolio(username)

    if chat_mode == CHAT_MODE_SIMPLE:
        try:
            llm_turbo = build_deepseek_flash_llm(streaming=False, temperature=0.2)
            runtime_context = _build_mobile_simple_runtime_context(username)
            response_text = simple_chatter_reply(
                normalized_prompt,
                llm_turbo,
                recent_context=str(context_payload.get("recent_context") or ""),
                memory_context=str(context_payload.get("memory_context") or ""),
                profile_context=str(context_payload.get("profile_context") or ""),
                is_followup=bool(context_payload.get("is_followup", False)),
                focus_entity=str(context_payload.get("focus_entity") or ""),
                focus_topic=str(context_payload.get("focus_topic") or ""),
                focus_aspect=str(context_payload.get("focus_aspect") or ""),
                conversation_memory_query=bool(context_payload.get("conversation_memory_query", False)),
                conversation_memory_label=str(context_payload.get("conversation_memory_label") or ""),
                runtime_context=runtime_context,
                context_payload=context_payload,
            )
        except Exception as exc:
            response_text = f"AI闲聊服务暂时不可用: {exc}"
            print(f"[mobile-chat] simple_chat_failed user={username} err={exc}", flush=True)
            feedback_allowed = _save_chat_answer_event(
                task_id=f"immediate-{uuid.uuid4()}",
                user_id=username,
                trace_id=trace_id,
                answer_id=answer_id,
                prompt_text=raw_prompt or normalized_prompt,
                response_text=response_text,
                intent_domain=intent_domain,
                feedback_allowed=False,
            )
            return {
                "delivery_mode": "immediate",
                "task_id": "",
                "message": "AI闲聊服务暂不可用",
                "chat_mode": CHAT_MODE_SIMPLE,
                "trace_id": trace_id,
                "answer_id": answer_id,
                "feedback_allowed": feedback_allowed,
                "result": {
                    "status": "error",
                    "response": response_text,
                    "chart": None,
                    "attachments": [],
                    "error": response_text,
                },
            }
        feedback_allowed = _save_chat_answer_event(
            task_id=f"immediate-{uuid.uuid4()}",
            user_id=username,
            trace_id=trace_id,
            answer_id=answer_id,
            prompt_text=raw_prompt or normalized_prompt,
            response_text=response_text,
            intent_domain=intent_domain,
            feedback_allowed=True,
        )
        _queue_mobile_chat_memory_persist(
            task_id=f"immediate-memory-{uuid.uuid4()}",
            username=username,
            user_prompt=raw_prompt or normalized_prompt,
            ai_response=response_text,
        )
        return {
            "delivery_mode": "immediate",
            "task_id": "",
            "message": "已直接回复",
            "chat_mode": CHAT_MODE_SIMPLE,
            "trace_id": trace_id,
            "answer_id": answer_id,
            "feedback_allowed": feedback_allowed,
            "result": {
                "status": "success",
                "response": response_text,
                "chart": None,
                "attachments": [],
                "error": None,
            },
        }

    market_hybrid_candidate = _is_market_move_quick_answer_candidate(
        normalized_prompt,
        chat_mode,
        context_payload,
    )
    freshness_hybrid_candidate = bool(context_payload.get("freshness_required")) and not market_hybrid_candidate
    hybrid_candidate = market_hybrid_candidate or freshness_hybrid_candidate
    if market_hybrid_candidate:
        context_payload["delivery_mode"] = "hybrid"
        context_payload["quick_answer_scenario"] = _detect_hybrid_quick_scenario(normalized_prompt)
        context_payload["quick_answer_target"] = _extract_market_move_target(normalized_prompt, context_payload)
        context_payload["quick_answer_direction"] = _extract_market_move_direction(normalized_prompt)
    elif freshness_hybrid_candidate:
        context_payload["delivery_mode"] = "hybrid"
        context_payload["quick_answer_scenario"] = "freshness"
        context_payload["quick_answer_target"] = str(context_payload.get("freshness_query_target") or "")

    try:
        if chat_mode == CHAT_MODE_KNOWLEDGE:
            task_id = TaskManager.create_knowledge_task(
                user_id=username,
                prompt=normalized_prompt,
                risk_preference=risk,
                history_messages=history_for_task,
                context_payload=context_payload,
            )
        else:
            task_id = TaskManager.create_task(
                user_id=username,
                prompt=normalized_prompt,
                risk_preference=risk,
                history_messages=history_for_task,
                context_payload=context_payload,
                has_portfolio=has_portfolio,
            )
    except UserTaskQueueFullError as e:
        raise HTTPException(
            status_code=429,
            detail=f"你前面已有 {e.active_count} 个处理中、{e.queued_count} 个排队问题，请等待结果后再继续提问。",
        )

    task_meta = TaskManager.get_task_meta(task_id)
    task_state = str(task_meta.get("status") or "pending").strip().lower()
    progress_text = str(task_meta.get("progress") or default_progress_for_chat_mode(chat_mode, status="pending"))
    if task_state == "queued":
        queue_ahead = 0
        for meta in TaskManager.get_user_task_queue(username):
            if str(meta.get("task_id") or "").strip() == task_id:
                queue_ahead = int(meta.get("queue_ahead") or 0)
                break
        progress_text = f"排队中，前面还有 {queue_ahead} 个问题" if queue_ahead > 0 else "排队中，等待开始处理..."

    delivery_mode = "hybrid" if hybrid_candidate else "task"
    quick_response = ""
    quick_source = ""
    quick_elapsed_ms = 0
    quick_freshness_status = ""
    quick_as_of = ""
    if market_hybrid_candidate:
        quick_response, quick_source, quick_elapsed_ms = _generate_market_move_quick_answer(
            normalized_prompt,
            context_payload,
        )
    elif freshness_hybrid_candidate:
        (
            quick_response,
            quick_source,
            quick_elapsed_ms,
            quick_freshness_status,
            quick_as_of,
        ) = _generate_freshness_quick_answer(normalized_prompt, context_payload)
        context_payload["freshness_quick_status"] = quick_freshness_status
    if hybrid_candidate:
        print(
            "[mobile-chat] hybrid_quick "
            f"trace_id={trace_id} task_id={task_id} chat_mode={chat_mode} "
            f"quick_source={quick_source} quick_ms={quick_elapsed_ms} "
            f"freshness_status={quick_freshness_status or '-'} "
            f"background_status={task_state}",
            flush=True,
        )

    _write_mobile_chat_state(
        task_id=task_id,
        user_id=username,
        status=task_state or "pending",
        error="",
        finished=False,
        extra_fields={
            "trace_id": trace_id,
            "answer_id": answer_id,
            "prompt_text": raw_prompt or normalized_prompt,
            "intent_domain": intent_domain,
            "feedback_allowed": False,
            "chat_mode": chat_mode,
            "progress": progress_text,
            "delivery_mode": delivery_mode,
            "quick_answer_source": quick_source,
            "quick_answer_ms": quick_elapsed_ms,
            "quick_answer_freshness_status": quick_freshness_status,
            "quick_answer_as_of": quick_as_of,
        },
    )
    _set_mobile_chat_last_task(username, task_id)
    try:
        if raw_prompt:
            _redis.setex(_mobile_chat_prompt_key(task_id), _MOBILE_CHAT_PROMPT_TTL, raw_prompt)
    except Exception as e:
        print(f"[mobile-memory] prompt cache failed task_id={task_id} err={e}")
    if task_state == "queued":
        submit_message = progress_text
    else:
        submit_message = "任务已提交，正在整理知识回答..." if chat_mode == CHAT_MODE_KNOWLEDGE else "任务已提交，正在分析..."
    if hybrid_candidate:
        quick_result = {
            "status": "success",
            "response": quick_response,
            "source": quick_source or "template",
        }
        if quick_freshness_status:
            quick_result["freshness_status"] = quick_freshness_status
        if quick_as_of:
            quick_result["as_of"] = quick_as_of
        return {
            "delivery_mode": "hybrid",
            "task_id": task_id,
            "message": "已先给快速判断，后台继续深度分析...",
            "chat_mode": chat_mode,
            "trace_id": trace_id,
            "answer_id": answer_id,
            "quick_result": quick_result,
            "background": {
                "status": task_state or "pending",
                "progress": progress_text,
            },
        }
    return {
        "delivery_mode": "task",
        "task_id": task_id,
        "message": submit_message,
        "chat_mode": chat_mode,
    }


@app.get("/api/chat/status/{task_id}", tags=["AI问答"])
def chat_status(task_id: str, username: str = Depends(get_current_user)):
    """
    轮询 AI 任务状态。
    status 值: pending | processing | success | error
    """
    runtime_snapshot = _build_mobile_chat_runtime_snapshot(task_id=task_id, username=username)
    if runtime_snapshot:
        status_name = str(runtime_snapshot.get("status") or "").strip().lower()
        if status_name == "success":
            prompt_key = _mobile_chat_prompt_key(task_id)
            user_prompt = ""
            try:
                user_prompt = str(_redis.get(prompt_key) or "").strip()
            except Exception as e:
                print(f"[mobile-memory] prompt load failed task_id={task_id} err={e}")

            ai_response = ""
            result_payload = runtime_snapshot.get("result")
            if isinstance(result_payload, dict):
                ai_response = str(result_payload.get("response") or "").strip()

            queue_status = _queue_mobile_chat_memory_persist(
                task_id=task_id,
                username=username,
                user_prompt=user_prompt,
                ai_response=ai_response,
            )
            if queue_status in {"queued", "already_queued", "invalid_payload"}:
                try:
                    _redis.delete(prompt_key)
                except Exception as e:
                    print(f"[mobile-memory] prompt cleanup failed task_id={task_id} err={e}")

            TaskManager.complete_user_task(username, task_id)
            _clear_mobile_chat_last_task_if_matches(username, task_id)
            return runtime_snapshot
        elif status_name == "error":
            TaskManager.complete_user_task(username, task_id)
            _clear_mobile_chat_last_task_if_matches(username, task_id)
            return runtime_snapshot

    status = TaskManager.get_task_status(task_id)
    status_name = str(status.get("status") or "").strip().lower()
    if status_name == "success":
        prompt_key = _mobile_chat_prompt_key(task_id)
        user_prompt = ""
        try:
            user_prompt = str(_redis.get(prompt_key) or "").strip()
        except Exception as e:
            print(f"[mobile-memory] prompt load failed task_id={task_id} err={e}")

        ai_response = ""
        result = status.get("result")
        if isinstance(result, dict):
            ai_response = str(result.get("response") or "").strip()
            _write_mobile_chat_result(task_id=task_id, user_id=username, result_payload=result)
            existing_state = _read_mobile_chat_state(task_id)
            _write_mobile_chat_state(
                task_id=task_id,
                user_id=username,
                status="success",
                error="",
                finished=True,
                extra_fields={
                    "feedback_allowed": True,
                    "trace_id": str(existing_state.get("trace_id") or "").strip(),
                    "answer_id": str(existing_state.get("answer_id") or "").strip(),
                    "prompt_text": str(existing_state.get("prompt_text") or "").strip(),
                    "intent_domain": str(existing_state.get("intent_domain") or "general").strip(),
                    "chat_mode": str(existing_state.get("chat_mode") or status.get("chat_mode") or CHAT_MODE_ANALYSIS),
                },
            )

        queue_status = _queue_mobile_chat_memory_persist(
            task_id=task_id,
            username=username,
            user_prompt=user_prompt,
            ai_response=ai_response,
        )
        if queue_status in {"queued", "already_queued", "invalid_payload"}:
            try:
                _redis.delete(prompt_key)
            except Exception as e:
                print(f"[mobile-memory] prompt cleanup failed task_id={task_id} err={e}")
        TaskManager.complete_user_task(username, task_id)
        _clear_mobile_chat_last_task_if_matches(username, task_id)
    elif status_name == "error":
        err_msg = str(status.get("error") or "分析失败，请稍后重试。")
        _write_mobile_chat_state(task_id=task_id, user_id=username, status="error", error=err_msg, finished=True)
        TaskManager.complete_user_task(username, task_id)
        _clear_mobile_chat_last_task_if_matches(username, task_id)
    elif status_name in {"pending", "processing"}:
        # 兼容旧任务（尚未写入 state），根据用户 pending 元信息做超时兜底
        pending_meta = TaskManager.get_user_pending_task(username) or {}
        start_ts = float(pending_meta.get("start_time") or 0.0)
        state_for_timeout = _read_mobile_chat_state(task_id)
        if str(status.get("delivery_mode") or "").strip() and not str(state_for_timeout.get("delivery_mode") or "").strip():
            state_for_timeout = {
                **state_for_timeout,
                "delivery_mode": str(status.get("delivery_mode") or "").strip(),
            }
        timeout_seconds = _mobile_chat_timeout_seconds_for_state(state_for_timeout)
        if start_ts > 0 and (time.time() - start_ts) >= timeout_seconds:
            timeout_msg = _mobile_chat_timeout_error_for_state(state_for_timeout)
            _write_mobile_chat_state(
                task_id=task_id,
                user_id=username,
                status="timeout",
                error=timeout_msg,
                finished=True,
            )
            TaskManager.complete_user_task(username, task_id)
            _clear_mobile_chat_last_task_if_matches(username, task_id)
            return _build_mobile_chat_error_response(timeout_msg, code="task_timeout", state=state_for_timeout)
    if status_name == "success":
        refreshed = _build_mobile_chat_runtime_snapshot(task_id=task_id, username=username)
        if refreshed:
            return refreshed
    elif runtime_snapshot and str(runtime_snapshot.get("status") or "").strip().lower() in {"pending", "processing"}:
        return runtime_snapshot
    return status


@app.get("/api/chat/pending", tags=["AI问答"])
def chat_pending(username: str = Depends(get_current_user)):
    """
    返回当前用户最近一条聊天任务的恢复态。
    """
    task_id = _get_mobile_chat_last_task(username)
    if not task_id:
        pending_meta = TaskManager.get_user_pending_task(username) or {}
        task_id = str(pending_meta.get("task_id") or "").strip()
    if not task_id:
        return {"has_task": False}

    snapshot = _build_mobile_chat_runtime_snapshot(task_id=task_id, username=username)
    if not snapshot:
        snapshot = TaskManager.get_task_status(task_id)

    status_name = str(snapshot.get("status") or "").strip().lower()
    result_payload = snapshot.get("result") if isinstance(snapshot.get("result"), dict) else None
    err_msg = str(snapshot.get("error") or "").strip()

    state = _read_mobile_chat_state(task_id)
    updated_at = str(state.get("updated_at") or state.get("created_at") or "")
    if not updated_at:
        updated_at = datetime.now().isoformat()

    # 终态任务只回传一次，避免每次 onShow 重复回放
    if status_name in {"success", "error", "canceled", "timeout"}:
        TaskManager.complete_user_task(username, task_id)
        _clear_mobile_chat_last_task_if_matches(username, task_id)

    payload = {
        "has_task": True,
        "task_id": task_id,
        "status": status_name or "pending",
        "updated_at": updated_at,
    }
    chat_mode = str(snapshot.get("chat_mode") or state.get("chat_mode") or "").strip()
    if chat_mode:
        payload["chat_mode"] = chat_mode
    delivery_mode = str(snapshot.get("delivery_mode") or state.get("delivery_mode") or "").strip()
    if delivery_mode:
        payload["delivery_mode"] = delivery_mode
    if result_payload is not None:
        payload["result"] = result_payload
    if err_msg:
        payload["error"] = err_msg
    return payload


@app.post("/api/chat/cancel", tags=["AI问答"])
def chat_cancel(
    body: ChatCancelRequest,
    username: str = Depends(get_current_user),
):
    """
    取消当前用户聊天任务（用于清空对话、超时兜底）。
    """
    task_id = str(body.task_id or "").strip()
    if not task_id:
        task_id = _get_mobile_chat_last_task(username)
    if not task_id:
        pending_meta = TaskManager.get_user_pending_task(username) or {}
        task_id = str(pending_meta.get("task_id") or "").strip()

    if not task_id:
        return {"status": "ok", "message": "无可取消任务"}

    state = _read_mobile_chat_state(task_id)
    owner = str(state.get("user_id") or "").strip()
    if owner and owner != username:
        raise HTTPException(status_code=403, detail="无权限取消该任务")

    reason = str(body.reason or "manual").strip().lower()
    if reason not in {"clear", "timeout", "manual"}:
        reason = "manual"

    try:
        from celery.result import AsyncResult
        from tasks import process_ai_query

        ar = AsyncResult(task_id, app=process_ai_query.app)
        ar.revoke(terminate=False)
    except Exception as e:
        print(f"[mobile-chat] cancel revoke failed task_id={task_id} err={e}")

    err_msg = "任务已取消。"
    if reason == "clear":
        err_msg = "已清空并取消当前任务。"
    elif reason == "timeout":
        err_msg = _mobile_chat_timeout_error_for_state(state)

    _write_mobile_chat_state(
        task_id=task_id,
        user_id=username,
        status="canceled" if reason != "timeout" else "timeout",
        error=err_msg,
        finished=True,
    )
    TaskManager.remove_user_task(username, task_id)
    _clear_mobile_chat_last_task_if_matches(username, task_id)
    try:
        _redis.delete(_mobile_chat_prompt_key(task_id))
    except Exception:
        pass

    return {"status": "ok", "task_id": task_id, "message": err_msg}


@app.post("/api/chat/feedback", tags=["AI问答"])
def chat_feedback(
    body: ChatFeedbackRequest,
    username: str = Depends(get_current_user),
):
    answer_id = str(body.answer_id or "").strip()
    trace_id = str(body.trace_id or "").strip()
    if not answer_id or not trace_id:
        raise HTTPException(status_code=400, detail="trace_id and answer_id are required")

    result = submit_chat_feedback(
        _get_chat_feedback_engine(),
        answer_id=answer_id,
        trace_id=trace_id,
        user_id=username,
        feedback_type=str(body.feedback_type or ""),
        reason_code=str(body.reason_code or ""),
        feedback_text=str(body.feedback_text or "").strip(),
    )
    code = str(result.get("code") or "")
    if code == "answer_not_found":
        raise HTTPException(status_code=404, detail="answer not found")
    if code == "forbidden":
        raise HTTPException(status_code=403, detail="no permission to rate this answer")
    if code == "trace_mismatch":
        raise HTTPException(status_code=400, detail="trace_id does not match answer")
    if code == "unsupported_feedback_type":
        raise HTTPException(status_code=400, detail="unsupported feedback_type")
    if code == "invalid_reason_code":
        raise HTTPException(status_code=400, detail="invalid reason_code")
    if code != "ok":
        raise HTTPException(status_code=500, detail="failed to save feedback")

    return {"status": "ok", "message": "feedback received"}


@app.get("/api/chat/feedback/failure-candidates", tags=["AI问答"])
def chat_feedback_failure_candidates(
    limit: int = Query(20, ge=1, le=100),
    intent_domain: str = Query(""),
    reason_code: str = Query(""),
    keyword: str = Query(""),
    start_at: str = Query(""),
    end_at: str = Query(""),
    min_occurrence: int = Query(1, ge=1, le=20),
    username: str = Depends(get_current_user),
):
    _ensure_chat_feedback_admin(username)
    items = _list_chat_feedback_failure_candidates(
        limit=limit,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
        start_at=start_at,
        end_at=end_at,
        min_occurrence=min_occurrence,
    )
    return {
        "status": "ok",
        "items": items,
        "count": len(items),
    }


@app.get("/api/chat/feedback/events", tags=["AI问答"])
def chat_feedback_events(
    limit: int = Query(100, ge=1, le=200),
    feedback_type: str = Query(""),
    answer_id: str = Query(""),
    user_id: str = Query(""),
    intent_domain: str = Query(""),
    reason_code: str = Query(""),
    keyword: str = Query(""),
    start_at: str = Query(""),
    end_at: str = Query(""),
    username: str = Depends(get_current_user),
):
    _ensure_chat_feedback_admin(username)
    items = _list_chat_feedback_events(
        limit=limit,
        feedback_type=feedback_type,
        answer_id=answer_id,
        user_id=user_id,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
        start_at=start_at,
        end_at=end_at,
    )
    return {
        "status": "ok",
        "items": items,
        "count": len(items),
    }


@app.get("/api/chat/feedback/samples", tags=["AI问答"])
def chat_feedback_samples(
    limit: int = Query(100, ge=1, le=200),
    sample_status: str = Query(""),
    optimization_type: str = Query(""),
    intent_domain: str = Query(""),
    reason_code: str = Query(""),
    keyword: str = Query(""),
    username: str = Depends(get_current_user),
):
    _ensure_chat_feedback_admin(username)
    items = _list_chat_feedback_samples(
        limit=limit,
        sample_status=sample_status,
        optimization_type=optimization_type,
        intent_domain=intent_domain,
        reason_code=reason_code,
        keyword=keyword,
    )
    return {
        "status": "ok",
        "items": items,
        "count": len(items),
    }


@app.post("/api/chat/feedback/samples", tags=["AI问答"])
def chat_feedback_sample_create(
    body: ChatFeedbackSampleCreateRequest,
    username: str = Depends(get_current_user),
):
    _ensure_chat_feedback_admin(username)
    result = _upsert_chat_feedback_sample(
        prompt_text=str(body.prompt_text or "").strip(),
        reason_code=str(body.reason_code or "").strip(),
        intent_domain=str(body.intent_domain or "general").strip(),
        occurrence_count=int(body.occurrence_count or 1),
        latest_feedback_at=str(body.latest_feedback_at or "").strip(),
        latest_feedback_text=str(body.latest_feedback_text or "").strip(),
        sample_answer_id=str(body.sample_answer_id or "").strip(),
        sample_trace_id=str(body.sample_trace_id or "").strip(),
        sample_response_text=str(body.sample_response_text or "").strip(),
        created_by=username,
        sample_status=str(body.sample_status or "new").strip(),
        optimization_type=str(body.optimization_type or "").strip(),
        review_notes=str(body.review_notes or "").strip(),
    )
    code = str(result.get("code") or "")
    if code == "invalid_candidate":
        raise HTTPException(status_code=400, detail="invalid candidate payload")
    if code != "created" and code != "updated":
        raise HTTPException(status_code=500, detail="failed to save feedback sample")
    return {
        "status": "ok",
        "code": code,
        "sample": result.get("sample") or {},
    }


@app.post("/api/chat/feedback/samples/update", tags=["AI问答"])
def chat_feedback_sample_update(
    body: ChatFeedbackSampleUpdateRequest,
    username: str = Depends(get_current_user),
):
    _ensure_chat_feedback_admin(username)
    result = _update_chat_feedback_sample(
        sample_key=str(body.sample_key or "").strip(),
        sample_status=body.sample_status,
        optimization_type=body.optimization_type,
        review_notes=body.review_notes,
        reviewed_by=username,
    )
    code = str(result.get("code") or "")
    if code == "sample_key_required":
        raise HTTPException(status_code=400, detail="sample_key is required")
    if code == "sample_not_found":
        raise HTTPException(status_code=404, detail="sample not found")
    if code != "ok":
        raise HTTPException(status_code=500, detail="failed to update feedback sample")
    return {
        "status": "ok",
        "sample": result.get("sample") or {},
    }


# ════════════════════════════════════════════════════════════
#  INTEL — 情报站晚报
# ════════════════════════════════════════════════════════════

# 可以自助订阅的频道 code 白名单（默认空，环境变量灰度开启）
# 示例: FREE_SELF_SUBSCRIBE_CHANNEL_CODES=daily_report,expiry_option_radar
_FREE_CHANNEL_CODES = {
    item.strip().lower()
    for item in str(os.getenv("FREE_SELF_SUBSCRIBE_CHANNEL_CODES", "")).split(",")
    if item.strip()
}
_FORCE_PAID_CHANNEL_CODES = {
    "daily_report",
    "expiry_option_radar",
    "broker_position_report",
    "fund_flow_report",
    "macro_risk_radar",
    "safe_stock_report",
}
_EFFECTIVE_FREE_CHANNEL_CODES = _FREE_CHANNEL_CODES - _FORCE_PAID_CHANNEL_CODES
_INTEL_SELF_SUBSCRIBE_API_ENABLED = (
    str(os.getenv("INTEL_SELF_SUBSCRIBE_API_ENABLED", "false")).strip().lower()
    in {"1", "true", "on", "yes"}
)
_ALLOW_SELF_SUB_IN_PROD = (
    str(os.getenv("INTEL_SELF_SUBSCRIBE_ALLOW_IN_PROD", "false")).strip().lower()
    in {"1", "true", "on", "yes"}
)
_INTEL_CHANNEL_CODE_ALIASES = {
    # 历史小程序包曾使用该 code，兼容到后端真实频道码
    "expiry_option_report": "expiry_option_radar",
}

_SAFE_STOCK_HEADER_KEY_MAP = {
    "排名": "rank",
    "板块": "sector",
    "类型": "type",
    "分数": "score",
    "资金改善": "money_improve",
    "流入天数": "inflow_days",
    "近窗涨幅": "recent_change",
    "代码": "symbol",
    "名称": "name",
    "板块排名": "sector_rank",
    "价格": "price",
    "信号/说明": "note",
    "当前状态": "status",
    "已持有天数": "hold_days",
    "今日操作": "action",
    "收益": "return_pct",
    "原因": "reason",
}

_BROKER_POSITION_HEADER_KEY_MAP = {
    "品种": "product",
    "合计": "total",
    "明细": "details",
    "合计净多": "total",
    "合计净空": "total",
    "潜在信号": "signal",
}


def _normalize_intel_channel_code(code: Optional[str]) -> Optional[str]:
    raw = str(code or "").strip().lower()
    if not raw:
        return None
    return _INTEL_CHANNEL_CODE_ALIASES.get(raw, raw)


def _extract_published_at(payload: dict) -> str:
    # 兼容 subscription_service 的 publish_time 与旧字段 published_at
    ts = payload.get("publish_time")
    if ts is None:
        ts = payload.get("published_at", "")
    return str(ts or "")


def _plain_from_html_fragment(raw: Any) -> str:
    text_value = str(raw or "")
    if not text_value:
        return ""
    text_value = re.sub(r"<style[\s\S]*?</style>", "", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"<script[\s\S]*?</script>", "", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"<br\s*/?>", "\n", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"<[^>]+>", " ", text_value)
    text_value = html_lib.unescape(text_value)
    return re.sub(r"[ \t\r\f\v]+", " ", text_value).strip()


def _extract_first_html_text(pattern: str, html_text: str) -> str:
    match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
    return _plain_from_html_fragment(match.group(1)) if match else ""


def _parse_html_table_rows(table_html: str, key_map: Optional[Dict[str, str]] = None) -> List[Dict[str, str]]:
    headers = [
        _plain_from_html_fragment(cell)
        for cell in re.findall(r"<th[^>]*>([\s\S]*?)</th>", table_html, flags=re.IGNORECASE)
    ]
    lookup = key_map or {}
    keys = [lookup.get(h, h) for h in headers]
    rows: List[Dict[str, str]] = []
    for tr_html in re.findall(r"<tr[^>]*>([\s\S]*?)</tr>", table_html, flags=re.IGNORECASE):
        cells = re.findall(r"<td[^>]*>([\s\S]*?)</td>", tr_html, flags=re.IGNORECASE)
        if not cells:
            continue
        item: Dict[str, str] = {}
        for idx, cell in enumerate(cells):
            key = keys[idx] if idx < len(keys) else f"col_{idx}"
            item[str(key)] = _plain_from_html_fragment(cell)
        rows.append(item)
    return rows


def _parse_safe_stock_table_rows(table_html: str) -> List[Dict[str, str]]:
    return _parse_html_table_rows(table_html, _SAFE_STOCK_HEADER_KEY_MAP)


def _extract_tables_from_html(html_text: str) -> List[str]:
    return re.findall(r"<table[^>]*>([\s\S]*?)</table>", html_text or "", flags=re.IGNORECASE)


def _extract_safe_stock_section_rows(html_text: str, section_title: str) -> List[Dict[str, str]]:
    section_match = re.search(
        rf"<h2[^>]*>\s*{re.escape(section_title)}\s*</h2>([\s\S]*?)(?=<h2[^>]*>|<div\s+class=['\"]risk['\"]|</div>\s*</body>|</body>|$)",
        html_text,
        flags=re.IGNORECASE,
    )
    if not section_match:
        return []
    table_match = re.search(r"<table[^>]*>([\s\S]*?)</table>", section_match.group(1), flags=re.IGNORECASE)
    if not table_match:
        return []
    return _parse_safe_stock_table_rows(table_match.group(1))


def _extract_h2_section_html(html_text: str, section_title: str) -> str:
    for match in re.finditer(r"<h2[^>]*>([\s\S]*?)</h2>", html_text or "", flags=re.IGNORECASE):
        title = _plain_from_html_fragment(match.group(1))
        if section_title not in title:
            continue
        start = match.end()
        next_h2 = re.search(r"<h2[^>]*>", html_text[start:], flags=re.IGNORECASE)
        end = start + next_h2.start() if next_h2 else len(html_text)
        return html_text[start:end]
    return ""


def _split_plain_lines(raw: str) -> List[str]:
    return [
        line.strip(" \t\r\n-•")
        for line in str(raw or "").splitlines()
        if line.strip(" \t\r\n-•")
    ]


def _plain_block_from_html(raw: Any) -> str:
    text_value = str(raw or "")
    if not text_value:
        return ""
    text_value = re.sub(r"<style[\s\S]*?</style>", "", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"<script[\s\S]*?</script>", "", text_value, flags=re.IGNORECASE)
    text_value = re.sub(r"<!--[\s\S]*?-->", "", text_value)
    text_value = re.sub(r"<br\s*/?>", "\n", text_value, flags=re.IGNORECASE)
    text_value = re.sub(
        r"</?(p|div|section|article|h[1-6]|li|ul|ol|tr|table|blockquote)[^>]*>",
        "\n",
        text_value,
        flags=re.IGNORECASE,
    )
    text_value = re.sub(r"<[^>]+>", " ", text_value)
    text_value = html_lib.unescape(text_value)
    lines = [re.sub(r"[ \t\r\f\v]+", " ", line).strip() for line in text_value.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _extract_first_html_block_text(pattern: str, html_text: str) -> str:
    match = re.search(pattern, html_text, flags=re.IGNORECASE | re.DOTALL)
    return _plain_block_from_html(match.group(1)) if match else ""


def _daily_section_text(html_text: str, section_title: str) -> str:
    return _plain_block_from_html(_extract_h2_section_html(html_text, section_title))


def _daily_first_paragraph_text(html_text: str, section_title: str) -> str:
    section_html = _extract_h2_section_html(html_text, section_title)
    return _extract_first_html_block_text(r"<p[^>]*>([\s\S]*?)</p>", section_html) or _plain_block_from_html(section_html)


def _parse_daily_volatility(section_html: str) -> Tuple[List[Dict[str, str]], str]:
    lines = _split_plain_lines(_plain_block_from_html(section_html))
    items: List[Dict[str, str]] = []
    summary_lines: List[str] = []
    level_pattern = r"(极低|偏低|低|中|偏高|高|极高)"
    idx = 0
    while idx < len(lines):
        line = lines[idx]
        combined = re.match(rf"^(.+?)\s+([0-9]+(?:\.[0-9]+)?)%\s*{level_pattern}$", line)
        if combined:
            items.append({
                "name": combined.group(1).strip(),
                "value": f"{combined.group(2)}%",
                "level": combined.group(3),
            })
            idx += 1
            continue

        next_line = lines[idx + 1] if idx + 1 < len(lines) else ""
        metric = re.match(rf"^([0-9]+(?:\.[0-9]+)?)%\s*{level_pattern}$", next_line)
        if metric and "%" not in line and len(line) <= 24:
            items.append({
                "name": line,
                "value": f"{metric.group(1)}%",
                "level": metric.group(2),
            })
            idx += 2
            continue

        summary_lines.append(line)
        idx += 1
    return items, "\n".join(summary_lines).strip()


def _parse_daily_card_blocks(section_html: str) -> List[Dict[str, str]]:
    blocks = re.findall(r"<td[^>]*>([\s\S]*?)</td>", section_html or "", flags=re.IGNORECASE)
    if not blocks:
        blocks = re.findall(
            r"<div[^>]*class=['\"][^'\"]*\bglass-card\b[^'\"]*['\"][^>]*>([\s\S]*?)</div>",
            section_html or "",
            flags=re.IGNORECASE,
        )

    cards: List[Dict[str, str]] = []
    for block in blocks:
        title = _extract_first_html_block_text(r"<h4[^>]*>([\s\S]*?)</h4>", block)
        spans = [
            _plain_block_from_html(span)
            for span in re.findall(r"<span[^>]*>([\s\S]*?)</span>", block, flags=re.IGNORECASE)
        ]
        spans = [span for span in spans if span]
        badge = ""
        if not title and spans:
            title = spans[0]
            badge = spans[1] if len(spans) > 1 else ""
        elif len(spans) >= 2:
            badge = spans[-1]

        body = _extract_first_html_block_text(r"<p[^>]*>([\s\S]*?)</p>", block)
        if not body:
            body_source = re.sub(r"<h4[^>]*>[\s\S]*?</h4>", "", block, flags=re.IGNORECASE)
            body = _plain_block_from_html(body_source)
            if title and body.startswith(title):
                body = body[len(title):].strip()
            if badge and body.endswith(badge):
                body = body[: -len(badge)].strip()

        if title or badge or body:
            cards.append({"title": title, "badge": badge, "body": body})
    return cards


def _build_daily_report_mobile_render(html_text: Any) -> Optional[Dict[str, Any]]:
    raw = str(html_text or "")
    if not raw or ("复盘晚报" not in raw and "爱波塔复盘" not in raw):
        return None
    try:
        title = _extract_first_html_text(r"<h1[^>]*>([\s\S]*?)</h1>", raw) or "爱波塔复盘晚报"
        subtitle = _extract_first_html_text(
            r"<div[^>]*class=['\"][^'\"]*\bglass-header\b[^'\"]*['\"][^>]*>[\s\S]*?<p[^>]*>([\s\S]*?)</p>",
            raw,
        )
        fund_flow_section = _extract_h2_section_html(raw, "资金暗流")
        commodities_section = _extract_h2_section_html(raw, "商品期货全景")
        volatility_section = _extract_h2_section_html(raw, "期权波动率")
        volatility_items, volatility_summary = _parse_daily_volatility(volatility_section)
        payload = {
            "type": "daily_report",
            "hero": {
                "title": title,
                "subtitle": subtitle,
            },
            "headline": _daily_section_text(raw, "市场头条"),
            "fund_flow": _parse_daily_card_blocks(fund_flow_section),
            "commodities": _parse_daily_card_blocks(commodities_section),
            "volatility": volatility_summary,
            "volatility_items": volatility_items,
            "bull_stock": _daily_section_text(raw, "每日牛股"),
            "risk_warning": _daily_section_text(raw, "风险警示"),
            "tomorrow_strategy": _daily_first_paragraph_text(raw, "明日策略"),
        }
        if not any(
            payload.get(key)
            for key in (
                "headline",
                "fund_flow",
                "commodities",
                "volatility",
                "bull_stock",
                "risk_warning",
                "tomorrow_strategy",
            )
        ):
            return None
        return payload
    except Exception:
        return None


def _build_safe_stock_mobile_render(html_text: Any) -> Optional[Dict[str, Any]]:
    raw = str(html_text or "")
    if not raw or "小爱选股晚报" not in raw:
        return None
    try:
        title = _extract_first_html_text(r"<h1[^>]*>([\s\S]*?)</h1>", raw) or "小爱选股晚报"
        meta = _extract_first_html_text(r"<div[^>]*class=['\"][^'\"]*\bmuted\b[^'\"]*['\"][^>]*>([\s\S]*?)</div>", raw)
        market_note = _extract_first_html_text(
            r"<div[^>]*class=['\"][^'\"]*\bmarket-note\b[^'\"]*['\"][^>]*>([\s\S]*?)</div>",
            raw,
        )
        trade_date = ""
        generated_at = ""
        if meta:
            trade_match = re.search(r"交易日[:：]\s*([^·\s]+)", meta)
            time_match = re.search(r"生成时间[:：]\s*(.+)$", meta)
            trade_date = trade_match.group(1).strip() if trade_match else ""
            generated_at = time_match.group(1).strip() if time_match else ""

        payload = {
            "type": "safe_stock_report",
            "hero": {
                "title": title,
                "trade_date": trade_date,
                "generated_at": generated_at,
                "market_note": market_note,
            },
            "sectors": _extract_safe_stock_section_rows(raw, "资金回流"),
            "buys": _extract_safe_stock_section_rows(raw, "可买标的"),
            "watches": _extract_safe_stock_section_rows(raw, "观察标的"),
            "tracking": _extract_safe_stock_section_rows(raw, "已买跟踪"),
        }
        if not any(payload.get(k) for k in ("sectors", "buys", "watches", "tracking")):
            return None
        return payload
    except Exception:
        return None


def _build_expiry_option_mobile_render(html_text: Any) -> Optional[Dict[str, Any]]:
    raw = str(html_text or "")
    if not raw or "末日期权晚报" not in raw:
        return None
    try:
        title = _extract_first_html_text(r"<h1[^>]*>([\s\S]*?)</h1>", raw) or "末日期权晚报"
        subtitle = _extract_first_html_text(r"<div[^>]*class=['\"][^'\"]*\bheader\b[^'\"]*['\"][^>]*>[\s\S]*?<p[^>]*>([\s\S]*?)</p>", raw)
        intro = ""
        intro_match = re.search(
            r"<div[^>]*class=['\"][^'\"]*\bcard\b[^'\"]*['\"][^>]*>[\s\S]*?<p[^>]*>([\s\S]*?)</p>",
            raw,
            flags=re.IGNORECASE,
        )
        if intro_match:
            intro = _plain_from_html_fragment(intro_match.group(1))

        items: List[Dict[str, Any]] = []
        block_pattern = (
            r"<div[^>]*style=['\"][^'\"]*margin-bottom\s*:\s*20px[^'\"]*['\"][^>]*>"
            r"([\s\S]*?<h2[^>]*class=['\"][^'\"]*\bsection-title\b[^'\"]*['\"][^>]*>[\s\S]*?</h2>[\s\S]*?"
            r"<div[^>]*class=['\"][^'\"]*\bcard\b[^'\"]*['\"][^>]*>[\s\S]*?</div>\s*)</div>"
        )
        for block in re.findall(block_pattern, raw, flags=re.IGNORECASE):
            h2_match = re.search(r"<h2[^>]*>([\s\S]*?)</h2>", block, flags=re.IGNORECASE)
            if not h2_match:
                continue
            heading = _plain_from_html_fragment(h2_match.group(1))
            if not heading or "风险提示" in heading:
                continue
            days_match = re.search(r"剩余\s*([0-9]+)\s*天", heading)
            days_left = days_match.group(1) if days_match else ""
            strategy = ""
            for candidate in ("买看涨", "买看跌", "卖看跌", "卖看涨", "牛市价差", "熊市价差", "双卖", "蝴蝶", "铁鹰"):
                if candidate in heading:
                    strategy = candidate
                    break
            name = heading
            if days_match:
                name = name[: days_match.start()]
            if strategy:
                name = name.replace(strategy, "")
            name = re.sub(r"^[^\w\u4e00-\u9fff]+", "", name).strip(" ｜|")

            fields: Dict[str, str] = {}
            for label, value in re.findall(
                r"<span[^>]*class=['\"][^'\"]*\blabel\b[^'\"]*['\"][^>]*>([\s\S]*?)</span>\s*"
                r"<span[^>]*class=['\"][^'\"]*\bvalue\b[^'\"]*['\"][^>]*>([\s\S]*?)</span>",
                block,
                flags=re.IGNORECASE,
            ):
                fields[_plain_from_html_fragment(label)] = _plain_from_html_fragment(value)

            contracts: List[Dict[str, str]] = []
            for contract_name_html, meta_html in re.findall(
                r"<div[^>]*class=['\"][^'\"]*\bcontract-name\b[^'\"]*['\"][^>]*>([\s\S]*?)</div>\s*"
                r"<div[^>]*class=['\"][^'\"]*\bcontract-meta\b[^'\"]*['\"][^>]*>([\s\S]*?)</div>",
                block,
                flags=re.IGNORECASE,
            ):
                contract_name = _plain_from_html_fragment(contract_name_html)
                meta_values = [
                    _plain_from_html_fragment(x)
                    for x in re.findall(r"<span[^>]*>([\s\S]*?)</span>", meta_html, flags=re.IGNORECASE)
                ]
                if contract_name or meta_values:
                    contracts.append({
                        "name": contract_name,
                        "premium": meta_values[0] if meta_values else "",
                        "holding": meta_values[1] if len(meta_values) > 1 else "",
                    })

            if name or fields or contracts:
                items.append({
                    "name": name or "标的",
                    "days_left": days_left,
                    "strategy": strategy,
                    "trend": fields.get("趋势研判", ""),
                    "reason": fields.get("策略理由", ""),
                    "price": fields.get("标的现价", ""),
                    "contracts": contracts,
                })

        risk_text = _extract_first_html_text(
            r"<div[^>]*class=['\"][^'\"]*\brisk-box\b[^'\"]*['\"][^>]*>[\s\S]*?<p[^>]*>([\s\S]*?)</p>",
            raw,
        )
        risks = _split_plain_lines(risk_text) if risk_text else []
        payload = {
            "type": "expiry_option_radar",
            "hero": {
                "title": title,
                "subtitle": subtitle,
                "intro": intro,
            },
            "items": items,
            "risks": risks,
        }
        if not items and not intro:
            return None
        return payload
    except Exception:
        return None


def _parse_broker_signal_items(section_html: str) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []
    for item_html in re.findall(
        r"<div[^>]*class=['\"][^'\"]*\bsignal-item\b[^'\"]*['\"][^>]*>([\s\S]*?)</div>",
        section_html or "",
        flags=re.IGNORECASE,
    ):
        title = _extract_first_html_text(r"<strong[^>]*>([\s\S]*?)</strong>", item_html)
        detail = _plain_from_html_fragment(re.sub(r"<strong[^>]*>[\s\S]*?</strong>", "", item_html, flags=re.IGNORECASE))
        if title or detail:
            items.append({"title": title, "detail": detail})
    return items


def _parse_broker_table(section_html: str, index: int) -> List[Dict[str, str]]:
    tables = _extract_tables_from_html(section_html)
    if index < 0 or index >= len(tables):
        return []
    return _parse_html_table_rows(tables[index], _BROKER_POSITION_HEADER_KEY_MAP)


def _parse_broker_5d_items(section_html: str, direction_title: str) -> List[Dict[str, str]]:
    section_text = _plain_from_html_fragment(section_html)
    start = section_text.find(direction_title)
    if start < 0:
        return []
    tail = section_text[start + len(direction_title):]
    other_title = "累计做空" if direction_title == "累计做多" else "累计做多"
    other_pos = tail.find(other_title)
    if other_pos >= 0:
        tail = tail[:other_pos]
    items: List[Dict[str, str]] = []
    for line in _split_plain_lines(tail):
        match = re.search(r"([0-9]+)\.\s*([^\s]+)\s+([+-][0-9,]+手)\s*(\(约[+-]?[0-9.]+亿\))?", line)
        if match:
            items.append({
                "rank": match.group(1),
                "product": match.group(2),
                "change": match.group(3),
                "value": match.group(4) or "",
            })
    return items


def _build_broker_position_mobile_render(html_text: Any) -> Optional[Dict[str, Any]]:
    raw = str(html_text or "")
    if not raw or ("期货商持仓" not in raw and "持仓数据流" not in raw):
        return None
    try:
        title = _extract_first_html_text(r"<h1[^>]*>([\s\S]*?)</h1>", raw) or "持仓晚报"
        subtitle = _extract_first_html_text(r"<p[^>]*class=['\"][^'\"]*\bsub-text\b[^'\"]*['\"][^>]*>([\s\S]*?)</p>", raw)
        core_section = _extract_h2_section_html(raw, "今日核心信号")
        institution_day = _extract_h2_section_html(raw, "机构当日动向")
        institution_5d = _extract_h2_section_html(raw, "机构5日累计布局")
        foreign_section = _extract_h2_section_html(raw, "外资风向标")
        contra_section = _extract_h2_section_html(raw, "反指标信号")
        comment_section = _extract_h2_section_html(raw, "AI毒舌点评")

        foreign_text = _plain_from_html_fragment(foreign_section)
        commentary = _plain_from_html_fragment(comment_section)
        payload = {
            "type": "broker_position_report",
            "hero": {
                "title": title,
                "subtitle": subtitle,
            },
            "core_signals": _parse_broker_signal_items(core_section),
            "institution_day": {
                "longs": _parse_broker_table(institution_day, 0),
                "shorts": _parse_broker_table(institution_day, 1),
            },
            "institution_5d": {
                "longs": _parse_broker_5d_items(institution_5d, "累计做多"),
                "shorts": _parse_broker_5d_items(institution_5d, "累计做空"),
            },
            "foreign_notes": _split_plain_lines(foreign_text),
            "contra": {
                "longs": _parse_broker_table(contra_section, 0),
                "shorts": _parse_broker_table(contra_section, 1),
            },
            "commentary": _split_plain_lines(commentary),
        }
        if not any(
            payload.get(key)
            for key in ("core_signals", "foreign_notes", "commentary")
        ) and not payload["institution_day"]["longs"] and not payload["contra"]["longs"]:
            return None
        return payload
    except Exception:
        return None


def _is_production_env() -> bool:
    env_val = (
        str(os.getenv("APP_ENV", "")).strip()
        or str(os.getenv("ENV", "")).strip()
        or str(os.getenv("DEPLOY_ENV", "")).strip()
    ).lower()
    return env_val in {"prod", "production", "online"}


def _normalize_trade_date_input(raw: Optional[str]) -> Optional[str]:
    digits = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if not digits:
        return None
    if len(digits) != 8:
        raise HTTPException(status_code=400, detail="trade_date 格式错误，应为 YYYYMMDD")
    return digits


def _normalize_ai_portfolio_id(raw: Optional[str]) -> str:
    if raw is not None and not isinstance(raw, (str, int, float)):
        default = getattr(raw, "default", None)
        raw = default if isinstance(default, (str, int, float)) else None
    value = str(raw or "").strip().lower()
    if not value or value in {OFFICIAL_PORTFOLIO_ID.lower(), "v1", "ai1", "ai_diary", "official"}:
        return OFFICIAL_PORTFOLIO_ID
    if value in {
        OFFICIAL_PORTFOLIO_3_ID.lower(),
        "v3",
        "ai3",
        "ai_diary2",
        "ai_diary_2",
        "stock3",
        "选股3号",
    }:
        return OFFICIAL_PORTFOLIO_3_ID
    raise HTTPException(status_code=400, detail="portfolio_id 不支持")


def _json_safe_value(value: Any):
    if value is None:
        return None

    # numpy scalar / pandas scalar
    if hasattr(value, "item") and callable(getattr(value, "item", None)):
        try:
            value = value.item()
        except Exception:
            pass

    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(v) for v in value]

    # 兜底处理 NaN / NaT
    try:
        if value != value:
            return None
    except Exception:
        pass
    return value


def _df_records_jsonable(df: Any, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []
    work = df.copy()
    if limit is not None and limit > 0:
        work = work.head(limit)
    rows = work.to_dict(orient="records")
    return [_json_safe_value(row) for row in rows]


@app.get("/api/intel/reports", tags=["情报站"])
def intel_reports(
    channel_code: Optional[str] = None,
    page: int = 1,
    page_size: int = 20,
    username: str = Depends(get_current_user),
):
    """
    获取情报站晚报列表。
    - channel_code: 可选频道筛选（如 "fund_flow_report"）
    - page / page_size: 分页参数
    """
    page = max(1, page)
    page_size = min(50, max(1, page_size))
    offset = (page - 1) * page_size
    channel_code = _normalize_intel_channel_code(channel_code)

    # 多拉一些再切片，避免多次 DB 查询
    raw = sub_svc.get_channel_contents(
        channel_code=channel_code,
        days=10,
        limit=offset + page_size,
    ) or []
    paged = raw[offset: offset + page_size]

    items = []
    for c in paged:
        content_text = c.get("content") or ""
        summary = _html_to_plain(content_text, max_len=120)
        items.append({
            "id": c.get("id"),
            "title": c.get("title", ""),
            "channel_name": c.get("channel_name", ""),
            "channel_code": c.get("channel_code", ""),
            "summary": summary,
            "published_at": _extract_published_at(c),
        })

    return {
        "items": items,
        "page": page,
        "page_size": page_size,
        "has_more": len(raw) > offset + page_size,
    }


@app.get("/api/intel/ai/overview", tags=["情报站"])
def intel_ai_overview(
    nav_days: int = 120,
    trades_days: int = 20,
    positions_limit: int = 24,
    review_limit: int = 260,
    portfolio_id: str = Query(default=OFFICIAL_PORTFOLIO_ID),
    username: str = Depends(get_current_user),
):
    """
    AI炒股总览数据（官方组合）：
    - snapshot / nav_series / positions / trades / latest_review / review_dates / watchlist
    """
    _ = username  # 仅鉴权，不做用户隔离
    pid = _normalize_ai_portfolio_id(portfolio_id)
    nav_days = min(250, max(30, int(nav_days)))
    trades_days = min(40, max(5, int(trades_days)))
    positions_limit = min(50, max(5, int(positions_limit)))
    review_limit = min(260, max(20, int(review_limit)))

    try:
        snapshot = _json_safe_value(ai_get_latest_snapshot(pid)) or {}
        if not snapshot.get("has_data"):
            return {
                "has_data": False,
                "portfolio_id": pid,
                "snapshot": snapshot,
                "review_dates": [],
                "latest_review": {
                    "has_data": False,
                    "summary_md": "暂无复盘数据。",
                    "buys_md": "",
                    "sells_md": "",
                    "risk_md": "",
                    "next_watchlist": [],
                },
                "nav_series": [],
                "positions": [],
                "trades": [],
                "closed_trade_extremes": {"top_gains": [], "top_losses": []},
                "watchlist": [],
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        snapshot_trade_date = str(snapshot.get("trade_date") or "")
        review_dates = ai_get_review_dates(pid, limit=review_limit) or []
        if snapshot_trade_date and snapshot_trade_date not in review_dates:
            review_dates = [snapshot_trade_date] + list(review_dates)

        latest_review_date = review_dates[0] if review_dates else snapshot_trade_date
        latest_review = _json_safe_value(
            ai_get_daily_review(pid, trade_date=latest_review_date)
        ) or {
            "has_data": False,
            "summary_md": "暂无复盘数据。",
            "buys_md": "",
            "sells_md": "",
            "risk_md": "",
            "next_watchlist": [],
        }

        nav_df = ai_get_nav_series(pid, days=nav_days)
        nav_rows = _df_records_jsonable(nav_df, limit=nav_days)
        if snapshot.get("sharpe_ratio") is None:
            snapshot["sharpe_ratio"] = ai_compute_sharpe_ratio_from_nav(nav_df)
        initial_capital = _safe_float(snapshot.get("initial_capital"), 1_000_000.0)
        if initial_capital <= 0:
            initial_capital = 1_000_000.0
        for row in nav_rows:
            nav_val = _safe_float(row.get("nav"), 0.0)
            row["nav_norm"] = round(nav_val / initial_capital, 6)
            row["bench_hs300"] = _safe_float(row.get("bench_hs300"), 0.0)
            row["bench_zz1000"] = _safe_float(row.get("bench_zz1000"), 0.0)
            row["trade_date"] = str(row.get("trade_date") or "")

        snapshot_position_value = _safe_float(snapshot.get("position_value"), 0.0)
        positions = []
        if snapshot_position_value > 0:
            pos_df = ai_get_positions(
                pid,
                as_of_date=snapshot_trade_date or None,
                strict_as_of=True,
            )
            # 仅在快照显示有持仓时，才允许回退到最近可用持仓，避免口径错位。
            if getattr(pos_df, "empty", True):
                pos_df = ai_get_positions(
                    pid,
                    as_of_date=snapshot_trade_date or None,
                    strict_as_of=False,
                )
            positions = _df_records_jsonable(pos_df, limit=positions_limit)
        for row in positions:
            row["trade_date"] = str(row.get("trade_date") or snapshot_trade_date)

        trades = _df_records_jsonable(ai_get_trades(pid, days=trades_days), limit=trades_days * 20)
        for row in trades:
            row["trade_date"] = str(row.get("trade_date") or "")
            if row.get("created_at") is not None:
                row["created_at"] = str(row.get("created_at"))
        closed_trade_extremes = _json_safe_value(
            ai_get_closed_trade_extremes(pid, days=9999, limit=3)
        ) or {"top_gains": [], "top_losses": []}

        watchlist = latest_review.get("next_watchlist") if isinstance(latest_review, dict) else []
        if not isinstance(watchlist, list):
            watchlist = []

        return {
            "has_data": True,
            "portfolio_id": pid,
            "snapshot": snapshot,
            "review_dates": [str(d) for d in review_dates],
            "latest_review": latest_review,
            "nav_series": nav_rows,
            "positions": positions,
            "trades": trades,
            "closed_trade_extremes": closed_trade_extremes,
            "watchlist": _json_safe_value(watchlist) or [],
            "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI炒股总览加载失败: {e}")


@app.get("/api/intel/ai/review", tags=["情报站"])
def intel_ai_review(
    trade_date: Optional[str] = None,
    portfolio_id: str = Query(default=OFFICIAL_PORTFOLIO_ID),
    username: str = Depends(get_current_user),
):
    """获取 AI炒股复盘（日级）。trade_date 可选，格式 YYYYMMDD。"""
    _ = username
    td = _normalize_trade_date_input(trade_date)
    pid = _normalize_ai_portfolio_id(portfolio_id)
    try:
        review = ai_get_daily_review(pid, trade_date=td)
        return _json_safe_value(review)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AI炒股复盘加载失败: {e}")


@app.get("/api/intel/report/{report_id}", tags=["情报站"])
def intel_report_detail(
    report_id: int,
    username: str = Depends(get_current_user),
):
    """获取单篇晚报的完整内容。"""
    content = sub_svc.get_content_by_id(report_id)
    if not content:
        raise HTTPException(status_code=404, detail="内容不存在或已下架")
    if content.get("is_premium"):
        access = sub_svc.check_subscription_access(username, int(content.get("channel_id") or 0))
        if not access.get("has_access"):
            raise HTTPException(status_code=403, detail="该内容需要订阅后查看")
    # 统一返回字段，避免客户端被 publish_time/published_at 命名差异影响
    content["published_at"] = _extract_published_at(content)
    channel_code = str(content.get("channel_code") or "").strip().lower()
    if channel_code == "safe_stock_report":
        mobile_render = _build_safe_stock_mobile_render(content.get("content") or "")
        if mobile_render:
            content["mobile_render"] = mobile_render
    elif channel_code == "daily_report":
        mobile_render = _build_daily_report_mobile_render(content.get("content") or "")
        if mobile_render:
            content["mobile_render"] = mobile_render
    elif channel_code == "expiry_option_radar":
        mobile_render = _build_expiry_option_mobile_render(content.get("content") or "")
        if mobile_render:
            content["mobile_render"] = mobile_render
    elif channel_code == "broker_position_report":
        mobile_render = _build_broker_position_mobile_render(content.get("content") or "")
        if mobile_render:
            content["mobile_render"] = mobile_render
    return content


@app.post("/api/intel/subscribe", tags=["情报站"])
def intel_subscribe(
    body: SubscribeRequest,
    username: str = Depends(get_current_user),
):
    """订阅白名单情报频道（默认关闭，通过环境变量灰度开启）。"""
    if not _INTEL_SELF_SUBSCRIBE_API_ENABLED:
        raise HTTPException(status_code=403, detail="当前环境未开启自助订阅接口")
    if _is_production_env() and not _ALLOW_SELF_SUB_IN_PROD:
        raise HTTPException(status_code=403, detail="生产环境默认关闭该接口")

    channel_code = _normalize_intel_channel_code(body.channel_code)
    if not channel_code:
        raise HTTPException(status_code=400, detail="channel_code 不能为空")
    channel = sub_svc.get_channel_by_code(channel_code)
    if not channel:
        raise HTTPException(status_code=404, detail="频道不存在")

    if channel_code.lower() not in _EFFECTIVE_FREE_CHANNEL_CODES:
        raise HTTPException(status_code=403, detail="该频道需要人工开通，请联系客服")

    result = sub_svc.add_subscription(
        username,
        channel["id"],
        days=3650,
        source_type="self_subscribe_whitelist",
        source_ref=f"api:intel_subscribe:{channel_code.lower()}",
        source_note="mobile_api_whitelist_free_subscribe",
        operator="user_self_service",
    )
    # add_subscription 返回 (success, message) 或单个 bool
    if isinstance(result, tuple):
        success, msg = result[0], result[1] if len(result) > 1 else "操作完成"
    else:
        success, msg = bool(result), "订阅成功" if result else "订阅失败"

    if not success:
        raise HTTPException(status_code=400, detail=msg)
    return {"message": msg}


@app.post("/api/alipay/notify", tags=["支付"])
async def alipay_notify(request: Request):
    """
    支付宝异步回调端点：
    - 不走 JWT 鉴权
    - 回调成功必须返回纯文本 success
    """
    try:
        form_data = await request.form()
        payload = {str(k): str(v) for k, v in form_data.items()}
    except Exception as exc:
        print(f"[mobile_api][alipay_notify] parse_form_failed err={exc}")
        return PlainTextResponse("failure", status_code=200)

    ok, reason = pay_svc.process_alipay_notify(payload)
    if ok:
        return PlainTextResponse("success", status_code=200)
    print(f"[mobile_api][alipay_notify] failed reason={reason}")
    return PlainTextResponse("failure", status_code=200)


# ════════════════════════════════════════════════════════════
#  PAYMENT — 充值与点数购买
# ════════════════════════════════════════════════════════════

@app.get("/api/pay/wallet", tags=["支付"])
def pay_wallet(username: str = Depends(get_current_user)):
    """获取点数钱包信息。"""
    try:
        points_info = pay_svc.get_user_points(username) or {}
        updated_at = points_info.get("updated_at")
        if hasattr(updated_at, "strftime"):
            updated_text = updated_at.strftime("%Y-%m-%d %H:%M:%S")
        else:
            updated_text = str(updated_at or "")
        return {
            "balance": int(points_info.get("balance") or 0),
            "total_earned": int(points_info.get("total_earned") or 0),
            "total_spent": int(points_info.get("total_spent") or 0),
            "updated_at": updated_text,
            "payment_enabled": bool(pay_svc.is_points_payment_enabled()),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"钱包信息获取失败: {e}")


@app.get("/api/pay/packages", tags=["支付"])
def pay_packages(username: str = Depends(get_current_user)):
    """获取充值套餐列表。"""
    _ = username
    try:
        items = []
        for pkg in getattr(pay_svc, "POINTS_PACKAGES", []):
            rmb = float(pkg.get("rmb") or 0)
            points = int(pkg.get("points") or 0)
            bonus = max(points - int(round(rmb * 10)), 0)
            items.append({
                "name": str(pkg.get("name") or ""),
                "rmb": round(rmb, 2),
                "points": points,
                "bonus_points": bonus,
            })
        return {"items": items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"充值套餐获取失败: {e}")


@app.get("/api/pay/products", tags=["支付"])
def pay_products(username: str = Depends(get_current_user)):
    """获取可点数购买产品（频道 + 套餐）。"""
    _ = username
    try:
        products = pay_svc.get_paid_products() or []

        # 与桌面端一致：兜底补齐 trade_signal 产品
        has_trade_signal = any(
            str(item.get("code") or "") == "trade_signal"
            and str(item.get("product_type") or "") == "channel"
            for item in products
        )
        if not has_trade_signal:
            trade_signal = sub_svc.get_channel_by_code("trade_signal")
            if trade_signal and trade_signal.get("id"):
                products.append({
                    "product_type": "channel",
                    "code": "trade_signal",
                    "id": int(trade_signal["id"]),
                    "name": trade_signal.get("name") or "交易信号",
                    "icon": trade_signal.get("icon") or "⚡",
                    "points_monthly": 800,
                    "months_options": [1, 3, 6, 12],
                })

        channel_order = {
            "safe_stock_report": 0,
            "daily_report": 1,
            "expiry_option_radar": 2,
            "broker_position_report": 3,
            "fund_flow_report": 4,
            "macro_risk_radar": 5,
            "trade_signal": 6,
        }

        def _sort_key(item: dict):
            ptype = str(item.get("product_type") or "")
            code = str(item.get("code") or "")
            return (
                0 if ptype == "channel" else 1,
                channel_order.get(code, 999),
                code,
            )

        norm_items = []
        for item in sorted(products, key=_sort_key):
            norm_items.append({
                "product_type": str(item.get("product_type") or ""),
                "code": str(item.get("code") or ""),
                "id": int(item.get("id")) if item.get("id") is not None else None,
                "name": str(item.get("name") or ""),
                "icon": str(item.get("icon") or ""),
                "points_monthly": int(item.get("points_monthly") or 0),
                "months_options": [int(x) for x in (item.get("months_options") or [1, 3, 6, 12])],
                "includes": [str(x) for x in (item.get("includes") or [])],
                "includes_names": [str(x) for x in (item.get("includes_names") or [])],
            })

        return {"items": norm_items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"付费产品获取失败: {e}")


@app.post("/api/pay/purchase", tags=["支付"])
def pay_purchase(
    body: PayPurchaseRequest,
    username: str = Depends(get_current_user),
):
    """使用点数购买频道或套餐权限。"""
    try:
        months = int(body.months or 0)
    except Exception:
        months = 0
    if months <= 0 or months > 36:
        raise HTTPException(status_code=400, detail="months 必须在 1~36 之间")

    product_type = str(body.product_type or "").strip().lower()
    code = str(body.code or "").strip().lower()
    if not code:
        raise HTTPException(status_code=400, detail="code 不能为空")

    if product_type == "channel":
        channel = sub_svc.get_channel_by_code(code)
        if not channel or not channel.get("id"):
            raise HTTPException(status_code=404, detail="频道不存在或已下架")
        ok, msg = pay_svc.purchase_subscription_with_points(
            username,
            int(channel["id"]),
            months=months,
        )
    elif product_type == "package":
        pkg_code = str(getattr(pay_svc, "INTEL_PACKAGE_PRODUCT", {}).get("code", "intel_package")).strip().lower()
        if code != pkg_code:
            raise HTTPException(status_code=404, detail="套餐不存在")
        ok, msg = pay_svc.purchase_intel_package_with_points(
            username,
            months=months,
        )
    else:
        raise HTTPException(status_code=400, detail="product_type 仅支持 channel 或 package")

    if ok:
        message = "购买成功" if str(msg) == "already_processed" else str(msg or "购买成功")
        return {"ok": True, "message": message}
    raise HTTPException(status_code=400, detail=str(msg or "购买失败"))


@app.get("/api/pay/config", tags=["支付"])
def pay_config(username: str = Depends(get_current_user)):
    """获取充值中心配置。"""
    _ = username
    recharge_url = str(os.getenv("MOBILE_RECHARGE_URL", "https://www.aiprota.com")).strip() or "https://www.aiprota.com"
    service_wechat = str(os.getenv("MOBILE_SERVICE_WECHAT", "trader-sec")).strip() or "trader-sec"
    service_phone = str(os.getenv("MOBILE_SERVICE_PHONE", "17521591756")).strip() or "17521591756"
    return {
        "recharge_url": recharge_url,
        "service_wechat": service_wechat,
        "service_phone": service_phone,
    }


# ════════════════════════════════════════════════════════════
#  US OPTIONS — 美股期权移动端数据
# ════════════════════════════════════════════════════════════

US_OPTION_DEFAULT_SYMBOL = "SPY"
US_OPTION_MAX_ANOMALY_LIMIT = 50


def _us_option_engine():
    return getattr(de, "engine", None)


def _normalize_mobile_us_option_symbol(raw: Optional[str], *, default: str = US_OPTION_DEFAULT_SYMBOL) -> str:
    text = str(raw or default or "").strip()
    symbol, reason = normalize_us_option_underlying(text)
    if not symbol:
        raise HTTPException(status_code=400, detail=reason or "不支持的美股期权标的")
    return symbol


def _us_option_profile(symbol: str) -> Dict[str, str]:
    try:
        return us_get_underlying_profile(symbol)
    except Exception:
        name = str(US_OPTION_DISPLAY_NAMES.get(symbol) or symbol)
        return {"symbol": symbol, "name": name, "asset_type": "stock"}


def _us_option_display_name(symbol: str) -> str:
    return str(_us_option_profile(symbol).get("name") or US_OPTION_DISPLAY_NAMES.get(symbol) or symbol)


def _us_option_safe_value(value: Any) -> Any:
    if value is None:
        return None
    try:
        import pandas as pd  # local import keeps mobile_api import tolerant in lightweight tests
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (int, str, bool)):
        return value
    return value


def _us_option_safe_dict(row: Mapping[str, Any]) -> Dict[str, Any]:
    return {str(key): _us_option_safe_value(value) for key, value in dict(row or {}).items()}


def _us_option_records(frame: Any, *, limit: Optional[int] = None, columns: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    if frame is None or getattr(frame, "empty", True):
        return []
    source = frame.copy()
    if columns:
        for col in columns:
            if col not in source.columns:
                source[col] = None
        source = source[columns]
    if limit is not None:
        source = source.head(max(int(limit), 0))
    return [_us_option_safe_dict(row) for row in source.to_dict(orient="records")]


def _us_option_latest_trade_date(symbol: str, engine=None) -> str:
    try:
        value = us_load_latest_option_trade_date(symbol, use_test_tables=False, engine=engine or _us_option_engine())
        return str(value or "").strip()
    except Exception:
        return ""


def _us_option_display_date(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _us_option_iv_history_records(history: Any) -> List[Dict[str, Any]]:
    if history is None or getattr(history, "empty", True):
        return []
    source = history.copy()
    date_col = "trade_date" if "trade_date" in source.columns else ("date" if "date" in source.columns else "")
    value_col = "atm_iv_pct" if "atm_iv_pct" in source.columns else ("iv_pct" if "iv_pct" in source.columns else "")
    if not date_col or not value_col:
        return []
    out = source[[date_col, value_col]].copy()
    out = out.rename(columns={date_col: "trade_date", value_col: "iv_pct"})
    try:
        out = out.dropna(subset=["trade_date", "iv_pct"]).sort_values("trade_date").tail(252)
    except Exception:
        pass
    rows = []
    for row in _us_option_records(out):
        trade_date = str(row.get("trade_date") or "")
        rows.append({
            "trade_date": trade_date,
            "display_date": _us_option_display_date(trade_date),
            "iv_pct": row.get("iv_pct"),
        })
    return rows


def _us_option_price_history_records(stock_daily: Any, *, limit: int = 180) -> List[Dict[str, Any]]:
    if stock_daily is None or getattr(stock_daily, "empty", True):
        return []
    source = stock_daily.copy()
    if "date" not in source.columns:
        return []
    for col in ("open", "high", "low", "close", "volume", "adjClose"):
        if col not in source.columns:
            source[col] = None
    try:
        source = source.sort_values("date").tail(max(min(int(limit or 180), 500), 1))
    except Exception:
        pass
    rows: List[Dict[str, Any]] = []
    for row in _us_option_records(source[["date", "open", "high", "low", "close", "volume", "adjClose"]]):
        raw_date = row.get("date")
        if isinstance(raw_date, str):
            trade_date = raw_date[:10].replace("-", "")
        else:
            try:
                trade_date = raw_date.strftime("%Y%m%d")
            except Exception:
                trade_date = str(raw_date or "").replace("-", "")[:8]
        rows.append({
            "trade_date": trade_date,
            "display_date": _us_option_display_date(trade_date),
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row.get("close"),
            "volume": row.get("volume"),
            "adj_close": row.get("adjClose"),
        })
    return rows


def _us_option_profile_card(symbol: str, profile: Mapping[str, Any], engine=None) -> Dict[str, Any]:
    try:
        card = us_build_underlying_profile_card(
            symbol,
            engine=engine or _us_option_engine(),
            as_of_date=date.today().strftime("%Y%m%d"),
            use_test_tables=False,
        )
    except Exception:
        card = dict(profile or {})

    asset_type = str(card.get("asset_type") or profile.get("asset_type") or "stock").lower()
    if asset_type == "etf":
        intro_label, style_label, risk_label, type_label = "ETF特色", "板块风格", "观察重点", "ETF"
    else:
        intro_label, style_label, risk_label, type_label = "主营业务", "优势", "短板/风险", "个股"

    source_refs = card.get("dynamic_source_refs") if isinstance(card.get("dynamic_source_refs"), list) else []
    sources: List[str] = []
    for ref in source_refs:
        if not isinstance(ref, Mapping):
            continue
        source = str(ref.get("source") or "").strip()
        if source and source not in sources:
            sources.append(source)

    earnings_date = str(card.get("earnings_date") or card.get("next_earnings_date") or "").strip()
    earnings_time = str(card.get("earnings_time") or "").strip()
    earnings_source = str(card.get("earnings_source") or "").strip()
    compact_time = earnings_time.split(" · ")[0] if earnings_time else ""
    earnings = " · ".join(part for part in (earnings_date, compact_time, earnings_source) if part) or "待更新"

    return {
        "symbol": symbol,
        "name": str(card.get("name") or profile.get("name") or _us_option_display_name(symbol)),
        "asset_type": asset_type,
        "type_label": type_label,
        "intro_label": intro_label,
        "style_label": style_label,
        "risk_label": risk_label,
        "business": str(card.get("business") or ""),
        "style": str(card.get("strength") or ""),
        "risk": str(card.get("risk") or ""),
        "earnings": earnings,
        "recent_hotspot": str(card.get("recent_hotspot") or card.get("recent_catalyst") or "近期热点待更新"),
        "option_data": str(card.get("option_data") or card.get("recent_risk") or "期权数据待更新"),
        "updated_label": str(card.get("dynamic_updated_at") or card.get("dynamic_as_of_date") or ""),
        "source_summary": " + ".join(sources[:3]) if sources else "待更新",
    }


def _us_option_status_brief(metrics: Mapping[str, Any], summary: Mapping[str, Any], gaps: List[str]) -> str:
    iv_rank = _safe_floatv(metrics.get("iv_rank"), None)
    if iv_rank is None:
        iv_text = "IV 分位数据不足"
    elif iv_rank >= 70:
        iv_text = "IV 处于历史偏高区"
    elif iv_rank <= 20:
        iv_text = "IV 处于历史偏低区"
    else:
        iv_text = "IV 处于历史中性区"

    oi_rows = int(_safe_floatv(summary.get("open_interest_rows"), 0) or 0)
    oi_text = "OI 数据完整" if oi_rows > 0 else "OI 数据不足"
    gap_text = "；".join(gaps[:2]) if gaps else ""
    return "，".join([item for item in (iv_text, oi_text, gap_text) if item])


def _us_option_empty_payload(symbol: str, message: str) -> Dict[str, Any]:
    return {
        "has_data": False,
        "symbol": symbol,
        "display_name": _us_option_display_name(symbol),
        "trade_date": "",
        "message": message,
    }


def _us_option_overview_payload(symbol: str) -> Dict[str, Any]:
    engine = _us_option_engine()
    profile = _us_option_profile(symbol)
    trade_date = _us_option_latest_trade_date(symbol, engine=engine)
    if not trade_date:
        return {
            **_us_option_empty_payload(symbol, "暂无该标的的本地美股期权数据"),
            "profile": profile,
            "profile_card": _us_option_profile_card(symbol, profile, engine=engine),
            "metrics": {},
            "chain_summary": {},
            "gaps": ["未找到期权交易日数据"],
            "price_history": [],
            "iv_history": [],
        }

    stock_df = us_load_stock_daily(symbol, limit=420, engine=engine)
    underlying_price = us_selected_underlying_price(stock_df, trade_date)
    market_metrics_history = us_load_market_metrics_history(symbol, window=252, use_test_tables=False, engine=engine)
    chain_summary = us_load_option_chain_summary(
        symbol,
        trade_date,
        include_short_cycle=True,
        include_iv_counts=True,
        use_test_tables=False,
        engine=engine,
    )
    iv_history_source = market_metrics_history

    if market_metrics_history is not None and not getattr(market_metrics_history, "empty", True):
        metrics = us_calculate_overview_metrics_from_market_history(
            stock_df=stock_df,
            market_metrics_history=market_metrics_history,
            trade_date=trade_date,
            underlying=symbol,
            use_test_tables=False,
            engine=engine,
        )
    else:
        chain_df = us_load_option_chain_daily(
            symbol,
            trade_date,
            include_short_cycle=True,
            use_test_tables=False,
            underlying_price=underlying_price,
            engine=engine,
        )
        chain_summary = us_summarize_option_chain(chain_df)
        iv_history_source = us_load_iv_history(symbol, window=252, use_test_tables=False, engine=engine)
        current_iv_pct = us_calculate_atm_iv_pct(chain_df, underlying_price=underlying_price)
        metrics = us_calculate_volatility_positioning_metrics(
            stock_df=stock_df,
            chain_df=chain_df,
            iv_history=iv_history_source,
            trade_date=trade_date,
            current_iv_pct=current_iv_pct,
            iv_rank=None,
            market_metrics_history=None,
        )

    rows = int(_safe_floatv(chain_summary.get("rows"), 0) or 0)
    gaps: List[str] = []
    if rows <= 0:
        gaps.append("期权链日线缺失")
    if int(_safe_floatv(chain_summary.get("provider_iv_rows"), 0) or 0) + int(_safe_floatv(chain_summary.get("computed_iv_rows"), 0) or 0) <= 0:
        gaps.append("IV 数据不足")
    if int(_safe_floatv(chain_summary.get("open_interest_rows"), 0) or 0) <= 0:
        gaps.append("OI 数据不足")

    safe_metrics = _us_option_safe_dict(metrics)
    safe_summary = _us_option_safe_dict(chain_summary)
    return {
        "has_data": rows > 0 or bool(safe_metrics.get("atm_iv_pct")),
        "symbol": symbol,
        "display_name": str(profile.get("name") or _us_option_display_name(symbol)),
        "asset_type": str(profile.get("asset_type") or "stock"),
        "trade_date": trade_date,
        "display_date": _us_option_display_date(trade_date),
        "underlying_price": _us_option_safe_value(underlying_price),
        "metrics": safe_metrics,
        "chain_summary": safe_summary,
        "gaps": gaps,
        "profile": profile,
        "profile_card": _us_option_profile_card(symbol, profile, engine=engine),
        "price_history": _us_option_price_history_records(stock_df),
        "iv_history": _us_option_iv_history_records(iv_history_source),
        "status_brief": _us_option_status_brief(safe_metrics, safe_summary, gaps),
        "message": "" if rows > 0 or safe_metrics.get("atm_iv_pct") is not None else "暂无可展示的美股期权总览数据",
    }


@app.get("/api/us-options/products", tags=["美股期权"])
def us_options_products(username: str = Depends(get_current_user)):
    """移动端美股期权标的池。"""
    _ = username
    engine = _us_option_engine()
    items: List[Dict[str, Any]] = []
    for raw_symbol in US_OPTION_DEFAULT_UNDERLYINGS:
        symbol = str(raw_symbol or "").strip().upper()
        if not symbol:
            continue
        profile = _us_option_profile(symbol)
        latest_trade_date = _us_option_latest_trade_date(symbol, engine=engine)
        items.append({
            "symbol": symbol,
            "name": str(profile.get("name") or US_OPTION_DISPLAY_NAMES.get(symbol) or symbol),
            "asset_type": str(profile.get("asset_type") or "stock"),
            "has_data": bool(latest_trade_date),
            "latest_trade_date": latest_trade_date,
        })
    return {
        "items": items,
        "default_symbol": US_OPTION_DEFAULT_SYMBOL,
        "message": "" if items else "暂无美股期权标的池",
    }


@app.get("/api/us-options/overview", tags=["美股期权"])
def us_options_overview(
    symbol: str = Query(default=US_OPTION_DEFAULT_SYMBOL, description="美股或美股ETF代码，如 SPY / QQQ / NVDA"),
    username: str = Depends(get_current_user),
):
    """移动端美股期权总览。"""
    _ = username
    target = _normalize_mobile_us_option_symbol(symbol)
    try:
        return _us_option_overview_payload(target)
    except HTTPException:
        raise
    except Exception as exc:
        print(f"[us_options_overview] fallback_on_error symbol={target} err={exc}", flush=True)
        return {
            **_us_option_empty_payload(target, "美股期权总览加载失败，请稍后重试"),
            "metrics": {},
            "chain_summary": {},
            "gaps": [str(exc)],
            "iv_history": [],
        }


@app.get("/api/us-options/surface", tags=["美股期权"])
def us_options_surface(
    symbol: str = Query(default=US_OPTION_DEFAULT_SYMBOL, description="美股或美股ETF代码，如 SPY / QQQ / NVDA"),
    username: str = Depends(get_current_user),
):
    """移动端美股期权波动率曲面。"""
    _ = username
    target = _normalize_mobile_us_option_symbol(symbol)
    engine = _us_option_engine()
    try:
        trade_date = _us_option_latest_trade_date(target, engine=engine)
        if not trade_date:
            return {
                **_us_option_empty_payload(target, "暂无该标的的波动率曲面数据"),
                "volatility_cone": [],
                "today_cone_line": [],
                "previous_cone_line": [],
                "today_otm_curve": [],
                "previous_otm_curve": [],
            }
        stock_df = us_load_stock_daily(target, limit=420, engine=engine)
        underlying_price = us_selected_underlying_price(stock_df, trade_date)
        available_dates = us_load_available_option_trade_dates(target, use_test_tables=False, limit=8, engine=engine)
        previous_trade_date = next((str(value) for value in available_dates if str(value or "") < str(trade_date)), "")
        previous_price = us_selected_underlying_price(stock_df, previous_trade_date) if previous_trade_date else None
        volatility_cone = us_load_volatility_cone_history(
            target,
            trade_date,
            window=252,
            use_test_tables=False,
            engine=engine,
        )
        today_cone_line = us_load_volatility_cone_line_snapshot(
            target,
            trade_date,
            use_test_tables=False,
            underlying_price=underlying_price,
            engine=engine,
        )
        previous_cone_line = us_load_volatility_cone_line_snapshot(
            target,
            previous_trade_date,
            use_test_tables=False,
            underlying_price=previous_price,
            engine=engine,
        ) if previous_trade_date else None
        today_otm_curve = us_load_otm_volatility_curve_snapshot(
            target,
            trade_date,
            use_test_tables=False,
            underlying_price=underlying_price,
            engine=engine,
        )
        previous_otm_curve = us_load_otm_volatility_curve_snapshot(
            target,
            previous_trade_date,
            use_test_tables=False,
            underlying_price=previous_price,
            engine=engine,
        ) if previous_trade_date else None
        has_data = any(
            bool(rows)
            for rows in (
                _us_option_records(volatility_cone),
                _us_option_records(today_cone_line),
                _us_option_records(today_otm_curve),
            )
        )
        return {
            "has_data": has_data,
            "symbol": target,
            "display_name": _us_option_display_name(target),
            "trade_date": trade_date,
            "display_date": _us_option_display_date(trade_date),
            "previous_trade_date": previous_trade_date,
            "volatility_cone": _us_option_records(volatility_cone),
            "today_cone_line": _us_option_records(today_cone_line),
            "previous_cone_line": _us_option_records(previous_cone_line),
            "today_otm_curve": _us_option_records(today_otm_curve),
            "previous_otm_curve": _us_option_records(previous_otm_curve),
            "message": "" if has_data else "暂无可展示的波动率曲面数据",
        }
    except Exception as exc:
        print(f"[us_options_surface] fallback_on_error symbol={target} err={exc}", flush=True)
        return {
            **_us_option_empty_payload(target, "美股期权波动率曲面加载失败，请稍后重试"),
            "volatility_cone": [],
            "today_cone_line": [],
            "previous_cone_line": [],
            "today_otm_curve": [],
            "previous_otm_curve": [],
        }


@app.get("/api/us-options/defense", tags=["美股期权"])
def us_options_defense(
    symbol: str = Query(default=US_OPTION_DEFAULT_SYMBOL, description="美股或美股ETF代码，如 SPY / QQQ / NVDA"),
    username: str = Depends(get_current_user),
):
    """移动端美股期权持仓防线。"""
    _ = username
    target = _normalize_mobile_us_option_symbol(symbol)
    engine = _us_option_engine()
    try:
        trade_date = _us_option_latest_trade_date(target, engine=engine)
        if not trade_date:
            return {
                **_us_option_empty_payload(target, "暂无该标的的持仓防线数据"),
                "latest": None,
                "history": [],
            }
        history = us_load_oi_defense_history(
            target,
            trade_date,
            window=20,
            use_test_tables=False,
            engine=engine,
        )
        rows = _us_option_records(history)
        latest = rows[-1] if rows else None
        return {
            "has_data": bool(rows),
            "symbol": target,
            "display_name": _us_option_display_name(target),
            "trade_date": trade_date,
            "display_date": _us_option_display_date(trade_date),
            "latest": latest,
            "history": rows,
            "message": "" if rows else "暂无可展示的持仓防线数据",
        }
    except Exception as exc:
        print(f"[us_options_defense] fallback_on_error symbol={target} err={exc}", flush=True)
        return {
            **_us_option_empty_payload(target, "美股期权持仓防线加载失败，请稍后重试"),
            "latest": None,
            "history": [],
        }


@app.get("/api/us-options/anomalies", tags=["美股期权"])
def us_options_anomalies(
    symbol: Optional[str] = Query(default=None, description="可选，美股或美股ETF代码；为空表示全观察池"),
    limit: int = Query(default=20, description="返回条数，1-50"),
    username: str = Depends(get_current_user),
):
    """移动端美股期权异动观察。"""
    _ = username
    target = _normalize_mobile_us_option_symbol(symbol) if symbol else ""
    try:
        limit_count = max(1, min(int(limit or 20), US_OPTION_MAX_ANOMALY_LIMIT))
    except Exception:
        limit_count = 20
    try:
        scan = us_load_option_anomaly_scan(
            underlyings=[target] if target else None,
            prefer_cache=True,
            use_test_tables=False,
            engine=_us_option_engine(),
        )
        rows = _us_option_records(scan, limit=limit_count)
        trade_date = str(rows[0].get("trade_date") or "") if rows else ""
        return {
            "has_data": bool(rows),
            "symbol": target,
            "display_name": _us_option_display_name(target) if target else "全部观察池",
            "trade_date": trade_date,
            "display_date": _us_option_display_date(trade_date),
            "items": rows,
            "limit": limit_count,
            "message": "" if rows else "暂无可展示的美股期权异动数据",
        }
    except Exception as exc:
        print(f"[us_options_anomalies] fallback_on_error symbol={target or 'ALL'} err={exc}", flush=True)
        return {
            "has_data": False,
            "symbol": target,
            "display_name": _us_option_display_name(target) if target else "全部观察池",
            "trade_date": "",
            "items": [],
            "limit": limit_count,
            "message": "美股期权异动观察加载失败，请稍后重试",
        }


# ════════════════════════════════════════════════════════════
#  MARKET — 综合行情快照
# ════════════════════════════════════════════════════════════

TERM_STRUCTURE_PRODUCTS = [
    {"code": "IH", "name": "上证50", "is_index": True},
    {"code": "IF", "name": "沪深300", "is_index": True},
    {"code": "IC", "name": "中证500", "is_index": True},
    {"code": "IM", "name": "中证1000", "is_index": True},
    {"code": "TS", "name": "2年期国债", "is_index": False},
    {"code": "T", "name": "10年期国债", "is_index": False},
    {"code": "TL", "name": "30年期国债", "is_index": False},
    {"code": "LC", "name": "碳酸锂", "is_index": False},
    {"code": "SI", "name": "工业硅", "is_index": False},
    {"code": "PS", "name": "多晶硅", "is_index": False},
    {"code": "PT", "name": "铂金", "is_index": False},
    {"code": "PD", "name": "钯金", "is_index": False},
    {"code": "AU", "name": "黄金", "is_index": False},
    {"code": "AG", "name": "白银", "is_index": False},
    {"code": "CU", "name": "沪铜", "is_index": False},
    {"code": "AL", "name": "沪铝", "is_index": False},
    {"code": "ZN", "name": "沪锌", "is_index": False},
    {"code": "NI", "name": "沪镍", "is_index": False},
    {"code": "SN", "name": "沪锡", "is_index": False},
    {"code": "PB", "name": "沪铅", "is_index": False},
    {"code": "RU", "name": "橡胶", "is_index": False},
    {"code": "BR", "name": "BR橡胶", "is_index": False},
    {"code": "I", "name": "铁矿石", "is_index": False},
    {"code": "JM", "name": "焦煤", "is_index": False},
    {"code": "J", "name": "焦炭", "is_index": False},
    {"code": "RB", "name": "螺纹钢", "is_index": False},
    {"code": "HC", "name": "热卷", "is_index": False},
    {"code": "SP", "name": "纸浆", "is_index": False},
    {"code": "LG", "name": "原木", "is_index": False},
    {"code": "AO", "name": "氧化铝", "is_index": False},
    {"code": "SH", "name": "烧碱", "is_index": False},
    {"code": "FG", "name": "玻璃", "is_index": False},
    {"code": "SA", "name": "纯碱", "is_index": False},
    {"code": "M", "name": "豆粕", "is_index": False},
    {"code": "A", "name": "豆一", "is_index": False},
    {"code": "B", "name": "豆二", "is_index": False},
    {"code": "C", "name": "玉米", "is_index": False},
    {"code": "LH", "name": "生猪", "is_index": False},
    {"code": "JD", "name": "鸡蛋", "is_index": False},
    {"code": "CJ", "name": "红枣", "is_index": False},
    {"code": "P", "name": "棕榈油", "is_index": False},
    {"code": "Y", "name": "豆油", "is_index": False},
    {"code": "OI", "name": "菜油", "is_index": False},
    {"code": "L", "name": "塑料", "is_index": False},
    {"code": "PK", "name": "花生", "is_index": False},
    {"code": "RM", "name": "菜粕", "is_index": False},
    {"code": "MA", "name": "甲醇", "is_index": False},
    {"code": "TA", "name": "PTA", "is_index": False},
    {"code": "PX", "name": "对二甲苯", "is_index": False},
    {"code": "PR", "name": "瓶片", "is_index": False},
    {"code": "PP", "name": "聚丙烯", "is_index": False},
    {"code": "V", "name": "PVC", "is_index": False},
    {"code": "EB", "name": "苯乙烯", "is_index": False},
    {"code": "EG", "name": "乙二醇", "is_index": False},
    {"code": "SS", "name": "不锈钢", "is_index": False},
    {"code": "AD", "name": "铝合金", "is_index": False},
    {"code": "BU", "name": "沥青", "is_index": False},
    {"code": "FU", "name": "燃料油", "is_index": False},
    {"code": "EC", "name": "集运欧线", "is_index": False},
    {"code": "UR", "name": "尿素", "is_index": False},
    {"code": "SR", "name": "白糖", "is_index": False},
    {"code": "CF", "name": "棉花", "is_index": False},
    {"code": "AP", "name": "苹果", "is_index": False},
]
TERM_STRUCTURE_PRODUCT_MAP = {item["code"]: item for item in TERM_STRUCTURE_PRODUCTS}
TERM_STRUCTURE_WINDOWS = [
    {"key": key, "label": TERM_WINDOW_LABELS.get(key, key)}
    for key in ("3d", "1w", "2w", "1m")
]
TERM_STRUCTURE_INDEX_PRODUCTS = {"IF", "IH", "IC", "IM"}


def _normalize_term_product(product: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", str(product or "").upper())


def _normalize_term_window(window: str) -> str:
    value = str(window or "3d").strip()
    return value if value in TERM_WINDOW_LABELS else "3d"


def _clamp_term_slots(slots: int) -> int:
    try:
        value = int(slots)
    except Exception:
        value = 7
    return max(2, min(value, 12))


@app.get("/api/market/snapshot", tags=["行情"])
def market_snapshot(username: str = Depends(get_current_user)):
    """获取综合行情快照（原始接口，兼容旧版）"""
    try:
        data = de.get_comprehensive_market_data()
        if isinstance(data, dict) and "error" in data:
            raise HTTPException(status_code=500, detail=data["error"])
        return {"data": data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"行情获取失败: {e}")


@app.get("/api/market/term-structure/products", tags=["行情"])
def market_term_structure_products(username: str = Depends(get_current_user)):
    """期限结构可选品种与观察窗口。"""
    _ = username
    return {
        "items": TERM_STRUCTURE_PRODUCTS,
        "windows": TERM_STRUCTURE_WINDOWS,
        "default_product": "IH",
        "default_window": "3d",
        "default_slots": 7,
    }


@app.get("/api/market/term-structure", tags=["行情"])
def market_term_structure(
    product: str = Query(default="IH", description="品种代码，如 IH / RB / CU"),
    window: str = Query(default="3d", description="观察窗口：3d / 1w / 2w / 1m"),
    slots: int = Query(default=7, description="展示合约档位数，2-12"),
    username: str = Depends(get_current_user),
):
    """移动端期限结构数据，股指品种额外返回升贴水结构。"""
    _ = username
    product_code = _normalize_term_product(product)
    if product_code not in TERM_STRUCTURE_PRODUCT_MAP:
        raise HTTPException(status_code=400, detail="不支持的品种")

    window_key = _normalize_term_window(window)
    slot_count = _clamp_term_slots(slots)
    product_info = TERM_STRUCTURE_PRODUCT_MAP[product_code]
    is_index = product_code in TERM_STRUCTURE_INDEX_PRODUCTS

    try:
        main_payload = build_term_structure_payload(
            engine=de.engine,
            product_code=product_code,
            window_key=window_key,
            contract_slots=slot_count,
        )
        basis_anchor = None
        basis_longterm = None
        if is_index:
            basis_anchor = build_index_basis_term_structure_payload(
                engine=de.engine,
                product_code=product_code,
                window_key=window_key,
                contract_slots=slot_count,
            )
            basis_longterm = build_index_basis_longterm_payload(
                engine=de.engine,
                product_code=product_code,
                lookback_years=1,
            )

        has_data = bool(main_payload.get("contracts") and main_payload.get("series"))
        return {
            "product": product_code,
            "product_name": product_info["name"],
            "is_index": is_index,
            "has_data": has_data,
            "window": window_key,
            "window_label": TERM_WINDOW_LABELS.get(window_key, window_key),
            "slots": slot_count,
            "windows": TERM_STRUCTURE_WINDOWS,
            "main": main_payload,
            "basis_anchor": basis_anchor,
            "basis_longterm": basis_longterm,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"期限结构获取失败: {e}")


@app.get("/api/market/chaos", tags=["行情"])
def market_chaos(username: str = Depends(get_current_user)):
    """
    世界混乱指数（小程序轻量版）：
    - 核心指数
    - 监控市场（最多12条）
    - 主要推升项（最多5条）
    - 风险来源分布
    """
    _ = username
    try:
        snapshot = de.get_latest_geopolitical_risk_snapshot()
        recent_snapshots = de.get_recent_geopolitical_risk_snapshots(limit=8)
        trend_map = _build_chaos_market_trend_map(
            recent_snapshots if isinstance(recent_snapshots, list) else [],
            threshold=0.005,
        )
        return _build_chaos_payload(
            snapshot if isinstance(snapshot, dict) else {},
            trend_map=trend_map,
        )
    except Exception as e:
        print(f"[market_chaos] fallback_on_error: {e}", flush=True)
        return _empty_chaos_payload()


@app.get("/api/market/options", tags=["行情"])
def market_options(username: str = Depends(get_current_user)):
    """
    商品期权行情：合约名称、当前IV、IV Rank、当日涨跌%、当日IV变动。
    建议客户端缓存 60 秒。
    """
    try:
        df = de.get_comprehensive_market_data()
        if isinstance(df, dict) and df.get("error"):
            print(f"[market_options] data_error: {df.get('error')}", flush=True)
            return {"items": [], "updated_at": ""}
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"items": [], "updated_at": ""}

        import pandas as pd
        option_product_codes = _get_option_product_codes()
        records = []
        for _, row in df.iterrows():
            iv_rank = row.get("IV Rank", 0)
            is_expiring = str(iv_rank).strip() == "快到期"
            raw_iv = _safe_float(row.get("当前IV"), 0.0)
            iv_rank_num = (
                _safe_float(iv_rank, IV_RANK_EXPIRING)
                if iv_rank not in ("快到期", None, "")
                else IV_RANK_EXPIRING
            )

            # 提取品种代码（合约格式如 "m2605 (豆粕)"，取括号前的字母部分）
            name_str = str(row.get("合约", ""))
            product_code = _extract_product_code_from_contract(name_str)
            has_option = (
                product_code in option_product_codes
                or _row_implies_has_option(raw_iv, iv_rank)
            )
            # IV变动(日)：优先使用综合数据，缺失时走历史回退计算
            iv_chg_raw = row.get("IV变动(日)", None)
            iv_chg_missing = _is_missing_value(iv_chg_raw)
            raw_iv_chg = 0.0 if iv_chg_missing else _safe_float(iv_chg_raw, 0.0)

            # IV Rank 状态归一化：
            # -2: 无期权；-1: 快到期；-3: 有期权但缺IV；>=0: 正常分位
            if not has_option:
                iv_rank_num = IV_RANK_NO_OPTION
            elif is_expiring:
                iv_rank_num = IV_RANK_EXPIRING
            elif raw_iv <= 0:
                iv_rank_num = IV_RANK_MISSING

            records.append({
                "name":         name_str,
                "product_code": product_code,
                "iv":           round(raw_iv, 1),
                "iv_rank":      iv_rank_num,
                "iv_chg_1d":    round(raw_iv_chg, 2),
                "pct_1d":       round(_safe_float(row.get("涨跌%(日)"), 0.0), 2),
                "pct_5d":       round(_safe_float(row.get("涨跌%(5日)"), 0.0), 2),
                "retail_chg":   int(_safe_float(row.get("散户变动(日)"), 0.0)),
                "inst_chg":     int(_safe_float(row.get("机构变动(日)"), 0.0)),
                "cur_price":    0.0,  # 下面批量回填
                "_iv_chg_missing": iv_chg_missing,
            })

        # ── IV变动缺失回退：按最近两个收盘日计算（合约级）──────────
        if records:
            try:
                code_set = set()
                for r in records:
                    c = r["name"].split("(")[0].strip().lower()
                    if re.match(r"^[a-z]+[0-9]{3,4}$", c):
                        code_set.add(c)
                if code_set:
                    codes_sql = "','".join(sorted(code_set))
                    iv_recent_df = pd.read_sql(
                        f"""
                        SELECT
                            LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code,
                            REPLACE(trade_date,'-','')              AS td,
                            iv
                        FROM commodity_iv_history
                        WHERE LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) IN ('{codes_sql}')
                          AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 14 DAY)
                          AND iv IS NOT NULL
                        ORDER BY code ASC, td DESC
                        """,
                        de.engine
                    )
                    iv_fallback_map = _compute_iv_chg_fallback_map(iv_recent_df)
                    for r in records:
                        code = r["name"].split("(")[0].strip().lower()
                        if code in iv_fallback_map:
                            # 只要历史两日可算，优先采用回退值（覆盖 0 占位场景）
                            r["iv_chg_1d"] = iv_fallback_map[code]
            except Exception:
                pass

        # ── 批量查询最新收盘价 ────────────────────────────────
        db_trade_day_map: dict[str, str] = {}

        if records:
            try:
                # name 格式如 "EB2604 (苯乙烯)"，提取合约代码小写
                codes = list({r["name"].split("(")[0].strip().lower() for r in records})
                codes_sql = "','".join(codes)
                price_df = pd.read_sql(
                    f"""
                    SELECT
                        LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) AS code,
                        REPLACE(trade_date,'-','')              AS td,
                        close_price
                    FROM futures_price
                    WHERE LOWER(SUBSTRING_INDEX(ts_code, '.', 1)) IN ('{codes_sql}')
                      AND ts_code NOT LIKE '%%TAS%%'
                      AND trade_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                    ORDER BY trade_date DESC
                    """,
                    de.engine
                )
                if not price_df.empty:
                    latest_price_df = price_df.drop_duplicates(subset=["code"], keep="first")
                    price_map = latest_price_df.set_index("code")["close_price"].to_dict()
                    db_trade_day_map = latest_price_df.set_index("code")["td"].astype(str).to_dict()
                    for r in records:
                        code = r["name"].split("(")[0].strip().lower()
                        r["cur_price"] = round(_safe_float(price_map.get(code, 0), 0.0), 2)
                        r["_db_td"] = str(db_trade_day_map.get(code) or "")
            except Exception:
                pass  # 价格查询失败不影响其他字段

        # ── 用实时缓存覆盖列表价格/涨跌：
        # 1) 实时交易日 > DB交易日：无条件覆盖（直到DB追平）
        # 2) 同交易日：仅新鲜快照覆盖
        try:
            live_payload = _load_shared_prices_payload()
            fresh_live_contracts = _get_fresh_live_contracts_map(live_payload)
            all_live_contracts = live_payload.get("contracts", {}) if isinstance(live_payload, dict) else {}
            if isinstance(all_live_contracts, dict) and all_live_contracts:
                for r in records:
                    contract_code = _extract_contract_code(r.get("name", ""))
                    if not contract_code:
                        continue
                    live = all_live_contracts.get(contract_code)
                    if not isinstance(live, dict):
                        continue
                    db_td = str(r.get("_db_td") or "")
                    is_fresh = isinstance(fresh_live_contracts.get(contract_code), dict)
                    if not _should_use_live_contract_for_display(live, db_td, fresh=is_fresh):
                        continue
                    live_price = _safe_float(live.get("price"), 0.0)
                    if live_price > 0:
                        r["cur_price"] = round(live_price, 2)
                    if live.get("pct") is not None:
                        r["pct_1d"] = round(_safe_float(live.get("pct"), 0.0), 2)
        except Exception:
            pass

        for r in records:
            r.pop("_iv_chg_missing", None)
            r.pop("_db_td", None)

        # 按 IV Rank 降序排列，快到期(-1)排最后
        records.sort(key=lambda x: x["iv_rank"] if x["iv_rank"] >= 0 else -999, reverse=True)
        import datetime
        return {"items": records, "updated_at": datetime.datetime.now().strftime("%H:%M")}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"期权行情获取失败: {e}")


@app.get("/api/market/contracts/{product}", tags=["行情"])
def market_contracts(product: str, username: str = Depends(get_current_user)):
    """
    指定品种的全部月份合约行情（供 App 品种 chip 点击后展开）。
    product: 品种代码小写，如 m / rb / cu
    返回格式与 /api/market/options 的 OptionItem 相同。
    """
    try:
        import pandas as pd
        import datetime as dt
        from sqlalchemy import text as _text

        prod = product.strip().lower()
        option_product_codes = _get_option_product_codes()
        has_option = prod in option_product_codes
        pattern = f'^{prod}[0-9]'  # 匹配 m2605 / m2609 等

        # ── 最近两个交易日（按该品种自身日历）──
        dates_sql = _text(
            "SELECT DISTINCT REPLACE(trade_date,'-','') as td FROM futures_price "
            "WHERE ts_code REGEXP :pattern AND ts_code NOT LIKE '%%TAS%%' "
            "ORDER BY td DESC LIMIT 2"
        )
        dates_df = pd.read_sql(dates_sql, de.engine, params={"pattern": pattern})
        if dates_df.empty:
            return {"items": []}
        latest_date = str(dates_df.iloc[0]['td'])
        prev_date   = str(dates_df.iloc[1]['td']) if len(dates_df) > 1 else ""

        # ── 最新日 + 前一日价格（一次取，用于计算涨跌）──
        if prev_date:
            price_sql = _text(
                "SELECT ts_code, close_price, oi, "
                "       REPLACE(trade_date,'-','') as trade_date "
                "FROM futures_price "
                "WHERE REPLACE(trade_date,'-','') IN (:latest_date, :prev_date) "
                "  AND ts_code REGEXP :pattern "
                "  AND ts_code NOT LIKE '%%TAS%%' "
                "  AND oi > 0"
            )
            price_params = {"latest_date": latest_date, "prev_date": prev_date, "pattern": pattern}
        else:
            price_sql = _text(
                "SELECT ts_code, close_price, oi, "
                "       REPLACE(trade_date,'-','') as trade_date "
                "FROM futures_price "
                "WHERE REPLACE(trade_date,'-','') = :latest_date "
                "  AND ts_code REGEXP :pattern "
                "  AND ts_code NOT LIKE '%%TAS%%' "
                "  AND oi > 0"
            )
            price_params = {"latest_date": latest_date, "pattern": pattern}
        df_price_all = pd.read_sql(price_sql, de.engine, params=price_params)
        df_price = df_price_all[df_price_all['trade_date'] == latest_date].copy()
        df_price_prev = df_price_all[df_price_all['trade_date'] == prev_date].copy() if prev_date else pd.DataFrame()
        if df_price.empty:
            return {"items": []}
        df_price = df_price.sort_values('oi', ascending=False)

        # 前一日收盘价 map（用于计算涨跌）
        prev_close_map: dict = {}
        if not df_price_prev.empty:
            prev_close_map = dict(zip(df_price_prev['ts_code'], df_price_prev['close_price']))

        # ── IV：取 commodity_iv_history 最近两个日期（各自的日历）──
        iv_dates_sql = _text(
            "SELECT DISTINCT REPLACE(trade_date,'-','') as td FROM commodity_iv_history "
            "WHERE ts_code REGEXP :pattern ORDER BY td DESC LIMIT 2"
        )
        iv_dates_df = pd.read_sql(iv_dates_sql, de.engine, params={"pattern": pattern})
        iv_latest_date = str(iv_dates_df.iloc[0]['td']) if not iv_dates_df.empty else ""
        iv_prev_date   = str(iv_dates_df.iloc[1]['td']) if len(iv_dates_df) > 1 else ""

        iv_map: dict = {}
        iv_prev_map: dict = {}
        if iv_latest_date:
            if iv_prev_date:
                iv_sql = _text("""
                    SELECT ts_code, iv, REPLACE(trade_date,'-','') as td
                    FROM commodity_iv_history
                    WHERE ts_code REGEXP :pattern
                      AND REPLACE(trade_date,'-','') IN (:iv_latest_date, :iv_prev_date)
                """)
                iv_params = {"pattern": pattern, "iv_latest_date": iv_latest_date, "iv_prev_date": iv_prev_date}
            else:
                iv_sql = _text("""
                    SELECT ts_code, iv, REPLACE(trade_date,'-','') as td
                    FROM commodity_iv_history
                    WHERE ts_code REGEXP :pattern
                      AND REPLACE(trade_date,'-','') = :iv_latest_date
                """)
                iv_params = {"pattern": pattern, "iv_latest_date": iv_latest_date}
            df_iv = pd.read_sql(iv_sql, de.engine, params=iv_params)
            iv_map      = dict(zip(df_iv[df_iv['td'] == iv_latest_date]['ts_code'],
                                   df_iv[df_iv['td'] == iv_latest_date]['iv']))
            if iv_prev_date:
                iv_prev_map = dict(zip(df_iv[df_iv['td'] == iv_prev_date]['ts_code'],
                                       df_iv[df_iv['td'] == iv_prev_date]['iv']))

        # ── 1年 IV 历史（计算 IV Rank）──
        date_1y = (dt.datetime.now() - dt.timedelta(days=365)).strftime('%Y%m%d')
        iv_hist_sql = _text("""
            SELECT ts_code, iv FROM commodity_iv_history
            WHERE REPLACE(trade_date,'-','') >= :date_1y
              AND ts_code REGEXP :pattern
              AND iv > 0
        """)
        df_iv_hist = pd.read_sql(iv_hist_sql, de.engine, params={"date_1y": date_1y, "pattern": pattern})
        if not df_iv_hist.empty:
            df_iv_hist["product"] = (
                df_iv_hist["ts_code"]
                .astype(str)
                .str.split(".", n=1)
                .str[0]
                .str.extract(r"^([A-Za-z]+)", expand=False)
                .str.lower()
            )

        def iv_rank(ts_code: str, cur_iv: float) -> float:
            if cur_iv <= 0:
                return IV_RANK_MISSING
            hist = df_iv_hist[df_iv_hist['ts_code'] == ts_code]['iv']
            if len(hist) < 20 and "product" in df_iv_hist.columns:
                hist = df_iv_hist[df_iv_hist['product'] == prod]['iv']
            if len(hist) < 20:
                return IV_RANK_MISSING
            return round((hist < cur_iv).sum() / len(hist) * 100, 1)

        records = []
        for _, row in df_price.iterrows():
            ts = str(row['ts_code'])
            cur_close  = float(row.get('close_price', 0) or 0)
            prev_close = float(prev_close_map.get(ts, 0) or 0)
            pct_1d = round((cur_close - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0.0

            cur_iv  = float(iv_map.get(ts, 0) or 0)
            prev_iv = float(iv_prev_map.get(ts, 0) or 0)
            iv_chg  = round(cur_iv - prev_iv, 2) if prev_iv > 0 else 0.0
            row_has_option = has_option or cur_iv > 0 or prev_iv > 0
            if not row_has_option:
                rank = IV_RANK_NO_OPTION
            else:
                rank = iv_rank(ts, cur_iv)

            records.append({
                "name":         ts.upper(),
                "product_code": prod,
                "iv":           round(cur_iv, 1),
                "iv_rank":      rank,
                "iv_chg_1d":    iv_chg,
                "pct_1d":       pct_1d,
                "pct_5d":       0.0,
                "retail_chg":   0,
                "inst_chg":     0,
            })

        records.sort(key=lambda x: x['oi'] if 'oi' in x else 0, reverse=True)
        return {"items": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"合约列表获取失败: {e}")


@app.get("/api/market/holding/{product}", tags=["行情"])
def market_holding(product: str, username: str = Depends(get_current_user)):
    """
    指定品种的期货商持仓分析：各期货商得分排名（近150天累计）。
    product: 品种代码，如 m / rb / cu / sc
    """
    try:
        symbol = product.lower()   # 与桌面端一致，传品种代码如 'm'、'rb'、'cu'
        df = de.calculate_broker_rankings(symbol=symbol, lookback_days=150)
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"product": product, "brokers": [], "trade_date": ""}

        # 聚合：每家期货商的累计得分 + 最新净持仓方向
        df["broker"] = df["broker"].str.replace(r"[（\(]代客[）\)]", "", regex=True).str.strip()
        agg = df.groupby("broker").agg(
            score=("score", "sum"),
            net_vol=("net_vol", "last"),
        ).reset_index()
        agg = agg.sort_values("score", ascending=False)

        latest_date = df["trade_date"].max() if not df.empty else ""

        brokers = []
        for _, row in agg.iterrows():
            s = float(row["score"])
            nv = float(row["net_vol"])
            if abs(s) < 1:
                continue
            brokers.append({
                "broker":   str(row["broker"]),
                "score":    round(s, 0),
                "net_vol":  int(nv),
                "direction": "多" if nv > 0 else "空" if nv < 0 else "平",
            })

        return {"product": product, "brokers": brokers, "trade_date": str(latest_date)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"持仓分析失败: {e}")


@app.get("/api/market/broker/{product}", tags=["行情"])
def market_broker_detail(
    product: str,
    broker: str = Query(..., description="期货商名称"),
    username: str = Depends(get_current_user)
):
    """
    指定品种 + 期货商的近150天逐日持仓与得分明细。
    product: 品种代码，如 m / rb / cu
    broker: 期货商名称，如 中信期货
    """
    try:
        symbol = product.lower()
        df = de.calculate_broker_rankings(symbol=symbol, lookback_days=150)
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"product": product, "broker": broker, "rows": [], "total_score": 0}

        df = df.copy()
        df["broker"] = df["broker"].str.replace(r"[（\(]代客[）\)]", "", regex=True).str.strip()
        df_broker = df[df["broker"] == broker].sort_values("trade_date").copy()

        if df_broker.empty:
            return {"product": product, "broker": broker, "rows": [], "total_score": 0}

        df_broker["cum_score"] = df_broker["score"].cumsum()
        total = float(df_broker["score"].sum())

        rows = []
        for _, row in df_broker.iterrows():
            dt = row["trade_date"]
            if hasattr(dt, 'strftime'):
                dt_str = dt.strftime('%Y-%m-%d')
            else:
                dt_str = str(dt)
                if len(dt_str) == 8 and '-' not in dt_str:
                    dt_str = f"{dt_str[:4]}-{dt_str[4:6]}-{dt_str[6:8]}"
            rows.append({
                "dt": dt_str,
                "net_vol": int(row["net_vol"]),
                "pct_chg": round(float(row.get("pct_chg", 0)), 2),  # DB已是百分比格式(2.0=2%)
                "score": round(float(row["score"]), 1),
                "cum_score": round(float(row["cum_score"]), 1),
            })

        return {
            "product": product,
            "broker": broker,
            "total_score": round(total, 0),
            "rows": rows,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"期货商明细获取失败: {e}")


@app.get("/api/market/chart/{product}", tags=["行情"])
def market_chart(
    product: str,
    contract: Optional[str] = Query(default=None, description="可选，指定具体合约，如 MA2609"),
    username: str = Depends(get_current_user),
):
    """
    指定品种近 60 天的价格 + IV 数据，用于前端绘制折线图。
    product: 如 m / rb / cu
    contract: 可选，传入时优先按该合约绘制；不传时沿用主力合约逻辑。
    """
    try:
        import datetime as _dt
        from sqlalchemy import text as _text

        eng = de.engine
        if eng is None:
            raise HTTPException(status_code=500, detail="数据库未连接")

        prod_upper = product.upper()
        cn_name = de.PRODUCT_MAP.get(prod_upper, product.upper())
        selected_contract = str(contract or "").strip().upper()
        selected_contract = selected_contract if re.match(r"^[A-Z]+[0-9]{3,4}$", selected_contract) else ""
        if selected_contract and not selected_contract.startswith(prod_upper):
            selected_contract = ""
        is_trading_now = _is_trading_hours()
        cache_key = _market_chart_cache_key(product, selected_contract)
        try:
            cached_raw = _redis.get(cache_key)
            if cached_raw:
                cached_payload = json.loads(cached_raw)
                if isinstance(cached_payload, dict):
                    if is_trading_now:
                        return cached_payload
                    cache_contract = selected_contract or str(cached_payload.get("main_contract") or "").strip().upper()
                    if cache_contract:
                        live_payload_for_cache = _load_shared_prices_payload()
                        fresh_contracts = _get_fresh_live_contracts_map(live_payload_for_cache)
                        all_contracts = (
                            live_payload_for_cache.get("contracts", {})
                            if isinstance(live_payload_for_cache, dict) else {}
                        )
                        live_row = all_contracts.get(cache_contract) if isinstance(all_contracts, dict) else None
                        db_td_cached = str(cached_payload.get("db_cur_td") or "")
                        is_fresh = isinstance(fresh_contracts.get(cache_contract), dict)
                        if _should_use_live_contract_for_display(live_row, db_td_cached, fresh=is_fresh):
                            live_price = _safe_float((live_row or {}).get("price"), 0.0)
                            cached_cur = _safe_float(cached_payload.get("cur_price"), 0.0)
                            if live_price > 0 and abs(live_price - cached_cur) >= 1e-8:
                                # 收盘后如果实时目标价与缓存价不一致，强制回源重算
                                raise ValueError("stale_chart_cache_vs_live_price")
                    # 收盘后：若缓存是盘中实时覆盖产物（cur != db close），或旧缓存缺少 db 对照字段，则不直接复用
                    db_cur = cached_payload.get("db_cur_price")
                    cur = cached_payload.get("cur_price")
                    if db_cur is not None and cur is not None:
                        if abs(_safe_float(cur) - _safe_float(db_cur)) < 1e-8:
                            return cached_payload
        except Exception as cache_err:
            print(f"[market_chart] cache_read_failed key={cache_key} err={cache_err}", flush=True)

        import pandas as _pd
        since = (_dt.datetime.now() - _dt.timedelta(days=400)).strftime("%Y%m%d")

        # ── 1. 决定图表目标合约 ts_code ─────────────────────────
        # 传入 contract 时优先使用指定合约；否则使用主力合约。
        pattern = f"^{product.upper()}[0-9]"
        pattern2 = f"^{product.lower()}[0-9]"

        if selected_contract:
            main_contract = selected_contract
        else:
            # 用品种代码前缀 REGEXP 匹配，取最新日期持仓量最大的合约（当前主力）
            main_sql = _text("""
                SELECT ts_code FROM futures_price
                WHERE ts_code REGEXP :pattern
                  AND ts_code NOT LIKE '%%TAS%%'
                  AND REPLACE(trade_date,'-','') = (
                      SELECT MAX(REPLACE(trade_date,'-','')) FROM futures_price
                      WHERE ts_code REGEXP :pattern
                        AND ts_code NOT LIKE '%%TAS%%'
                  )
                ORDER BY oi DESC LIMIT 1
            """)
            main_df = _pd.read_sql(main_sql, eng, params={"pattern": pattern})
            if main_df.empty:
                main_sql2 = _text("""
                    SELECT ts_code FROM futures_price
                    WHERE ts_code REGEXP :pattern2
                      AND ts_code NOT LIKE '%%TAS%%'
                      AND REPLACE(trade_date,'-','') = (
                          SELECT MAX(REPLACE(trade_date,'-','')) FROM futures_price
                          WHERE ts_code REGEXP :pattern2
                            AND ts_code NOT LIKE '%%TAS%%'
                      )
                    ORDER BY oi DESC LIMIT 1
                """)
                main_df = _pd.read_sql(main_sql2, eng, params={"pattern2": pattern2})

            if main_df.empty:
                empty_payload = {
                    "product": product.lower(),
                    "cn_name": cn_name,
                    "cur_price": None,
                    "cur_pct": None,
                    "cur_iv": None,
                    "ohlc": [],
                    "iv": [],
                }
                try:
                    _redis.setex(cache_key, _MARKET_CHART_CACHE_TTL, json.dumps(empty_payload, ensure_ascii=False))
                except Exception as cache_err:
                    print(f"[market_chart] cache_write_failed key={cache_key} err={cache_err}", flush=True)
                return empty_payload
            main_contract = str(main_df.iloc[0]["ts_code"]).upper()

        # ── 2. 拉取 OHLC 数据（近1年K线）────────────────────────
        ohlc_sql = _text("""
            SELECT
                REPLACE(trade_date,'-','') as dt,
                open_price  as o,
                high_price  as h,
                low_price   as l,
                close_price as c,
                pct_chg,
                oi
            FROM futures_price
            WHERE UPPER(ts_code) = :main_contract
              AND REPLACE(trade_date,'-','') >= :since
            ORDER BY trade_date ASC
            LIMIT 300
        """)
        ohlc_df = _pd.read_sql(ohlc_sql, eng, params={"main_contract": main_contract, "since": since})
        if not ohlc_df.empty:
            ohlc_df["dt"] = ohlc_df["dt"].astype(str)
        db_cur_price = round(float(ohlc_df.iloc[-1]["c"]), 2) if not ohlc_df.empty else None
        db_cur_pct = round(float(ohlc_df.iloc[-1]["pct_chg"]), 2) if not ohlc_df.empty else None
        db_cur_td = str(ohlc_df.iloc[-1]["dt"]) if not ohlc_df.empty else ""

        # ── 2.1 用实时缓存覆盖末根K线：
        # 若实时交易日领先DB交易日，收盘后也继续保留最后一笔实时价，直到DB追平。
        live_payload = _load_shared_prices_payload()
        fresh_live_contracts = _get_fresh_live_contracts_map(live_payload)
        all_live_contracts = live_payload.get("contracts", {}) if isinstance(live_payload, dict) else {}
        live_row_raw = all_live_contracts.get(main_contract) if isinstance(all_live_contracts, dict) else None
        use_live_for_display = _should_use_live_contract_for_display(
            live_row_raw,
            db_cur_td,
            fresh=isinstance((fresh_live_contracts or {}).get(main_contract), dict),
        )
        live_row = live_row_raw if use_live_for_display else None
        live_td = ""
        live_price = 0.0
        live_pct: Optional[float] = None
        if isinstance(live_row, dict):
            live_td = str(live_row.get("trading_day") or "").strip()
            live_price = _safe_float(live_row.get("price"), 0.0)
            if live_row.get("pct") is not None:
                live_pct = round(_safe_float(live_row.get("pct"), 0.0), 2)

        if live_td and not ohlc_df.empty:
            # 若 DB 出现晚于实时 trading_day 的“幽灵行”，直接剔除，避免多一根K线。
            filtered_df = ohlc_df[ohlc_df["dt"] <= live_td].copy()
            if not filtered_df.empty:
                ohlc_df = filtered_df

        if live_price > 0:
            if ohlc_df.empty:
                dt_value = live_td or datetime.now().strftime("%Y%m%d")
                pct_value = live_pct if live_pct is not None else 0.0
                ohlc_df = _pd.DataFrame([{
                    "dt": dt_value,
                    "o": live_price,
                    "h": live_price,
                    "l": live_price,
                    "c": live_price,
                    "pct_chg": pct_value,
                    "oi": 0,
                }])
            else:
                last_idx = ohlc_df.index[-1]
                last_dt = str(ohlc_df.at[last_idx, "dt"])
                if live_td and last_dt == live_td:
                    last_open = _safe_float(ohlc_df.at[last_idx, "o"], live_price)
                    last_high = _safe_float(ohlc_df.at[last_idx, "h"], live_price)
                    last_low = _safe_float(ohlc_df.at[last_idx, "l"], live_price)
                    ohlc_df.at[last_idx, "c"] = live_price
                    ohlc_df.at[last_idx, "h"] = max(last_high, live_price, last_open)
                    ohlc_df.at[last_idx, "l"] = min(last_low, live_price, last_open)
                    if live_pct is not None:
                        ohlc_df.at[last_idx, "pct_chg"] = live_pct
                elif live_td and last_dt < live_td:
                    prev_close = _safe_float(ohlc_df.at[last_idx, "c"], live_price)
                    if live_pct is not None:
                        pct_value = live_pct
                    elif prev_close > 0:
                        pct_value = round((live_price - prev_close) / prev_close * 100, 2)
                    else:
                        pct_value = 0.0
                    new_row = {
                        "dt": live_td,
                        "o": prev_close if prev_close > 0 else live_price,
                        "h": max(prev_close, live_price) if prev_close > 0 else live_price,
                        "l": min(prev_close, live_price) if prev_close > 0 else live_price,
                        "c": live_price,
                        "pct_chg": pct_value,
                        "oi": _safe_float(ohlc_df.at[last_idx, "oi"], 0.0),
                    }
                    ohlc_df = _pd.concat([ohlc_df, _pd.DataFrame([new_row])], ignore_index=True)

        # ── 3. 拉取 IV 历史（与价格数据时间范围对齐）────────────
        if selected_contract:
            iv_sql = _text("""
                SELECT REPLACE(trade_date,'-','') as dt, iv
                FROM commodity_iv_history
                WHERE UPPER(ts_code) = :main_contract
                  AND REPLACE(trade_date,'-','') >= :since
                ORDER BY trade_date ASC
            """)
            iv_df = _pd.read_sql(iv_sql, eng, params={"main_contract": main_contract, "since": since})
        else:
            iv_sql = _text("""
                SELECT REPLACE(trade_date,'-','') as dt, iv
                FROM commodity_iv_history
                WHERE ts_code REGEXP :pattern
                  AND REPLACE(trade_date,'-','') >= :since
                ORDER BY trade_date ASC
            """)
            iv_df = _pd.read_sql(iv_sql, eng, params={"pattern": pattern, "since": since})
            if iv_df.empty:
                # 尝试小写
                iv_sql2 = _text("""
                    SELECT REPLACE(trade_date,'-','') as dt, iv
                    FROM commodity_iv_history
                    WHERE ts_code REGEXP :pattern2
                      AND REPLACE(trade_date,'-','') >= :since
                    ORDER BY trade_date ASC
                """)
                iv_df = _pd.read_sql(iv_sql2, eng, params={"pattern2": pattern2, "since": since})

        # 不指定合约时，IV按全品种日均；指定合约时按该合约原值。
        if not iv_df.empty and not selected_contract:
            iv_df = iv_df.groupby("dt")["iv"].mean().reset_index().sort_values("dt")

        # ── 4. 拉取反指标/正指标持仓数据 ─────────────────────────
        BROKERS_DUMB  = ['中信建投', '东方财富', '方正中期']
        BROKERS_SMART = ['海通期货', '东证期货', '国泰君安']
        hold_product  = ''.join(c for c in product if not c.isalpha() or True).lower()
        hold_product  = ''.join(c for c in product.lower() if c.isalpha())

        hold_df = _pd.DataFrame()
        try:
            hold_sql = _text("""
                SELECT REPLACE(trade_date,'-','') as dt, broker,
                       long_vol, short_vol
                FROM futures_holding
                WHERE ts_code = :hold_product
                  AND REPLACE(trade_date,'-','') >= :since
                ORDER BY trade_date ASC
            """)
            hold_df = _pd.read_sql(hold_sql, eng, params={"hold_product": hold_product, "since": since})
        except Exception:
            pass

        dumb_list = []
        smart_list = []
        total_oi_list = []
        dumb_chg_1d = None

        if not hold_df.empty:
            hold_df['net_vol'] = hold_df['long_vol'].fillna(0) - hold_df['short_vol'].fillna(0)
            # 清理代客后缀
            hold_df['broker_clean'] = hold_df['broker'].astype(str).str.replace(
                r'[（\(]代客[）\)]', '', regex=True).str.strip()

            def _broker_type(b):
                for d in BROKERS_DUMB:
                    if d in b: return 'dumb'
                for s in BROKERS_SMART:
                    if s in b: return 'smart'
                return 'other'

            hold_df['type'] = hold_df['broker_clean'].apply(_broker_type)

            dumb_agg  = hold_df[hold_df['type']=='dumb'].groupby('dt')['net_vol'].sum().reset_index().sort_values('dt')
            smart_agg = hold_df[hold_df['type']=='smart'].groupby('dt')['net_vol'].sum().reset_index().sort_values('dt')

            dumb_agg['chg']  = dumb_agg['net_vol'].diff(1).fillna(0)
            smart_agg['chg'] = smart_agg['net_vol'].diff(1).fillna(0)

            dumb_list  = [{"dt": str(r["dt"]), "net": int(r["net_vol"]), "chg": int(r["chg"])} for _, r in dumb_agg.iterrows()]
            smart_list = [{"dt": str(r["dt"]), "net": int(r["net_vol"]), "chg": int(r["chg"])} for _, r in smart_agg.iterrows()]
            dumb_chg_1d = dumb_list[-1]["chg"] if dumb_list else None

        # 总持仓量（主力合约 oi）
        if not ohlc_df.empty and 'oi' in ohlc_df.columns:
            total_oi_list = [
                {"dt": str(r["dt"]), "v": int(r["oi"]) if r["oi"] == r["oi"] else 0}
                for _, r in ohlc_df.iterrows()
            ]

        # ── 5. 整理输出 ───────────────────────────────────────
        ohlc_list = []
        for _, r in ohlc_df.iterrows():
            try:
                ohlc_list.append({
                    "dt": str(r["dt"]),
                    "o": round(float(r["o"]), 2),
                    "h": round(float(r["h"]), 2),
                    "l": round(float(r["l"]), 2),
                    "c": round(float(r["c"]), 2),
                    "pct": round(float(r["pct_chg"]), 2),
                })
            except Exception:
                pass

        # iv 已是百分比形式（如 18.53 = 18.53%），直接返回
        iv_list = [
            {"dt": str(r["dt"]), "v": round(float(r["iv"]), 2)}
            for _, r in iv_df.iterrows()
            if r["iv"] is not None
        ] if not iv_df.empty else []

        cur_price = ohlc_list[-1]["c"] if ohlc_list else None
        cur_pct   = round(float(ohlc_df.iloc[-1]["pct_chg"]), 2) if not ohlc_df.empty else None
        if live_price > 0:
            cur_price = round(live_price, 2)
        if live_pct is not None:
            cur_pct = live_pct
        cur_iv    = iv_list[-1]["v"] if iv_list else None

        payload = {
            "product":       product.lower(),
            "cn_name":       cn_name,
            "main_contract": main_contract,
            "cur_price":     cur_price,
            "cur_pct":       cur_pct,
            "db_cur_price":  db_cur_price,
            "db_cur_pct":    db_cur_pct,
            "db_cur_td":     db_cur_td,
            "cur_iv":        cur_iv,
            "dumb_chg_1d":   dumb_chg_1d,
            "ohlc":          ohlc_list,
            "iv":            iv_list,
            "dumb":          dumb_list,
            "smart":         smart_list,
            "total_oi":      total_oi_list,
        }
        try:
            _redis.setex(cache_key, _MARKET_CHART_CACHE_TTL, json.dumps(payload, ensure_ascii=False))
        except Exception as cache_err:
            print(f"[market_chart] cache_write_failed key={cache_key} err={cache_err}", flush=True)
        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"图表数据获取失败: {e}")


@app.get("/api/market/prices", tags=["行情"])
def market_prices(username: str = Depends(get_current_user)):
    """
    Shared futures prices from server-side cache.
    All users read the same Redis snapshot; no repeated upstream fetch per request.
    """
    _touch_prices_consumer_heartbeat()

    try:
        raw = _redis.get(_PRICES_KEY)
    except Exception as e:
        print(f"[market_prices] REDIS_READ_FAIL: {e}", flush=True)
        raw = None

    if raw:
        try:
            payload = json.loads(raw)
            if isinstance(payload, dict):
                _save_last_prices_payload(payload)
                return payload
        except Exception as e:
            print(f"[market_prices] JSON_DECODE_FAIL: {e}", flush=True)

    # Redis cold-start or temporary failure fallback
    local_fallback = _load_last_prices_payload()
    if local_fallback.get("items") or local_fallback.get("contracts"):
        return local_fallback

    return {"items": [], "is_trading": _is_trading_hours(), "refreshed_at": "", "refreshed_ts": 0, "contracts": {}}


# ════════════════════════════════════════════════════════════
#  PORTFOLIO — 持仓体检
# ════════════════════════════════════════════════════════════

class _BytesFileWrapper(io.BytesIO):
    """让 BytesIO 兼容 vision_tools.analyze_portfolio_image 的 seek/read 接口。"""
    pass


def _render_mobile_option_leg(leg: Dict[str, Any]) -> str:
    month = leg.get("month")
    month_text = f"{int(month)}月" if isinstance(month, (int, float)) else ""
    strike = leg.get("strike")
    strike_text = f"{float(strike):.3f}".rstrip("0").rstrip(".") if isinstance(strike, (int, float)) else "待确认"
    cp = "认购" if str(leg.get("cp", "")).lower() == "call" else "认沽"
    side = "买方" if str(leg.get("side", "")).lower() == "long" else "卖方"
    qty = int(leg.get("qty") or abs(int(leg.get("signed_qty", 0))) or 0)
    return f"{month_text}{strike_text}{cp}{side}{qty}张"


def _build_mobile_upload_option_prompt(vision_result: Dict[str, Any]) -> str:
    domain = str(vision_result.get("domain", "")).strip().lower()
    option_legs = vision_result.get("option_legs") or []
    legs_text = "；".join(_render_mobile_option_leg(leg) for leg in option_legs[:6] if isinstance(leg, dict))
    underlying_hint = ""
    if option_legs:
        underlying_hint = str((option_legs[0] or {}).get("underlying_hint") or "").strip().upper()
    prefix = f"我上传了{underlying_hint}期权持仓截图。" if underlying_hint else "我上传了期权持仓截图。"
    mixed_hint = "已识别到股票持仓，本轮未展开股票体检。" if domain == "mixed" else ""
    return (
        f"{prefix}{mixed_hint}识别到的期权腿：{legs_text or '待确认'}。"
        "请按期权持仓深度模板输出，重点给出DeltaCash、Delta Ratio、目标区间、偏离与建议调整量。"
    )


@app.post("/api/position/upload", tags=["持仓体检"])
async def position_upload(
    file: UploadFile = File(..., description="持仓截图，支持 jpg/png，不超过 10MB"),
    username: str = Depends(get_current_user),
):
    """上传持仓截图后自动分流：股票->体检任务，期权/混合->聊天分析任务。"""
    image_bytes = await file.read()
    if len(image_bytes) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="图片不能超过 10MB")
    if not image_bytes:
        raise HTTPException(status_code=400, detail="图片内容为空")

    screenshot_hash = hashlib.md5(image_bytes).hexdigest()
    file_wrapper = _BytesFileWrapper(image_bytes)
    vision_result = analyze_position_image(file_wrapper)
    if not vision_result.get("ok"):
        raise HTTPException(
            status_code=422,
            detail=vision_result.get("error") or "无法识别持仓截图，请上传清晰的持仓页截图",
        )

    domain = str(vision_result.get("domain", "unknown")).strip().lower()
    stock_positions = vision_result.get("stock_positions") or []
    option_legs = vision_result.get("option_legs") or []

    if domain == "stock":
        if not stock_positions:
            raise HTTPException(status_code=422, detail="未从截图中识别到股票持仓，请确认截图内容")
        task_id = TaskManager.create_portfolio_task(
            user_id=username,
            positions=stock_positions,
            screenshot_hash=screenshot_hash,
            source_text=vision_result.get("raw_text", ""),
        )
        return {
            "task_id": task_id,
            "task_kind": "portfolio",
            "recognized_count": len(stock_positions),
            "domain": domain,
            "message": f"已识别 {len(stock_positions)} 只股票持仓，正在生成体检报告...",
        }

    if domain in {"option", "mixed"}:
        if not option_legs:
            raise HTTPException(status_code=422, detail="识别到期权域，但未提取到有效期权持仓腿")
        profile = de.get_user_profile(username) or {}
        risk = str(profile.get("risk_preference") or "稳健型")
        prompt = _build_mobile_upload_option_prompt(vision_result)
        context_payload = _build_mobile_context_payload(
            prompt_text=prompt,
            current_user=username,
            history=[],
            profile=profile,
        )
        context_payload["vision_position_payload"] = vision_result
        context_payload["vision_position_domain"] = domain
        has_portfolio = _detect_mobile_has_portfolio(username)
        task_id = TaskManager.create_task(
            user_id=username,
            prompt=prompt,
            image_context="",
            risk_preference=risk,
            history_messages=[],
            context_payload=context_payload,
            has_portfolio=has_portfolio,
        )
        return {
            "task_id": task_id,
            "task_kind": "chat",
            "recognized_count": len(option_legs),
            "domain": domain,
            "message": "已识别期权持仓，正在生成Delta与调仓建议...",
        }

    raise HTTPException(status_code=422, detail="未识别到股票或期权持仓，请上传更清晰截图")


@app.post("/api/portfolio/upload", tags=["持仓体检"])
async def portfolio_upload(
    file: UploadFile = File(..., description="持仓截图，支持 jpg/png，不超过 10MB"),
    username: str = Depends(get_current_user),
):
    """
    上传持仓截图，流程:
    1. 视觉 AI 识别截图中的持仓股票
    2. 提交后台 Celery 分析任务
    3. 返回 task_id，客户端轮询 /api/portfolio/status/{task_id}
    """
    image_bytes = await file.read()

    if len(image_bytes) > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="图片不能超过 10MB")

    if not image_bytes:
        raise HTTPException(status_code=400, detail="图片内容为空")

    # 生成截图指纹（去重用）
    screenshot_hash = hashlib.md5(image_bytes).hexdigest()

    # 视觉识别持仓
    file_wrapper = _BytesFileWrapper(image_bytes)
    vision_result = analyze_portfolio_image(file_wrapper)

    if not vision_result.get("ok"):
        raise HTTPException(
            status_code=422,
            detail=vision_result.get("error") or "无法识别持仓截图，请上传清晰的持仓页截图",
        )

    positions = vision_result.get("positions") or []
    if not positions:
        raise HTTPException(status_code=422, detail="未从截图中识别到任何持仓，请确认截图内容")

    # 提交后台分析任务
    task_id = TaskManager.create_portfolio_task(
        user_id=username,
        positions=positions,
        screenshot_hash=screenshot_hash,
        source_text=vision_result.get("raw_text", ""),
    )

    return {
        "task_id": task_id,
        "recognized_count": len(positions),
        "message": f"已识别 {len(positions)} 只持仓，正在生成体检报告...",
    }


@app.get("/api/portfolio/status/{task_id}", tags=["持仓体检"])
def portfolio_status(
    task_id: str,
    username: str = Depends(get_current_user),
):
    """
    轮询持仓分析任务进度。
    status 值: pending | processing | success | error
    """
    status = TaskManager.get_task_status(task_id)
    if status.get("status") in ("success", "error"):
        TaskManager.clear_user_pending_portfolio_task(username)
    return status


@app.get("/api/portfolio/result", tags=["持仓体检"])
def portfolio_result(username: str = Depends(get_current_user)):
    """
    获取用户最新的持仓体检结果（数据库中已完成的分析）。
    has_data=False 表示尚无数据，引导用户上传截图。
    """
    snapshot = de.get_user_portfolio_snapshot(username)
    if not snapshot:
        return {"has_data": False, "snapshot": None}
    return {"has_data": True, "snapshot": snapshot}


# ════════════════════════════════════════════════════════════
#  USER — 用户资料
# ════════════════════════════════════════════════════════════

@app.get("/api/user/profile", tags=["用户"])
def user_profile(username: str = Depends(get_current_user)):
    """获取用户基本资料、风险偏好与当前订阅频道。"""
    profile = de.get_user_profile(username) or {}
    info = auth.get_user_info(username) or {}
    subscriptions = sub_svc.get_user_subscriptions(username) or []

    return {
        "username": username,
        "email": auth.get_masked_email(username),
        "level": info.get("level", 1),
        "risk_preference": profile.get("risk_preference", "未知"),
        "focus_assets": profile.get("focus_assets", ""),
        "subscriptions": [
            {
                # 兼容 subscription_service 返回结构（code/name/expire_at）
                "channel_name": s.get("channel_name") or s.get("name") or "",
                "channel_code": (s.get("channel_code") or s.get("code") or "").strip().lower(),
                "expires_at": _fmt_expire_at(s.get("expires_at") or s.get("expire_at") or ""),
                "is_active": s.get("is_active", False),
            }
            for s in subscriptions
        ],
    }


# ════════════════════════════════════════════════════════════
#  KLINE TRAINING — K线训练
# ════════════════════════════════════════════════════════════

import kline_game as kg

def _parse_kline_bars(df) -> list:
    """从 DataFrame 解析 K 线 bars 列表（公共工具函数）"""
    import pandas as _pd

    def _sf(v):
        try:
            f = float(v)
            return f if _pd.notna(f) else None
        except Exception:
            return None

    bars = []
    for _, r in df.iterrows():
        o, h, l, c = _sf(r.get('open_price')), _sf(r.get('high_price')), _sf(r.get('low_price')), _sf(r.get('close_price'))
        if None in (o, h, l, c):
            continue
        v = _sf(r.get('vol')) or 0.0
        td = r.get('trade_date') if 'trade_date' in r else None
        if td is None and hasattr(r, 'name'):
            td = r.name
        import pandas as _pd2
        if isinstance(td, _pd2.Timestamp):
            td = td.strftime('%Y%m%d')
        elif td is not None:
            td = str(td)[:10].replace('-', '')
        bars.append({'dt': td or '', 'o': o, 'h': h, 'l': l, 'c': c, 'v': v})
    return bars


@app.get("/api/kline/data", tags=["K线训练"])
def kline_data(username: str = Depends(get_current_user)):
    """
    【第一步】仅获取随机K线数据，不创建游戏记录。
    玩家看到K线图开始运行后，再由前端调用 /api/kline/start 创建记录。
    这样加载期间离开不会被判定为中途放弃。
    """
    try:
        symbol, symbol_name, symbol_type, df = kg.get_random_kline_data(bars=100, history_bars=60)
        if df is None or len(df) < 100:
            raise HTTPException(status_code=500, detail="K线数据加载失败，请重试")

        bars = _parse_kline_bars(df)
        if len(bars) < 100:
            raise HTTPException(status_code=500, detail="K线数据质量不足，请重试")

        user_capital = kg.get_user_capital(username) or 100000
        return {
            "symbol": symbol,
            "symbol_name": symbol_name,
            "symbol_type": symbol_type,
            "capital": user_capital,
            "history_count": 60,
            "bars": bars,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"K线数据加载失败: {e}")


@app.post("/api/kline/start", tags=["K线训练"])
def kline_start(body: dict, username: str = Depends(get_current_user)):
    """
    【第二步】K线图开始运行时创建游戏记录（此时才算正式开始游戏）。
    body: {symbol, symbol_name, symbol_type, capital, leverage, speed}
    """
    try:
        symbol      = body.get("symbol", "")
        symbol_name = body.get("symbol_name", "")
        symbol_type = body.get("symbol_type", "futures")
        capital     = float(body.get("capital", 100000))
        leverage    = int(body.get("leverage", 1))
        speed       = int(body.get("speed", 1))
        game_id = kg.start_game(username, symbol, symbol_name, symbol_type, capital, leverage, speed)
        return {"game_id": game_id or 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建游戏记录失败: {e}")


# 保留旧端点兼容性（直接调用新流程）
@app.post("/api/kline/new", tags=["K线训练"])
def kline_new(username: str = Depends(get_current_user)):
    """旧端点，兼容旧版前端，内部等同于 /api/kline/data + /api/kline/start"""
    data = kline_data(username)
    start_body = {"symbol": data["symbol"], "symbol_name": data["symbol_name"],
                  "symbol_type": data["symbol_type"], "capital": data["capital"],
                  "leverage": 1, "speed": 1}
    start_res = kline_start(start_body, username)
    return {**data, "game_id": start_res["game_id"]}


@app.post("/api/kline/trades/batch", tags=["K线训练"])
def kline_trades_batch(body: dict):
    """
    保存 K 线训练交易明细（结算确认前调用）。
    body: {game_id, user_id, symbol, symbol_name, symbol_type, trades:[...]}
    """
    try:
        game_id = int(body.get("game_id") or 0)
        user_id = str(body.get("user_id") or "").strip()
        trades = body.get("trades") or []
        symbol = body.get("symbol")
        symbol_name = body.get("symbol_name")
        symbol_type = body.get("symbol_type")

        result = kg.save_trade_batch(
            game_id=game_id,
            user_id=user_id,
            trades=trades,
            symbol=symbol,
            symbol_name=symbol_name,
            symbol_type=symbol_type,
        )
        if result.get("ok"):
            print(f"[KLINE_TRADES_BATCH] ok: game_id={game_id}, user_id={user_id}, saved={result.get('saved')}, total_rows={result.get('total_rows')}")
            return result

        msg = str(result.get("message") or "save trade batch failed")
        print(f"[KLINE_TRADES_BATCH] fail: game_id={game_id}, user_id={user_id}, msg={msg}")
        raise HTTPException(status_code=400, detail=msg)
    except HTTPException:
        raise
    except Exception as e:
        print(f"[KLINE_TRADES_BATCH] exception: {e}")
        raise HTTPException(status_code=500, detail=f"交易明细保存失败: {e}")


@app.post("/api/kline/save", tags=["K线训练"])
def kline_save(body: dict, username: str = Depends(get_current_user)):
    """
    保存游戏结果（游戏结束时调用）。
    body: {game_id, profit, profit_rate, trade_count, max_drawdown}
    """
    try:
        game_id     = int(body.get('game_id') or 0)
        profit      = float(body.get('profit', 0))
        profit_rate = float(body.get('profit_rate', 0))
        trade_count = int(body.get('trade_count', 0))
        max_drawdown= float(body.get('max_drawdown', 0))
        capital     = float(body.get('capital', 100000))

        if game_id:
            kg.end_game(game_id, username, 'finished', 'completed',
                        profit, profit_rate, capital + profit, trade_count, max_drawdown)

        return {"ok": True, "profit": profit, "profit_rate": profit_rate}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"保存失败: {e}")


@app.get("/api/kline/check", tags=["K线训练"])
def kline_check(username: str = Depends(get_current_user)):
    """检查是否有未完成（中途离开）的游戏，有则返回游戏信息和惩罚金额。"""
    try:
        unfinished = kg.check_unfinished_game(username)
        if unfinished:
            return {
                "has_unfinished": True,
                "game_id": unfinished["id"],
                "symbol_name": unfinished.get("symbol_name", "???"),
                "capital": float(unfinished.get("capital_before", 100000)),
                "penalty": 20000,
            }
        return {"has_unfinished": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"检查失败: {e}")


@app.post("/api/kline/abandon", tags=["K线训练"])
def kline_abandon(body: dict, username: str = Depends(get_current_user)):
    """结算中途放弃的游戏，扣除固定惩罚 -20,000 元。"""
    try:
        game_id = int(body.get("game_id") or 0)
        if not game_id:
            raise HTTPException(status_code=400, detail="缺少 game_id")
        kg.settle_abandoned_game(username, game_id)
        return {"ok": True, "penalty": 20000}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"结算失败: {e}")


@app.get("/api/kline/entry", tags=["K线训练"])
def kline_entry(username: str = Depends(get_current_user)):
    """K线训练入口页：玩家当前资金 + 三榜排行榜（Top 10）。"""
    capital = kg.get_user_capital(username) or 100000
    boards  = kg.get_training_entry_leaderboards(limit=10, min_completed=2)
    return {"capital": capital, "leaderboard": boards}

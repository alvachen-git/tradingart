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
  POST   /api/auth/logout               登出当前设备
  GET    /api/auth/verify               验证 Token

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

  POST   /api/position/upload           上传持仓截图 → 自动分流(股票体检/期权分析)
  POST   /api/portfolio/upload          上传股票持仓截图 → 识别 → 提交体检
  GET    /api/portfolio/status/{id}     轮询持仓分析进度
  GET    /api/portfolio/result          获取最新持仓体检结果

  GET    /api/user/profile              获取用户资料与订阅状态
"""

import hashlib
import io
import json
import math
import os
import re
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, List, Any, Dict

import redis
from fastapi import FastAPI, HTTPException, Depends, UploadFile, File, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel
from llm_compat import ChatTongyiCompat as ChatTongyi

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
)
from chat_context_utils import (
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
import subscription_service as sub_svc
import payment_service as pay_svc
import data_engine as de
from user_profile_memory import build_profile_memory_context
from ai_simulation_service import (
    OFFICIAL_PORTFOLIO_ID,
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
_CHAT_FEEDBACK_SCHEMA_LOCK = threading.Lock()
_CHAT_FEEDBACK_SCHEMA_READY = False
_CHAT_FEEDBACK_SCHEMA_ENGINE_ID = ""
_CHAT_FEEDBACK_DEFAULT_ADMIN_USERS = {"mike0919"}
_CHAT_FEEDBACK_ENV_ADMIN_USERS = {
    item.strip() for item in str(os.getenv("AI_FEEDBACK_ADMIN_USERS", "")).split(",") if item.strip()
}
_CHAT_FEEDBACK_ADMIN_USERS = _CHAT_FEEDBACK_DEFAULT_ADMIN_USERS | _CHAT_FEEDBACK_ENV_ADMIN_USERS

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
        return f if f == f else default
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


def _pack_token(username: str, raw_token: str) -> str:
    _ = username
    return str(raw_token or "")


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
    correction_intent = _infer_correction_intent(
        prompt_text,
        recent_context=recent_context_for_focus,
        recent_focus_topic=focus_topic,
    )

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
    payload = {
        "task_id": task_id,
        "user_id": str(user_id or ""),
        "status": str(status or "").strip() or "pending",
        "error": str(error or "").strip(),
        "created_at": str(existing.get("created_at") or now_iso),
        "updated_at": now_iso,
        "finished_at": now_iso if finished else str(existing.get("finished_at") or ""),
    }
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
    return payload


def _build_mobile_chat_error_response(err_msg: str, code: str = "") -> dict:
    payload = {
        "status": "error",
        "progress": "任务失败",
        "result": None,
        "error": str(err_msg or "分析失败，请稍后重试"),
    }
    if code:
        payload["code"] = code
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
                msg = "AI思考太久，请重新提问。"
            elif status_name == "canceled":
                msg = "任务已取消。"
            else:
                msg = "分析失败，请稍后重试。"
        return _build_mobile_chat_error_response(msg, code=f"task_{status_name}")

    created_ts = _parse_iso_ts(state.get("created_at")) if state else 0.0
    if created_ts > 0 and status_name in {"pending", "processing"}:
        elapsed = time.time() - created_ts
        if elapsed >= _MOBILE_CHAT_MAX_PENDING_SECONDS:
            _write_mobile_chat_state(
                task_id=task_id,
                user_id=str(state.get("user_id") or username),
                status="timeout",
                error="AI思考太久，请重新提问。",
                finished=True,
            )
            TaskManager.complete_user_task(username, task_id)
            _clear_mobile_chat_last_task_if_matches(username, task_id)
            return _build_mobile_chat_error_response("AI思考太久，请重新提问。", code="task_timeout")

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
            correction_intent=bool(context_payload.get("correction_intent", False)),
        )
    context_payload["chat_mode"] = chat_mode

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

    has_portfolio = _detect_mobile_has_portfolio(username)

    if chat_mode == CHAT_MODE_SIMPLE:
        llm_turbo = ChatTongyi(model="qwen-turbo-latest", streaming=False, temperature=0.2)
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
        )
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
        if start_ts > 0 and (time.time() - start_ts) >= _MOBILE_CHAT_MAX_PENDING_SECONDS:
            _write_mobile_chat_state(
                task_id=task_id,
                user_id=username,
                status="timeout",
                error="AI思考太久，请重新提问。",
                finished=True,
            )
            TaskManager.complete_user_task(username, task_id)
            _clear_mobile_chat_last_task_if_matches(username, task_id)
            return _build_mobile_chat_error_response("AI思考太久，请重新提问。", code="task_timeout")
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
        err_msg = "AI思考太久，请重新提问。"

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
    username: str = Depends(get_current_user),
):
    """
    AI炒股总览数据（官方组合）：
    - snapshot / nav_series / positions / trades / latest_review / review_dates / watchlist
    """
    _ = username  # 仅鉴权，不做用户隔离
    nav_days = min(250, max(30, int(nav_days)))
    trades_days = min(40, max(5, int(trades_days)))
    positions_limit = min(50, max(5, int(positions_limit)))
    review_limit = min(260, max(20, int(review_limit)))

    try:
        snapshot = _json_safe_value(ai_get_latest_snapshot(OFFICIAL_PORTFOLIO_ID)) or {}
        if not snapshot.get("has_data"):
            return {
                "has_data": False,
                "portfolio_id": OFFICIAL_PORTFOLIO_ID,
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
                "watchlist": [],
                "fetched_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }

        snapshot_trade_date = str(snapshot.get("trade_date") or "")
        review_dates = ai_get_review_dates(OFFICIAL_PORTFOLIO_ID, limit=review_limit) or []
        if snapshot_trade_date and snapshot_trade_date not in review_dates:
            review_dates = [snapshot_trade_date] + list(review_dates)

        latest_review_date = review_dates[0] if review_dates else snapshot_trade_date
        latest_review = _json_safe_value(
            ai_get_daily_review(OFFICIAL_PORTFOLIO_ID, trade_date=latest_review_date)
        ) or {
            "has_data": False,
            "summary_md": "暂无复盘数据。",
            "buys_md": "",
            "sells_md": "",
            "risk_md": "",
            "next_watchlist": [],
        }

        nav_rows = _df_records_jsonable(ai_get_nav_series(OFFICIAL_PORTFOLIO_ID, days=nav_days), limit=nav_days)
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
                OFFICIAL_PORTFOLIO_ID,
                as_of_date=snapshot_trade_date or None,
                strict_as_of=True,
            )
            # 仅在快照显示有持仓时，才允许回退到最近可用持仓，避免口径错位。
            if getattr(pos_df, "empty", True):
                pos_df = ai_get_positions(
                    OFFICIAL_PORTFOLIO_ID,
                    as_of_date=snapshot_trade_date or None,
                    strict_as_of=False,
                )
            positions = _df_records_jsonable(pos_df, limit=positions_limit)
        for row in positions:
            row["trade_date"] = str(row.get("trade_date") or snapshot_trade_date)

        trades = _df_records_jsonable(ai_get_trades(OFFICIAL_PORTFOLIO_ID, days=trades_days), limit=trades_days * 20)
        for row in trades:
            row["trade_date"] = str(row.get("trade_date") or "")
            if row.get("created_at") is not None:
                row["created_at"] = str(row.get("created_at"))

        watchlist = latest_review.get("next_watchlist") if isinstance(latest_review, dict) else []
        if not isinstance(watchlist, list):
            watchlist = []

        return {
            "has_data": True,
            "portfolio_id": OFFICIAL_PORTFOLIO_ID,
            "snapshot": snapshot,
            "review_dates": [str(d) for d in review_dates],
            "latest_review": latest_review,
            "nav_series": nav_rows,
            "positions": positions,
            "trades": trades,
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
    username: str = Depends(get_current_user),
):
    """获取 AI炒股复盘（日级）。trade_date 可选，格式 YYYYMMDD。"""
    _ = username
    td = _normalize_trade_date_input(trade_date)
    try:
        review = ai_get_daily_review(OFFICIAL_PORTFOLIO_ID, trade_date=td)
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
        if df is None or (hasattr(df, 'empty') and df.empty):
            return {"items": [], "updated_at": ""}

        import pandas as pd
        option_product_codes = _get_option_product_codes()
        records = []
        for _, row in df.iterrows():
            iv_rank = row.get("IV Rank", 0)
            is_expiring = str(iv_rank).strip() == "快到期"
            raw_iv = float(row.get("当前IV", 0) or 0)
            try:
                iv_rank_num = float(iv_rank) if iv_rank not in ("快到期", None, "") else IV_RANK_EXPIRING
            except Exception:
                iv_rank_num = IV_RANK_EXPIRING

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
            raw_iv_chg = 0.0 if iv_chg_missing else float(iv_chg_raw or 0)

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
                "pct_1d":       round(float(row.get("涨跌%(日)", 0) or 0), 2),
                "pct_5d":       round(float(row.get("涨跌%(5日)", 0) or 0), 2),
                "retail_chg":   int(row.get("散户变动(日)", 0) or 0),
                "inst_chg":     int(row.get("机构变动(日)", 0) or 0),
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
                        r["cur_price"] = round(float(price_map.get(code, 0) or 0), 2)
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

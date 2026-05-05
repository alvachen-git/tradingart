from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo


ASIA_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
ASSISTANT_NAME = "爱波塔AI"
PRODUCT_IDENTITY = "你是爱波塔AI，由交易艺术汇团队开发"
SITE_SPECIALTY = "本站更擅长期权、K线、交易知识和市场分析"
TIMEZONE_LABEL = "北京时间（Asia/Shanghai）"

_WEEKDAY_NAMES = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
_TIME_PATTERNS = ("现在几点", "几点了")
_DATE_PATTERNS = ("今天几号", "今天多少号", "现在是几月几号", "今天是几月几号")
_WEEKDAY_PATTERNS = ("今天星期几", "今天周几", "今天礼拜几")


def build_simple_runtime_context(current_user_label: str = "", now: datetime | None = None) -> dict[str, str]:
    if now:
        current = now.replace(tzinfo=ASIA_SHANGHAI_TZ) if now.tzinfo is None else now.astimezone(ASIA_SHANGHAI_TZ)
    else:
        current = datetime.now(ASIA_SHANGHAI_TZ)
    return {
        "assistant_name": ASSISTANT_NAME,
        "product_identity": PRODUCT_IDENTITY,
        "site_specialty": SITE_SPECIALTY,
        "current_date": current.strftime("%Y年%-m月%-d日"),
        "current_time": current.strftime("%H:%M"),
        "current_weekday": _WEEKDAY_NAMES[current.weekday()],
        "timezone_label": TIMEZONE_LABEL,
        "current_user_label": str(current_user_label or "").strip() or "访客",
    }


def maybe_answer_simple_runtime_question(
    user_query: str,
    runtime_context: Mapping[str, Any] | None,
) -> str | None:
    if not runtime_context:
        return None

    normalized = _normalize_query(user_query)
    if not normalized:
        return None

    if any(pattern in normalized for pattern in _TIME_PATTERNS):
        return f"现在是{runtime_context.get('timezone_label', TIMEZONE_LABEL)}{runtime_context.get('current_time', '')}。"
    if any(pattern in normalized for pattern in _DATE_PATTERNS):
        return f"今天是{runtime_context.get('timezone_label', TIMEZONE_LABEL)}{runtime_context.get('current_date', '')}。"
    if any(pattern in normalized for pattern in _WEEKDAY_PATTERNS):
        return f"今天是{runtime_context.get('current_weekday', '')}。"
    return None


def format_simple_runtime_context(runtime_context: Mapping[str, Any] | None) -> str:
    if not runtime_context:
        return "无"
    identity = str(runtime_context.get("product_identity", "") or "").strip()
    specialty = str(runtime_context.get("site_specialty", "") or "").strip()
    current_date = str(runtime_context.get("current_date", "") or "").strip()
    current_time = str(runtime_context.get("current_time", "") or "").strip()
    current_weekday = str(runtime_context.get("current_weekday", "") or "").strip()
    timezone_label = str(runtime_context.get("timezone_label", "") or "").strip()
    current_user_label = str(runtime_context.get("current_user_label", "") or "").strip()

    lines = []
    if identity:
        lines.append(f"- 身份：{identity}")
    if specialty:
        lines.append(f"- 站点特色：{specialty}")
    if current_date or current_time or current_weekday:
        time_bits = " ".join(bit for bit in (current_date, current_weekday, current_time) if bit)
        lines.append(f"- 当前时间：{time_bits}".rstrip())
    if timezone_label:
        lines.append(f"- 时区：{timezone_label}")
    if current_user_label:
        lines.append(f"- 当前用户：{current_user_label}")
    return "\n".join(lines) if lines else "无"


def _normalize_query(user_query: str) -> str:
    normalized = str(user_query or "").strip().replace("？", "?").replace("。", "").replace("，", "")
    return normalized

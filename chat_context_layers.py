import json
import os
from datetime import datetime
from typing import Any, Iterable, Mapping


CONTEXT_LAYER_TRACE_TTL_SECONDS = int(
    str(os.getenv("CHAT_CONTEXT_TRACE_TTL_SECONDS", "86400")).strip() or 86400
)
CONTEXT_LAYER_TRACE_MAX_EVENTS = int(
    str(os.getenv("CHAT_CONTEXT_TRACE_MAX_EVENTS", "80")).strip() or 80
)

_LAYER_LIMITS = {
    "recent_turns": 1200,
    "long_memory": 1500,
    "profile": 1200,
    "focus": 700,
    "route_policy": 1000,
}

_LAYER_HEADINGS = {
    "recent_turns": "近期对话历史",
    "long_memory": "相关长期记忆",
    "profile": "用户专属画像",
    "focus": "当前上下文焦点",
    "route_policy": "追问/路由策略",
}

_LAYER_ORDER = ["recent_turns", "long_memory", "profile", "focus", "route_policy"]


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _truncate_text(value: Any, max_chars: int) -> str:
    text = _safe_text(value)
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


def _json_preview(value: Any, max_chars: int = 700) -> str:
    if not value:
        return ""
    try:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except Exception:
        text = str(value)
    return _truncate_text(text, max_chars)


def _build_layer(
    layer: str,
    content: Any,
    *,
    source: str,
    include_reason: str,
    topic: str = "",
    trust: str = "",
) -> dict:
    limit = _LAYER_LIMITS.get(layer, 1000)
    text = _truncate_text(content, limit)
    if not text:
        return {}
    return {
        "layer": layer,
        "content": text,
        "source": source,
        "include_reason": include_reason,
        "topic": _safe_text(topic),
        "trust": _safe_text(trust),
        "char_count": len(text),
    }


def _focus_layer_content(payload: Mapping[str, Any]) -> str:
    lines = []
    fields = [
        ("核心实体", payload.get("focus_entity")),
        ("核心主题", payload.get("focus_topic")),
        ("细分维度", payload.get("focus_aspect")),
        ("模式提示", payload.get("focus_mode_hint")),
        ("追问目标", payload.get("followup_goal")),
    ]
    for label, value in fields:
        text = _safe_text(value)
        if text:
            lines.append(f"{label}: {text}")
    confidence = payload.get("anchor_confidence")
    try:
        confidence_value = float(confidence)
    except Exception:
        confidence_value = 0.0
    if confidence_value > 0:
        lines.append(f"锚点置信度: {confidence_value:.2f}")
    if payload.get("followup_anchor_ambiguous"):
        clarify = _safe_text(payload.get("followup_anchor_clarify"))
        lines.append(f"追问对象可能不明确: {clarify or '是'}")
    return "\n".join(lines)


def _route_layer_content(payload: Mapping[str, Any]) -> str:
    lines = []
    action_context = _safe_text(payload.get("followup_action_context"))
    route_context = _safe_text(payload.get("followup_route_context"))
    task_policy = payload.get("followup_task_policy")
    if action_context:
        lines.append(f"上一轮可执行建议:\n{action_context}")
    if route_context:
        lines.append(route_context)
    if isinstance(task_policy, dict) and task_policy:
        policy_preview = _json_preview(task_policy)
        if policy_preview:
            lines.append(f"追问派工策略JSON:\n{policy_preview}")
    if payload.get("correction_intent"):
        lines.append("当前问题包含纠错/更正意图。")
    return "\n\n".join(lines)


def build_context_layers(
    context_payload: Mapping[str, Any] | None,
    *,
    prompt_text: str = "",
    channel: str = "",
) -> list[dict]:
    payload = context_payload or {}
    topic = _safe_text(payload.get("intent_domain") or payload.get("recent_domain"))
    layers = []

    recent_context = _safe_text(payload.get("recent_context"))
    if recent_context:
        reason = "followup_context" if payload.get("is_followup") else "semantic_context"
        if payload.get("conversation_memory_query") and payload.get("conversation_memory_source") == "recent":
            reason = "conversation_memory_recent"
        layers.append(
            _build_layer(
                "recent_turns",
                recent_context,
                source="session_history",
                include_reason=reason,
                topic=topic,
                trust="conversation",
            )
        )

    memory_context = _safe_text(payload.get("memory_context"))
    if memory_context:
        source = "conversation_memory" if payload.get("conversation_memory_query") else "vector_memory"
        layers.append(
            _build_layer(
                "long_memory",
                memory_context,
                source=source,
                include_reason="retrieved_for_current_request",
                topic=topic,
                trust="user_memory",
            )
        )

    profile_context = _safe_text(payload.get("profile_context"))
    if profile_context:
        layers.append(
            _build_layer(
                "profile",
                profile_context,
                source="user_profile_memory",
                include_reason="personalization",
                topic=topic,
                trust="preference_not_fact",
            )
        )

    focus_content = _focus_layer_content(payload)
    if focus_content:
        layers.append(
            _build_layer(
                "focus",
                focus_content,
                source="context_inference",
                include_reason="target_and_followup_resolution",
                topic=topic,
                trust="derived_context",
            )
        )

    route_content = _route_layer_content(payload)
    if route_content:
        layers.append(
            _build_layer(
                "route_policy",
                route_content,
                source="routing_policy",
                include_reason="route_and_followup_control",
                topic=topic,
                trust="deterministic_policy",
            )
        )

    return [layer for layer in layers if layer]


def summarize_context_layers(context_layers: Iterable[Mapping[str, Any]] | None) -> list[dict]:
    summary = []
    for item in context_layers or []:
        if not isinstance(item, Mapping):
            continue
        layer = _safe_text(item.get("layer"))
        if not layer:
            continue
        summary.append(
            {
                "layer": layer,
                "char_count": int(item.get("char_count") or len(_safe_text(item.get("content")))),
                "source": _safe_text(item.get("source")),
                "include_reason": _safe_text(item.get("include_reason")),
                "topic": _safe_text(item.get("topic")),
                "trust": _safe_text(item.get("trust")),
            }
        )
    return summary


def attach_context_layers(
    context_payload: Mapping[str, Any] | None,
    *,
    prompt_text: str = "",
    channel: str = "",
) -> dict:
    payload = dict(context_payload or {})
    layers = build_context_layers(payload, prompt_text=prompt_text, channel=channel)
    payload["context_layers"] = layers
    payload["context_layer_summary"] = summarize_context_layers(layers)
    payload["context_layer_channel"] = _safe_text(channel)
    return payload


def _usable_layers(context_payload: Mapping[str, Any] | None) -> list[Mapping[str, Any]]:
    payload = context_payload or {}
    raw_layers = payload.get("context_layers")
    if not isinstance(raw_layers, list):
        return []
    layers = []
    for item in raw_layers:
        if not isinstance(item, Mapping):
            continue
        if _safe_text(item.get("layer")) and _safe_text(item.get("content")):
            layers.append(item)
    return layers


def _render_layer(item: Mapping[str, Any]) -> str:
    layer = _safe_text(item.get("layer"))
    content = _safe_text(item.get("content"))
    if not layer or not content:
        return ""
    heading = _LAYER_HEADINGS.get(layer, layer)
    if layer == "profile":
        content = f"仅用于个性化表达和风险偏好参考，不作为行情事实来源。\n{content}"
    return f"【{heading}】\n{content}"


def _render_layers(layers: list[Mapping[str, Any]]) -> str:
    by_name: dict[str, list[Mapping[str, Any]]] = {}
    for item in layers:
        by_name.setdefault(_safe_text(item.get("layer")), []).append(item)
    blocks = []
    for layer_name in _LAYER_ORDER:
        for item in by_name.get(layer_name, []):
            rendered = _render_layer(item)
            if rendered:
                blocks.append(rendered)
    for item in layers:
        layer_name = _safe_text(item.get("layer"))
        if layer_name not in _LAYER_ORDER:
            rendered = _render_layer(item)
            if rendered:
                blocks.append(rendered)
    return "\n\n".join(blocks).strip() if blocks else "无"


def _render_legacy_context(context_payload: Mapping[str, Any] | None) -> str:
    payload = context_payload or {}
    blocks = []
    recent_context = _safe_text(payload.get("recent_context"))
    memory_context = _safe_text(payload.get("memory_context"))
    profile_context = _safe_text(payload.get("profile_context"))
    focus_content = _focus_layer_content(payload)
    route_content = _route_layer_content(payload)
    if recent_context:
        blocks.append(f"【近期对话历史】\n{recent_context}")
    if memory_context:
        blocks.append(f"【相关长期记忆】\n{memory_context}")
    if profile_context:
        blocks.append(
            "【用户专属画像】\n仅用于个性化表达和风险偏好参考，不作为行情事实来源。\n"
            f"{profile_context}"
        )
    focus_entity = _safe_text(payload.get("focus_entity"))
    focus_topic = _safe_text(payload.get("focus_topic"))
    focus_aspect = _safe_text(payload.get("focus_aspect"))
    if focus_entity:
        blocks.append(f"【当前核心实体】\n{focus_entity}")
    if focus_topic:
        blocks.append(f"【当前核心主题】\n{focus_topic}")
    if focus_aspect:
        blocks.append(f"【当前细分维度】\n{focus_aspect}")
    remaining_focus_lines = []
    for line in focus_content.splitlines():
        if not line.startswith(("核心实体:", "核心主题:", "细分维度:")):
            remaining_focus_lines.append(line)
    if remaining_focus_lines:
        blocks.append(f"【当前上下文焦点】\n" + "\n".join(remaining_focus_lines))
    if route_content:
        blocks.append(f"【追问/路由策略】\n{route_content}")
    return "\n\n".join(blocks).strip() if blocks else "无"


def render_agent_context(context_payload: Mapping[str, Any] | None, *, target: str = "") -> str:
    layers = _usable_layers(context_payload)
    if isinstance((context_payload or {}).get("context_layers"), list):
        return _render_layers(layers)
    return _render_legacy_context(context_payload)


def has_agent_context(context_payload: Mapping[str, Any] | None) -> bool:
    rendered = render_agent_context(context_payload)
    return bool(rendered and rendered != "无")


def _sanitize_trace_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_text(value, 240)
    if isinstance(value, Mapping):
        sanitized = {}
        for key, nested_value in value.items():
            normalized_key = str(key)
            if normalized_key in _TRACE_BLOCKED_KEYS:
                continue
            sanitized[normalized_key] = _sanitize_trace_value(nested_value)
        return sanitized
    if isinstance(value, (list, tuple)):
        return [_sanitize_trace_value(v) for v in value[:30]]
    return _truncate_text(value, 240)


_TRACE_BLOCKED_KEYS = {
    "content",
    "profile_context",
    "memory_context",
    "recent_context",
    "response",
    "response_text",
    "answer",
    "prompt_text",
}


def _sanitize_trace_payload(payload: Mapping[str, Any]) -> dict:
    out = {}
    for key, value in payload.items():
        normalized_key = str(key)
        if normalized_key in _TRACE_BLOCKED_KEYS:
            continue
        out[normalized_key] = _sanitize_trace_value(value)
    return out


def append_chat_trace_event(
    redis_client: Any,
    task_id: str,
    event: str,
    payload: Mapping[str, Any] | None = None,
    *,
    ttl_seconds: int | None = None,
) -> bool:
    normalized_task_id = _safe_text(task_id)
    event_name = _safe_text(event)
    if not redis_client or not normalized_task_id or not event_name:
        return False
    key = f"chat_trace:{normalized_task_id}"
    ttl = ttl_seconds or CONTEXT_LAYER_TRACE_TTL_SECONDS
    item = {
        "event": event_name,
        "ts": datetime.now().isoformat(),
    }
    if payload:
        item.update(_sanitize_trace_payload(dict(payload)))
    try:
        raw = redis_client.get(key)
        events = json.loads(raw) if raw else []
        if not isinstance(events, list):
            events = []
        events.append(item)
        events = events[-CONTEXT_LAYER_TRACE_MAX_EVENTS:]
        redis_client.setex(key, ttl, json.dumps(events, ensure_ascii=False))
        return True
    except Exception:
        return False

import os
import sys
from pathlib import Path

from dotenv import load_dotenv


def _bootstrap_env() -> None:
    current_file = Path(__file__).resolve()
    for parent in [current_file.parent, *current_file.parents]:
        env_path = parent / ".env"
        if env_path.exists():
            load_dotenv(dotenv_path=env_path, override=True)
            return
    load_dotenv(override=True)


_bootstrap_env()

import streamlit as st
import hashlib
from task_manager import TaskManager, UserTaskQueueFullError
import time
import pandas as pd
import data_engine as de
import subscription_service as sub_svc
import re
import plotly.io as pio
import json
import random
import markdown
import html
from typing import Optional, Dict, Any
import auth_utils as auth
import memory_utils as mem
from user_profile_memory import build_profile_memory_context
import threading
from datetime import datetime, timedelta
from auth_ui import show_auth_dialog, sidebar_user_menu
from agent_core import build_trading_graph, simple_chatter_reply
from chat_routing import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    CHAT_MODE_SIMPLE,
    classify_chat_mode,
)
from chat_context_utils import (
    build_topic_anchors as _build_topic_anchors,
    extract_focus_aspect as _shared_extract_focus_aspect,
    extract_focus_entity as _shared_extract_focus_entity,
    infer_correction_intent as _infer_correction_intent,
    infer_followup_intent as _infer_followup_intent,
    infer_followup_goal as _infer_followup_goal,
    infer_focus_topic as _infer_focus_topic,
    infer_lookup_followup_intent as _infer_lookup_followup_intent,
    is_semantically_related as _shared_is_semantically_related,
    select_target_anchor as _select_target_anchor,
    should_preserve_recent_context as _should_preserve_recent_context,
)
from vision_tools import analyze_financial_image, analyze_position_image
from data_engine import get_commodity_iv_info
import time
import extra_streamlit_components as stx
import streamlit.components.v1 as components
import uuid #用于生成唯一ID
import base64
from market_tools import get_market_snapshot,tool_query_specific_option
from ui_components import inject_sidebar_toggle_style
from sidebar_footer_menu import render_sidebar_footer_menu, _resolve_scheme
from invite_landing import render_invite_register_landing
from sqlalchemy import text
from zoneinfo import ZoneInfo
from simple_chat_runtime import build_simple_runtime_context
import requests
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
from chat_feedback_service import (
    CHAT_FEEDBACK_REASON_CODES,
    generate_chat_answer_id,
    generate_chat_trace_id,
    get_user_feedback_for_answer,
    save_chat_answer_event,
    submit_chat_feedback,
)
# --- AI 相关导入 ---
from llm_compat import ChatTongyiCompat as ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

# Streamlit 运行时/第三方组件仍可能访问旧别名，提前映射以避免弃用日志噪音。
if hasattr(st, "user"):
    st.experimental_user = st.user

try:
    from deep_task_manager import DeepTaskManager
except Exception:
    DeepTaskManager = None

try:
    import invite_service as invite_svc
except Exception:
    invite_svc = None

ENABLE_DEEP_MODE = False  # deep 模块开发中，首页先回退为普通模式

# --- 系统代理清理 ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

# ==================== 公告配置区 ====================
ENABLE_HOME_ANNOUNCEMENT = False  # 临时关闭首页公告
# Fast router is disabled by default to avoid false positives on
# historical/list queries (for example: "最近两周每天价格").
FAST_ROUTER_ENABLED = os.getenv("AIBOTA_FAST_ROUTER_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
HOME_PROMO_BANNER = {
    "enabled": True,
    "text": "【5月培训】机构是如何卖期权",
    "target_page": "pages/25_期权重盾班.py",
}


def _get_chat_waiting_card_config(chat_mode: str, analysis_mode_label: str = "") -> Dict[str, Any]:
    if analysis_mode_label == "option_position_upload":
        return {
            "title": "⚙️ 期权持仓分析中",
            "steps": [
                ("📥 正在解析期权持仓截图", "识别合约腿与方向"),
                ("📐 正在计算 Delta 与暴露", "核对标的、IV 与合约参数"),
                ("🧭 正在生成调仓方案", "输出目标区间与建议调整量"),
                ("📝 正在整理期权结论", "生成结构化执行清单"),
            ],
            "gradient": "linear-gradient(135deg, rgba(15, 23, 42, 0.92), rgba(30, 58, 138, 0.92))",
            "border": "rgba(56, 189, 248, 0.34)",
            "phase_color": "#7dd3fc",
            "meta_color": "#cbd5e1",
            "sub_color": "#e2e8f0",
            "dot_color": "#7dd3fc",
            "caption": "正在后台持续处理，完成后会自动返回结果。",
        }
    if chat_mode == CHAT_MODE_KNOWLEDGE:
        return {
            "title": "📚 正在整理知识回答",
            "steps": [
                ("🔎 正在检索知识库", "汇总内部资料与历史知识片段"),
                ("🧠 正在梳理关键概念", "提炼定义、原理与常见误区"),
                ("✍️ 正在生成通俗讲解", "组织成更容易理解的回答"),
            ],
            "gradient": "linear-gradient(135deg, rgba(22, 34, 24, 0.96), rgba(34, 94, 60, 0.96))",
            "border": "rgba(74, 222, 128, 0.32)",
            "phase_color": "#bbf7d0",
            "meta_color": "#dcfce7",
            "sub_color": "#f0fdf4",
            "dot_color": "#86efac",
            "caption": "知识问答正在慢思考，完成后会自动返回结果。",
        }
    if chat_mode == CHAT_MODE_SIMPLE:
        return {
            "title": "💬 正在生成聊天回复",
            "steps": [
                ("✨ 正在理解你的问题", "识别语气与上下文"),
                ("🗣️ 正在组织自然表达", "生成更口语化的简短回复"),
            ],
            "gradient": "linear-gradient(135deg, rgba(54, 28, 14, 0.96), rgba(146, 64, 14, 0.96))",
            "border": "rgba(251, 191, 36, 0.34)",
            "phase_color": "#fde68a",
            "meta_color": "#ffedd5",
            "sub_color": "#fff7ed",
            "dot_color": "#fbbf24",
            "caption": "轻量聊天通常会很快返回。",
        }
    return {
        "title": "🚀 团队正在协作分析",
        "steps": [
            ("🛰️ 正在检索市场数据", "读取行情、新闻与历史上下文"),
            ("🧠 正在进行策略推理", "多模型协作评估方向与风险"),
            ("🧪 正在校验关键结论", "交叉检查数据一致性与边界条件"),
            ("📝 正在整理最终回答", "生成结构化结论与可执行建议"),
        ],
        "gradient": "linear-gradient(135deg, rgba(15, 23, 42, 0.92), rgba(30, 58, 138, 0.92))",
        "border": "rgba(148, 163, 184, 0.28)",
        "phase_color": "#bfdbfe",
        "meta_color": "#94a3b8",
        "sub_color": "#cbd5e1",
        "dot_color": "#93c5fd",
        "caption": "正在后台持续处理，完成后会自动返回结果。",
    }


def _render_chat_waiting_card(
    *,
    chat_mode: str,
    progress_msg: str,
    elapsed_sec: int,
    analysis_mode_label: str = "",
) -> str:
    config = _get_chat_waiting_card_config(chat_mode, analysis_mode_label)
    steps = config["steps"]
    phase_idx = (elapsed_sec // 6) % len(steps)
    phase_title, phase_desc = steps[phase_idx]
    return f"""
    <style>
    .thinking-wrap {{
        background: {config["gradient"]};
        border: 1px solid {config["border"]};
        border-radius: 14px;
        padding: 14px 16px;
        color: #e2e8f0;
        margin-bottom: 8px;
        box-shadow: 0 14px 38px rgba(15, 23, 42, 0.18);
    }}
    .thinking-title {{
        font-weight: 700;
        font-size: 16px;
        display: flex;
        align-items: center;
        gap: 8px;
    }}
    .thinking-sub {{
        margin-top: 6px;
        font-size: 13px;
        color: {config["sub_color"]};
    }}
    .thinking-phase {{
        margin-top: 8px;
        color: {config["phase_color"]};
        font-size: 13px;
        font-weight: 600;
    }}
    .thinking-meta {{
        margin-top: 4px;
        color: {config["meta_color"]};
        font-size: 12px;
    }}
    .thinking-dots {{
        display: inline-flex;
        gap: 4px;
        margin-left: 2px;
    }}
    .thinking-dot {{
        width: 6px;
        height: 6px;
        border-radius: 999px;
        background: {config["dot_color"]};
        opacity: 0.35;
        animation: dotPulse 1.2s infinite ease-in-out;
    }}
    .thinking-dot:nth-child(2) {{ animation-delay: 0.2s; }}
    .thinking-dot:nth-child(3) {{ animation-delay: 0.4s; }}
    @keyframes dotPulse {{
        0%, 80%, 100% {{ transform: scale(0.8); opacity: 0.35; }}
        40% {{ transform: scale(1.2); opacity: 1; }}
    }}
    </style>
    <div class="thinking-wrap">
        <div class="thinking-title">
            {config["title"]}
            <span class="thinking-dots">
                <span class="thinking-dot"></span>
                <span class="thinking-dot"></span>
                <span class="thinking-dot"></span>
            </span>
        </div>
        <div class="thinking-phase">{phase_title}</div>
        <div class="thinking-sub">{progress_msg}</div>
        <div class="thinking-meta">{phase_desc} · 已等待 {elapsed_sec}s</div>
    </div>
    """


def _normalize_home_pending_task(task_meta: Dict[str, Any], overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = dict(task_meta or {})
    if overrides:
        data.update({k: v for k, v in overrides.items() if v is not None})
    return {
        "task_id": str(data.get("task_id") or "").strip(),
        "prompt": str(data.get("prompt") or "").strip(),
        "raw_prompt": str(data.get("raw_prompt") or data.get("prompt") or "").strip(),
        "image_context": str(data.get("image_context") or ""),
        "mode": str(data.get("mode") or "normal"),
        "risk": str(data.get("risk_preference") or data.get("risk") or "稳健型"),
        "context_payload": data.get("context_payload") or {},
        "start_time": float(data.get("start_time") or 0.0),
        "analysis_mode_label": str(data.get("analysis_mode_label") or ""),
        "trace_id": str(data.get("trace_id") or "").strip(),
        "answer_id": str(data.get("answer_id") or "").strip(),
        "intent_domain": str(data.get("intent_domain") or "general").strip() or "general",
        "chat_mode": str(data.get("chat_mode") or CHAT_MODE_ANALYSIS),
        "queue_state": str(data.get("queue_state") or "active"),
        "queue_ahead": int(data.get("queue_ahead") or 0),
    }


def _build_task_placeholder_message(
    *,
    task_id: str,
    prompt_text: str,
    trace_id: str = "",
    answer_id: str = "",
    intent_domain: str = "general",
    chat_mode: str = CHAT_MODE_ANALYSIS,
) -> Dict[str, Any]:
    return {
        "role": "ai",
        "content": "",
        "chart": "",
        "attachments": [],
        "trace_id": str(trace_id or "").strip(),
        "answer_id": str(answer_id or "").strip(),
        "feedback_allowed": False,
        "intent_domain": str(intent_domain or "general").strip() or "general",
        "chat_mode": str(chat_mode or CHAT_MODE_ANALYSIS),
        "linked_task_id": str(task_id or "").strip(),
        "linked_prompt": str(prompt_text or "").strip(),
        "is_task_placeholder": True,
    }


def _find_message_index_by_task_id(task_id: str, *, placeholder_only: bool = False) -> int:
    target = str(task_id or "").strip()
    if not target:
        return -1
    for idx, msg in enumerate(st.session_state.get("messages") or []):
        if str(msg.get("linked_task_id") or "").strip() != target:
            continue
        if placeholder_only and not bool(msg.get("is_task_placeholder")):
            continue
        return idx
    return -1


def _replace_task_placeholder_message(task_id: str, message_data: Dict[str, Any]) -> None:
    idx = _find_message_index_by_task_id(task_id, placeholder_only=True)
    payload = dict(message_data or {})
    payload["linked_task_id"] = str(task_id or "").strip()
    payload.pop("is_task_placeholder", None)
    if idx >= 0:
        st.session_state.messages[idx] = payload
    else:
        st.session_state.messages.append(payload)


def _replace_task_placeholder_with_text(
    task_id: str,
    content: str,
    *,
    chat_mode: str = CHAT_MODE_ANALYSIS,
    intent_domain: str = "general",
) -> None:
    _replace_task_placeholder_message(
        task_id,
        {
            "role": "ai",
            "content": str(content or "").strip(),
            "chart": "",
            "attachments": [],
            "feedback_allowed": False,
            "trace_id": "",
            "answer_id": "",
            "intent_domain": str(intent_domain or "general").strip() or "general",
            "chat_mode": str(chat_mode or CHAT_MODE_ANALYSIS),
        },
    )


def _collect_home_task_overrides() -> Dict[str, Dict[str, Any]]:
    overrides: Dict[str, Dict[str, Any]] = {}
    for task_info in st.session_state.get("pending_tasks") or []:
        task_id = str(task_info.get("task_id") or "").strip()
        if not task_id:
            continue
        overrides[task_id] = {
            "raw_prompt": task_info.get("raw_prompt"),
            "trace_id": task_info.get("trace_id"),
            "answer_id": task_info.get("answer_id"),
            "intent_domain": task_info.get("intent_domain"),
            "analysis_mode_label": task_info.get("analysis_mode_label"),
        }
    return overrides


def _refresh_home_pending_tasks(current_user: str, extra_overrides: Optional[Dict[str, Dict[str, Any]]] = None) -> list[Dict[str, Any]]:
    if not current_user or current_user == "访客":
        st.session_state.pending_tasks = []
        st.session_state.pending_task = None
        return []

    overrides = _collect_home_task_overrides()
    if extra_overrides:
        overrides.update(extra_overrides)

    normalized_tasks = []
    for task_meta in TaskManager().get_user_task_queue(current_user):
        task_id = str(task_meta.get("task_id") or "").strip()
        normalized_tasks.append(
            _normalize_home_pending_task(
                task_meta,
                overrides=overrides.get(task_id),
            )
        )

    st.session_state.pending_tasks = normalized_tasks
    st.session_state.pending_task = normalized_tasks[0] if normalized_tasks else None
    return normalized_tasks


def _render_queued_chat_task_card(task_info: Dict[str, Any]) -> str:
    prompt = html.escape(str(task_info.get("raw_prompt") or task_info.get("prompt") or "排队中的问题"))
    queue_ahead = int(task_info.get("queue_ahead") or 0)
    queue_msg = f"排队中，前面还有 {queue_ahead} 个问题" if queue_ahead > 0 else "排队中，等待开始处理"
    return f"""
    <style>
    .queued-task-wrap {{
        border-radius: 14px;
        border: 1px solid rgba(255,255,255,0.08);
        background: rgba(255,255,255,0.03);
        padding: 14px 16px;
        margin-top: 10px;
        color: #E5E7EB;
    }}
    .queued-task-title {{
        font-weight: 700;
        font-size: 15px;
        margin-bottom: 6px;
    }}
    .queued-task-sub {{
        color: #A7B2C7;
        font-size: 13px;
        line-height: 1.5;
    }}
    </style>
    <div class="queued-task-wrap">
        <div class="queued-task-title">🕒 排队中的问题</div>
        <div class="queued-task-sub">{prompt}</div>
        <div class="queued-task-sub" style="margin-top: 6px;">{queue_msg}</div>
    </div>
    """


def _render_inline_queued_chat_task_hint(task_info: Dict[str, Any]) -> str:
    queue_ahead = int(task_info.get("queue_ahead") or 0)
    queue_msg = f"排队中，前面还有 {queue_ahead} 个问题" if queue_ahead > 0 else "排队中，等待开始处理"
    return f"""
    <style>
    .queued-inline-wrap {{
        margin-top: 8px;
        color: #E5E7EB;
    }}
    .queued-inline-title {{
        font-weight: 700;
        font-size: 14px;
        margin-bottom: 2px;
    }}
    .queued-inline-sub {{
        color: #A7B2C7;
        font-size: 13px;
        line-height: 1.5;
    }}
    </style>
    <div class="queued-inline-wrap">
        <div class="queued-inline-title">🕒 排队中</div>
        <div class="queued-inline-sub">{queue_msg}</div>
    </div>
    """


@st.fragment(run_every=1.5)
def _render_pending_chat_task_fragment(task_info_snapshot: Dict[str, Any]) -> None:
    pending_tasks = st.session_state.get("pending_tasks") or []
    task_info = pending_tasks[0] if pending_tasks else (st.session_state.get("pending_task") or task_info_snapshot)
    if not task_info:
        return

    task_id = task_info["task_id"]
    task_start = float(task_info.get("start_time") or 0.0) or time.time()
    task_mode = task_info.get("mode", "normal")
    current_user = st.session_state.get("user_id", "访客")

    if time.time() - task_start >= 1800:
        st.warning("⏱️ 任务处理超时，请重新提问。")
        _replace_task_placeholder_with_text(
            task_id,
            "⏱️ 这条问题处理超时了，请稍后重试，或把问题拆得更具体一些。",
            chat_mode=chat_mode if 'chat_mode' in locals() else CHAT_MODE_ANALYSIS,
            intent_domain=str(task_info.get("intent_domain") or "general").strip() or "general",
        )
        TaskManager().complete_user_task(current_user, task_id)
        _refresh_home_pending_tasks(current_user)
        st.rerun()
        return

    status_placeholder = st.empty()
    content_placeholder = st.empty()

    use_deep_manager = (task_mode == "deep" and ENABLE_DEEP_MODE and DeepTaskManager is not None)
    task_manager = DeepTaskManager() if use_deep_manager else TaskManager()
    task_status = task_manager.get_task_status(task_id)
    current_status = task_status["status"]
    chat_mode = str(task_info.get("chat_mode") or task_status.get("chat_mode") or CHAT_MODE_ANALYSIS)

    if current_status in ["pending", "processing"]:
        progress_msg = task_status.get("progress", "正在处理...")
        elapsed_sec = int(max(0, time.time() - task_start))
        analysis_mode_label = str(task_info.get("analysis_mode_label", "") or "")
        status_placeholder.markdown(
            _render_chat_waiting_card(
                chat_mode=chat_mode,
                progress_msg=progress_msg,
                elapsed_sec=elapsed_sec,
                analysis_mode_label=analysis_mode_label,
            ),
            unsafe_allow_html=True,
        )

        with content_placeholder.container():
            waiting_caption = _get_chat_waiting_card_config(chat_mode, analysis_mode_label)["caption"]
            st.caption(waiting_caption)
        return

    if current_status == "success":
        status_placeholder.empty()
        result = task_status.get("result")

        if result and isinstance(result, dict):
            final_response_md = result.get("response", "")
            final_img_path = result.get("chart", "")
            attachments = result.get("attachments", [])

            if not final_response_md:
                final_response_md = "抱歉，AI 分析未返回有效结果。"

            if final_img_path:
                try:
                    render_chart_by_filename(final_img_path)
                except Exception as e:
                    print(f"图表渲染失败: {e}")

            final_response_md = clean_chart_tag(final_response_md)

            inline_state = {"has_inline": False, "used_indices": set()}
            if final_response_md and len(final_response_md) > 0:
                if attachments:
                    with content_placeholder.container():
                        inline_state = render_response_with_inline_attachments(
                            final_response_md,
                            attachments,
                            render_plain_when_no_token=False,
                        )

                if not inline_state["has_inline"]:
                    placeholder = content_placeholder.empty()
                    full_response = ""

                    if len(final_response_md) > 800:
                        update_interval = 100
                        chars = list(final_response_md)

                        for i in range(0, len(chars), update_interval):
                            chunk = ''.join(chars[i:i + update_interval])
                            full_response += chunk
                            placeholder.markdown(full_response + "▌", unsafe_allow_html=True)
                            time.sleep(0.05)

                        placeholder.markdown(full_response, unsafe_allow_html=True)
                    else:
                        delay_time = 0.01

                        for char in stream_text_generator(final_response_md, delay=delay_time):
                            full_response += char
                            placeholder.markdown(full_response + "▌", unsafe_allow_html=True)

                        placeholder.markdown(full_response, unsafe_allow_html=True)

            if attachments:
                with content_placeholder.container():
                    render_knowledge_attachments(attachments, exclude_indices=inline_state["used_indices"])

            trace_id = str(task_info.get("trace_id") or generate_chat_trace_id()).strip()
            answer_id = str(task_info.get("answer_id") or generate_chat_answer_id()).strip()
            intent_domain = str(task_info.get("intent_domain") or "general").strip() or "general"
            feedback_allowed = False
            if current_user != "访客" and final_response_md:
                feedback_allowed = save_chat_answer_event(
                    _get_home_feedback_engine(),
                    task_id=task_id,
                    user_id=current_user,
                    trace_id=trace_id,
                    answer_id=answer_id,
                    prompt_text=str(task_info.get("raw_prompt") or task_info.get("prompt") or "").strip(),
                    response_text=final_response_md,
                    intent_domain=intent_domain,
                    feedback_allowed=True,
                )

            message_data = {
                "role": "ai",
                "content": final_response_md,
                "chart": final_img_path,
                "attachments": attachments,
                "trace_id": trace_id,
                "answer_id": answer_id,
                "feedback_allowed": feedback_allowed,
                "intent_domain": intent_domain,
                "chat_mode": chat_mode,
            }
            _replace_task_placeholder_message(task_id, message_data)

            if current_user != "访客":
                try:
                    memory_record = _build_memory_record(final_response_md)
                    mem.save_interaction(
                        current_user,
                        task_info["prompt"],
                        memory_record,
                        topic=_classify_intent_domain(task_info.get("prompt", "")),
                    )
                except Exception as e:
                    print(f"记忆存储失败: {e}")

        task_manager.complete_user_task(current_user, task_id)
        _refresh_home_pending_tasks(current_user)
        st.rerun()
        return

    if current_status == "error":
        status_placeholder.error("❌ 分析失败")
        error_msg = task_status.get("error", "未知错误")
        content_placeholder.error(f"抱歉，分析过程出现问题：{error_msg[:100]}")
        _replace_task_placeholder_with_text(
            task_id,
            f"❌ 这条问题处理失败了：{str(error_msg or '未知错误')[:120]}",
            chat_mode=chat_mode,
            intent_domain=str(task_info.get("intent_domain") or "general").strip() or "general",
        )

        task_manager.complete_user_task(current_user, task_id)
        _refresh_home_pending_tasks(current_user)
        if task_mode == "deep":
            st.session_state.deep_mode_enabled = False
            fallback_prompt = str(task_info.get("prompt") or "").strip()
            if fallback_prompt and st.button("一键转普通分析", key=f"deep_fallback_error_{task_id}"):
                process_user_input(fallback_prompt, deep_mode=False)
                st.rerun()
            return

        st.rerun()
        return

    if current_status == "timeout":
        status_placeholder.warning("⏱️ Deep 报告超时")
        content_placeholder.warning("Deep 报告在预算与时限内未完成，建议缩小问题范围或切换普通分析。")
        _replace_task_placeholder_with_text(
            task_id,
            "⏱️ 这条问题处理超时了，建议缩小问题范围，或切换普通分析后再试一次。",
            chat_mode=chat_mode,
            intent_domain=str(task_info.get("intent_domain") or "general").strip() or "general",
        )

        task_manager.complete_user_task(current_user, task_id)
        _refresh_home_pending_tasks(current_user)
        st.session_state.deep_mode_enabled = False
        fallback_prompt = str(task_info.get("prompt") or "").strip()
        if fallback_prompt and st.button("一键转普通分析", key=f"deep_fallback_timeout_{task_id}"):
            process_user_input(fallback_prompt, deep_mode=False)
            st.rerun()
        return

ANNOUNCEMENT_CONTENT = {
    "title": "📡 情报站内容升级",
    "sections": [
        {
            "title": "🧠 你能在情报站看到什么",
            "items": [
                "复盘晚报：盘后提炼当日主线、关键异动与次日观察点。",
                "资金流晚报：跟踪主力流向与板块强弱，辅助判断市场节奏。",
                "交易信号：结合盘中数据，给出k线突破信号参考。",
                "持仓密报 / 末日期权晚报：面向实盘决策场景，提供重点风险与机会提示。",
            ]
        },
        {
            "title": "🎯 如何高效使用",
            "items": [
                "盘前看资金流晚报，确定重点方向。",
                "盘后看复盘晚报，更新交易计划。",
                "持仓密报揭示机构和散户的期货秘密",
            ],
        },
        {
            "title": "👉 立即查看",
            "items": [
                "在左侧导航进入「情报站」，按频道订阅并查看历史内容。",
            ]
        }
    ],
    "update_date": "2026-03-29"
}


# ==================== 公告工具函数 ====================

ANNOUNCEMENT_LAST_SHOWN_COOKIE_KEY = "announcement_last_shown_date"
ASIA_SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
ANNOUNCEMENT_RERUN_HOLDOFF_SECONDS = 6.0


def get_shanghai_today_str():
    """Return today's date string in Asia/Shanghai."""
    return datetime.now(ASIA_SHANGHAI_TZ).strftime("%Y-%m-%d")


def build_simple_runtime_context_payload(current_user: str) -> Dict[str, str]:
    user_label = str(current_user or "").strip() or "访客"
    return build_simple_runtime_context(current_user_label=user_label)


def _persist_announcement_shown_date(date_str: str) -> bool:
    """Persist shown-date cookie; return whether write succeeded."""
    try:
        cm = stx.CookieManager(key="announcement_cookie_setter")
        cm.set(
            ANNOUNCEMENT_LAST_SHOWN_COOKIE_KEY,
            date_str,
            expires_at=datetime.now() + timedelta(days=365),
        )
        return True
    except Exception as e:
        print(f"公告显示日期写入 cookie 失败: {e}")
        return False


def flush_pending_announcement_cookie():
    """Write pending shown-date cookie on a later rerun to avoid dialog flashing."""
    pending_date = st.session_state.get("announcement_cookie_pending_date")
    if not pending_date:
        return
    if _persist_announcement_shown_date(str(pending_date)):
        st.session_state.announcement_last_shown_date = str(pending_date)
        st.session_state.announcement_cookie_pending_date = None


def mark_announcement_shown_today():
    """Mark today's announcement as shown and defer cookie write to next rerun."""
    today = get_shanghai_today_str()
    st.session_state.announcement_last_shown_date = today
    st.session_state.announcement_acknowledged_date = today
    st.session_state.announcement_cookie_pending_date = today
    # Hold off background auto-reruns briefly so dialog won't flash-disappear.
    st.session_state.announcement_holdoff_until = time.time() + ANNOUNCEMENT_RERUN_HOLDOFF_SECONDS


def is_announcement_holdoff_active():
    """Return True when we should temporarily avoid auto-rerun loops."""
    try:
        return time.time() < float(st.session_state.get("announcement_holdoff_until", 0))
    except Exception:
        return False


@st.dialog(ANNOUNCEMENT_CONTENT["title"], width="large")
def show_announcement():
    """显示公告弹窗"""
    for section in ANNOUNCEMENT_CONTENT["sections"]:
        st.markdown(f"### {section['title']}")
        for item in section["items"]:
            st.markdown(f"- {item}")
        # VIP 区块：显示可点击的图片横幅
        if "vip_link" in section:
            vip_link = section["vip_link"]
            st.markdown(
                f"""
                <div style="margin-top:12px;">
                  <a href="{vip_link}" target="_blank" style="text-decoration:none;">
                    <div style="
                      background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
                      border: 1px solid #e94560;
                      border-radius: 12px;
                      padding: 24px 20px;
                      text-align: center;
                      cursor: pointer;
                    ">
                      <div style="font-size:36px; margin-bottom:8px;">👑</div>
                      <div style="color:#f5c518; font-size:20px; font-weight:bold; margin-bottom:6px;">加入 VIP 交流群</div>
                      <div style="color:#cccccc; font-size:13px; margin-bottom:14px;">
                        盘前语音 · 盘中交流 · 盘后复盘 · 每周VIP视频
                      </div>
                      <div style="
                        display:inline-block;
                        background:#e94560;
                        color:white;
                        padding:8px 28px;
                        border-radius:20px;
                        font-size:14px;
                        font-weight:bold;
                      ">点击了解详情 →</div>
                    </div>
                  </a>
                </div>
                """,
                unsafe_allow_html=True,
            )
        st.write("")

    st.caption(f"📅 更新时间：{ANNOUNCEMENT_CONTENT['update_date']}")

    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("我知道了", type="primary", use_container_width=True):
            # Button is only for confirm/close interaction.
            mark_announcement_shown_today()
            st.rerun()


def check_and_show_announcement():
    """检查是否需要显示公告：同浏览器/设备每天最多展示一次。"""
    today = get_shanghai_today_str()

    # session 兜底：即使 cookie 读取/写入偶发失败，也避免当天重复打扰
    acknowledged_date = st.session_state.get("announcement_acknowledged_date")
    if acknowledged_date == today:
        return

    # 保留原有行为：手动登录后本次先不弹，避免登录瞬间打扰
    if st.session_state.get('just_manual_logged_in', False):
        st.session_state['just_manual_logged_in'] = False
        return

    try:
        # 使用初始化阶段读取到 session_state 的 cookie 值
        last_shown_date = st.session_state.get("announcement_last_shown_date")
        if last_shown_date == today:
            st.session_state.announcement_acknowledged_date = today
            return

        # 先记录再展示：只要弹出就计为“今天已展示”
        mark_announcement_shown_today()
        show_announcement()
    except Exception as e:
        print(f"公告检查失败: {e}")
        # 异常时仍允许展示，但 session 内当天不重复
        if st.session_state.get("announcement_acknowledged_date") != today:
            st.session_state.announcement_acknowledged_date = today
            show_announcement()

_HOME_CHAT_FEEDBACK_REASON_LABELS = {
    "not_personalized": "不够贴合我的情况",
    "too_generic": "太泛了，不够具体",
    "wrong_fact": "有事实错误",
    "not_actionable": "没有给出可执行建议",
}


def _get_home_feedback_engine():
    return getattr(de, "engine", None)


def _get_home_feedback_reason_options():
    ordered_keys = [
        "not_personalized",
        "too_generic",
        "wrong_fact",
        "not_actionable",
    ]
    return [(key, _HOME_CHAT_FEEDBACK_REASON_LABELS[key]) for key in ordered_keys if key in CHAT_FEEDBACK_REASON_CODES]


def _get_home_feedback_cache() -> Dict[str, Dict[str, Any]]:
    cache = st.session_state.get("chat_feedback_submissions")
    if not isinstance(cache, dict):
        cache = {}
        st.session_state["chat_feedback_submissions"] = cache
    return cache


def _inject_home_feedback_styles():
    css_version = "home_feedback_scoped_20260427"
    if st.session_state.get("_home_feedback_css_version") == css_version:
        return
    st.markdown(
        """
        <style>
        div[data-testid="stExpander"] details {
            background: rgba(15, 23, 42, 0.45) !important;
            border: 1px solid rgba(148, 163, 184, 0.18) !important;
            border-radius: 12px !important;
        }
        div[data-testid="stExpander"] summary {
            color: #f8fafc !important;
        }
        div[data-testid="stExpander"] div[data-baseweb="select"] > div {
            background: #f8fafc !important;
            color: #0f172a !important;
            border: 1px solid rgba(148, 163, 184, 0.32) !important;
        }
        div[data-testid="stExpander"] div[data-baseweb="select"] span,
        div[data-testid="stExpander"] div[data-baseweb="select"] div,
        div[data-testid="stExpander"] div[data-baseweb="select"] svg,
        div[data-testid="stExpander"] div[data-baseweb="select"] p {
            color: #0f172a !important;
            fill: #64748b !important;
        }
        div[data-testid="stExpander"] div[data-baseweb="select"] input {
            color: #0f172a !important;
            -webkit-text-fill-color: #0f172a !important;
        }
        div[data-testid="stExpander"] div[data-baseweb="select"] input::placeholder,
        div[data-testid="stExpander"] div[data-baseweb="select"] [aria-placeholder="true"] {
            color: #64748b !important;
            -webkit-text-fill-color: #64748b !important;
            opacity: 1 !important;
        }
        div[role="listbox"] {
            background: #f8fafc !important;
            color: #0f172a !important;
            border: 1px solid rgba(148, 163, 184, 0.28) !important;
        }
        div[role="option"] {
            background: #f8fafc !important;
            color: #0f172a !important;
        }
        div[role="option"][aria-selected="true"] {
            background: #1d4ed8 !important;
            color: #f8fafc !important;
        }
        div[data-testid="stExpander"] textarea {
            background: rgba(15, 23, 42, 0.92) !important;
            color: #f8fafc !important;
        }
        div[data-testid="stExpander"] textarea::placeholder {
            color: #94a3b8 !important;
            opacity: 1 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_home_feedback_css_version"] = css_version


def _submit_home_chat_feedback(
    *,
    answer_id: str,
    trace_id: str,
    feedback_type: str,
    reason_code: str = "",
    feedback_text: str = "",
) -> Dict[str, Any]:
    current_user = str(st.session_state.get("user_id") or "").strip()
    if not current_user or current_user == "访客":
        return {"ok": False, "message": "请先登录后再提交反馈"}

    result = submit_chat_feedback(
        _get_home_feedback_engine(),
        answer_id=answer_id,
        trace_id=trace_id,
        user_id=current_user,
        feedback_type=feedback_type,
        reason_code=reason_code,
        feedback_text=feedback_text,
    )
    code = str(result.get("code") or "")
    if code in {"ok", "already_submitted"}:
        return {"ok": True, "message": "反馈已记录，后续会用于优化回答"}
    if code == "invalid_reason_code":
        return {"ok": False, "message": "请选择一个有效的差评原因"}
    if code == "answer_not_found":
        return {"ok": False, "message": "这条回答暂时还没有可反馈记录，请稍后再试"}
    if code == "forbidden":
        return {"ok": False, "message": "这条回答不属于当前登录用户"}
    if code == "trace_mismatch":
        return {"ok": False, "message": "反馈标识已失效，请重新提问后再试"}
    if code == "unsupported_feedback_type":
        return {"ok": False, "message": "暂不支持这种反馈类型"}
    return {"ok": False, "message": "反馈保存失败，请稍后重试"}


def _render_chat_feedback_controls(msg: Dict[str, Any], index: int):
    if str(msg.get("chat_mode") or "").strip() == CHAT_MODE_SIMPLE:
        return

    answer_id = str(msg.get("answer_id") or "").strip()
    trace_id = str(msg.get("trace_id") or "").strip()
    if not answer_id or not trace_id or not bool(msg.get("feedback_allowed")):
        return

    _inject_home_feedback_styles()
    cache = _get_home_feedback_cache()
    existing = cache.get(answer_id)
    if not isinstance(existing, dict):
        current_user = str(st.session_state.get("user_id") or "").strip()
        if current_user and current_user != "访客":
            existing = get_user_feedback_for_answer(
                _get_home_feedback_engine(),
                answer_id=answer_id,
                user_id=current_user,
            )
            if isinstance(existing, dict) and existing:
                cache[answer_id] = existing
    if isinstance(existing, dict) and existing:
        feedback_type = str(existing.get("feedback_type") or "").strip()
        if feedback_type == "up":
            st.caption("你已标记这条回答“有帮助”")
            return
        reason_label = _HOME_CHAT_FEEDBACK_REASON_LABELS.get(
            str(existing.get("reason_code") or "").strip(),
            "已提交反馈",
        )
        st.caption(f"你已提交反馈：{reason_label}")
        return

    st.caption("这条回答对你有帮助吗？")
    col_up, col_down = st.columns([0.9, 4.1], gap="small")
    with col_up:
        if st.button("有帮助", key=f"feedback_up_{answer_id}", type="secondary"):
            submit_result = _submit_home_chat_feedback(
                answer_id=answer_id,
                trace_id=trace_id,
                feedback_type="up",
            )
            if submit_result.get("ok"):
                cache[answer_id] = {"feedback_type": "up"}
                st.toast(str(submit_result.get("message") or "反馈已提交"))
                st.rerun()
            st.warning(str(submit_result.get("message") or "反馈提交失败"))

    with col_down:
        with st.expander("没帮助，告诉我哪里不对", expanded=False):
            options = [("", "请选择一个原因"), *_get_home_feedback_reason_options()]
            labels = [label for _, label in options]
            selected_label = st.selectbox(
                "差评原因",
                options=labels,
                key=f"feedback_reason_{answer_id}",
            )
            reason_code = ""
            for code, label in options:
                if label == selected_label:
                    reason_code = code
                    break
            feedback_text = st.text_area(
                "补充说明（可选）",
                key=f"feedback_text_{answer_id}",
                placeholder="例如：希望明确到仓位比例、止损线，或者指出哪句不对",
                height=80,
            )
            if st.button("提交反馈", key=f"feedback_down_{answer_id}", type="secondary"):
                submit_result = _submit_home_chat_feedback(
                    answer_id=answer_id,
                    trace_id=trace_id,
                    feedback_type="down",
                    reason_code=reason_code,
                    feedback_text=str(feedback_text or "").strip(),
                )
                if submit_result.get("ok"):
                    cache[answer_id] = {
                        "feedback_type": "down",
                        "reason_code": reason_code,
                    }
                    st.toast(str(submit_result.get("message") or "反馈已提交"))
                    st.rerun()
                st.warning(str(submit_result.get("message") or "反馈提交失败"))

# ==========================================
#  1. 页面配置 (必须在第一行) [修改点：改为 centered 布局]
# ==========================================
st.set_page_config(
    page_title="爱波塔-懂期权的AI | K线分析+期权策略",
    page_icon="favicon.ico",
    layout="centered",
    initial_sidebar_state="expanded"
)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHART_DIR = os.path.join(BASE_DIR, "static", "charts")

def stream_text_generator(text, delay=0.01):
    """
    打字机效果生成器：将长文本拆分成字符逐个输出
    delay: 每个字符的延迟时间 (秒)，0.005-0.02 之间体感最佳
    """
    for char in text:
        yield char
        time.sleep(delay)


def _render_simple_chat_typing_indicator() -> str:
    return """
    <style>
    .simple-chat-typing {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 10px 14px;
        border-radius: 14px;
        background: rgba(30, 41, 59, 0.78);
        border: 1px solid rgba(148, 163, 184, 0.22);
        box-shadow: 0 8px 24px rgba(15, 23, 42, 0.16);
    }
    .simple-chat-dot {
        width: 7px;
        height: 7px;
        border-radius: 999px;
        background: #dbeafe;
        opacity: 0.35;
        animation: simpleChatTypingPulse 1.2s infinite ease-in-out;
    }
    .simple-chat-dot:nth-child(2) { animation-delay: 0.18s; }
    .simple-chat-dot:nth-child(3) { animation-delay: 0.36s; }
    @keyframes simpleChatTypingPulse {
        0%, 80%, 100% { transform: translateY(0); opacity: 0.35; }
        40% { transform: translateY(-2px); opacity: 1; }
    }
    </style>
    <div class="simple-chat-typing" aria-label="AI 正在输入">
        <span class="simple-chat-dot"></span>
        <span class="simple-chat-dot"></span>
        <span class="simple-chat-dot"></span>
    </div>
    """
# ==========================================
#  2. 极简主义 CSS 注入 [修改点：新增卡片样式]
# ==========================================
st.markdown("""
<style>
     /* 1. 强制全局背景为深空蓝黑 */
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
        color: white !important; /* 强制全局文字变白 */
    }
    /* --- 核心修复：拓宽中间主内容区域 --- */
    /* 默认 centered 大概只有 730px，这里我们强制拓宽到 1000px 或更宽 */
    [data-testid="stMainBlockContainer"] {
        max-width: 65rem !important; /* 约 960px，您可以改成 65rem 或 70rem 甚至更宽 */
        padding-left: 2rem;
        padding-right: 2rem;
    }
/* --- 修复 1：找回侧边栏按钮 --- */
    /* 不要隐藏整个 Header，否则按钮也没了。只把背景变透明 */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    /* 隐藏顶部的彩虹装饰线条 */
    [data-testid="stDecoration"] {
        display: none;
    }
    
    /* --- 修复 2：强制 AI 回答文字变白 --- */
    /* 针对聊天气泡内的所有文本元素强制设为白色 */
    [data-testid="stChatMessageContent"] p,
    [data-testid="stChatMessageContent"] span,
    [data-testid="stChatMessageContent"] div,
    [data-testid="stChatMessageContent"] li {
        color: #ffffff !important;
        line-height: 1.6;
    }
    /* 稍微调亮 Markdown 中的加粗文字 */
    [data-testid="stChatMessageContent"] strong {
        color: #fcf7f7 !important; /* 金黄色高亮，更易读 */
    }
    
    /* --- [关键修复 1] 代码块样式修复 --- */
    /* 强制代码块背景为深色，文字为亮色 */
    [data-testid="stChatMessageContent"] code {
    background-color: #1e3a5f !important;  /* 深蓝色背景 */
    color: #ffd700 !important;             /* 金黄色文字（更醒目）*/
    border: 1px solid #4a90e2;             /* 亮蓝色边框 */
    border-radius: 4px;
    padding: 0.2rem 0.4rem;
    font-weight: 500;                      /* 字体加粗 */
    font-size: 0.95em;                     /* 稍微放大 */
    }
    /* 多行代码块容器 */
    [data-testid="stChatMessageContent"] pre {
        background-color: #2b313e !important;
        border: 1px solid #3b4252;
        border-radius: 8px;
    }

/* --- [核心修改] 快捷指令卡片样式：深色背景 + 亮色文字 --- */
    .suggestion-card {
        background-color: #1E2329; /* 改为深色背景 */
        border: 1px solid #2d333b; /* 微亮的边框 */
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        cursor: pointer;
        transition: all 0.2s ease-in-out;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* 加点阴影更有质感 */
    }
  /* --- [关键修复] 按钮样式重写 (解决白字白底问题) --- */
    /* 针对所有的 st.button */
    div.stButton > button {
        background-color: #1E2329 !important; /* 强制深色背景 */
        color: #e6e6e6 !important;            /* 强制亮色文字 */
        border: 1px solid #31333F !important; /* 边框 */
        border-radius: 8px !important;
        padding: 1.2rem !important;           /* 增加内边距 */
        height: auto !important;              /* 高度自适应 */
        white-space: pre-wrap !important;     /* 关键：允许文字换行 (\n) */
        width: 100% !important;               /* 填满列宽 */
        transition: all 0.2s ease-in-out !important;
        font-family: "Source Sans Pro", sans-serif !important;
    }

    /* 按钮 Hover (悬停) 状态 */
    div.stButton > button:hover {
        border-color: #ff4b4b !important;     /* 红色边框 */
        background-color: #262c36 !important; /* 稍微变亮的背景 */
        color: #ffffff !important;            /* 纯白文字 */
        transform: translateY(-2px);          /* 微微上浮效果 */
    }
    
    /* 按钮 Active/Focus (点击) 状态 */
    div.stButton > button:active, div.stButton > button:focus {
        background-color: #262c36 !important;
        color: #ffffff !important;
        border-color: #ff4b4b !important;
        box-shadow: none !important;
    }
    .card-icon { font-size: 24px; margin-bottom: 8px; }
    
    /* 标题文字：亮白色 */
    .card-title { 
        font-weight: bold; 
        font-size: 15px; 
        color: #e6e6e6 !important; /* 强制亮白 */
        margin-bottom: 4px;
    }
    /* 描述文字：稍暗的灰色 */
    .card-desc { 
        font-size: 13px; 
        color: #8b949e !important; /* 强制灰白 */
    }
    
    /* 调整底部输入框样式 */
    .stChatInput {
        padding-bottom: 5px;
    }

        /* 3. 侧边栏文字强制变白 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
    }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }
    @media (max-width: 768px) {
        
        /* 1. 极致利用屏幕宽度 */
        /* 把左右留白从默认的 1rem 压缩到 0.5rem，让文字几乎贴边显示，增加阅读视野 */
        .block-container {
            padding-left: 12px !important;
            padding-right: 12px !important;
        }

        /* 2. 聊天气泡紧凑化 */
        /* 减小头像和文字之间的距离，减少气泡内部的留白 */
        .stChatMessage {
            gap: 0.5rem !important;
        }
        [data-testid="stChatMessageContent"] {
            padding-right: 0 !important; /* 防止右侧留白过多 */
        }

        /* 3. 强制缩小标题字号 (关键！) */
        /* 无论 AI 输出的是 # 还是 ##，在手机上都强制变成小标题样式 */
        [data-testid="stChatMessageContent"] h1,
        [data-testid="stChatMessageContent"] h2,
        [data-testid="stChatMessageContent"] h3 {
            font-size: 17px !important; /* 从 24px+ 降到 17px，接近正文略大一点 */
            font-weight: 700 !important;
            margin-top: 12px !important;
            margin-bottom: 6px !important;
            line-height: 1.4 !important;
            letter-spacing: 0.5px;
        }

        /* 4. 正文排版优化 */
        /* 调整行高和字号，使其更像原生 APP */
        [data-testid="stChatMessageContent"] p, 
        [data-testid="stChatMessageContent"] li {
            font-size: 15px !important; /* 黄金阅读字号 */
            line-height: 1.6 !important; /* 舒适的行高 */
            margin-bottom: 8px !important;
            text-align: justify; /* 两端对齐，让文字块更整齐 */
        }

        /* 5. 列表（ul/ol）紧凑化 */
        [data-testid="stChatMessageContent"] ul,
        [data-testid="stChatMessageContent"] ol {
            padding-left: 20px !important; /* 减小缩进 */
            margin-bottom: 10px !important;
        }
        [data-testid="stChatMessageContent"] li {
            margin-bottom: 4px !important; /* 列表项之间紧凑一点 */
        }

        /* 6. 表格（Table）样式大整形 (针对图2那样的大表格) */
        [data-testid="stChatMessageContent"] table {
            font-size: 13px !important; /* 表格字要小 */
            width: 100% !important;
            display: table !important; /* 强制表格布局 */
        }
        /* 表头和单元格 */
        [data-testid="stChatMessageContent"] th,
        [data-testid="stChatMessageContent"] td {
            padding: 6px 8px !important; /* 极度压缩单元格内边距 */
            line-height: 1.3 !important;
        }
        /* 表头背景微调，融合深色模式 */
        [data-testid="stChatMessageContent"] th {
            background-color: rgba(255,255,255,0.05) !important;
        }
        
        /* 7. 代码块优化 */
        [data-testid="stChatMessageContent"] code {
            font-size: 13px !important;
            padding: 2px 4px !important;
        }
    }
    [data-testid="stStatusWidget"] {
        background-color: #151b26 !important; /* 深蓝黑背景 */
        border: 1px solid #3b82f6 !important; /* 蓝色边框 */
        border-radius: 10px !important;
        padding: 15px !important;
    }

    /* 强制内部所有文字变白 */
    [data-testid="stStatusWidget"] p,
    [data-testid="stStatusWidget"] div,
    [data-testid="stStatusWidget"] span,
    [data-testid="stStatusWidget"] label {
        color: #e2e8f0 !important;
    }

    /* 修复标题栏 */
    [data-testid="stStatusWidget"] header {
        background-color: transparent !important;
        color: #ffffff !important;
    }

    /* 修复图标颜色 */
    [data-testid="stStatusWidget"] svg {
        fill: #3b82f6 !important;
        color: #3b82f6 !important;
    }
    
    /* 展开后的内容区域背景 */
    [data-testid="stStatusWidget"] > div {
        background-color: transparent !important;
    }
    /* =============================================
       🔥 [修复 1] 折叠框 (Expander) 样式修复
       ============================================= */
    /* 折叠框的头部 (点击区域) */
    [data-testid="stExpander"] summary {
        color: #e2e8f0 !important; /* 亮灰白文字 */
        font-weight: 600 !important;
    }
    [data-testid="stExpander"] summary:hover {
        color: #3b82f6 !important; /* 悬停变蓝 */
    }
    [data-testid="stExpander"] svg {
        fill: #e2e8f0 !important; /* 箭头变白 */
    }
    /* 折叠框展开后的内部区域 */
    [data-testid="stExpanderDetails"] {
        background-color: transparent !important; /* 去掉默认白底 */
        color: #cbd5e1 !important; /* 内部文字颜色 */
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 8px;
    }


    /* 🔥 隐藏 Streamlit 默认的页面导航（使用自定义分组导航） */
    [data-testid="stSidebarNav"] {
        display: none !important;
    }

    /* 自定义导航标题样式 */
    .st-sidebar h3 {
        color: #e2e8f0 !important;
        font-size: 1.1rem !important;
        margin-bottom: 0.5rem !important;
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")


# ==========================================
#  🔥 [新增函数] 根据文件名直接渲染图表
#  放在 Home.py 的工具函数区域
# ==========================================
def render_chart_by_filename(filename):
    """
    直接读取 static/charts 下的 json 文件并渲染
    """
    if not filename:
        return

    # 拼凑绝对路径
    filepath = os.path.join(CHART_DIR, filename)

    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                fig_json = f.read()

            # 还原图表对象
            fig = pio.from_json(fig_json)
            config = {
                'modeBarButtonsToAdd': [
                    'drawline',
                    'drawopenpath',
                    'drawcircle',
                    'drawrect',
                    'eraseshape'
                ],
                'displaylogo': False,  # 隐藏 Plotly logo
                'scrollZoom': True  # 允许滚轮缩放
            }

            # 使用 Streamlit 原生渲染，key 设为文件名防止冲突
            st.plotly_chart(fig, use_container_width=True, key=f"history_{filename}", config=config)
        except Exception as e:
            st.error(f"图表加载异常: {e}")
    # 如果文件不存在，静默失败（不显示报错），保持界面整洁


def _parse_iso_datetime(value: str):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


INLINE_ATTACHMENT_PATTERN = re.compile(r"\[\[KNOWLEDGE_IMAGE_(\d+)\]\]")


def _normalize_attachment_items(attachments):
    if not attachments:
        return []
    return [item for item in attachments if isinstance(item, dict)][:3]


def _render_knowledge_attachment_item(item, idx: int):
    title = str(item.get("title") or f"参考图片{idx}")
    source = str(item.get("source") or "未知来源")
    score = item.get("score")
    url = str(item.get("url") or "").strip()
    expires_at = str(item.get("expires_at") or "").strip()
    exp_dt = _parse_iso_datetime(expires_at)

    if exp_dt and datetime.now(exp_dt.tzinfo) > exp_dt:
        st.info(f"🕒 {title}（图片链接已过期，请重新提问刷新）")
        st.caption(f"来源: {source}")
        return

    if url:
        st.image(url, caption=f"{title} | 来源: {source}")
        meta_parts = []
        try:
            if score is not None:
                meta_parts.append(f"匹配度: {float(score):.2f}")
        except Exception:
            pass
        if expires_at:
            meta_parts.append(f"有效期至: {expires_at}")
        if meta_parts:
            st.caption(" | ".join(meta_parts))
    else:
        st.info(f"📎 {title}（暂无可用链接）")
        st.caption(f"来源: {source}")


def render_knowledge_attachments(attachments, exclude_indices=None):
    """在回答末尾渲染知识库图片附件。"""
    valid_items = _normalize_attachment_items(attachments)
    if not valid_items:
        return

    excluded = set(exclude_indices or [])
    remaining = [(idx, item) for idx, item in enumerate(valid_items, start=1) if idx not in excluded]
    if not remaining:
        return

    st.markdown("#### 📚 参考图片")
    for idx, item in remaining:
        _render_knowledge_attachment_item(item, idx)


def render_response_with_inline_attachments(response_text: str, attachments, render_plain_when_no_token: bool = True):
    """解析回答中的 [[KNOWLEDGE_IMAGE_n]] 占位符并在文中插图。"""
    text = response_text or ""
    valid_items = _normalize_attachment_items(attachments)
    matches = list(INLINE_ATTACHMENT_PATTERN.finditer(text))

    if not matches:
        if render_plain_when_no_token:
            st.markdown(text, unsafe_allow_html=True)
        return {"has_inline": False, "used_indices": set()}

    used_indices = set()
    cursor = 0
    for match in matches:
        chunk = text[cursor:match.start()]
        if chunk and chunk.strip():
            st.markdown(chunk.strip(), unsafe_allow_html=True)

        token_idx = int(match.group(1))
        if 1 <= token_idx <= len(valid_items):
            _render_knowledge_attachment_item(valid_items[token_idx - 1], token_idx)
            used_indices.add(token_idx)
        cursor = match.end()

    tail = text[cursor:]
    if tail and tail.strip():
        st.markdown(tail.strip(), unsafe_allow_html=True)

    return {"has_inline": True, "used_indices": used_indices}


def clean_chart_tag(response_text):
    """清理 AI 乱加的图片链接和标记"""
    if not response_text:
        return ""

    text = response_text

    # 1. 删掉所有 Markdown 图片语法
    text = re.sub(r'!\[.*?\]\(.*?\)', '', text)

    # 2. 删掉 chart_xxx.json 相关链接
    text = re.sub(r'\[.*?\]\(.*?chart_[a-f0-9]+_[a-f0-9]+\.json.*?\)', '', text)

    # 3. 删掉旧标记
    text = re.sub(r'\[CHART_FILE:.*?\]', '', text)
    text = re.sub(r'\[CHART_JSON:.*?\]', '', text)

    # 4. 删掉 IMAGE_CREATED 标记
    text = re.sub(r'IMAGE_CREATED:chart_[a-zA-Z0-9_]+\.json', '', text)

    # 5. 清理多余空行（最多保留2个换行）
    text = re.sub(r'\n{3,}', '\n\n', text)

    # 🔥 [新增] 优化列表格式
    # 确保列表项前后有空行
    text = re.sub(r'([^\n])\n([•\-\*])', r'\1\n\n\2', text)

    # 🔥 [新增] 优化标题格式
    # 确保 Markdown 标题前后有空行
    text = re.sub(r'([^\n])\n(#{1,3} )', r'\1\n\n\2', text)

    return text


class TokenMonitorCallback(BaseCallbackHandler):
    """自定义回调：专门用于监听 Token 消耗并写入数据库 (异步非阻塞版)"""

    def __init__(self, username, query_text):
        self.username = username
        self.query_text = query_text

    def on_llm_end(self, response: LLMResult, **kwargs):
        """当 LLM 生成结束时触发"""
        try:
            # 1. 遍历所有生成结果
            for generation in response.generations:
                for gen in generation:
                    usage = {}
                    # 尝试提取 token_usage
                    if response.llm_output and 'token_usage' in response.llm_output:
                        usage = response.llm_output['token_usage']
                    elif gen.generation_info and 'token_usage' in gen.generation_info:
                        usage = gen.generation_info['token_usage']

                    if usage:
                        input_tokens = usage.get('input_tokens', 0)
                        output_tokens = usage.get('output_tokens', 0)

                        if input_tokens > 0 or output_tokens > 0:
                            # =================================================
                            # 🔥【核心优化】开启一个新线程去写数据库
                            # 这样主程序会立刻往下走，不会等待数据库写入完成
                            # =================================================
                            task = threading.Thread(
                                target=de.log_token_usage,
                                args=(
                                    self.username,
                                    "qwen-plus",
                                    input_tokens,
                                    output_tokens,
                                    self.query_text
                                )
                            )
                            # 设置为守护线程 (可选，意味着主程序退出它也退出，防止挂起)
                            task.daemon = True
                            task.start()

                            # print(f"🚀 已启动后台记账线程...")

        except Exception as e:
            # 这里的报错只会打印在后台，绝对不会崩掉前端页面
            print(f"Callback Error: {e}")


def native_share_button(user_content, ai_content, key):
    """
    生成分享按钮，将对话内容转为图片
    优先使用原生分享（和个人资料页一样的逻辑）
    """
    unique_id = str(uuid.uuid4())[:8]
    container_id = f"share-container-{unique_id}"
    btn_id = f"btn-{unique_id}"
    share_qr_url = "https://aiprota-img.oss-cn-beijing.aliyuncs.com/%E6%88%AA%E5%B1%8F2026-04-09%20%E4%B8%8B%E5%8D%886.05.01.png"
    share_qr_src = share_qr_url
    try:
        # OSS 未开启 CORS 时，浏览器侧 html2canvas 无法稳定抓取跨域图；先在后端转 data URL。
        resp = requests.get(
            share_qr_url,
            timeout=8,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": "https://www.aiprota.com/",
            },
        )
        resp.raise_for_status()
        mime_type = (resp.headers.get("Content-Type") or "image/png").split(";")[0].strip() or "image/png"
        share_qr_src = f"data:{mime_type};base64,{base64.b64encode(resp.content).decode('ascii')}"
    except Exception as e:
        print(f"[Share] QR code preload failed, fallback to url: {e}")

    # Markdown 转 HTML
    html_content = markdown.markdown(
        ai_content,
        extensions=['tables', 'fenced_code', 'nl2br']
    )

    # 构建分享卡片 HTML
    styled_html = f"""
    <div id="{container_id}" style="
        background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%);
        color: #e6e6e6;
        padding: 25px;
        border-radius: 16px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        line-height: 1.6;
        width: 400px;
        position: fixed; top: -9999px; left: -9999px;
        box-sizing: border-box;
    ">
        <style>
            #{container_id} table {{ border-collapse: collapse; width: 100%; margin: 10px 0; font-size: 12px; color: #e6e6e6; }}
            #{container_id} th, #{container_id} td {{ border: 1px solid #475569; padding: 6px 8px; text-align: left; }}
            #{container_id} th {{ background-color: rgba(255, 255, 255, 0.1); color: #fff; font-weight: bold; }}
            #{container_id} h1, #{container_id} h2, #{container_id} h3, #{container_id} h4 {{ color: #ffffff; margin-top: 15px; margin-bottom: 8px; font-weight: 700; }}
            #{container_id} strong {{ color: #FFD700; }}
            #{container_id} ul, #{container_id} ol {{ padding-left: 20px; margin: 5px 0; }}
            #{container_id} li {{ margin-bottom: 4px; }}
            #{container_id} p {{ margin-bottom: 8px; }}
        </style>

        <div style="display: flex; align-items: center; margin-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 15px;">
            <div style="font-size: 24px; margin-right: 10px;">📊</div>
            <div>
                <div style="font-weight: 900; font-size: 16px; color: #fff;">来自爱波塔-最懂期权的AI</div>
                <div style="font-size: 11px; color: #94a3b8;">www.aiprota.com</div>
            </div>
        </div>

        <div style="
            background: rgba(255,255,255,0.08); 
            border-left: 4px solid #3b82f6; 
            padding: 12px; 
            border-radius: 6px; 
            margin-bottom: 20px;
        ">
            <div style="font-size: 12px; color: #94a3b8; margin-bottom: 4px; font-weight:bold;">👤 提问:</div>
            <div style="font-size: 14px; color: #fff; font-weight: 500;">{user_content}</div>
        </div>

        <div style="margin-bottom: 20px;">
            <div style="font-size: 12px; color: #10b981; margin-bottom: 6px; font-weight:bold;">🤖 AI 分析:</div>
            <div style="font-size: 13px; color: #cbd5e1;">{html_content}</div>
        </div>

        <div style="
            display: flex; justify-content: space-between; align-items: flex-end;
            border-top: 1px dashed rgba(255,255,255,0.1); padding-top: 12px; margin-top: 18px;
        ">
            <div>
                <div style="font-size: 11px; color: #64748b;">Generated by 爱波塔</div>
                <div style="font-size: 11px; color: #3b82f6;">www.aiprota.com</div>
            </div>
            <div style="text-align:center; margin-left: 12px;">
                <img
                    src="{share_qr_src}"
                    alt="爱波塔小程序二维码"
                    onerror="this.style.display='none';"
                    style="display:block; width:72px; height:72px; padding:4px; border-radius:8px; background:#ffffff;"
                />
                <div style="margin-top:4px; font-size:10px; color:#94a3b8;">扫码使用小程序</div>
            </div>
        </div>
    </div>
    """

    # 🔥 关键：JS 逻辑完全复制自个人资料页（已验证能正常工作）
    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        .share-btn {{
            background-color: transparent; border: 1px solid #4B5563; color: #9CA3AF;
            padding: 5px 12px; border-radius: 20px; font-size: 12px; cursor: pointer;
            display: inline-flex; align-items: center; margin-top: 8px; transition: all 0.2s;
        }}
        .share-btn:hover {{ background-color: #3b82f6; color: white; border-color: #3b82f6; }}
    </style>
    </head>
    <body>
        {styled_html}
        <button class="share-btn" id="{btn_id}" onclick="generateAndShare()">
            <i class="fas fa-share-square" style="margin-right:5px;"></i> 分享完整对话
        </button>
        <script>
        function waitForImages(node) {{
            const images = Array.from(node.querySelectorAll('img'));
            if (!images.length) {{
                return Promise.resolve();
            }}
            return Promise.all(images.map((img) => {{
                if (img.complete) {{
                    return Promise.resolve();
                }}
                return new Promise((resolve) => {{
                    const done = () => resolve();
                    img.addEventListener('load', done, {{ once: true }});
                    img.addEventListener('error', done, {{ once: true }});
                }});
            }}));
        }}

        function generateAndShare() {{
            const btn = document.getElementById('{btn_id}');
            const originalText = btn.innerHTML;
            const target = document.getElementById('{container_id}');
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 生成中...';

            waitForImages(target).then(() => html2canvas(target, {{
                backgroundColor: null,
                scale: 2,
                logging: false,
                useCORS: true
            }})).then(canvas => {{
                canvas.toBlob(function(blob) {{
                    if (!blob) {{
                        resetBtn(btn, originalText);
                        alert("分享图生成失败，请稍后重试。");
                        return;
                    }}
                    const file = new File([blob], "aiprota_analysis.png", {{ type: "image/png" }});
                    if (navigator.canShare && navigator.canShare({{ files: [file] }})) {{
                        navigator.share({{ files: [file], title: '爱波塔 AI 分析' }}).then(() => resetBtn(btn, originalText)).catch(() => resetBtn(btn, originalText));
                    }} else {{
                        alert("您的浏览器不支持直接分享，请截图保存。");
                        resetBtn(btn, originalText);
                    }}
                }}, 'image/png');
            }}).catch(() => {{
                resetBtn(btn, originalText);
                alert("分享图生成失败，请稍后重试。");
            }});
        }}
        function resetBtn(btn, text) {{ btn.innerHTML = text; }}
        </script>
    </body>
    </html>
    """
    components.html(html_code, height=50)


# ==========================================
#  3. Auth & State 初始化 (保持不变)
# ==========================================
cookie_manager = stx.CookieManager(key="main_cookie_manager")
cookies = cookie_manager.get_all() or {}
cookie_user = str(cookies.get("username") or "").strip()
cookie_token = str(cookies.get("token") or "").strip()
if "home_cookie_retry_once" not in st.session_state:
    st.session_state.home_cookie_retry_once = False
if "home_invalid_retry_count" not in st.session_state:
    st.session_state.home_invalid_retry_count = 0
if "home_post_login_restore_needed" not in st.session_state:
    st.session_state.home_post_login_restore_needed = False
if "home_post_login_restore_done" not in st.session_state:
    st.session_state.home_post_login_restore_done = False
if "home_auto_login_toast_token" not in st.session_state:
    st.session_state.home_auto_login_toast_token = ""
if "home_auth_verify_needed" not in st.session_state:
    st.session_state.home_auth_verify_needed = False
if "home_auth_verified_sig" not in st.session_state:
    st.session_state.home_auth_verified_sig = ""
if "home_invalid_token_signature" not in st.session_state:
    st.session_state.home_invalid_token_signature = ""
if "home_masked_email_user" not in st.session_state:
    st.session_state.home_masked_email_user = ""
if "home_masked_email_value" not in st.session_state:
    st.session_state.home_masked_email_value = ""
if "home_invite_code" not in st.session_state:
    st.session_state.home_invite_code = ""
if "home_invite_landing_active" not in st.session_state:
    st.session_state.home_invite_landing_active = False
if "home_invite_landing_session_id" not in st.session_state:
    st.session_state.home_invite_landing_session_id = ""
try:
    qp_invite = st.query_params.get("invite", "")
    if isinstance(qp_invite, list):
        qp_invite = qp_invite[0] if qp_invite else ""
    qp_invite = "".join(ch for ch in str(qp_invite or "").strip() if ch.isalnum())[:64]
    if qp_invite:
        st.session_state.home_invite_code = qp_invite
        st.session_state.home_invite_landing_active = True
    else:
        st.session_state.home_invite_landing_active = False
except Exception:
    pass


def _restore_login_with_cookie_state(cookies: dict):
    """
    返回:
    - restored: bool
    - state: ok | empty | partial | invalid | error
    """
    cookies = cookies or {}
    try:
        restored = auth.restore_login_from_cookies(cookies)
    except Exception:
        return False, "error"

    if restored:
        return True, "ok"

    c_user = str(cookies.get("username") or "").strip()
    c_token = str(cookies.get("token") or "").strip()
    if not c_user and not c_token:
        return False, "empty"
    if (c_user and not c_token) or (c_token and not c_user):
        return False, "partial"
    return False, "invalid"


def _auth_signature(username: str, token: str) -> str:
    user = str(username or "").strip()
    tok = str(token or "").strip()
    if not user or not tok:
        return ""
    return f"{user}:{tok}"


def _mark_auth_verified(username: str, token: str):
    st.session_state.home_auth_verify_needed = False
    st.session_state.home_auth_verified_sig = _auth_signature(username, token)
    st.session_state.home_invalid_token_signature = ""


def _ensure_auth_verified_for_protected_action() -> bool:
    """Strictly verify token for privileged actions (task submit/recovery)."""
    if not st.session_state.get("is_logged_in", False):
        return True

    user = str(st.session_state.get("user_id") or "").strip()
    token = str(st.session_state.get("token") or "").strip()
    if not user or not token:
        return True

    sig = _auth_signature(user, token)
    if (
        not st.session_state.get("home_auth_verify_needed", False)
        and st.session_state.get("home_auth_verified_sig", "") == sig
    ):
        return True

    try:
        ok = bool(auth.check_token(user, token))
    except Exception as e:
        print(f"鉴权校验异常: {e}")
        ok = False

    if ok:
        _mark_auth_verified(user, token)
        return True

    # token 失效：避免下一次再次乐观恢复同一个失效 token
    st.session_state.home_invalid_token_signature = sig
    st.session_state["is_logged_in"] = False
    st.session_state["user_id"] = None
    st.session_state["token"] = None
    st.session_state.home_post_login_restore_needed = False
    st.session_state.home_post_login_restore_done = False
    st.session_state.home_auto_login_toast_token = ""
    st.session_state.home_auth_verify_needed = False
    st.session_state.home_auth_verified_sig = ""
    st.session_state.home_masked_email_user = ""
    st.session_state.home_masked_email_value = ""
    try:
        cookie_manager.delete("username", key="auth_verify_del_user")
        cookie_manager.delete("token", key="auth_verify_del_token")
    except Exception:
        pass
    st.warning("登录状态已过期，请重新登录。")
    return False


def _extract_client_ip_and_device_fingerprint() -> tuple[str, str]:
    ip = ""
    device = ""
    try:
        headers = dict(st.context.headers or {})
    except Exception:
        headers = {}
    try:
        ip = str(getattr(st.context, "ip_address", "") or "").strip()
    except Exception:
        ip = ""
    if not ip:
        xff = str(headers.get("X-Forwarded-For") or headers.get("x-forwarded-for") or "").strip()
        if xff:
            ip = xff.split(",")[0].strip()
    device = str(headers.get("User-Agent") or headers.get("user-agent") or "").strip()
    return ip, device


def _resolve_base_url_from_request() -> str:
    try:
        headers = dict(st.context.headers or {})
    except Exception:
        headers = {}
    proto = str(headers.get("X-Forwarded-Proto") or headers.get("x-forwarded-proto") or "").strip()
    host = str(headers.get("Host") or headers.get("host") or "").strip()
    if host:
        return f"{_resolve_scheme(host, proto)}://{host}"
    return "https://www.aiprota.com"


def _clear_invite_query_state():
    st.session_state.home_invite_code = ""
    st.session_state.home_invite_landing_active = False
    try:
        if "invite" in st.query_params:
            del st.query_params["invite"]
    except Exception:
        try:
            st.query_params.clear()
        except Exception:
            pass


def _complete_logged_in_session(username: str, token: str, *, cookie_key_prefix: str, clear_invite_query: bool = False):
    st.session_state['is_logged_in'] = True
    st.session_state['user_id'] = username
    st.session_state['token'] = token
    st.session_state['just_manual_logged_in'] = True
    _mark_auth_verified(username, token)
    st.session_state.home_post_login_restore_needed = True
    st.session_state.home_post_login_restore_done = False
    st.session_state.home_masked_email_user = ""
    st.session_state.home_masked_email_value = ""

    expires = datetime.now() + timedelta(days=30)
    cookie_manager.set("username", username, expires_at=expires, key=f"{cookie_key_prefix}_user_cookie")
    cookie_manager.set("token", token, expires_at=expires, key=f"{cookie_key_prefix}_token_cookie")

    if clear_invite_query:
        _clear_invite_query_state()


def _clear_register_flow_state(prefix: str):
    for suffix in [
        "step1_ok",
        "step1_username",
        "step1_password",
        "verified_phone",
        "phone",
        "sms_code",
        "step1_username_input",
        "step1_password_input",
        "step1_password2_input",
        "view_tracked_code",
    ]:
        st.session_state.pop(f"{prefix}_{suffix}", None)


def _get_invite_landing_session_id() -> str:
    current = str(st.session_state.get("home_invite_landing_session_id") or "").strip()
    if current:
        return current
    current = str(uuid.uuid4())
    st.session_state.home_invite_landing_session_id = current
    return current


def _track_invite_event_safe(event_type: str, invite_code: str, *, invitee_user_id: str = "", metadata: Optional[Dict[str, Any]] = None):
    if invite_svc is None:
        return
    register_ip, device_fingerprint = _extract_client_ip_and_device_fingerprint()
    try:
        invite_svc.track_invite_event(
            invite_code,
            event_type,
            session_id=_get_invite_landing_session_id(),
            invitee_user_id=invitee_user_id,
            register_ip=register_ip,
            device_fingerprint=device_fingerprint,
            metadata=metadata or {},
        )
    except Exception as e:
        print(f"[invite][event] type={event_type} code={invite_code} err={e}")


def _render_invite_register_form(invite_context: Dict[str, Any]):
    prefix = "invite_reg"
    step1_ok = st.session_state.get(f"{prefix}_step1_ok", False)
    verified_phone = st.session_state.get(f"{prefix}_verified_phone", "")
    invite_code = str(invite_context.get("invite_code") or "").strip()
    invite_is_valid = bool(invite_context.get("is_valid"))

    st.caption("步骤 1：先设置账号与密码")
    if step1_ok:
        step1_user = st.session_state.get(f"{prefix}_step1_username", "")
        st.success(f"账号已就绪：{step1_user}")
        if st.button("修改账号信息", key=f"{prefix}_btn_reset_step1", use_container_width=True):
            _clear_register_flow_state(prefix)
            st.rerun()
    else:
        with st.form(key=f"{prefix}_form_step1", clear_on_submit=False):
            reg_username = st.text_input(
                "账号",
                key=f"{prefix}_step1_username_input",
                placeholder="至少 3 个字符",
            )
            reg_password = st.text_input(
                "密码",
                type="password",
                key=f"{prefix}_step1_password_input",
                placeholder="至少 6 位",
            )
            reg_password2 = st.text_input(
                "确认密码",
                type="password",
                key=f"{prefix}_step1_password2_input",
                placeholder="再次输入密码",
            )
            submitted_step1 = st.form_submit_button("继续设置手机号", type="primary", use_container_width=True)
        if submitted_step1:
            ok, msg, normalized_username = auth.validate_register_step1(reg_username, reg_password, reg_password2)
            if ok:
                st.session_state[f"{prefix}_step1_ok"] = True
                st.session_state[f"{prefix}_step1_username"] = normalized_username
                st.session_state[f"{prefix}_step1_password"] = reg_password
                st.success("账号信息已通过校验，请继续绑定手机号。")
                st.rerun()
            else:
                st.error(msg)

    if not st.session_state.get(f"{prefix}_step1_ok"):
        return

    st.caption("步骤 2：绑定手机号并验证")
    if verified_phone:
        st.success(f"手机号已验证：{verified_phone}")
        if st.button("更换手机号", key=f"{prefix}_btn_reset_phone", use_container_width=True):
            st.session_state.pop(f"{prefix}_verified_phone", None)
            st.session_state.pop(f"{prefix}_phone", None)
            st.session_state.pop(f"{prefix}_sms_code", None)
            st.rerun()
    else:
        reg_phone = st.text_input(
            "手机号（仅 +86）",
            key=f"{prefix}_phone",
            placeholder="例如 13800138000",
        )
        reg_sms_code = st.text_input(
            "短信验证码",
            key=f"{prefix}_sms_code",
            max_chars=6,
            placeholder="输入 6 位验证码",
        )
        send_col, verify_col = st.columns(2)
        with send_col:
            if st.button("发送验证码", use_container_width=True, key=f"{prefix}_btn_send_code"):
                if not reg_phone:
                    st.warning("请先输入手机号")
                else:
                    ok, msg = auth.send_register_phone_code(reg_phone)
                    if ok:
                        st.success(msg or "验证码已发送，请注意查收")
                    else:
                        st.error(msg)
        with verify_col:
            if st.button("验证手机号", use_container_width=True, key=f"{prefix}_btn_verify_code"):
                if not reg_phone:
                    st.warning("请先输入手机号")
                elif not reg_sms_code:
                    st.warning("请输入短信验证码")
                else:
                    ok, msg, normalized_phone = auth.verify_register_phone_code(reg_phone, reg_sms_code)
                    if ok:
                        st.session_state[f"{prefix}_verified_phone"] = normalized_phone
                        st.success("手机号验证通过，可直接完成注册。")
                        st.rerun()
                    else:
                        st.error(msg)

    if not st.session_state.get(f"{prefix}_verified_phone"):
        return

    if invite_is_valid:
        st.info("邀请码已锁定，注册成功后将自动计入邀请活动。")
    else:
        st.warning("当前邀请码无效，本次注册可继续，但不会计入邀请奖励。")

    if st.button("完成注册并登录", type="primary", use_container_width=True, key=f"{prefix}_btn_finish"):
        final_username = st.session_state.get(f"{prefix}_step1_username", "")
        final_password = st.session_state.get(f"{prefix}_step1_password", "")
        final_phone = st.session_state.get(f"{prefix}_verified_phone", "")
        register_ip, device_fingerprint = _extract_client_ip_and_device_fingerprint()

        _track_invite_event_safe(
            "register_submit",
            invite_code,
            invitee_user_id=final_username,
            metadata={"invite_valid": invite_is_valid},
        )

        success, msg = auth.register_with_username_phone(
            final_username,
            final_password,
            final_phone,
            invite_code=invite_code if invite_is_valid else "",
            register_ip=register_ip,
            device_fingerprint=device_fingerprint,
        )
        if not success:
            st.error(msg)
            return

        _track_invite_event_safe(
            "register_success",
            invite_code,
            invitee_user_id=final_username,
            metadata={"invite_valid": invite_is_valid},
        )
        st.success(msg if msg else "注册成功")
        st.balloons()

        sess_ok, sess_msg, token = auth.create_user_session(final_username)
        if sess_ok:
            _complete_logged_in_session(
                final_username,
                token,
                cookie_key_prefix=f"{prefix}_login",
                clear_invite_query=True,
            )
            _clear_register_flow_state(prefix)
            time.sleep(0.25)
            st.rerun()
        else:
            st.warning(sess_msg if sess_msg else "注册成功，请登录")


def _render_logged_in_invite_panel():
    current_user = str(st.session_state.get("user_id") or "").strip()
    st.warning(f"当前浏览器已登录为 {current_user or '当前账号'}，邀请注册页不会直接覆盖已有登录态。")
    st.caption("如果你是帮朋友测试或准备注册新账号，请先退出当前账号，再继续使用这个邀请码注册。")

    action_col1, action_col2 = st.columns(2)
    with action_col1:
        if st.button("退出当前账号并继续注册", type="primary", use_container_width=True, key="invite_logout_then_register"):
            logout_user = str(st.session_state.get("user_id") or "").strip()
            logout_token = str(st.session_state.get("token") or "").strip()
            logout_sig = _auth_signature(logout_user, logout_token)

            if logout_user and logout_user != "访客":
                try:
                    auth.logout_user(logout_user, logout_token)
                except Exception as e:
                    print(f"[invite][logout] user={logout_user} err={e}")

            try:
                cookie_manager.delete("username", key="invite_logout_del_user")
                cookie_manager.delete("token", key="invite_logout_del_token")
            except Exception as e:
                print(f"[invite][logout] cookie delete err={e}")

            st.session_state['is_logged_in'] = False
            st.session_state['user_id'] = None
            st.session_state['token'] = None
            st.session_state['just_logged_out'] = False
            st.session_state.home_post_login_restore_needed = False
            st.session_state.home_post_login_restore_done = False
            st.session_state.home_auto_login_toast_token = ""
            st.session_state.home_auth_verify_needed = False
            st.session_state.home_auth_verified_sig = ""
            st.session_state.home_invalid_token_signature = logout_sig
            st.session_state.home_masked_email_user = ""
            st.session_state.home_masked_email_value = ""
            st.success("已退出当前账号，正在切换到邀请注册页。")
            time.sleep(0.2)
            st.rerun()
    with action_col2:
        if st.button("继续进入首页", use_container_width=True, key="invite_go_home_logged_in"):
            _clear_invite_query_state()
            st.rerun()


def _render_invite_register_page_if_needed() -> bool:
    invite_code = str(st.session_state.get("home_invite_code") or "").strip()
    if not st.session_state.get("home_invite_landing_active", False) or not invite_code:
        return False

    invite_context = {
        "invite_code": invite_code,
        "is_valid": False,
        "inviter_user_id": "",
        "reward_points": 300,
    }
    if invite_svc is not None:
        try:
            invite_context = invite_svc.get_invite_context(invite_code)
        except Exception as e:
            print(f"[invite][landing] context failed: {e}")

    tracked_code = str(st.session_state.get("invite_reg_view_tracked_code") or "").strip()
    if tracked_code != invite_code:
        _track_invite_event_safe(
            "landing_view",
            invite_code,
            metadata={"invite_valid": bool(invite_context.get("is_valid"))},
        )
        st.session_state["invite_reg_view_tracked_code"] = invite_code

    if st.session_state.get("is_logged_in"):
        render_invite_register_landing(invite_context, _render_logged_in_invite_panel)
    else:
        render_invite_register_landing(invite_context, lambda: _render_invite_register_form(invite_context))
    return True


# 🔥 [关键修复] 在任何 rerun 之前，先读取公告状态并保存到 session_state
# 这样即使后面触发 rerun，公告状态也不会丢失
if ENABLE_HOME_ANNOUNCEMENT and 'announcement_cookie_loaded' not in st.session_state:
    try:
        cm = stx.CookieManager(key="early_announcement_reader")
        announcement_cookies = cm.get_all() or {}
        st.session_state.announcement_last_shown_date = announcement_cookies.get(
            ANNOUNCEMENT_LAST_SHOWN_COOKIE_KEY,
            None,
        )
        st.session_state.announcement_cookie_loaded = True
    except:
        st.session_state.announcement_last_shown_date = None
        st.session_state.announcement_cookie_loaded = True

# 延迟落 cookie：避免公告刚弹出就被组件 rerun 冲掉
if ENABLE_HOME_ANNOUNCEMENT:
    flush_pending_announcement_cookie()

# 初始化待处理任务状态
if "pending_task" not in st.session_state:
    st.session_state.pending_task = None
if "pending_tasks" not in st.session_state:
    st.session_state.pending_tasks = []
if "deep_mode_enabled" not in st.session_state:
    st.session_state.deep_mode_enabled = False
if "pending_portfolio_task" not in st.session_state:
    st.session_state.pending_portfolio_task = None
if "portfolio_last_attempt_hash" not in st.session_state:
    st.session_state.portfolio_last_attempt_hash = None
if "portfolio_latest_result" not in st.session_state:
    st.session_state.portfolio_latest_result = None

# 尝试从 Cookie 恢复登录
# 【关键修复 1】增加 'just_logged_out' 判断，如果刚点了登出，绝不执行自动登录
should_auto_login = (
    not st.session_state.get('is_logged_in', False)
    and not st.session_state.get('just_logged_out', False)  # ← 这行很重要！
)

if should_auto_login:
    c_user = cookie_user
    c_token = cookie_token
    cookie_sig = _auth_signature(c_user, c_token)
    invalid_sig = str(st.session_state.get("home_invalid_token_signature") or "").strip()

    if c_user and c_token and cookie_sig and cookie_sig != invalid_sig:
        # 乐观恢复：优先恢复登录态并渲染首屏，严校验延后到关键动作前。
        st.session_state["is_logged_in"] = True
        st.session_state["user_id"] = c_user
        st.session_state["token"] = c_token
        st.session_state.home_cookie_retry_once = False
        st.session_state.home_invalid_retry_count = 0
        st.session_state.home_auth_verify_needed = True
        st.session_state.home_auth_verified_sig = ""
        st.session_state.home_post_login_restore_needed = True
        st.session_state.home_post_login_restore_done = False
    elif (not c_user and not c_token) and not st.session_state.get("home_cookie_retry_once", False):
        # 某些浏览器首轮 Cookie 组件还没就绪，允许一次重跑。
        st.session_state.home_cookie_retry_once = True
        st.rerun()
    elif ((c_user and not c_token) or (c_token and not c_user)) and not st.session_state.get("home_cookie_retry_once", False):
        # 只读到部分字段，允许一次重跑。
        st.session_state.home_cookie_retry_once = True
        st.rerun()

if st.session_state.get('just_logged_out', False):
    # 仅在确认 Cookie 已清空后才允许重新进入自动登录流程。
    if not cookie_user and not cookie_token:
        st.session_state['just_logged_out'] = False

# 只有第一次运行时才初始化，如果已经登录了，不要重置它
if 'is_logged_in' not in st.session_state:
    st.session_state['is_logged_in'] = False
    st.session_state['user_id'] = None
    st.session_state['username'] = None
    st.session_state['token'] = None
    st.session_state.home_auth_verify_needed = False
    st.session_state.home_auth_verified_sig = ""




# 初始化聊天记录
if "messages" not in st.session_state:
    st.session_state.messages = []
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = str(uuid.uuid4())

# 🔥 [新增] 图片上传器的动态 key，用于清除图片
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0


def _restore_pending_tasks_after_auto_login_once():
    """Restore pending tasks once after auto login, without blocking first paint."""
    if not st.session_state.get("is_logged_in", False):
        return
    # 登录首屏优先，待用户触发关键动作并完成严校验后再恢复后台任务。
    if st.session_state.get("home_auth_verify_needed", False):
        return
    if not st.session_state.get("home_post_login_restore_needed", False):
        return
    if st.session_state.get("home_post_login_restore_done", False):
        return

    c_user = str(st.session_state.get("user_id") or "").strip()
    c_token = str(st.session_state.get("token") or "").strip()
    if not c_user:
        st.session_state.home_post_login_restore_needed = False
        st.session_state.home_post_login_restore_done = True
        return

    task_manager = TaskManager()
    pending_task_data = task_manager.get_user_pending_task(c_user)
    pending_deep_task_data = None
    if ENABLE_DEEP_MODE and DeepTaskManager is not None:
        deep_task_manager = DeepTaskManager()
        pending_deep_task_data = deep_task_manager.get_user_pending_task(c_user)
    pending_portfolio_data = task_manager.get_user_pending_portfolio_task(c_user)
    if not pending_task_data and pending_deep_task_data:
        pending_task_data = pending_deep_task_data

    restored_any = False
    if pending_task_data and not st.session_state.get("pending_tasks"):
        restored_tasks = _refresh_home_pending_tasks(c_user)
        if restored_tasks and not st.session_state.get("messages"):
            restored_task = restored_tasks[0]
            restored_prompt = str(restored_task.get("raw_prompt") or restored_task.get("prompt") or "")
            restored_task_id = str(restored_task.get("task_id") or "")
            st.session_state.messages = [
                {"role": "user", "content": restored_prompt, "linked_task_id": restored_task_id},
                _build_task_placeholder_message(
                    task_id=restored_task_id,
                    prompt_text=restored_prompt,
                    trace_id=str(restored_task.get("trace_id") or ""),
                    answer_id=str(restored_task.get("answer_id") or ""),
                    intent_domain=str(restored_task.get("intent_domain") or "general"),
                    chat_mode=str(restored_task.get("chat_mode") or CHAT_MODE_ANALYSIS),
                ),
            ]
        restored_any = bool(restored_tasks)
        if restored_tasks:
            print(f"✅ 自动登录后恢复任务队列: {restored_tasks[0]['task_id']}")

    if pending_portfolio_data and not st.session_state.get("pending_portfolio_task"):
        st.session_state.pending_portfolio_task = {
            "task_id": pending_portfolio_data["task_id"],
            "start_time": pending_portfolio_data["start_time"],
            "screenshot_hash": pending_portfolio_data.get("screenshot_hash", ""),
            "positions_count": pending_portfolio_data.get("positions_count", 0),
        }
        restored_any = True
        print(f"✅ 自动登录后恢复持仓任务: {pending_portfolio_data['task_id']}")

    toast_token = f"{c_user}:{c_token}"
    if st.session_state.get("home_auto_login_toast_token") != toast_token:
        if restored_any:
            st.toast(f"欢迎回来，{c_user} (已恢复您的任务)")
        else:
            st.toast(f"欢迎回来，{c_user} (自动登录)")
        st.session_state.home_auto_login_toast_token = toast_token

    st.session_state.home_post_login_restore_done = True
    st.session_state.home_post_login_restore_needed = False


def _get_cached_masked_email(user: str) -> str:
    user = str(user or "").strip()
    if not user:
        return ""
    cached_user = str(st.session_state.get("home_masked_email_user") or "").strip()
    if cached_user == user:
        return str(st.session_state.get("home_masked_email_value") or "")
    masked = str(auth.get_masked_email(user) or "")
    st.session_state.home_masked_email_user = user
    st.session_state.home_masked_email_value = masked
    return masked

# ==========================================
#  4. AI Agent 定义 (完全保留您的核心逻辑)
# ==========================================
def get_agent(current_user="访客", user_query=""):
    # 1. 初始化 LLM (保持不变)
    api_key = os.getenv("DASHSCOPE_API_KEY")
    if not api_key:
        st.error("❌ 未配置 API KEY")
        return None

    # 1. ⚡ 快马 (Turbo): 便宜、速度快 -> 用于主管分发、简单闲聊
    llm_turbo = ChatTongyi(
        model="qwen-turbo",
        temperature=0.1,
        api_key=api_key
    )

    # 2. ⚖️ 专才 (Plus): 性价比高、能力均衡 -> 用于技术分析、数据总结
    llm_plus = ChatTongyi(
        model="qwen3.6-plus",
        temperature=0.2,  # 稍微增加一点创造性
        api_key=api_key
    )

    # 3. 🧠 大脑 (Max): 最贵、逻辑最强 -> 用于期权策略、王牌分析、CIO总结
    llm_max = ChatTongyi(
        model="qwen3-max",
        temperature=0.4,
        api_key=api_key,
        request_timeout=300  # 复杂任务给多点时间
    )
    # 2. 构建图 (The Graph)
    # 直接调用 agent_core.py 里的函数
    graph_app = build_trading_graph(
        fast_llm=llm_turbo,
        mid_llm=llm_plus,
        smart_llm=llm_max
    )

    return graph_app


# 定义随机幽默加载文案
LOADING_JOKES = [
    " AI正在思考，这问题太简单，我该如何回答...",
    "⚡️ AI正在思考，回想Jack老师的教导...",
    "📈 AI正在思考，顺便用紫微斗数模拟未来 1000 种走势...",
    "📈 AI正在思考，默默拿出K线战法偷看...",
    "🧘‍♂️ AI正在思考，平复最近赚钱激动的心，保持客观...",
    "📞 AI正在思考，连线华尔街内幕人士...",
    "📞 AI正在思考，给主力资金打电话核实...",
    "📞 AI正在思考，准备求助游资大佬...",
    "📞 AI正在思考，哪里可以定KTV...",
    " AI正在思考，偷偷拿出水晶球...",
    " AI正在思考，应该说实话吗...",
    "📉 AI正在思考，顺便检查这根 K 线是不是骗线...",
    " AI正在思考，牛市里应该怎么做...",
    " AI正在思考，尽力跳脱刚才亏钱的思绪里...",
    "🧠 AI正在思考，回想您上次亏损是不是因为没听我劝...",
    "🧠 AI正在思考，感觉这个用户好像很贪心...",
    "🧠 AI正在思考，不知道这用户在害怕什么...",
    "🧠 AI正在思考，要不要建议你飞龙在天...",
    "🧠 AI正在思考，是不是应该劝你all in...",
    "⚡️ AI正在思考，准备请教陈老师..."
]


# ==========================================
#  🔥 新增：极速伪路由 (Fast Pass)
# ==========================================
def fast_router_check(user_query):
    """
    检查用户问题是否可以走快速通道。
    返回: (bool, str) -> (是否命中, 回复内容)
    """

    # ============================================================
    # 🔥 [新增功能] 晚报订阅/退订 极速拦截
    # ============================================================

    # 1. 定义关键词
    sub_keywords = ["订阅晚报", "订阅日报", "开启晚报", "开通晚报", "订阅复盘"]
    unsub_keywords = ["取消订阅", "退订", "关闭晚报", "不要晚报", "取消日报"]

    # 2. 检查是否命中
    is_sub_intent = any(k in user_query for k in sub_keywords)
    is_unsub_intent = any(k in user_query for k in unsub_keywords)

    if is_sub_intent or is_unsub_intent:
        # 获取当前用户
        current_user = st.session_state.get("user_id", "访客")

        # A. 如果未登录
        if current_user == "访客" or not current_user:
            return True, "🔒 **请先登录**\n\n您需要登录后才能管理晚报订阅设置。"

        if is_sub_intent:
            return True, (
                "### 💳 晚报订阅已升级为点数购买\n\n"
                "请前往左侧 **个人中心 → 充值中心** 完成充值并购买订阅。\n\n"
                "购买后权限会自动生效，内容无需人工开通。"
            )

        # 退订：保留仅关闭邮件通知，不删除订阅记录
        with st.chat_message("assistant", avatar="🤖"):
            with st.status("⚙️ 正在更新订阅设置...", expanded=True) as status:
                channel = sub_svc.get_channel_by_code("daily_report")
                if not channel:
                    status.update(label="❌ 系统配置错误", state="error")
                    return True, "⚠️ 系统错误：找不到【复盘晚报】频道配置，请联系管理员。"

                success = sub_svc.update_notification_settings(current_user, channel["id"], notify_email=False)
                if success:
                    status.update(label="✅ 已关闭通知", state="complete")
                    return True, "### ✅ 已取消订阅\n\n您的邮件通知已关闭，将不再收到复盘晚报邮件。\n\n(您依然可以在【情报站】查看历史内容)"

                status.update(label="❌ 操作失败", state="error")
                return True, "⚠️ 系统繁忙，操作失败，请稍后再试。"

    # 1. 定义【绝对需要 Agent 思考】的复杂词 (负面清单 - 增强版)
    # 🔥 [修改点] 增加了 "概率", "几成", "可能", "战争", "局势" 等词，防止地缘政治问题被拦截
    complex_keywords = [
        "策略", "建议", "怎么做", "分析", "为何", "原因", "预测","K线","技术面","分析",
        "教学", "是什么", "含义", "解释", "持仓", "风险", "复盘", "总结","账户","资金",
        "止损", "止盈", "平仓", "割肉", "买入", "卖出", "加仓","相关性","相关度",
        "牛市价差", "熊市价差", "备兑", "跨式", "双卖","为什么","期权","距离","吗",
        "高吗", "低吗", "合适吗", "能买吗","IV","波动率","国债","利率",
        "概率", "几成", "可能性", "胜率", "吧"
    ]

    # 2. 定义【简单查询】的触发词 (正面清单 - 收紧版)
    # 🔥 [修改点] 去掉了单独的 "多少"，改为 "多少钱", "多少点"
    price_keywords = [
        "价格多少", "现价", "收盘", "开盘", "最新价", "报价",
        "多少点", "多少钱", "几点", "股价", "价格"
    ]

    # 3. 状态判断
    has_complex = any(k in user_query for k in complex_keywords)
    has_price = any(k in user_query for k in price_keywords)
    q_lower = str(user_query or "").lower()
    en_price_keywords = ["price", "quote", "last", "close", "now", "snapshot"]
    has_en_price = any(k in q_lower for k in en_price_keywords)
    us_ticker_pattern = r"\b(AAPL|TSLA|NVDA|MSFT|AMZN|GOOG|META|AVGO|AMD|INTC|TSM)\b"
    has_us_ticker = re.search(us_ticker_pattern, str(user_query).upper()) is not None
    us_alias_keywords = [
        "美股", "纳斯达克", "纽交所", "道琼斯", "标普", "apple", "tesla", "nvidia", "microsoft",
        "amazon", "google", "meta", "broadcom", "amd", "intel", "aapl", "tsla", "nvda", "msft",
        "amzn", "goog", "meta", "avgo", "intc", "tsm", "苹果", "特斯拉", "英伟达", "微软", "亚马逊", "谷歌",
    ]
    has_us_context = any(k in q_lower for k in [s.lower() for s in us_alias_keywords])

    # 4. 特殊补丁：如果用户只输入了 代码+多少 (例如 "茅台多少")
    # 虽然 "多少" 被删了，但我们要允许 "代码+多少" 的模糊匹配，前提是它不包含 complex 词
    # 简单的正则判断：是否有 "多少" 且没有 "概率/人口" 等
    is_fuzzy_price = "多少" in user_query and not has_complex

    # 过滤词 (有些词虽然像查询，但其实是期权链查询，不适合 snapshot)
    forbidden_terms = ["iv", "IV", "波动率", "期权", "认购", "认沽", "call", "put"]
    has_forbidden = any(k in user_query for k in forbidden_terms)

    # 5. 路由逻辑
    # 只有在 (命中精准价格词 OR 命中模糊多少) AND (没有复杂词) AND (没有禁用词) 时才触发
    is_price_like = has_price or is_fuzzy_price or has_en_price
    is_us_fast_price = (has_us_ticker or has_us_context) and (has_en_price or has_price or is_fuzzy_price)
    is_fast_query = (is_price_like or is_us_fast_price) and (not has_complex) and (not has_forbidden)

    if not is_fast_query:
        return False, None

    # --- ⚡ 走快速通道处理 (仅限 Snapshot) ---
    with st.chat_message("assistant", avatar="🤖"):
        with st.status("⚡ 正在连接交易所行情...", expanded=True) as status:
            try:
                # 🔥🔥🔥 [核心修复 1] 字符串清洗 (脱水处理)
                # 目的：把 "长江电力价格多少" 变成 "长江电力"
                clean_query = user_query

                # 按长度降序排，优先删长词 (防止删了"价格"剩下"多少")
                target_kws = sorted(price_keywords + ["多少"] + en_price_keywords, key=len, reverse=True)

                for kw in target_kws:
                    clean_query = clean_query.replace(kw, "")
                    clean_query = re.sub(rf"(?i)\b{re.escape(kw)}\b", "", clean_query)

                # 去除标点和空格
                clean_query = clean_query.replace("?", "").replace("？", "").strip()

                # 如果洗完是空的(用户只发了"价格")，就还原，虽然大概率查不到
                final_query = clean_query if clean_query else user_query

                # 🔥🔥🔥 [核心修复 2] 传入清洗后的关键词
                res = get_market_snapshot.invoke(final_query)

                status.write(res)
                status.update(label="✅ 报价完成", state="complete", expanded=True)

                return True, res

            except Exception as e:
                print(f"❌ 快速行情查询失败: {e}")
                status.update(label="❌ 查询失败，转入深度分析", state="error")
                return False, None

    return False, None


FOLLOWUP_KEYWORDS = (
    "刚刚", "刚才", "上一个", "上一条", "上次", "前面",
    "继续", "接着", "承接", "基于刚才", "刚聊到", "上一轮",
    "详细说明", "详细说", "展开说", "再展开", "再详细", "那为什么", "为什么呢", "补充一下",
)

INTENT_OPTION_KEYWORDS = (
    "期权", "认购", "认沽", "行权价", "牛市价差", "熊市价差", "跨式", "宽跨", "勒式",
    "call", "put", "delta", "gamma", "vega", "theta", "iv", "波动率", "权利金",
)

INTENT_STOCK_PORTFOLIO_KEYWORDS = (
    "持仓体检", "我的持仓", "我的股票", "股票持仓", "持仓分析", "仓位", "调仓", "加仓", "减仓",
    "股票组合", "股票账户", "前3大持仓", "行业分布",
)

FOCUS_ENTITY_SUFFIXES = (
    "股份", "集团", "科技", "技术", "控股", "电子", "电气", "机械", "汽车", "能源",
    "药业", "银行", "证券", "制造", "动力", "材料", "智能", "软件", "通信", "航空",
    "医药", "生物", "实业", "新材",
)

FOCUS_ENTITY_PATTERN = re.compile(
    r"[一-龥]{2,10}(?:%s)" % "|".join(FOCUS_ENTITY_SUFFIXES)
)

FOCUS_ASPECT_KEYWORDS = (
    "机器人业务", "汽车业务", "机器人", "汽车", "工业自动化", "工业软件",
    "协作机器人", "服务机器人", "工业机器人", "业务线", "这块业务", "这个业务",
)

COMPANY_NEWS_TOPIC_KEYWORDS = (
    "最近有什么好消息", "最近有没有好消息", "最近有什么动态", "最近动态", "最近进展",
    "最近催化", "最近有没有催化", "最近消息", "最新消息", "近期动态", "近期进展",
    "近期催化", "最近怎么样", "业务最近怎么样", "业务最近如何",
)

FOCUS_ENTITY_BAD_SUBSTRINGS = ("的", "业务", "或")
FOCUS_PRONOUN_HINTS = ("他", "她", "它", "他的", "她的", "它的", "这家公司", "这个公司")


def _classify_intent_domain(text: str) -> str:
    text_norm = str(text or "").strip().lower()
    if not text_norm:
        return "general"
    if any(kw in text_norm for kw in INTENT_OPTION_KEYWORDS):
        return "option"
    if any(kw in text_norm for kw in INTENT_STOCK_PORTFOLIO_KEYWORDS):
        return "stock_portfolio"
    return "general"


def _extract_similarity_tokens(text: str):
    """轻量语义相关度分词（中英文混合）"""
    if not text:
        return set()
    normalized = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fff]+", " ", str(text).lower())
    tokens = set()
    for word in normalized.split():
        if len(word) >= 2:
            tokens.add(word)
        if re.search(r"[\u4e00-\u9fff]", word) and len(word) >= 2:
            for i in range(len(word) - 1):
                tokens.add(word[i : i + 2])
    return tokens


def _is_semantically_related(prompt_text: str, recent_turns, threshold: float = 0.18) -> bool:
    """基于 Jaccard 的轻量语义相关判定"""
    return _shared_is_semantically_related(prompt_text, recent_turns, threshold=threshold)


def _build_recent_context_text(recent_turns, max_chars: int = 1200) -> str:
    role_map = {"user": "用户", "assistant": "AI", "ai": "AI"}
    lines = []
    for turn in recent_turns:
        role = role_map.get(turn.get("role", ""), turn.get("role", ""))
        content = str(turn.get("content", "")).strip()
        if not content:
            continue
        lines.append(f"{role}: {content[:260]}")
    return "\n".join(lines)[:max_chars]


def _get_latest_user_turn_content(recent_turns) -> str:
    for turn in reversed(recent_turns):
        if str(turn.get("role", "")).strip() == "user":
            return str(turn.get("content", "")).strip()
    return ""


def _filter_memory_context_by_domain(memory_context: str, intent_domain: str, max_chars: int = 1500) -> str:
    if not memory_context:
        return ""
    if intent_domain != "option":
        return memory_context[:max_chars]

    chunks = []
    current = []
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

    option_chunks = [chunk for chunk in chunks if _classify_intent_domain(chunk) == "option"]
    return "\n".join(option_chunks)[:max_chars] if option_chunks else ""


def _extract_focus_entity(text: str) -> str:
    return _shared_extract_focus_entity(text)


def _extract_focus_aspect(text: str) -> str:
    return _shared_extract_focus_aspect(text)


def _looks_like_company_news_topic(text: str) -> bool:
    raw = str(text or "").strip().lower()
    if not raw:
        return False
    return any(keyword in raw for keyword in COMPANY_NEWS_TOPIC_KEYWORDS)


def _build_memory_record(ai_response: str, max_chars: int = 4000) -> str:
    """将回答压缩成结构化摘要+片段，提升后续召回稳定性"""
    if not ai_response:
        return ""
    cleaned = re.sub(r"```[\s\S]*?```", " ", ai_response)
    cleaned = re.sub(r"[#>*`]+", " ", cleaned)
    lines = [ln.strip() for ln in cleaned.splitlines() if ln.strip()]
    summary = "；".join(lines[:3]) if lines else cleaned[:220]
    summary = summary[:240]
    snippet = ai_response[:max_chars]
    return f"【结构化摘要】{summary}\n【回答片段】{snippet}"


def build_context_payload(
    prompt_text: str,
    current_user: str,
    vision_position_payload: Optional[Dict[str, Any]] = None,
    vision_position_domain: str = "",
):
    """构建连续对话上下文载荷（会话+长期记忆）"""
    all_messages = list(st.session_state.get("messages", []))
    recent_turns = [
        {"role": msg.get("role", ""), "content": str(msg.get("content", ""))}
        for msg in all_messages[-4:]  # 最近两轮（user+ai）
    ]
    intent_domain = _classify_intent_domain(prompt_text)
    latest_user_content = _get_latest_user_turn_content(recent_turns)
    recent_domain = _classify_intent_domain(latest_user_content)
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
    topic_anchors = _build_topic_anchors(all_messages[-12:], max_anchors=3)
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
        or _extract_focus_entity(recent_context_for_focus)
        or _extract_focus_entity(latest_user_content)
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
    should_load_long_memory = should_include_recent_context
    account_total_capital = None

    if current_user != "访客":
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
                profile = de.get_user_profile(current_user) or {}
                profile_capital = profile.get("account_total_capital")
                normalized = de.normalize_account_total_capital(profile_capital)
                if normalized:
                    account_total_capital = float(normalized)
        except Exception as e:
            print(f"⚠️ 账户总资金画像读取/更新失败: {e}")

    if not should_include_recent_context:
        recent_context = ""
    else:
        recent_context = recent_context_for_focus

    pronoun_followup = any(hint in str(prompt_text or "") for hint in FOCUS_PRONOUN_HINTS)
    explicit_focus_entity = _extract_focus_entity(prompt_text)
    explicit_focus_aspect = _extract_focus_aspect(prompt_text)
    recent_focus_aspect = str(target_anchor.get("focus_aspect") or "") or _extract_focus_aspect(recent_context_for_focus)
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
    if current_user != "访客" and should_load_long_memory:
        try:
            found = mem.retrieve_relevant_memory(
                user_id=current_user,
                query=prompt_text,
                k=2,
                query_topic=intent_domain,
                strict_topic=(intent_domain == "option"),
            )
            if found:
                memory_context = _filter_memory_context_by_domain(found, intent_domain=intent_domain)
        except Exception as e:
            print(f"❌ 长期记忆检索失败: {e}")

    profile_memory_payload = {
        "profile_context": "",
        "memory_action": "guest_skip" if current_user == "访客" else "context",
        "confirmation": "",
        "should_short_circuit": False,
        "temporary_overrides": {},
    }
    if current_user != "访客":
        try:
            profile_memory_payload = build_profile_memory_context(
                de.engine,
                user_id=current_user,
                prompt_text=prompt_text,
                portfolio_snapshot_loader=de.get_user_portfolio_snapshot,
            )
        except Exception as e:
            print(f"⚠️ 交易画像记忆构建失败: {e}")

    conversation_id = st.session_state.get("conversation_id")
    if not conversation_id:
        conversation_id = str(uuid.uuid4())
        st.session_state["conversation_id"] = conversation_id

    return {
        "is_followup": bool(is_followup),
        "intent_domain": intent_domain,
        "recent_domain": recent_domain,
        "recent_turns": recent_turns,
        "recent_context": recent_context,
        "memory_context": memory_context,
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
        "conversation_id": conversation_id,
        "account_total_capital": account_total_capital,
        "vision_position_payload": vision_position_payload if isinstance(vision_position_payload, dict) else None,
        "vision_position_domain": str(vision_position_domain or ""),
    }


def _hash_uploaded_file(uploaded_file) -> str:
    if uploaded_file is None:
        return ""
    try:
        raw = uploaded_file.getvalue()
        return hashlib.md5(raw).hexdigest()
    except Exception:
        try:
            uploaded_file.seek(0)
            raw = uploaded_file.read()
            uploaded_file.seek(0)
            return hashlib.md5(raw).hexdigest()
        except Exception:
            return ""


def _render_option_leg_text(leg: Dict[str, Any]) -> str:
    underlying = str(leg.get("underlying_hint") or "").strip().upper()
    month = leg.get("month")
    month_text = f"{int(month)}月" if isinstance(month, (int, float)) else ""
    strike = leg.get("strike")
    strike_text = f"{float(strike):.3f}".rstrip("0").rstrip(".") if isinstance(strike, (int, float)) else ""
    cp_raw = str(leg.get("cp") or "").lower()
    if cp_raw == "call":
        cp = "认购"
    elif cp_raw == "put":
        cp = "认沽"
    else:
        cp = ""
    side_raw = str(leg.get("side") or "").lower()
    if side_raw == "long":
        side = "买方"
    elif side_raw == "short":
        side = "卖方"
    else:
        side = ""
    qty = int(leg.get("qty") or abs(int(leg.get("signed_qty", 0))) or 0)
    contract_code = str(leg.get("contract_code") or "").strip().upper()
    core_text = ""
    if not (month_text or strike_text or cp or side):
        core_text = f"{contract_code or '期权合约'}{qty}张"
    else:
        core_text = f"{month_text}{strike_text}{cp}{side}{qty}张"
    if contract_code and contract_code not in core_text:
        core_text = f"{core_text}({contract_code})"
    if underlying:
        return f"[{underlying}] {core_text}"
    return core_text


def _build_upload_option_prompt(vision_struct: Dict[str, Any]) -> str:
    domain = str(vision_struct.get("domain", "")).strip().lower()
    legs = vision_struct.get("option_legs") or []
    legs_text = "；".join(_render_option_leg_text(leg) for leg in legs[:12] if isinstance(leg, dict))
    unique_underlyings = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        hint = str(leg.get("underlying_hint") or "").strip().upper()
        if hint and hint not in unique_underlyings:
            unique_underlyings.append(hint)
    if len(unique_underlyings) > 1:
        prefix = f"我上传了多标的期权持仓截图（{', '.join(unique_underlyings)}）。"
    elif len(unique_underlyings) == 1:
        prefix = f"我上传了{unique_underlyings[0]}期权持仓截图。"
    else:
        prefix = "我上传了期权持仓截图。"
    mixed_hint = "已识别到股票持仓，本轮未展开股票体检。" if domain == "mixed" else ""
    return (
        f"{prefix}{mixed_hint}识别到的期权腿：{legs_text or '待确认'}。"
        "请按期权持仓深度模板输出：持仓拆解、净暴露与到期错配、三情景分支、保守/进攻两套方案、风控阈值、当日执行清单；"
        "并给出DeltaCash、Delta Ratio、目标区间、偏离与建议调整量。"
    )


def auto_submit_position_task(uploaded_img):
    """上传截图后自动触发股票体检或期权深度分析（仅登录用户）。"""
    current_user = st.session_state.get("user_id", "访客")
    if current_user == "访客":
        return
    if not uploaded_img:
        return
    if st.session_state.get("pending_portfolio_task"):
        return

    image_hash = _hash_uploaded_file(uploaded_img)
    if not image_hash:
        return
    if st.session_state.get("portfolio_last_attempt_hash") == image_hash:
        return

    st.session_state.portfolio_last_attempt_hash = image_hash
    st.session_state["position_upload_last_error"] = ""
    st.session_state["position_upload_last_warnings"] = []

    with st.status("📊 正在识别持仓截图并自动启动分析...", expanded=True) as status:
        vision_struct = analyze_position_image(uploaded_img)
        if not vision_struct.get("ok"):
            status.update(label="❌ 持仓识别失败", state="error", expanded=False)
            err_msg = vision_struct.get("error", "未识别到有效持仓")
            warn_list = list(vision_struct.get("warnings") or [])
            st.session_state["position_upload_last_error"] = str(err_msg or "")
            st.session_state["position_upload_last_warnings"] = warn_list
            st.warning(f"持仓识别失败：{err_msg}")
            return

        domain = str(vision_struct.get("domain", "unknown")).strip().lower()
        stock_positions = vision_struct.get("stock_positions") or []
        option_legs = vision_struct.get("option_legs") or []

        if domain == "stock":
            if not stock_positions:
                status.update(label="❌ 未识别到有效持仓", state="error", expanded=False)
                st.warning("未识别到有效持仓数据，请换一张更清晰的截图后重试。")
                return
            task_manager = TaskManager()
            try:
                task_id = task_manager.create_portfolio_task(
                    user_id=current_user,
                    positions=stock_positions,
                    screenshot_hash=image_hash,
                    source_text=vision_struct.get("raw_text", ""),
                )
            except Exception as e:
                status.update(label="❌ 任务创建失败", state="error", expanded=False)
                st.error(f"持仓体检任务创建失败：{e}")
                return
            st.session_state.pending_portfolio_task = {
                "task_id": task_id,
                "start_time": time.time(),
                "screenshot_hash": image_hash,
                "positions_count": len(stock_positions),
            }
            st.session_state["position_upload_last_error"] = ""
            st.session_state["position_upload_last_warnings"] = []
            status.update(label="✅ 已自动提交股票持仓体检任务", state="complete", expanded=False)
            st.toast(f"持仓体检任务已启动（识别到 {len(stock_positions)} 只股票）")
            st.rerun()
            return

        if domain in {"option", "mixed"}:
            if not option_legs:
                status.update(label="❌ 未识别到有效期权持仓", state="error", expanded=False)
                st.warning("识别到期权域，但未提取到有效期权腿，请上传更清晰截图。")
                return
            option_prompt = _build_upload_option_prompt(vision_struct)
            st.session_state["position_upload_last_error"] = ""
            st.session_state["position_upload_last_warnings"] = []
            status.update(label="✅ 已自动提交期权持仓深度分析", state="complete", expanded=False)
            process_user_input(
                option_prompt,
                deep_mode=bool(st.session_state.get("deep_mode_enabled", False)),
                vision_position_payload=vision_struct,
                vision_position_domain=domain,
                analysis_mode_label="option_position_upload",
            )
            st.rerun()
            return

        status.update(label="❌ 暂无法判定持仓类型", state="error", expanded=False)
        st.session_state["position_upload_last_error"] = "未识别到股票或期权持仓，请上传更清晰的持仓截图后重试。"
        st.session_state["position_upload_last_warnings"] = list(vision_struct.get("warnings") or [])
        st.warning("未识别到股票或期权持仓，请上传更清晰的持仓截图后重试。")


# ==========================================
#  5. 核心逻辑处理函数 [修改点：封装成函数以便复用]
# ==========================================
def process_user_input(
    prompt_text,
    deep_mode=False,
    vision_position_payload: Optional[Dict[str, Any]] = None,
    vision_position_domain: str = "",
    analysis_mode_label: str = "",
):
    """处理用户输入（无论是来自输入框还是快捷卡片）"""
    deep_mode = bool(deep_mode and ENABLE_DEEP_MODE and DeepTaskManager is not None)
    current_user = st.session_state.get('user_id', "访客")

    # 关键动作前再做严格鉴权，避免首页每次自动登录都阻塞。
    if current_user != "访客":
        if not _ensure_auth_verified_for_protected_action():
            st.stop()
        # 用户开始新交互后，不再自动恢复旧待处理任务，避免状态回灌冲突。
        st.session_state.home_post_login_restore_needed = False
        st.session_state.home_post_login_restore_done = True

    # --- 1. 图片识别逻辑 (保留) ---
    image_context = ""
    current_uploader_key = f"portfolio_uploader_{st.session_state.uploader_key}"
    uploaded_image = st.session_state.get(current_uploader_key)
    has_structured_upload = isinstance(vision_position_payload, dict) and bool(vision_position_payload)

    if uploaded_image and not has_structured_upload:
        with st.status("📸 正在识别持仓截图...", expanded=True) as status:
            st.write("AI 正在观察图片...")
            vision_result = analyze_financial_image(uploaded_image)
            status.update(label="✅ 图片识别完成", state="complete", expanded=False)
            image_context = f"\n\n【用户上传图信息】：\n{vision_result}\n----------------\n"
            with st.chat_message("ai"):
                st.caption(f"已识别图片内容：\n{vision_result[:100]}...")
    elif uploaded_image and has_structured_upload:
        with st.chat_message("ai"):
            st.caption("已识别上传持仓截图，并注入结构化持仓数据。")

    # 在追加当前问题前构造上下文，避免本轮内容混进历史
    context_payload = build_context_payload(
        prompt_text=prompt_text,
        current_user=current_user,
        vision_position_payload=vision_position_payload,
        vision_position_domain=vision_position_domain,
    )
    if bool(context_payload.get("profile_memory_should_short_circuit", False)):
        chat_mode = CHAT_MODE_SIMPLE
    elif context_payload.get("followup_anchor_ambiguous") and context_payload.get("followup_anchor_clarify"):
        st.session_state.messages.append({"role": "user", "content": prompt_text, "linked_task_id": ""})
        clarify_text = str(context_payload.get("followup_anchor_clarify") or "").strip()
        with st.chat_message("ai"):
            st.markdown(clarify_text)
        st.session_state.messages.append({"role": "ai", "content": clarify_text})
        return
    else:
        chat_mode = classify_chat_mode(
            prompt_text,
            is_followup=bool(context_payload.get("is_followup", False)),
            recent_context=str(context_payload.get("recent_context") or ""),
            focus_entity=str(context_payload.get("focus_entity") or ""),
            focus_topic=str(context_payload.get("focus_topic") or ""),
            focus_aspect=str(context_payload.get("focus_aspect") or ""),
            focus_mode_hint=str(context_payload.get("focus_mode_hint") or ""),
            followup_goal=str(context_payload.get("followup_goal") or ""),
            correction_intent=bool(context_payload.get("correction_intent", False)),
            has_uploaded_image=bool(uploaded_image),
            has_structured_payload=has_structured_upload,
            vision_position_domain=vision_position_domain,
        )
    if deep_mode:
        chat_mode = CHAT_MODE_ANALYSIS
    context_payload["chat_mode"] = chat_mode

    # --- 2. 显示用户提问 (保留) ---
    st.session_state.messages.append({"role": "user", "content": prompt_text, "linked_task_id": ""})

    if bool(context_payload.get("profile_memory_should_short_circuit", False)):
        confirmation = str(context_payload.get("profile_memory_confirmation") or "好，我记住了。")
        with st.chat_message("ai"):
            st.markdown(confirmation, unsafe_allow_html=True)
        st.session_state.messages.append(
            {
                "role": "ai",
                "content": confirmation,
                "chart": "",
                "attachments": [],
                "trace_id": generate_chat_trace_id(),
                "answer_id": generate_chat_answer_id(),
                "feedback_allowed": False,
                "intent_domain": str(context_payload.get("intent_domain") or "general"),
                "chat_mode": CHAT_MODE_SIMPLE,
            }
        )
        st.rerun()
        return

    # 构造最终 Prompt
    final_prompt = image_context + prompt_text
    trace_id = generate_chat_trace_id()
    answer_id = generate_chat_answer_id()
    intent_domain = str(context_payload.get("intent_domain") or "general")

    if chat_mode == CHAT_MODE_SIMPLE and not deep_mode:
        with st.chat_message("ai"):
            typing_placeholder = st.empty()
            typing_placeholder.markdown(_render_simple_chat_typing_indicator(), unsafe_allow_html=True)
            time.sleep(0.08)
            llm_turbo = ChatTongyi(model="qwen-turbo-latest", temperature=0.2)
            runtime_context = build_simple_runtime_context_payload(current_user)
            simple_response = simple_chatter_reply(
                prompt_text,
                llm_turbo,
                recent_context=str(context_payload.get("recent_context") or ""),
                memory_context=str(context_payload.get("memory_context") or ""),
                profile_context=str(context_payload.get("profile_context") or ""),
                is_followup=bool(context_payload.get("is_followup", False)),
                focus_entity=str(context_payload.get("focus_entity") or ""),
                focus_topic=str(context_payload.get("focus_topic") or ""),
                focus_aspect=str(context_payload.get("focus_aspect") or ""),
                runtime_context=runtime_context,
            )
            typing_placeholder.empty()
            response_placeholder = st.empty()
            full_response = ""

            if len(simple_response) > 220:
                update_interval = 40
                chars = list(simple_response)
                for i in range(0, len(chars), update_interval):
                    chunk = ''.join(chars[i:i + update_interval])
                    full_response += chunk
                    response_placeholder.markdown(full_response + "▌", unsafe_allow_html=True)
                    time.sleep(0.03)
            else:
                for char in stream_text_generator(simple_response, delay=0.008):
                    full_response += char
                    response_placeholder.markdown(full_response + "▌", unsafe_allow_html=True)

            response_placeholder.markdown(simple_response, unsafe_allow_html=True)

        st.session_state.messages.append(
            {
                "role": "ai",
                "content": simple_response,
                "chart": "",
                "attachments": [],
                "trace_id": trace_id,
                "answer_id": answer_id,
                "feedback_allowed": False,
                "intent_domain": intent_domain,
                "chat_mode": CHAT_MODE_SIMPLE,
            }
        )
        if current_user != "访客":
            try:
                mem.save_interaction(
                    current_user,
                    prompt_text,
                    _build_memory_record(simple_response),
                    topic=_classify_intent_domain(prompt_text),
                )
            except Exception as e:
                print(f"记忆存储失败: {e}")
        st.rerun()
        return

    # --- 3. 极速伪路由检查（默认关闭，可通过环境变量显式开启） ---
    if FAST_ROUTER_ENABLED and chat_mode == CHAT_MODE_ANALYSIS and not deep_mode:
        is_hit, fast_response = fast_router_check(final_prompt)
        if is_hit:
            st.session_state.messages.append({"role": "assistant", "content": fast_response})
            # 🔥 [新增] 清除已使用的图片
            if uploaded_image:
                st.session_state.uploader_key += 1
            st.rerun()
            return

    # ============================================================
    # 🔥🔥🔥 [修正区域]：LangGraph + 记忆检索 (RAG)
    # ============================================================

    # 读取用户画像（风险偏好）
    risk = "稳健型"
    if current_user != "访客":
        try:
            user_profile = de.get_user_profile(current_user)
            risk = user_profile.get('risk_preference', '稳健型')
        except Exception as e:
            print(f"读取用户画像失败: {e}")


    # ==========================================
    # 🔥 [新增] 检查用户持仓状态
    # ==========================================
    has_portfolio = False
    if current_user != "访客":
        try:
            from portfolio_analysis_service import get_user_portfolio_snapshot
            portfolio_snapshot = get_user_portfolio_snapshot(current_user)
            has_portfolio = bool(portfolio_snapshot and portfolio_snapshot.get('recognized_count', 0) > 0)
        except Exception as e:
            print(f"检查持仓状态失败: {e}")
            has_portfolio = False

    # ==========================================
    # 🔥 [修改] process_user_input 函数中的执行部分
    # ==========================================
    # 创建任务管理器
    task_manager = DeepTaskManager() if deep_mode else TaskManager()

    # 准备历史消息
    history_msgs = st.session_state.messages[:-1] if len(st.session_state.messages) > 1 else []
    recent_history = history_msgs[-4:] if len(history_msgs) > 4 else history_msgs
    history_for_task = [{"role": msg["role"], "content": msg["content"]} for msg in recent_history]

    try:
        if deep_mode:
            deep_risk = "balanced"
            task_id = task_manager.create_task(
                user_id=current_user,
                prompt=final_prompt,
                risk_preference=deep_risk,
                history_messages=history_for_task,
                context_payload=context_payload,
            )
        elif chat_mode == CHAT_MODE_KNOWLEDGE:
            task_id = task_manager.create_knowledge_task(
                user_id=current_user,
                prompt=prompt_text,
                risk_preference=risk,
                history_messages=history_for_task,
                context_payload=context_payload,
            )
        else:
            task_id = task_manager.create_task(
                user_id=current_user,
                prompt=final_prompt,
                image_context=image_context,
                risk_preference=risk,
                history_messages=history_for_task,
                context_payload=context_payload,
                has_portfolio=has_portfolio
            )
    except UserTaskQueueFullError as e:
        if st.session_state.messages and st.session_state.messages[-1].get("role") == "user":
            st.session_state.messages.pop()
        st.warning(f"⏳ 你前面已有 {e.active_count} 个处理中、{e.queued_count} 个排队问题，请等前面的结果回来后再继续提问。")
        return

    # 🔥 [新增] 异步更新用户画像（带防重复机制）
    if current_user != "访客" and len(prompt_text) > 5:
        try:
            # 🔥🔥 第一道防线：Session State 防重复（防止 Streamlit rerun 导致的重复）
            if "profile_update_fingerprints" not in st.session_state:
                st.session_state.profile_update_fingerprints = set()

            # 生成消息指纹
            msg_fingerprint = hashlib.md5(f"{current_user}:{prompt_text}".encode()).hexdigest()

            # 🔥 关键优化：先检查并立即标记，避免并发问题
            if msg_fingerprint in st.session_state.profile_update_fingerprints:
                print(f"⏭️ [Session] 跳过重复的画像更新任务: {current_user}")
                # 直接跳过，不再执行后续逻辑
            else:
                # ✅ 立即标记（在任何异步操作之前）
                st.session_state.profile_update_fingerprints.add(msg_fingerprint)

                # 智能判断：只在特定情况下触发画像更新
                PROFILE_UPDATE_KEYWORDS = [
                    "做空", "做多", "梭哈", "保守", "激进", "风险", "止损",
                    "持仓", "买入", "卖出", "看涨", "看跌", "策略", "建议",
                    "怕亏", "对冲", "保护", "翻倍", "虚值"
                ]
                should_update = any(kw in prompt_text for kw in PROFILE_UPDATE_KEYWORDS)

                # 或者：如果是长文本（超过20字），也触发更新
                if len(prompt_text) > 20:
                    should_update = True

                if should_update:
                    # 🔥🔥 第二道防线：Redis 防重复（跨 session 防护）
                    import redis
                    redis_client = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)

                    cache_key = f"profile_update_lock:{msg_fingerprint}"

                    # 检查是否刚刚处理过（60秒内）
                    if not redis_client.exists(cache_key):
                        # 设置锁，60秒过期
                        redis_client.setex(cache_key, 60, "1")

                        # 触发任务
                        from tasks import update_user_profile_task
                        update_user_profile_task.delay(current_user, prompt_text)

                        print(f"🧠 已触发用户画像更新任务: {current_user}")
                    else:
                        print(f"⏭️ [Redis] 跳过重复的画像更新任务（60秒内已处理）: {current_user}")
                else:
                    # 如果不满足触发条件，移除标记（让下次可以检查）
                    st.session_state.profile_update_fingerprints.discard(msg_fingerprint)

        except Exception as e:
            print(f"⚠️ 触发用户画像更新失败（不影响主流程）: {e}")
            # 发生异常时，移除标记
            try:
                st.session_state.profile_update_fingerprints.discard(msg_fingerprint)
            except:
                pass

    # 🔥 [新增] 保存任务信息
    task_overrides = {
        task_id: {
            "raw_prompt": prompt_text,
            "trace_id": trace_id,
            "answer_id": answer_id,
            "intent_domain": intent_domain,
            "analysis_mode_label": str(analysis_mode_label or ""),
            "mode": "deep" if deep_mode else "normal",
            "risk": "balanced" if deep_mode else risk,
            "image_context": image_context,
            "context_payload": context_payload,
            "chat_mode": chat_mode,
            "prompt": final_prompt,
        }
    }
    pending_tasks = _refresh_home_pending_tasks(current_user, extra_overrides=task_overrides)
    if st.session_state.messages and st.session_state.messages[-1].get("role") == "user":
        st.session_state.messages[-1]["linked_task_id"] = task_id
        st.session_state.messages.append(
            _build_task_placeholder_message(
                task_id=task_id,
                prompt_text=prompt_text,
                trace_id=trace_id,
                answer_id=answer_id,
                intent_domain=intent_domain,
                chat_mode=chat_mode,
            )
        )
    if not pending_tasks:
        st.session_state.pending_task = None


# ==========================================
#  6. 页面渲染：Welcome Screen (空状态) [修改点：新增]
# ==========================================
def show_welcome_screen():
    st.markdown("<br>", unsafe_allow_html=True)

    # --- 1. 注入酷炫的 CSS 动画和样式 ---
    st.markdown("""
        <style>
        /* A. 标题流光渐变效果 (保持不变) */
        .hero-title {
            /* 1.8rem (手机) -> 6vw (平板) -> 4rem (电脑) */
            font-size: clamp(1.8rem, 6vw, 4rem);
            
            font-weight: 900;
            background: linear-gradient(120deg, #ffffff 0%, #3b82f6 50%, #8b5cf6 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-align: center;
            margin-bottom: 10px;
            filter: drop-shadow(0 0 15px rgba(59, 130, 246, 0.5));
            animation: breathe 3s ease-in-out infinite alternate;
            
            /* 🔥 修改2：强制只有一行，绝不换行 */
            white-space: nowrap;
        }
        @keyframes breathe {
            from { filter: drop-shadow(0 0 10px rgba(59, 130, 246, 0.4)); }
            to { filter: drop-shadow(0 0 25px rgba(139, 92, 246, 0.7)); }
        }

        /* B. 副标题容器 (Flex布局居中) */
        .hero-subtitle {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-bottom: 40px;
        }

       /* 🔥 [核心修改] 打字机无限循环特效 */
        .typewriter-text {
            color: #94a3b8;
            font-family: 'Courier New', monospace;
            font-size: clamp(1rem, 2vw, 1.2rem);
            letter-spacing: 2px;
            
            overflow: hidden;
            white-space: nowrap;
            border-right: 3px solid #3b82f6; /* 光标 */
            
            width: 0;
            
            /* 修改点说明：
               1. typing 5s: 延长到5秒，动作更优雅。
               2. infinite: 无限循环。
               3. alternate: 往返播放 (打字 -> 删字 -> 打字 -> 删字...) 
               这样看起来像是 AI 在不断输入、修正。
            */
            animation: 
                typing 5s steps(22, end) infinite alternate, 
                blink-caret 0.75s step-end infinite;
        }

        /* 宽度展开动画 */
        @keyframes typing {
            from { width: 0; }
            to { width: 23ch; } 
        }

        /* 光标闪烁动画 */
        @keyframes blink-caret {
            from, to { border-color: transparent; }
            50% { border-color: #3b82f6; }
        }

        /* C. 按钮变身：酷炫卡片 (居中版) */
        .stMainBlockContainer div.stButton > button {
            background: rgba(30, 41, 59, 0.6) !important;
            backdrop-filter: blur(10px) !important;
            border: 1px solid rgba(255, 255, 255, 0.1) !important;
            color: #e2e8f0 !important;
            border-radius: 16px !important;
            padding: 25px 20px !important;
            font-size: 16px !important;
            font-weight: 600 !important;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1) !important;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.2) !important;
            text-align: center !important;
            display: flex !important;
            flex-direction: column !important;
            align-items: center !important;
            justify-content: center !important;
            position: relative !important;
            overflow: hidden !important;
        }

        .stMainBlockContainer div.stButton > button:hover {
            transform: translateY(-5px) scale(1.02) !important;
            background: linear-gradient(135deg, rgba(59, 130, 246, 0.2) 0%, rgba(30, 41, 59, 0.8) 100%) !important;
            border-color: #3b82f6 !important;
            box-shadow: 0 15px 30px rgba(59, 130, 246, 0.3) !important;
            color: #ffffff !important;
        }
        
        .stMainBlockContainer div.stButton > button:active {
            transform: scale(0.98) !important;
            box-shadow: 0 2px 10px rgba(59, 130, 246, 0.2) !important;
        }

        /* 装饰箭头 */
        .stMainBlockContainer div.stButton > button::after {
            content: "➜";
            position: absolute;
            right: 20px;
            top: 50%;
            transform: translateY(-50%);
            opacity: 0;
            transition: all 0.3s ease;
            font-size: 20px;
            color: #3b82f6;
        }
        .stMainBlockContainer div.stButton > button:hover::after {
            opacity: 1;
            right: 15px;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) {
            position: relative;
            padding: 20px 0 6px;
            max-width: 58rem;
            margin: 0 auto;
        }

        .home-hero-copy {
            padding: 20px 0 0;
        }

        .home-promo-shell {
            width: 1px;
            height: 1px;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has(.home-promo-shell) {
            display: none;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) {
            position: absolute;
            top: 0;
            right: 0;
            width: auto;
            max-width: 18rem;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) a {
            min-height: 0 !important;
            padding: 9px 14px !important;
            background: rgba(252, 211, 77, 0.12) !important;
            border: 1px solid rgba(252, 211, 77, 0.68) !important;
            border-radius: 999px !important;
            box-shadow: 0 8px 20px rgba(15, 23, 42, 0.18) !important;
            font-size: 13px !important;
            font-weight: 700 !important;
            line-height: 1.2 !important;
            color: #fde68a !important;
            text-align: center !important;
            display: inline-flex !important;
            align-items: center !important;
            justify-content: center !important;
            gap: 8px !important;
            white-space: nowrap !important;
            text-decoration: none !important;
            transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease !important;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) a:hover {
            transform: translateY(-1px) !important;
            background: rgba(252, 211, 77, 0.18) !important;
            box-shadow: 0 12px 24px rgba(15, 23, 42, 0.24) !important;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) a::before {
            content: "";
            width: 8px;
            height: 8px;
            border-radius: 999px;
            background: #fb923c;
            box-shadow: 0 0 0 3px rgba(251, 146, 60, 0.16);
            flex: 0 0 auto;
        }

        div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) a p {
            margin: 0 !important;
            color: #fde68a !important;
            font-size: 13px !important;
            font-weight: 700 !important;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        @media (max-width: 1100px) {
            div[data-testid="stVerticalBlock"]:has(.home-hero-copy) {
                max-width: 52rem;
                padding-top: 64px;
            }

            div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) {
                top: 0;
            }
        }

        @media (max-width: 768px) {
            div[data-testid="stVerticalBlock"]:has(.home-hero-copy) {
                padding-top: 0;
            }

            div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) {
                position: static;
                width: 100%;
                max-width: none;
            }

            div[data-testid="stVerticalBlock"]:has(.home-hero-copy) > div:has([data-testid="stPageLink"]) a {
                margin: 0 auto 16px !important;
                width: fit-content !important;
                max-width: 100% !important;
                white-space: normal !important;
                text-align: center !important;
                padding: 10px 14px !important;
            }
        }
        </style>
    """, unsafe_allow_html=True)

    # --- 2. 渲染标题 + 右上角轻提示 ---
    if HOME_PROMO_BANNER.get("enabled"):
        st.markdown('<div class="home-promo-shell"></div>', unsafe_allow_html=True)
        st.page_link(
            str(HOME_PROMO_BANNER["target_page"]),
            label=str(HOME_PROMO_BANNER["text"]),
            use_container_width=False,
        )

    st.markdown("""
            <div class="home-hero-copy">
                <div class="hero-title">
                    ⚡ 嗨，我是爱波塔
                </div>
                <div class="hero-subtitle">
                    <div class="typewriter-text">
                        陪你在金融市场奋斗
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

    # --- 快捷指令卡片 ---
    col1, col2, col3 = st.columns(3)

    # 定义点击回调
    # --- 关键修改：定义回调函数 ---
    # 这个函数会在页面重新加载前优先执行，确保数据这就位
    def set_prompt_callback(text):
        st.session_state.pending_prompt = text

    with col1:
        st.button("比较宁德时代和阳光电源",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("比较宁德时代和阳光电源的基本面和技术面",)
         )

    with col2:
        st.button("期权学习-什么是牛市价差？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("牛市价差策略是什么？",)
         )

    with col3:
        st.button("创业板期权做什么策略？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("创业板期权做什么策略好",)
         )

# ==========================================
#  7. 主程序入口
# ==========================================

if _render_invite_register_page_if_needed():
    st.stop()

# A. 侧边栏：登录/设置 (折叠起来保持清爽)
with st.sidebar:
    # 🔥 [新增] 统一的分组导航菜单
    from sidebar_navigation import show_navigation
    show_navigation()

    if not st.session_state['is_logged_in']:
        # --- A. 未登录状态：账号体系 ---
        account_tab_login, account_tab_register = st.tabs(["登录", "注册"])

        # ============ 账号登录 ============
        with account_tab_login:
            login_account = st.text_input(
                "账号",
                key="account_login_username",
                placeholder="输入账号",
            )
            login_password = st.text_input(
                "密码",
                type="password",
                key="account_login_password",
                placeholder="输入登录密码",
            )

            if st.button("登录", type="primary", use_container_width=True, key="btn_account_login_pwd"):
                if not login_account:
                    st.warning("请输入账号")
                elif not login_password:
                    st.warning("请输入密码")
                else:
                    success, msg, token, real_username = auth.login_user(login_account, login_password)
                    if success:
                        st.session_state['is_logged_in'] = True
                        st.session_state['user_id'] = real_username
                        st.session_state['token'] = token
                        st.session_state['just_manual_logged_in'] = True
                        _mark_auth_verified(real_username, token)
                        st.session_state.home_post_login_restore_needed = True
                        st.session_state.home_post_login_restore_done = False
                        st.session_state.home_masked_email_user = ""
                        st.session_state.home_masked_email_value = ""

                        expires = datetime.now() + timedelta(days=30)
                        cookie_manager.set("username", real_username, expires_at=expires, key="set_user_cookie")
                        cookie_manager.set("token", token, expires_at=expires, key="set_token_cookie")
                        st.success("登录成功")
                        time.sleep(0.3)
                        st.rerun()
                    else:
                        st.error(msg)

        # ============ 账号注册（两步） ============
        with account_tab_register:
            step1_ok = st.session_state.get("reg_step1_ok", False)
            verified_phone = st.session_state.get("reg_verified_phone", "")

            st.caption("步骤1：填写账号和密码")
            if step1_ok:
                step1_user = st.session_state.get("reg_step1_username", "")
                st.success(f"步骤1已完成：账号 {step1_user}")
                if st.button("修改账号/密码", key="btn_reg_reset_step1"):
                    st.session_state.pop("reg_step1_ok", None)
                    st.session_state.pop("reg_step1_username", None)
                    st.session_state.pop("reg_step1_password", None)
                    st.session_state.pop("reg_verified_phone", None)
                    st.session_state.pop("reg_phone", None)
                    st.session_state.pop("reg_sms_code", None)
                    st.rerun()
            else:
                reg_username = st.text_input(
                    "账号（必填）",
                    key="reg_step1_username_input",
                    placeholder="至少3个字符",
                )
                reg_password = st.text_input(
                    "设置密码",
                    type="password",
                    key="reg_step1_password_input",
                    placeholder="至少6位",
                )
                reg_password2 = st.text_input(
                    "确认密码",
                    type="password",
                    key="reg_step1_password2_input",
                    placeholder="再次输入密码",
                )

                if st.button("继续", use_container_width=True, key="btn_reg_step1_confirm"):
                    ok, msg, normalized_username = auth.validate_register_step1(
                        reg_username,
                        reg_password,
                        reg_password2,
                    )
                    if ok:
                        st.session_state["reg_step1_ok"] = True
                        st.session_state["reg_step1_username"] = normalized_username
                        st.session_state["reg_step1_password"] = reg_password
                        st.success("步骤1验证通过，请继续步骤2")
                        st.rerun()
                    else:
                        st.error(msg)

            if st.session_state.get("reg_step1_ok"):
                st.caption("步骤2：绑定手机号并验证")
                if verified_phone:
                    st.success(f"手机号已验证：{verified_phone}")
                    if st.button("更换手机号", key="btn_reg_reset_phone"):
                        st.session_state.pop("reg_verified_phone", None)
                        st.session_state.pop("reg_phone", None)
                        st.session_state.pop("reg_sms_code", None)
                        st.rerun()
                else:
                    reg_phone = st.text_input(
                        "手机号（仅 +86）",
                        key="reg_phone",
                        placeholder="例如 13800138000",
                    )
                    reg_sms_code = st.text_input(
                        "短信验证码",
                        key="reg_sms_code",
                        max_chars=6,
                        placeholder="输入6位验证码",
                    )
                    send_col, verify_col = st.columns(2)
                    with send_col:
                        if st.button("发验证码", use_container_width=True, key="btn_reg_send_code"):
                            if not reg_phone:
                                st.warning("请先输入手机号")
                            else:
                                ok, msg = auth.send_register_phone_code(reg_phone)
                                if ok:
                                    st.success(msg or "验证码已发送，请注意查收")
                                else:
                                    st.error(msg)
                    with verify_col:
                        if st.button("验证", use_container_width=True, key="btn_reg_verify_code"):
                            if not reg_phone:
                                st.warning("请先输入手机号")
                            elif not reg_sms_code:
                                st.warning("请输入短信验证码")
                            else:
                                ok, msg, normalized_phone = auth.verify_register_phone_code(reg_phone, reg_sms_code)
                                if ok:
                                    st.session_state["reg_verified_phone"] = normalized_phone
                                    st.success("手机号验证通过，可完成注册")
                                    st.rerun()
                                else:
                                    st.error(msg)

                if st.session_state.get("reg_verified_phone"):
                    if st.button("完成注册并登录", type="primary", use_container_width=True, key="btn_reg_finish"):
                        final_username = st.session_state.get("reg_step1_username", "")
                        final_password = st.session_state.get("reg_step1_password", "")
                        final_phone = st.session_state.get("reg_verified_phone", "")
                        register_ip, device_fingerprint = _extract_client_ip_and_device_fingerprint()
                        success, msg = auth.register_with_username_phone(
                            final_username,
                            final_password,
                            final_phone,
                            invite_code=st.session_state.get("home_invite_code", ""),
                            register_ip=register_ip,
                            device_fingerprint=device_fingerprint,
                        )
                        if success:
                            st.success(msg if msg else "注册成功")
                            st.balloons()

                            sess_ok, sess_msg, token = auth.create_user_session(final_username)
                            if sess_ok:
                                st.session_state['is_logged_in'] = True
                                st.session_state['user_id'] = final_username
                                st.session_state['token'] = token
                                st.session_state['just_manual_logged_in'] = True
                                _mark_auth_verified(final_username, token)
                                st.session_state.home_post_login_restore_needed = True
                                st.session_state.home_post_login_restore_done = False
                                st.session_state.home_masked_email_user = ""
                                st.session_state.home_masked_email_value = ""

                                for k in [
                                    "reg_step1_ok",
                                    "reg_step1_username",
                                    "reg_step1_password",
                                    "reg_verified_phone",
                                    "reg_phone",
                                    "reg_sms_code",
                                    "reg_step1_username_input",
                                    "reg_step1_password_input",
                                    "reg_step1_password2_input",
                                ]:
                                    st.session_state.pop(k, None)

                                expires = datetime.now() + timedelta(days=30)
                                cookie_manager.set("username", final_username, expires_at=expires, key="reg_set_user")
                                cookie_manager.set("token", token, expires_at=expires, key="reg_set_token")
                                time.sleep(0.3)
                                st.rerun()
                            else:
                                st.warning(sess_msg if sess_msg else "注册成功，请登录")
                        else:
                            st.error(msg)

        with st.expander("✉️ 忘记密码", expanded=False):
            st.caption("当前仅保留邮箱找回密码")
            reset_email = st.text_input("注册邮箱", key="reset_email", placeholder="your@email.com")
            reset_c1, reset_c2 = st.columns([2, 1])
            with reset_c1:
                reset_code = st.text_input("验证码", key="reset_code", max_chars=6)
            with reset_c2:
                st.write("")
                if st.button("发送", key="btn_send_reset_code", use_container_width=True):
                    if reset_email:
                        from email_utils import send_reset_password_code

                        ok, msg = send_reset_password_code(reset_email)
                        if ok:
                            st.success("已发送")
                        else:
                            st.error(msg)
                    else:
                        st.warning("请输入邮箱")

            new_pwd = st.text_input("新密码", type="password", key="reset_new_pwd")
            new_pwd2 = st.text_input("确认密码", type="password", key="reset_new_pwd2")
            if st.button("重置密码", type="primary", use_container_width=True, key="btn_reset_pwd"):
                if not reset_email:
                    st.warning("请输入邮箱")
                elif not reset_code:
                    st.warning("请输入验证码")
                elif not new_pwd or len(new_pwd) < 6:
                    st.warning("密码至少6位")
                elif new_pwd != new_pwd2:
                    st.error("两次密码不一致")
                else:
                    ok, msg = auth.reset_password_with_email(reset_email, reset_code, new_pwd)
                    if ok:
                        st.success(msg)
                        st.balloons()
                    else:
                        st.error(msg)

    else:
        # --- B. 已登录状态 ---
        user = st.session_state['user_id']


        # 🔥 登出回调函数
        def do_logout():
            logout_user = str(st.session_state.get("user_id") or "").strip()
            logout_token = str(st.session_state.get("token") or "").strip()
            logout_sig = _auth_signature(logout_user, logout_token)

            # 1. 使数据库中的 token 失效（只删当前设备，不影响其他设备）
            if user != "访客":
                auth.logout_user(user, st.session_state.get("token"))
                try:
                    tm = TaskManager()
                    tm.clear_user_pending_task(user)
                    tm.clear_user_pending_portfolio_task(user)
                    if ENABLE_DEEP_MODE and DeepTaskManager is not None:
                        DeepTaskManager().clear_user_pending_task(user)
                except Exception as e:
                    print(f"清理待处理任务失败: {e}")

            # 2. 删除 Cookie（组件写入可能异步，just_logged_out 会阻断乐观自动登录）
            try:
                cookie_manager.delete("username", key="logout_del_user")
                cookie_manager.delete("token", key="logout_del_token")
            except Exception as e:
                print(f"登出删除 cookie 失败: {e}")

            # 2. 清除 session state
            st.session_state['is_logged_in'] = False
            st.session_state['user_id'] = None
            st.session_state['just_logged_out'] = True
            st.session_state['pending_task'] = None
            st.session_state['pending_tasks'] = []
            st.session_state['pending_portfolio_task'] = None
            st.session_state['portfolio_last_attempt_hash'] = None
            st.session_state['home_post_login_restore_needed'] = False
            st.session_state['home_post_login_restore_done'] = False
            st.session_state['home_auto_login_toast_token'] = ""
            st.session_state['home_auth_verify_needed'] = False
            st.session_state['home_auth_verified_sig'] = ""
            st.session_state['home_invalid_token_signature'] = logout_sig
            st.session_state['home_masked_email_user'] = ""
            st.session_state['home_masked_email_value'] = ""
            if 'token' in st.session_state:
                del st.session_state['token']

        # 清空对话历史：放在用户菜单上方，避免沉到最底部
        with st.popover("🗑️ 清空对话历史", use_container_width=True):
            st.markdown("⚠️ **确定要删除所有聊天记录吗？**\n\n此操作无法撤销。")
            if st.button("🚨 确认删除", type="primary", use_container_width=True, key="btn_clear_chat"):
                st.session_state.messages = []
                st.session_state.conversation_id = str(uuid.uuid4())
                st.session_state.uploader_key += 1
                st.rerun()

        invite_code = ""
        invite_stats = {"invited_count": 0, "rewarded_points": 0}
        invite_preview_mode = True
        if invite_svc is not None and user != "访客":
            try:
                invite_code = invite_svc.get_or_create_invite_code(user)
                invite_stats = invite_svc.get_invite_stats(user)
                invite_preview_mode = False
            except Exception as e:
                print(f"[invite] sidebar fetch failed: {e}")

        render_sidebar_footer_menu(
            page="home",
            user_id=user,
            is_logged_in=True,
            on_logout=do_logout,
            show_invite_entry=True,
            base_url=_resolve_base_url_from_request(),
            invite_code=invite_code,
            invite_stats=invite_stats,
            invite_preview_mode=invite_preview_mode,
            reward_points=300,
        )
# 只在用户登录后显示公告
if st.session_state.get('is_logged_in', False) and ENABLE_HOME_ANNOUNCEMENT:
    check_and_show_announcement()

# B. 处理卡片点击产生的 Pending Prompt [修改点：处理快捷指令]
if "pending_prompt" in st.session_state:
    # 防止公告弹窗刚弹出就被后续自动 rerun 冲掉
    if not is_announcement_holdoff_active():
        prompt = st.session_state.pending_prompt
        del st.session_state.pending_prompt  # 消费掉，防止循环
        process_user_input(prompt, deep_mode=bool(st.session_state.get("deep_mode_enabled", False)))
        st.rerun()  # 重新加载以显示新消息

# 自动登录后置恢复：首屏渲染后执行一次，降低首页切换卡顿感。
_restore_pending_tasks_after_auto_login_once()

# ==========================================
#  B. 界面显示逻辑
# ==========================================
rendered_pending_task_ids = set()
if not st.session_state.messages:
    show_welcome_screen()
else:
    st.markdown('<div style="height: 20px;"></div>', unsafe_allow_html=True)
    pending_tasks = st.session_state.get("pending_tasks") or []
    pending_task_map = {
        str(task_info.get("task_id") or "").strip(): task_info
        for task_info in pending_tasks
        if str(task_info.get("task_id") or "").strip()
    }
    for i, msg in enumerate(st.session_state.messages):
        linked_task_id = str(msg.get("linked_task_id") or msg.get("task_id") or "").strip()
        linked_task = pending_task_map.get(linked_task_id)
        is_task_placeholder = bool(msg.get("is_task_placeholder"))
        with st.chat_message(msg["role"]):
            if is_task_placeholder and not linked_task:
                continue
            if is_task_placeholder and linked_task:
                rendered_pending_task_ids.add(linked_task_id)
                if str(linked_task.get("queue_state") or "active") == "active":
                    _render_pending_chat_task_fragment(linked_task)
                else:
                    st.markdown(_render_inline_queued_chat_task_hint(linked_task), unsafe_allow_html=True)
                continue

            inline_state = {"has_inline": False, "used_indices": set()}
            msg_attachments = msg.get("attachments", [])

            if msg["role"] in ["assistant", "ai"]:
                inline_state = render_response_with_inline_attachments(
                    msg.get("content", ""),
                    msg_attachments,
                    render_plain_when_no_token=True,
                )
            else:
                st.markdown(msg["content"], unsafe_allow_html=True)

            # 如果这条消息里有 "chart" 字段，且不为空，就把它画出来
            if msg.get("chart"):
                render_chart_by_filename(msg["chart"])
            if msg_attachments:
                render_knowledge_attachments(msg_attachments, exclude_indices=inline_state["used_indices"])

            # [关键修改]
            if msg["role"] in ["assistant", "ai"]:
                _render_chat_feedback_controls(msg, i)
            if msg["role"] == "ai":
                # 尝试获取上一条消息作为“提问”
                user_question = "（上下文关联提问）"
                if i > 0 and st.session_state.messages[i - 1]["role"] == "user":
                    user_question = st.session_state.messages[i - 1]["content"]

                # 传入两个参数：问题 + 回答
                native_share_button(user_question, msg["content"], key=f"share_history_{i}")

# ==========================================
# 🔥 [新增] 持仓体检任务恢复机制
# ==========================================
if "pending_portfolio_task" in st.session_state and st.session_state.pending_portfolio_task:
    ptask = st.session_state.pending_portfolio_task
    ptask_id = ptask["task_id"]
    ptask_start = ptask["start_time"]
    current_user = st.session_state.get("user_id", "访客")

    if time.time() - ptask_start < 1800:
        task_manager = TaskManager()
        task_status = task_manager.get_task_status(ptask_id)
        current_status = task_status["status"]

        if current_status in ["pending", "processing"]:
            progress_text = str(task_status.get("progress", "正在处理..."))
            recognized_count = int(ptask.get("positions_count", 0) or 0)
            st.markdown(
                f"""
<div style="
    border:1px solid #38bdf8;
    background:linear-gradient(135deg, rgba(15,23,42,0.95), rgba(30,41,59,0.92));
    border-radius:12px;
    padding:14px 16px;
    box-shadow:0 0 0 1px rgba(56,189,248,0.12) inset;
">
  <div style="color:#f8fafc;font-size:18px;font-weight:700;line-height:1.4;">
    📊 持仓体检进行中
  </div>
  <div style="color:#e2e8f0;font-size:16px;margin-top:6px;line-height:1.65;">
    ⏳ {progress_text}（已识别 {recognized_count} 只）
  </div>
</div>
                """,
                unsafe_allow_html=True,
            )
            if not is_announcement_holdoff_active():
                time.sleep(1.2)
                st.rerun()
        elif current_status == "success":
            result = task_status.get("result") or {}
            payload = result.get("result") if isinstance(result, dict) else {}
            summary = ""
            if isinstance(payload, dict):
                summary = str(payload.get("summary_text") or "")
                st.session_state.portfolio_latest_result = payload
            if not summary and isinstance(result, dict):
                summary = str(result.get("response") or "")
            if not summary:
                summary = "持仓体检已完成。"
            detail_hint = "完整分析请到左侧栏「持仓体检」页面查看。"
            if detail_hint not in summary:
                summary = f"{summary}\n\n{detail_hint}"

            st.success("✅ 持仓体检完成")
            st.markdown(summary)

            if current_user != "访客":
                try:
                    retrieval_text = ""
                    if isinstance(result, dict):
                        retrieval_text = result.get("retrieval_summary", "")
                    mem.save_interaction(
                        current_user,
                        "自动持仓体检",
                        retrieval_text or summary,
                        topic="stock_portfolio",
                    )
                except Exception as e:
                    print(f"持仓体检记忆写入失败: {e}")

            st.session_state.messages.append(
                {"role": "ai", "content": f"📊 持仓体检完成\n\n{summary}"}
            )

            st.session_state.pending_portfolio_task = None
            task_manager.clear_user_pending_portfolio_task(current_user)
            st.session_state.uploader_key += 1
            time.sleep(0.5)
            st.rerun()
        elif current_status == "error":
            err = task_status.get("error", "未知错误")
            st.error(f"持仓体检失败：{err[:120]}")
            st.session_state.pending_portfolio_task = None
            task_manager.clear_user_pending_portfolio_task(current_user)
    else:
        st.warning("⏱️ 持仓体检任务超时，请重新上传截图。")
        st.session_state.pending_portfolio_task = None
        if current_user != "访客":
            TaskManager().clear_user_pending_portfolio_task(current_user)

# ==========================================
# 🔥 [新增] 任务恢复机制（兜底未绑定消息的任务）
# ==========================================
pending_tasks = st.session_state.get("pending_tasks") or []
orphan_pending_tasks = [
    task_info
    for task_info in pending_tasks
    if str(task_info.get("task_id") or "").strip() not in rendered_pending_task_ids
]
if orphan_pending_tasks:
    active_orphan = next(
        (task_info for task_info in orphan_pending_tasks if str(task_info.get("queue_state") or "active") == "active"),
        None,
    )
    if active_orphan:
        _render_pending_chat_task_fragment(active_orphan)
    for queued_task in orphan_pending_tasks:
        if active_orphan and str(queued_task.get("task_id") or "").strip() == str(active_orphan.get("task_id") or "").strip():
            continue
        st.markdown(_render_queued_chat_task_card(queued_task), unsafe_allow_html=True)
elif "pending_task" in st.session_state and st.session_state.pending_task and not st.session_state.get("messages"):
    _render_pending_chat_task_fragment(st.session_state.pending_task)



# ==========================================
#  E. 图片上传区 (新增)
# ==========================================
with st.container():
    # 使用 Expander 把上传控件收起来，避免占用太高空间
    with st.expander("📸 可以上传持仓图来做诊断", expanded=False):
        # 🔥 [修改] 使用动态 key，便于清除图片
        uploader_key = f"portfolio_uploader_{st.session_state.uploader_key}"
        uploaded_img = st.file_uploader("支持 JPG/PNG，截图越清晰越好", type=["jpg", "jpeg", "png"],
                                        key=uploader_key)

        if uploaded_img:
            st.image(uploaded_img, caption="已加载截图", width=200)
            current_user = st.session_state.get("user_id", "访客")
            if current_user == "访客":
                st.markdown("""
                            <div style="
                                background-color: rgba(239, 68, 68, 0.16);
                                border: 1px solid rgba(239, 68, 68, 0.7);
                                color: #ffffff !important;
                                padding: 12px;
                                border-radius: 8px;
                                margin-top: 10px;
                                line-height: 1.5;
                            ">
                                <strong style="color: #FCA5A5;">⚠ 请先登录</strong><br>
                                登录后上传截图会自动启动持仓体检，并写入你的专属资料库。
                            </div>
                            """, unsafe_allow_html=True)
            else:
                st.markdown("""
                            <div style="
                                background-color: rgba(59, 130, 246, 0.2);
                                border: 1px solid #3b82f6;
                                color: #ffffff !important;
                                padding: 12px;
                                border-radius: 8px;
                                margin-top: 10px;
                                line-height: 1.5;
                            ">
                                <strong style="color: #FFD700;">✅ 图片已就绪</strong><br>
                                系统将自动识别股票/期权持仓并启动对应分析任务。
                            </div>
                            """, unsafe_allow_html=True)
                auto_submit_position_task(uploaded_img)
                last_err = str(st.session_state.get("position_upload_last_error", "") or "").strip()
                last_warn = list(st.session_state.get("position_upload_last_warnings") or [])
                if last_err:
                    st.error(f"识别失败详情：{last_err}")
                if last_warn:
                    with st.expander("查看识别诊断信息", expanded=False):
                        for w in last_warn[:8]:
                            st.write(f"- {w}")
        else:
            st.session_state.portfolio_last_attempt_hash = None
            st.session_state["position_upload_last_error"] = ""
            st.session_state["position_upload_last_warnings"] = []

# 侧栏按钮样式最终兜底（只命中左上角侧栏开关，不影响右上角菜单）
st.markdown("""
<style>
button[data-testid="stExpandSidebarButton"],
[data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] {
    width: 36px !important;
    height: 36px !important;
    min-width: 36px !important;
    min-height: 36px !important;
    background: #2563eb !important;
    border: 2px solid rgba(255, 255, 255, 0.9) !important;
    border-radius: 10px !important;
    box-shadow: 0 6px 16px rgba(2, 6, 23, 0.65) !important;
    opacity: 1 !important;
}

button[data-testid="stExpandSidebarButton"] {
    position: fixed !important;
    top: 14px !important;
    left: 14px !important;
    z-index: 999997 !important;
}

button[data-testid="stExpandSidebarButton"] *,
[data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] * {
    color: #ffffff !important;
    fill: #ffffff !important;
    stroke: #ffffff !important;
    opacity: 1 !important;
    text-shadow: 0 1px 2px rgba(0,0,0,0.55) !important;
}

button[data-testid="stExpandSidebarButton"],
[data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] {
    font-size: 0 !important;
    color: transparent !important;
}

button[data-testid="stExpandSidebarButton"] span,
[data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] span,
button[data-testid="stExpandSidebarButton"] i,
[data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] i {
    font-family: "Material Symbols Rounded", "Material Symbols Outlined", "Material Icons", sans-serif !important;
    font-size: 20px !important;
    line-height: 20px !important;
    font-weight: 800 !important;
    letter-spacing: normal !important;
    text-transform: none !important;
    white-space: nowrap !important;
}

button[data-testid="stExpandSidebarButton"] svg,
[data-testid="stSidebarHeader"] button[data-testid="stBaseButton-headerNoPadding"] svg {
    width: 20px !important;
    height: 20px !important;
}
</style>
""", unsafe_allow_html=True)

# D. 底部输入框 (Sticky Footer) [修改点：使用 st.chat_input]
if ENABLE_DEEP_MODE and DeepTaskManager is not None:
    st.session_state.deep_mode_enabled = st.toggle(
        "Deep 报告模式",
        value=bool(st.session_state.get("deep_mode_enabled", False)),
        help="开启后将走独立 Deep 队列并输出结构化交易报告。",
    )
else:
    st.session_state.deep_mode_enabled = False

if prompt := st.chat_input("我受过交易汇训练，欢迎问我任何实战交易问题..."):
    if not st.session_state['is_logged_in']:
        st.warning("🔒 请先在左侧侧边栏登录")
    else:
        normalized_prompt = str(prompt).strip()
        use_deep = bool(st.session_state.get("deep_mode_enabled", False))
        if normalized_prompt.lower().startswith("/deep "):
            use_deep = True
            normalized_prompt = normalized_prompt[6:].strip()
        if not normalized_prompt:
            st.warning("请输入有效的问题内容。")
            st.stop()
        process_user_input(normalized_prompt, deep_mode=use_deep)
        st.rerun()  # 确保界面更新

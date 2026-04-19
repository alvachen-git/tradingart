from __future__ import annotations

from typing import Any, Callable, Dict

import streamlit as st


def _truncate_user(user_id: str, max_len: int = 18) -> str:
    user = str(user_id or "").strip()
    if len(user) <= max_len:
        return user
    return f"{user[:max_len - 3]}..."


def build_invite_landing_copy(invite_context: Dict[str, Any]) -> Dict[str, str]:
    inviter = _truncate_user(str(invite_context.get("inviter_user_id") or "").strip(), 20)
    invite_code = str(invite_context.get("invite_code") or "").strip()
    is_valid = bool(invite_context.get("is_valid"))

    if is_valid:
        status_title = "邀请码已锁定"
        status_note = f"来自 {inviter} 的专属邀请。"
    else:
        status_title = "邀请码待确认"
        status_note = "当前邀请码未通过校验，请确认链接是否完整。"

    return {
        "headline": "懂期权实战的AI",
        "subheadline": "爱波塔不是一般大模型，而是受过专业交易训练，能根据行情给出合适的股票、期货、期权交易策略，不是空泛的分析。",
        "status_title": status_title,
        "status_note": status_note,
        "invite_code": invite_code,
    }


def render_invite_register_landing(invite_context: Dict[str, Any], render_register_panel: Callable[[], None]) -> None:
    copy = build_invite_landing_copy(invite_context)
    is_valid = bool(invite_context.get("is_valid"))
    status_text_color = "#15803d" if is_valid else "#b45309"
    status_bg_color = "rgba(34, 197, 94, 0.12)" if is_valid else "rgba(245, 158, 11, 0.14)"

    css = """
        <style>
        [data-testid="stSidebar"],
        [data-testid="stSidebarCollapsedControl"],
        button[kind="header"] {
            display: none !important;
        }
        [data-testid="stMainBlockContainer"] {
            max-width: 78rem !important;
            padding-top: 2.2rem !important;
            padding-left: 2rem !important;
            padding-right: 2rem !important;
        }
        .stApp {
            background:
                radial-gradient(circle at 18% 18%, rgba(255, 214, 102, 0.18), transparent 34%),
                radial-gradient(circle at 72% 22%, rgba(96, 165, 250, 0.16), transparent 30%),
                linear-gradient(180deg, #f8fbff 0%, #eef4ff 100%) !important;
            color: #0f172a !important;
        }
        .stApp, .stApp * {
            font-family: "Avenir Next", "PingFang SC", "Noto Sans SC", sans-serif;
        }
        .st-key-invite_landing_shell {
            padding: 2.2rem 0 1.4rem;
        }
        .st-key-invite_hero_panel,
        .st-key-invite_register_panel {
            min-height: 680px;
            box-sizing: border-box;
        }
        .st-key-invite_hero_panel {
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            padding: 2.8rem 2.6rem;
            border-radius: 28px;
            background:
                linear-gradient(140deg, rgba(255, 255, 255, 0.92), rgba(250, 251, 255, 0.82)),
                linear-gradient(120deg, rgba(255, 206, 102, 0.10), rgba(96, 165, 250, 0.10));
            box-shadow: 0 30px 80px rgba(15, 23, 42, 0.08);
            border: 1px solid rgba(148, 163, 184, 0.18);
        }
        .invite-headline {
            font-size: clamp(2.4rem, 5vw, 4.8rem);
            line-height: 1.02;
            letter-spacing: -0.04em;
            font-weight: 800;
            color: #0f172a;
            margin: 0 0 1.1rem;
            white-space: pre-line;
        }
        .invite-subheadline {
            max-width: 35rem;
            font-size: 1.02rem;
            line-height: 1.9;
            color: #516076;
            margin-bottom: 1.9rem;
        }
        .invite-hero-visual {
            position: relative;
            overflow: hidden;
            min-height: 360px;
            margin-top: auto;
            border-radius: 26px;
            background: linear-gradient(145deg, #0f172a 0%, #13233f 52%, #1f3a6b 100%);
            border: 1px solid rgba(148, 163, 184, 0.16);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.06);
        }
        .invite-hero-image {
            position: absolute;
            inset: 0;
            width: 100%;
            height: 100%;
            object-fit: cover;
            object-position: center;
        }
        .invite-hero-overlay {
            position: absolute;
            inset: 0;
            background:
                linear-gradient(180deg, rgba(15, 23, 42, 0.08), rgba(15, 23, 42, 0.32)),
                radial-gradient(circle at 82% 18%, rgba(255, 214, 102, 0.14), transparent 26%);
        }
        .st-key-invite_register_panel {
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            padding: 1.8rem 1.6rem;
            border-radius: 26px;
            background: rgba(255, 255, 255, 0.94);
            box-shadow: 0 30px 70px rgba(15, 23, 42, 0.12);
            border: 1px solid rgba(148, 163, 184, 0.18);
        }
        .invite-brand {
            font-size: 1.9rem;
            font-weight: 800;
            color: #0f172a;
            letter-spacing: -0.03em;
            margin-bottom: 0.9rem;
        }
        .invite-status-row {
            display: flex;
            gap: 0.6rem;
            align-items: center;
            flex-wrap: wrap;
            margin-bottom: 0.6rem;
        }
        .invite-status-pill {
            display: inline-flex;
            align-items: center;
            padding: 0.32rem 0.72rem;
            border-radius: 999px;
            font-size: 0.85rem;
            font-weight: 700;
            color: __STATUS_TEXT_COLOR__;
            background: __STATUS_BG_COLOR__;
        }
        .invite-status-note {
            color: #60708a;
            font-size: 0.92rem;
            line-height: 1.7;
            margin-bottom: 1rem;
        }
        .invite-code-shell {
            margin: 0.9rem 0 1.1rem;
            padding: 0.92rem 1rem;
            border-radius: 16px;
            border: 1px solid rgba(96, 165, 250, 0.28);
            background: rgba(240, 246, 255, 0.92);
        }
        .invite-code-label {
            font-size: 0.82rem;
            color: #60708a;
            margin-bottom: 0.3rem;
        }
        .invite-code-value {
            font-size: 1.08rem;
            font-weight: 800;
            color: #10213f;
            letter-spacing: 0.04em;
        }
        .invite-panel-footer {
            margin-top: 0.85rem;
            font-size: 0.88rem;
            color: #72829c;
            line-height: 1.7;
        }
        .invite-panel-footer a {
            color: #34538b;
            text-decoration: none;
            font-weight: 700;
        }
        .st-key-invite_register_panel .stTextInput label,
        .st-key-invite_register_panel .stForm label,
        .st-key-invite_register_panel .stCaption {
            color: #334155 !important;
        }
        .st-key-invite_register_panel .stTextInput input {
            background: rgba(248, 251, 255, 0.98) !important;
            color: #0f172a !important;
            border-radius: 14px !important;
            border: 1px solid rgba(148, 163, 184, 0.22) !important;
            min-height: 48px !important;
        }
        .st-key-invite_register_panel .stTextInput input:focus {
            border-color: #60a5fa !important;
            box-shadow: 0 0 0 1px rgba(96, 165, 250, 0.24) !important;
        }
        .st-key-invite_register_panel .stButton > button,
        .st-key-invite_register_panel .stFormSubmitButton > button {
            border-radius: 14px !important;
            min-height: 46px !important;
            font-size: 1rem !important;
            font-weight: 700 !important;
            border: 1px solid rgba(15, 23, 42, 0.08) !important;
            box-shadow: none !important;
            transform: none !important;
        }
        .st-key-invite_register_panel .stButton > button[kind="primary"],
        .st-key-invite_register_panel .stFormSubmitButton > button[kind="primary"] {
            background: linear-gradient(135deg, #111827, #1f2937) !important;
            color: #f8fafc !important;
        }
        .st-key-invite_register_panel .stButton > button:not([kind="primary"]),
        .st-key-invite_register_panel .stFormSubmitButton > button:not([kind="primary"]) {
            background: rgba(245, 248, 255, 0.92) !important;
            color: #10213f !important;
        }
        @media (max-width: 900px) {
            [data-testid="stMainBlockContainer"] {
                max-width: 100% !important;
                padding-left: 1rem !important;
                padding-right: 1rem !important;
                padding-top: 1.2rem !important;
            }
            .st-key-invite_hero_panel,
            .st-key-invite_register_panel {
                min-height: auto;
                padding: 1.4rem 1.2rem;
            }
            .invite-hero-visual {
                min-height: 260px;
            }
        }
        </style>
        """
    css = css.replace("__STATUS_TEXT_COLOR__", status_text_color).replace("__STATUS_BG_COLOR__", status_bg_color)
    st.markdown(css, unsafe_allow_html=True)

    with st.container(key="invite_landing_shell"):
        left_col, right_col = st.columns([1.18, 0.82], gap="large")
        with left_col:
            with st.container(key="invite_hero_panel"):
                st.markdown(f"<div class='invite-headline'>{copy['headline']}</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='invite-subheadline'>{copy['subheadline']}</div>", unsafe_allow_html=True)
                st.markdown(
                    """
                    <div class="invite-hero-visual">
                        <img
                            class="invite-hero-image"
                            src="https://aiprota-img.oss-cn-beijing.aliyuncs.com/inviteai"
                            alt="AI 期权交易员"
                        />
                        <div class="invite-hero-overlay"></div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

        with right_col:
            with st.container(key="invite_register_panel"):
                st.markdown("<div class='invite-brand'>爱波塔</div>", unsafe_allow_html=True)
                st.markdown(
                    f"""
                    <div class="invite-status-row">
                        <div class="invite-status-pill">{copy['status_title']}</div>
                    </div>
                    <div class="invite-status-note">{copy['status_note']}</div>
                    <div class="invite-code-shell">
                        <div class="invite-code-label">当前邀请码</div>
                        <div class="invite-code-value">{copy['invite_code'] or '未识别'}</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                render_register_panel()
                st.markdown(
                    """
                    <div class="invite-panel-footer">
                        注册即代表你同意服务条款与隐私约定。
                        已有账号？<a href="/">返回首页登录</a>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

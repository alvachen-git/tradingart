from __future__ import annotations

import json
import re
from typing import Callable, Optional
from urllib.parse import urlencode

import streamlit as st
import streamlit.components.v1 as components


def _truncate_user(user_id: str, max_len: int = 18) -> str:
    user = str(user_id or "").strip()
    if len(user) <= max_len:
        return user
    return f"{user[:max_len - 3]}..."


def _safe_switch_page(path: str) -> None:
    try:
        st.switch_page(path)
    except Exception as e:
        # 直接提示具体错误，避免“点了没反应”
        st.error(f"页面跳转失败：{e}")


def _resolve_scheme(host: str, proto: str) -> str:
    host_value = str(host or "").strip().lower()
    proto_value = str(proto or "").strip().lower()
    if proto_value in {"http", "https"}:
        return proto_value
    if (
        host_value.startswith("localhost")
        or host_value.startswith("127.0.0.1")
        or host_value.startswith("0.0.0.0")
    ):
        return "http"
    return "https"


def _infer_base_url() -> str:
    try:
        headers = dict(st.context.headers or {})
    except Exception:
        headers = {}
    host = str(headers.get("Host") or headers.get("host") or "").strip()
    proto = str(headers.get("X-Forwarded-Proto") or headers.get("x-forwarded-proto") or "").strip()
    if host:
        return f"{_resolve_scheme(host, proto)}://{host}"
    return "https://www.aiprota.com"


def _build_invite_link(base_url: str, invite_code: str, user_id: str, preview_mode: bool = True) -> str:
    base = str(base_url or "").strip() or _infer_base_url()
    code = str(invite_code or "").strip()
    if not code:
        code = f"preview_{str(user_id or '').strip()}"
    query = urlencode({"invite": code})
    return f"{base.rstrip('/')}/?{query}"


@st.dialog("邀请好友，领积分", width="large")
def _invite_dialog(payload: dict):
    user_id = str(payload.get("user_id") or "").strip()
    invite_link = str(payload.get("invite_link") or "").strip()
    preview_mode = bool(payload.get("preview_mode", True))
    reward_points = int(payload.get("reward_points") or 300)
    stats = payload.get("stats") or {}
    invited_count = int(stats.get("invited_count") or 0)
    rewarded_points = int(stats.get("rewarded_points") or 0)

    st.markdown(
        """
        <style>
        div[data-testid="stDialog"] [data-testid="stMarkdownContainer"] p {
            line-height: 1.6;
        }
        div[data-testid="stDialog"] .stButton > button {
            min-height: 50px !important;
            border-radius: 16px !important;
            border: 1px solid rgba(15, 23, 42, 0.10) !important;
            background: linear-gradient(180deg, #18202f 0%, #0f1725 100%) !important;
            color: #f8fbff !important;
            font-size: 16px !important;
            font-weight: 700 !important;
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.18) !important;
            transition: transform 120ms ease, box-shadow 120ms ease, background 120ms ease !important;
        }
        div[data-testid="stDialog"] .stButton > button:hover,
        div[data-testid="stDialog"] .stButton > button:focus,
        div[data-testid="stDialog"] .stButton > button:focus-visible {
            transform: translateY(-1px) !important;
            box-shadow: 0 16px 34px rgba(15, 23, 42, 0.22) !important;
            background: linear-gradient(180deg, #1d283a 0%, #121c2b 100%) !important;
        }
        div[data-testid="stDialog"] .stTextInput input {
            min-height: 48px !important;
            border-radius: 14px !important;
            background: #f6f9ff !important;
            color: #0f172a !important;
            border: 1px solid rgba(148, 163, 184, 0.24) !important;
            font-size: 15px !important;
        }
        div[data-testid="stDialog"] label[data-testid="stWidgetLabel"] {
            color: #5f6f85 !important;
            font-size: 13px !important;
        }
        .invite-hero-shell {
            position: relative;
            overflow: hidden;
            border-radius: 24px;
            padding: 24px 24px 18px;
            background:
                radial-gradient(circle at top left, rgba(255, 220, 150, 0.30), transparent 34%),
                radial-gradient(circle at 88% 14%, rgba(96, 165, 250, 0.16), transparent 24%),
                linear-gradient(180deg, #ffffff 0%, #f6f9ff 100%);
            border: 1px solid rgba(201, 213, 232, 0.75);
            box-shadow: 0 24px 60px rgba(15, 23, 42, 0.10);
            margin-bottom: 16px;
        }
        .invite-hero-grid {
            position: relative;
            z-index: 1;
            display: grid;
            grid-template-columns: minmax(0, 1.45fr) 240px;
            gap: 18px;
            align-items: start;
        }
        .invite-hero-copy {
            min-width: 0;
        }
        .invite-hero-shell::after {
            content: "";
            position: absolute;
            inset: auto -40px -64px auto;
            width: 180px;
            height: 180px;
            border-radius: 999px;
            background: radial-gradient(circle, rgba(96, 165, 250, 0.10), transparent 68%);
            pointer-events: none;
        }
        .invite-eyebrow {
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 6px 12px;
            border-radius: 999px;
            font-size: 12px;
            font-weight: 700;
            color: #335a8c;
            background: rgba(219, 234, 254, 0.92);
            border: 1px solid rgba(147, 197, 253, 0.42);
            margin-bottom: 14px;
        }
        .invite-title {
            font-size: 34px;
            line-height: 1.04;
            font-weight: 800;
            letter-spacing: -0.04em;
            color: #111827;
            margin: 0;
        }
        .invite-subtitle {
            margin-top: 10px;
            color: #5b6b82;
            font-size: 15px;
            line-height: 1.75;
            max-width: 640px;
        }
        .invite-chip-row {
            display: flex;
            gap: 10px;
            flex-wrap: wrap;
            margin-top: 16px;
        }
        .invite-chip {
            display: inline-flex;
            align-items: center;
            min-height: 38px;
            padding: 0 14px;
            border-radius: 999px;
            background: rgba(17, 24, 39, 0.04);
            border: 1px solid rgba(148, 163, 184, 0.20);
            color: #1e293b;
            font-size: 14px;
            font-weight: 600;
        }
        .invite-link-label {
            color: #64748b;
            font-size: 13px;
            font-weight: 600;
            margin: 2px 0 8px;
        }
        .invite-section-title {
            color: #16253b;
            font-size: 15px;
            font-weight: 800;
            margin: 18px 0 10px;
            letter-spacing: -0.01em;
        }
        .invite-step {
            border-radius: 18px;
            padding: 16px 16px 14px;
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.96), rgba(244, 248, 255, 0.98));
            border: 1px solid rgba(203, 213, 225, 0.70);
            min-height: 118px;
            box-shadow: 0 10px 30px rgba(15, 23, 42, 0.05);
        }
        .invite-step-index {
            color: #f59e0b;
            font-size: 12px;
            font-weight: 800;
            letter-spacing: 0.08em;
            margin-bottom: 8px;
        }
        .invite-step-title {
            color: #122033;
            font-size: 17px;
            font-weight: 800;
            margin-bottom: 6px;
            letter-spacing: -0.02em;
        }
        .invite-step-desc {
            color: #66768d;
            font-size: 13px;
            line-height: 1.65;
        }
        .invite-miniapp-shell,
        .invite-metric {
            border-radius: 20px;
            border: 1px solid rgba(203, 213, 225, 0.76);
            background: linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(246, 249, 255, 0.98));
            box-shadow: 0 12px 30px rgba(15, 23, 42, 0.06);
        }
        .invite-miniapp-shell {
            padding: 16px 16px 14px;
            text-align: center;
            min-height: 100%;
        }
        .invite-miniapp-shell.invite-miniapp-hero {
            padding: 14px 14px 12px;
            box-shadow: 0 14px 34px rgba(15, 23, 42, 0.07);
        }
        .invite-miniapp-title {
            color: #15243b;
            font-size: 18px;
            font-weight: 800;
            margin-bottom: 4px;
            letter-spacing: -0.02em;
        }
        .invite-miniapp-note {
            color: #64748b;
            font-size: 12px;
            line-height: 1.6;
            margin: 0 0 12px;
        }
        .invite-miniapp-qr {
            width: 154px;
            max-width: 100%;
            border-radius: 14px;
            border: 1px solid rgba(203, 213, 225, 0.86);
            background: #fff;
            box-shadow: 0 10px 24px rgba(15, 23, 42, 0.06);
        }
        .invite-metric {
            padding: 18px 18px 16px;
            min-height: 112px;
        }
        .invite-metric-label {
            color: #64748b;
            font-size: 13px;
            font-weight: 600;
        }
        .invite-metric-value {
            color: #101827;
            font-size: 30px;
            font-weight: 800;
            margin-top: 6px;
            letter-spacing: -0.03em;
        }
        .invite-metric-foot {
            color: #8a99b0;
            font-size: 12px;
            margin-top: 8px;
        }
        @media (max-width: 900px) {
            .invite-hero-grid {
                grid-template-columns: 1fr;
            }
            .invite-title {
                font-size: 28px;
            }
            .invite-subtitle {
                font-size: 14px;
            }
            .invite-step {
                min-height: auto;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    mode_text = "预览模式" if preview_mode else "邀请活动"
    st.markdown(
        f"""
        <div class="invite-hero-shell">
            <div class="invite-hero-grid">
                <div class="invite-hero-copy">
                    <div class="invite-eyebrow">{mode_text}</div>
                    <h2 class="invite-title">邀请好友，领积分</h2>
                    <div class="invite-subtitle">
                        把你的专属注册链接发给朋友，对方完成注册后，奖励会自动记入你的通用点数余额。
                    </div>
                    <div class="invite-chip-row">
                        <div class="invite-chip">最高 {reward_points} 点 / 人</div>
                        <div class="invite-chip">账号：{_truncate_user(user_id, 28)}</div>
                    </div>
                </div>
                <div class="invite-miniapp-shell invite-miniapp-hero">
                    <div class="invite-miniapp-title">爱波塔小程序</div>
                    <div class="invite-miniapp-note">也可以把小程序码一起发给好友，先看产品体验。</div>
                    <img
                        class="invite-miniapp-qr"
                        src="https://aiprota-img.oss-cn-beijing.aliyuncs.com/4768.JPG"
                        alt="爱波塔小程序二维码"
                    />
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("复制链接", type="primary", use_container_width=True, key=f"invite_copy_link_{user_id}"):
        payload_js = json.dumps(invite_link, ensure_ascii=False)
        components.html(
            f"""
            <script>
            (async function () {{
                try {{ await navigator.clipboard.writeText({payload_js}); }}
                catch (e) {{ console.warn('copy_failed', e); }}
            }})();
            </script>
            """,
            height=0,
        )
        st.success("已尝试复制邀请链接；若浏览器拦截，请手动复制下方链接。")

    st.markdown(
        '<div class="invite-link-label">邀请链接</div>',
        unsafe_allow_html=True,
    )
    st.text_input(
        "邀请链接",
        value=invite_link,
        key=f"invite_link_preview_{user_id}",
        label_visibility="collapsed",
    )

    st.markdown('<div class="invite-section-title">3 步完成一次有效邀请</div>', unsafe_allow_html=True)

    step_cols = st.columns(3, gap="small")
    steps = [
        ("STEP 01", "发给好友", "复制专属链接，发给准备注册的新朋友。"),
        ("STEP 02", "完成注册", "好友通过链接注册成功，系统自动完成归因。"),
        ("STEP 03", "奖励到账", f"每位有效邀请奖励 {reward_points} 点，可直接用于订阅购买。"),
    ]
    for col, (idx, title, desc) in zip(step_cols, steps):
        with col:
            st.markdown(
                f"""
                <div class="invite-step">
                    <div class="invite-step-index">{idx}</div>
                    <div class="invite-step-title">{title}</div>
                    <div class="invite-step-desc">{desc}</div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('<div class="invite-section-title">邀请数据</div>', unsafe_allow_html=True)

    col1, col2 = st.columns(2, gap="small")
    with col1:
        st.markdown(
            f"""
            <div class="invite-metric">
                <div class="invite-metric-label">累计获得积分</div>
                <div class="invite-metric-value">{rewarded_points}</div>
                <div class="invite-metric-foot">奖励直接进入通用点数余额</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            f"""
            <div class="invite-metric">
                <div class="invite-metric-label">累计邀请好友</div>
                <div class="invite-metric-value">{invited_count}</div>
                <div class="invite-metric-foot">按注册成功人数统计</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def show_invite_preview_dialog(
    user_id: str,
    base_url: str,
    *,
    invite_code: str = "",
    stats: Optional[dict] = None,
    preview_mode: bool = True,
    reward_points: int = 300,
):
    payload = {
        "user_id": str(user_id or "").strip(),
        "invite_link": _build_invite_link(base_url, invite_code, user_id, preview_mode=preview_mode),
        "stats": stats or {},
        "preview_mode": bool(preview_mode),
        "reward_points": int(reward_points or 300),
    }
    _invite_dialog(payload)


def render_sidebar_footer_menu(
    page: str,
    user_id: str | None,
    is_logged_in: bool,
    on_logout: Callable[[], None] | None,
    show_invite_entry: bool = True,
    *,
    base_url: str = "",
    invite_code: str = "",
    invite_stats: Optional[dict] = None,
    invite_preview_mode: bool = True,
    reward_points: int = 300,
) -> None:
    if not is_logged_in or not user_id:
        return

    user = str(user_id).strip()
    page_key = re.sub(r"[^a-zA-Z0-9_]+", "_", str(page or "home")).lower()
    invite_open_key = f"sidebar_invite_dialog_open_{page_key}"

    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] .st-key-sidebar_footer_shell {
            position: sticky;
            bottom: 0;
            margin-top: 8px;
            padding: 6px 0 0;
            z-index: 40;
            background: linear-gradient(180deg, rgba(7, 15, 30, 0.0) 0%, rgba(7, 15, 30, 0.78) 40%);
        }
        [data-testid="stSidebar"] .st-key-sidebar_footer_shell .stButton > button {
            border-radius: 11px !important;
            min-height: 40px !important;
            line-height: 1 !important;
            font-size: 16px !important;
            font-weight: 600 !important;
            justify-content: center !important;
            padding: 0 12px !important;
            border: 1px solid rgba(138, 165, 209, 0.3) !important;
            background: rgba(235, 244, 255, 0.08) !important;
            color: #dbe8fd !important;
            box-shadow: none !important;
            transform: none !important;
            transition: background 110ms ease-in-out, border-color 110ms ease-in-out, color 110ms ease-in-out;
        }
        [data-testid="stSidebar"] .st-key-sidebar_footer_shell .stButton > button:hover,
        [data-testid="stSidebar"] .st-key-sidebar_footer_shell .stButton > button:focus,
        [data-testid="stSidebar"] .st-key-sidebar_footer_shell .stButton > button:focus-visible,
        [data-testid="stSidebar"] .st-key-sidebar_footer_shell .stButton > button:active {
            background: rgba(145, 171, 214, 0.22) !important;
            border-color: rgba(138, 165, 209, 0.38) !important;
            color: #f5f8ff !important;
            box-shadow: none !important;
            transform: none !important;
            outline: none !important;
        }
        [data-testid="stSidebar"] .sidebar-filing-text {
            margin-top: 2px;
            text-align: center;
            font-size: 12px;
            color: rgba(218, 231, 251, 0.95);
            letter-spacing: 0.01em;
            padding-bottom: 2px;
        }

        @keyframes footerMenuFadeUpIn {
            from {
                opacity: 0;
                translate: 0 8px;
            }
            to {
                opacity: 1;
                translate: 0 0;
            }
        }

        div[data-baseweb="popover"]:has(.footer-menu-anchor) {
            --footer-menu-row-height: 44px;
            --footer-menu-row-radius: 8px;
            --footer-menu-row-px: 10px;
            --footer-menu-font-size: 16px;
            --footer-menu-font-weight: 600;
            min-width: 232px !important;
            max-width: 232px !important;
            border: 1px solid rgba(188, 204, 230, 0.88) !important;
            background: linear-gradient(165deg, rgba(246, 250, 255, 0.99), rgba(236, 244, 255, 0.98)) !important;
            box-shadow: 0 10px 20px rgba(2, 8, 20, 0.2), inset 0 1px 0 rgba(255, 255, 255, 0.9) !important;
            border-radius: 14px !important;
            padding: 8px !important;
            animation: footerMenuFadeUpIn 200ms cubic-bezier(0.22, 1, 0.36, 1) both;
            will-change: translate, opacity;
            overflow: visible !important;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .element-container {
            margin: 0 !important;
            padding: 0 !important;
            overflow: visible !important;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) button,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) a,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) a * {
            min-height: var(--footer-menu-row-height) !important;
            height: var(--footer-menu-row-height) !important;
            line-height: 1 !important;
            border: none !important;
            border-radius: var(--footer-menu-row-radius) !important;
            background: transparent !important;
            color: #12335f !important;
            font-size: var(--footer-menu-font-size) !important;
            font-weight: var(--footer-menu-font-weight) !important;
            justify-content: flex-start !important;
            align-items: center !important;
            padding: 0 var(--footer-menu-row-px) !important;
            box-shadow: none !important;
            transform: none !important;
            opacity: 1 !important;
            -webkit-text-fill-color: #12335f !important;
            text-decoration: none !important;
            letter-spacing: 0 !important;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button:hover,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button:focus,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button:focus-visible,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button:active {
            background: rgba(74, 126, 207, 0.12) !important;
            color: #0f3f8a !important;
            box-shadow: none !important;
            border: none !important;
            outline: none !important;
            transform: none !important;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button > div,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button span {
            font-size: var(--footer-menu-font-size) !important;
            font-weight: var(--footer-menu-font-weight) !important;
            letter-spacing: 0 !important;
            line-height: 1 !important;
            display: flex !important;
            align-items: center !important;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button p,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .stButton > button div[data-testid="stMarkdownContainer"] p {
            margin: 0 !important;
            padding: 0 !important;
            font-size: var(--footer-menu-font-size) !important;
            font-weight: var(--footer-menu-font-weight) !important;
            line-height: 1 !important;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-menu-divider {
            height: 1px;
            margin: 0;
            background: rgba(145, 168, 205, 0.34);
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-hover-wrap,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-miniapp-hover-wrap {
            position: relative;
            overflow: visible;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-trigger,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-miniapp-trigger {
            box-sizing: border-box;
            min-height: var(--footer-menu-row-height);
            height: var(--footer-menu-row-height);
            border-radius: var(--footer-menu-row-radius);
            display: flex;
            align-items: center;
            justify-content: flex-start;
            padding: 0 var(--footer-menu-row-px);
            color: #12335f;
            font-size: var(--footer-menu-font-size);
            font-weight: var(--footer-menu-font-weight);
            line-height: 1;
            letter-spacing: 0;
            user-select: none;
            cursor: default;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-hover-wrap:hover .footer-contact-trigger,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-miniapp-hover-wrap:hover .footer-miniapp-trigger {
            background: rgba(74, 126, 207, 0.12);
            color: #0f3f8a;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-flyout,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-miniapp-flyout {
            position: absolute;
            left: calc(100% + 10px);
            top: -10px;
            width: 210px;
            border: 1px solid rgba(188, 204, 230, 0.88);
            border-radius: 12px;
            background: linear-gradient(165deg, rgba(246, 250, 255, 0.99), rgba(236, 244, 255, 0.98));
            box-shadow: 0 12px 24px rgba(2, 8, 20, 0.22);
            padding: 10px;
            opacity: 0;
            visibility: hidden;
            pointer-events: none;
            translate: 6px 0;
            transition: opacity 140ms ease, translate 200ms ease, visibility 0s linear 140ms;
            z-index: 9999;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-hover-wrap:hover .footer-contact-flyout,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-hover-wrap:focus-within .footer-contact-flyout,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-miniapp-hover-wrap:hover .footer-miniapp-flyout,
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-miniapp-hover-wrap:focus-within .footer-miniapp-flyout {
            opacity: 1;
            visibility: visible;
            pointer-events: auto;
            translate: 0 0;
            transition-delay: 0s;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-title {
            color: #12335f;
            font-size: 14px;
            font-weight: 700;
            margin-bottom: 6px;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-line {
            color: #355781;
            font-size: 13px;
            line-height: 1.5;
        }
        div[data-baseweb="popover"]:has(.footer-menu-anchor) .footer-contact-qr {
            width: 132px;
            height: 132px;
            object-fit: cover;
            display: block;
            margin: 8px auto 2px;
            border-radius: 8px;
            border: 1px solid rgba(145, 168, 205, 0.45);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    with st.container(key="sidebar_footer_shell"):
        if show_invite_entry:
            if st.button("邀请好友 领积分", key=f"btn_invite_entry_{page_key}", use_container_width=True):
                st.session_state[invite_open_key] = True

        with st.popover(f"👤 {_truncate_user(user, 16)}", use_container_width=True):
            st.markdown('<div class="footer-menu-anchor"></div>', unsafe_allow_html=True)
            if st.button("个人资料", key=f"btn_footer_profile_{page_key}", use_container_width=True):
                _safe_switch_page("pages/15_个人资料.py")
            st.markdown('<div class="footer-menu-divider"></div>', unsafe_allow_html=True)
            if st.button("充值中心", key=f"btn_footer_recharge_{page_key}", use_container_width=True):
                _safe_switch_page("pages/17_充值中心.py")
            st.markdown('<div class="footer-menu-divider"></div>', unsafe_allow_html=True)
            st.markdown(
                """
                <div class="footer-contact-hover-wrap">
                    <div class="footer-contact-trigger">联系客服</div>
                    <div class="footer-contact-flyout">
                        <div class="footer-contact-title">客服联系方式</div>
                        <div class="footer-contact-line">微信：trader-sec</div>
                        <div class="footer-contact-line">电话：17521591756</div>
                        <img
                            class="footer-contact-qr"
                            src="https://aiprota-img.oss-cn-beijing.aliyuncs.com/QQ%E6%88%AA%E5%9B%BE20231211171619.png"
                            alt="客服二维码"
                        />
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown('<div class="footer-menu-divider"></div>', unsafe_allow_html=True)
            st.markdown(
                """
                <div class="footer-miniapp-hover-wrap">
                    <div class="footer-miniapp-trigger">小程序</div>
                    <div class="footer-miniapp-flyout">
                        <div class="footer-contact-title">爱波塔小程序</div>
                        <img
                            class="footer-contact-qr"
                            src="https://aiprota-img.oss-cn-beijing.aliyuncs.com/4768.JPG"
                            alt="爱波塔小程序二维码"
                        />
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            st.markdown('<div class="footer-menu-divider"></div>', unsafe_allow_html=True)
            if st.button("登出", key=f"btn_footer_logout_{page_key}", use_container_width=True):
                if callable(on_logout):
                    on_logout()
                else:
                    st.session_state["is_logged_in"] = False
                    st.session_state["user_id"] = None
                    st.session_state["token"] = None
                    st.rerun()

        st.markdown('<div class="sidebar-filing-text">沪ICP备2021018087号-2</div>', unsafe_allow_html=True)

    if st.session_state.get(invite_open_key, False):
        st.session_state[invite_open_key] = False
        show_invite_preview_dialog(
            user_id=user,
            base_url=base_url or _infer_base_url(),
            invite_code=invite_code,
            stats=invite_stats or {"invited_count": 0, "rewarded_points": 0},
            preview_mode=invite_preview_mode,
            reward_points=reward_points,
        )

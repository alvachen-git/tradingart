import re
import time
import streamlit as st
from datetime import datetime, timedelta, timezone

BEIJING_TZ = timezone(timedelta(hours=8))
import subscription_service as sub_svc
import auth_utils as auth
import extra_streamlit_components as stx
import streamlit.components.v1 as components
from share_utils import add_share_button
from ui_components import (
    inject_sidebar_toggle_style,
    inject_quant_ops_header_style,
    render_quant_ops_header,
)


# ==========================================
# 页面配置
# ==========================================
st.set_page_config(
    page_title="情报站 - 爱波塔",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)



# 🔥 添加统一的侧边栏导航
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation

# ==========================================
# 🔧 自助订阅频道配置（点击即可自助开通，无需联系客服）
# 如需新增自助订阅频道，直接往这个列表里加频道名称即可
# ==========================================
FREE_SELF_SUBSCRIBE_CHANNELS = ["复盘晚报", "末日期权晚报"]
with st.sidebar:
    show_navigation()

# ==========================================
# 🎨 高级样式注入
# ==========================================
st.markdown("""
<style>
    /* ========== 全局背景 ========== */
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
    }

    /* 拓宽主内容区 */
    [data-testid="stMainBlockContainer"] {
        max-width: 88rem !important;
        padding-top: 0.8rem !important;
        padding-bottom: 1.2rem !important;
        padding-left: 1.2rem;
        padding-right: 1.2rem;
    }

    /* 情报站标题高对比修复 */
    .quant-hero-shell .quant-hero-title {
        color: #f8fbff !important;
        font-weight: 800 !important;
        text-shadow:
            0 0 20px rgba(59, 130, 246, 0.55),
            0 0 2px rgba(255, 255, 255, 0.85) !important;
        -webkit-text-stroke: 0.5px rgba(186, 224, 255, 0.5);
    }
    .quant-hero-shell .quant-hero-sub,
    .quant-hero-shell .quant-hero-note {
        color: #dbe9ff !important;
    }

    @media (max-width: 768px) {
        [data-testid="stMainBlockContainer"] {
            max-width: 100% !important;
            padding-top: 0.5rem !important;
            padding-left: 0.8rem;
            padding-right: 0.8rem;
        }
    }

    /* 隐藏顶部装饰 */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    [data-testid="stDecoration"] {
        display: none;
    }

    /* ========== 页面标题样式 ========== */
    .page-header {
        background: linear-gradient(135deg, rgba(251,191,36,0.1) 0%, rgba(30,41,59,0.6) 100%);
        border: 1px solid rgba(251,191,36,0.2);
        border-radius: 20px;
        padding: 30px;
        margin-bottom: 30px;
        text-align: center;
    }
    .page-title {
        color: #fbbf24;
        font-size: 32px;
        font-weight: 700;
        margin: 0;
        letter-spacing: 2px;
    }
    .page-subtitle {
        color: #94a3b8;
        font-size: 14px;
        margin-top: 8px;
    }

    /* ========== 频道卡片网格 ========== */
    .channel-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 16px;
        margin-bottom: 30px;
    }
    .channel-card {
        background: rgba(30, 41, 59, 0.6);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 16px;
        padding: 20px;
        text-align: center;
        cursor: pointer;
        transition: all 0.3s ease;
        position: relative;
        overflow: hidden;
    }
    .channel-card:hover {
        transform: translateY(-4px);
        border-color: rgba(251, 191, 36, 0.4);
        box-shadow: 0 12px 40px rgba(251, 191, 36, 0.15);
    }
    .channel-card.active {
        border-color: #fbbf24;
        background: linear-gradient(135deg, rgba(251,191,36,0.15) 0%, rgba(30,41,59,0.8) 100%);
    }
    .channel-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: 0;
        right: 0;
        height: 3px;
        background: linear-gradient(90deg, #fbbf24, #f59e0b);
        opacity: 0;
        transition: opacity 0.3s;
    }
    .channel-card.active::before,
    .channel-card:hover::before {
        opacity: 1;
    }
    .channel-icon {
        font-size: 36px;
        margin-bottom: 12px;
    }
    .channel-name {
        color: #e2e8f0;
        font-size: 16px;
        font-weight: 600;
        margin-bottom: 8px;
    }
    .channel-desc {
        color: #64748b;
        font-size: 12px;
        line-height: 1.4;
        margin-bottom: 12px;
    }

    /* 订阅状态徽章 */
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 20px;
        font-size: 11px;
        font-weight: 600;
    }
    .status-active {
        background: rgba(34, 197, 94, 0.2);
        color: #22c55e;
        border: 1px solid rgba(34, 197, 94, 0.3);
    }
    .status-expired {
        background: rgba(239, 68, 68, 0.2);
        color: #ef4444;
        border: 1px solid rgba(239, 68, 68, 0.3);
    }
    .status-none {
        background: rgba(100, 116, 139, 0.2);
        color: #94a3b8;
        border: 1px solid rgba(100, 116, 139, 0.3);
    }
    .status-free {
        background: rgba(56, 189, 248, 0.2);
        color: #38bdf8;
        border: 1px solid rgba(56, 189, 248, 0.3);
    }

    /* ========== 内容区域 ========== */
    .section-header {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 20px;
        padding-bottom: 12px;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .section-title {
        /* 🔥 修改点：改为亮白色，并加粗 */
        color: #f1f5f9 !important; 
        font-size: 22px;  /* 字号加大一点 */
        font-weight: 700; /* 加粗 */
        margin: 0;
        /* 增加文字阴影，让它从背景中浮出来 */
        text-shadow: 0 2px 4px rgba(0,0,0,0.6); 
        letter-spacing: 1px; /* 增加字间距 */
    }
    .section-icon {
        width: 5px; /* 稍微加宽装饰条 */
        height: 24px;
        background: linear-gradient(180deg, #fbbf24, #f59e0b);
        border-radius: 2px;
        box-shadow: 0 0 10px rgba(251, 191, 36, 0.5); /* 让旁边的金条发光 */
    }

    /* ========== 内容卡片 ========== */
    .content-card {
        background: rgba(30, 41, 59, 0.5);
        border: 1px solid rgba(255, 255, 255, 0.06);
        border-radius: 16px;
        margin-bottom: 16px;
        overflow: hidden;
        transition: all 0.2s ease;
    }
    .content-card:hover {
        border-color: rgba(255, 255, 255, 0.12);
        background: rgba(30, 41, 59, 0.7);
    }
    .content-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 16px 20px;
        border-bottom: 1px solid rgba(255,255,255,0.04);
    }
    .content-title {
        color: #e2e8f0;
        font-size: 15px;
        font-weight: 600;
    }
    .content-meta {
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .content-time {
        color: #64748b;
        font-size: 12px;
    }
    .content-channel-tag {
        background: rgba(251, 191, 36, 0.15);
        color: #fbbf24;
        padding: 2px 8px;
        border-radius: 4px;
        font-size: 11px;
    }
    .content-body {
        padding: 20px;
    }
    .content-summary {
        color: #94a3b8;
        font-size: 14px;
        line-height: 1.6;
    }

    /* ========== 锁定遮罩 ========== */
    .locked-card {
        background: linear-gradient(135deg, rgba(15,23,42,0.95) 0%, rgba(30,41,59,0.9) 100%);
        border: 1px solid rgba(251, 191, 36, 0.2);
        border-radius: 16px;
        padding: 40px 30px;
        text-align: center;
        margin-bottom: 16px;
    }
    .locked-icon {
        font-size: 48px;
        margin-bottom: 16px;
        opacity: 0.8;
    }
    .locked-title {
        color: #e2e8f0;
        font-size: 16px;
        font-weight: 600;
        margin-bottom: 8px;
    }
    .locked-desc {
        color: #64748b;
        font-size: 13px;
        margin-bottom: 20px;
    }

    /* ========== 消息通知按钮 ========== */
    .notification-btn {
        position: relative;
        background: rgba(30, 41, 59, 0.8);
        border: 1px solid rgba(255,255,255,0.1);
        border-radius: 12px;
        padding: 10px 16px;
        color: #e2e8f0;
        cursor: pointer;
        transition: all 0.2s;
    }
    .notification-btn:hover {
        background: rgba(251, 191, 36, 0.1);
        border-color: rgba(251, 191, 36, 0.3);
    }
    .notification-badge {
        position: absolute;
        top: -6px;
        right: -6px;
        background: #ef4444;
        color: white;
        font-size: 11px;
        font-weight: 600;
        padding: 2px 6px;
        border-radius: 10px;
        min-width: 18px;
        text-align: center;
    }

    /* ========== 日期分隔线 ========== */
    .date-divider {
        display: flex;
        align-items: center;
        gap: 16px;
        margin: 24px 0 16px 0;
    }
    .date-divider::before,
    .date-divider::after {
        content: '';
        flex: 1;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
    }
    .date-label {
        color: #64748b;
        font-size: 13px;
        font-weight: 500;
        white-space: nowrap;
    }

    /* ========== 空状态 ========== */
    .empty-state {
        background: rgba(30, 41, 59, 0.4);
        border: 1px dashed rgba(255,255,255,0.1);
        border-radius: 16px;
        padding: 60px 30px;
        text-align: center;
    }
    .empty-icon {
        font-size: 56px;
        margin-bottom: 16px;
        opacity: 0.6;
    }
    .empty-text {
        color: #64748b;
        font-size: 15px;
    }

    /* ========== 空状态 ========== */
    .empty-state {
        background: rgba(30, 41, 59, 0.4);
        border: 1px dashed rgba(255,255,255,0.1);
        border-radius: 16px;
        padding: 60px 30px;
        text-align: center;
    }
    .empty-icon {
        font-size: 56px;
        margin-bottom: 16px;
        opacity: 0.6;
    }
    .empty-text {
        color: #64748b;
        font-size: 15px;
    }

    /* 🔥🔥🔥 [新增] 强制侧边栏背景与 Home.py 一致 🔥🔥🔥 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important; /* 深蓝黑背景 */
        border-right: 1px solid #1e293b;      /* 可选：右侧分割线 */
    }

    /* 🔥🔥🔥 [新增] 强制侧边栏文字变白/灰 (覆盖默认黑色) 🔥🔥🔥 */
    [data-testid="stSidebar"] p, 
    [data-testid="stSidebar"] span, 
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] label {
        color: #cbd5e1 !important;
    }

    /* ========== 侧边栏统一样式 ========== */
    .sidebar-card {
        background-color: #1E2329;
        border: 1px solid #31333F;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 12px;
    }
    .sidebar-title {
        font-size: 14px;
        font-weight: bold;
        color: #e6e6e6;
        margin-bottom: 10px;
    }
    .sidebar-stat {
        display: flex;
        justify-content: space-between;
        padding: 8px 0;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    .sidebar-stat:last-child {
        border-bottom: none;
    }
    .stat-label {
        color: #8b949e;
        font-size: 13px;
    }
    .stat-value {
        color: #fbbf24;
        font-size: 13px;
        font-weight: 600;
    }

    /* 客服卡片 */
    .contact-card {
        background-color: #1E2329;
        border: 1px solid #31333F;
        border-radius: 8px;
        padding: 15px;
        margin-top: 10px;
        text-align: center;
    }
    .contact-title {
        font-size: 14px;
        font-weight: bold;
        color: #e6e6e6;
        margin-bottom: 8px;
    }
    .contact-item {
        font-size: 13px;
        color: #8b949e;
        margin-bottom: 4px;
    }
    .wechat-highlight {
        color: #00e676;
        font-weight: bold;
    }

    /* ========== 按钮样式覆盖 ========== */
    div.stButton > button {
        background-color: #1E2329 !important;
        color: #e6e6e6 !important;
        border: 1px solid #31333F !important;
        border-radius: 8px !important;
        transition: all 0.2s ease-in-out !important;
    }
    div.stButton > button:hover {
        border-color: #fbbf24 !important;
        background-color: rgba(251,191,36,0.1) !important;
        color: #ffffff !important;
        transform: translateY(-2px);
    }
    /* 1. 隐藏原生丑陋的三角形 */
    details > summary {
        list-style: none;
        cursor: pointer;
        outline: none;
    }
    details > summary::-webkit-details-marker {
        display: none;
    }

    /* 2. 定义 Summary (也就是标题栏) 的布局 */
    .native-summary {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 16px 20px;
        background: transparent;
        border-radius: 16px;
        transition: background 0.2s;
    }
    .native-summary:hover {
        background: rgba(255, 255, 255, 0.03);
    }

    /* 3. 定义展开后的内容容器 */
    .native-content {
        padding: 0 20px 20px 20px;
        margin-top: -10px; /* 让内容紧贴标题 */
        border-top: 1px solid rgba(255,255,255,0.06);
        padding-top: 20px;
        /* 🔥 关键：添加进入动画 */
        animation: slideDown 0.3s ease-out forwards;
    }

    /* 4. 定义按钮文字变化 (纯CSS实现 展开/收起 切换) */
    .toggle-text::after {
        content: "展开";
        display: inline-block;
        padding: 4px 12px;
        background: rgba(255, 255, 255, 0.1);
        border-radius: 6px;
        color: #e2e8f0;
        font-size: 13px;
        transition: all 0.2s;
    }

    /* 当 details 处于 open 状态时，文字变更为 收起 */
    details[open] .toggle-text::after {
        content: "收起";
        background: rgba(251, 191, 36, 0.2);
        color: #fbbf24;
    }

    /* 5. 动画关键帧 */
    @keyframes slideDown {
        from { opacity: 0; transform: translateY(-10px); }
        to { opacity: 1; transform: translateY(0); }
    }

    /* 6. 容器边框 (替代原来的 col 布局) */
    .native-card-container {
        border-bottom: 1px solid rgba(255,255,255,0.1);
        margin-bottom: 16px;
    }

    /* =============================================
       🔥 [修复] 折叠框 (Expander) 标题高亮样式
       ============================================= */

    /* 1. 强制折叠框标题栏 (summary) 的样式 */
    [data-testid="stExpander"] details > summary {
        color: #fbbf24 !important;      /* 强制亮金色字体 */
        font-size: 12px;     /* 字号适中 */
        font-weight: 400 !important;    /* 加粗 */
        background-color: rgba(255, 255, 255, 0.05) !important; /* 微微发亮的背景 */
        border: 1px solid rgba(255, 255, 255, 0.1) !important;  /* 加上边框更像个按钮 */
        border-radius: 8px !important;
        padding: 10px 15px !important;  /* 增加点击区域 */
        transition: all 0.3s ease !important;
    }

    /* 2. 修复折叠框内部的文字元素 (防止被 span/p 覆盖) */
    [data-testid="stExpander"] details > summary p,
    [data-testid="stExpander"] details > summary span {
        color: #fbbf24 !important;
        font-weight: 600 !important;
    }

    /* 3. 强制箭头图标变色 */
    [data-testid="stExpander"] details > summary svg {
        fill: #fbbf24 !important;       /* 箭头变成金色 */
        color: #fbbf24 !important;
    }

    /* 4. 鼠标悬停时的交互效果 */
    [data-testid="stExpander"] details > summary:hover {
        background-color: rgba(251, 191, 36, 0.15) !important; /* 悬停背景变亮 */
        border-color: #fbbf24 !important;                      /* 边框变亮 */
    }

    [data-testid="stExpander"] details > summary:hover p,
    [data-testid="stExpander"] details > summary:hover span {
        color: #ffffff !important;      /* 悬停时文字变白，提示可点击 */
    }

    [data-testid="stExpander"] details > summary:hover svg {
        fill: #ffffff !important;       /* 悬停时箭头变白 */
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")
inject_quant_ops_header_style()

# ==========================================
# 初始化 Session State
# ==========================================
if 'selected_channel' not in st.session_state:
    st.session_state.selected_channel = 'all'
if 'expanded_content' not in st.session_state:
    st.session_state.expanded_content = set()
if 'is_logged_in' not in st.session_state:
    st.session_state.is_logged_in = False
    st.session_state.user_id = None
    st.session_state.token = None
if 'intel_cookie_retry_once' not in st.session_state:
    st.session_state.intel_cookie_retry_once = False

cookie_manager = stx.CookieManager(key="intel_cookie_manager")
cookies = cookie_manager.get_all() or {}


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


if not st.session_state.get("is_logged_in") and not st.session_state.get("just_logged_out", False):
    restored, restore_state = _restore_login_with_cookie_state(cookies)
    if restored:
        st.session_state.intel_cookie_retry_once = False
    elif restore_state in ("empty", "partial", "error") and not st.session_state.get("intel_cookie_retry_once", False):
        st.session_state.intel_cookie_retry_once = True
        time.sleep(0.15)
        st.rerun()
    elif restore_state == "invalid":
        try:
            cookie_manager.delete("username", key="intel_del_user")
            cookie_manager.delete("token", key="intel_del_token")
        except:
            pass

if st.session_state.get("just_logged_out", False):
    st.session_state.just_logged_out = False


# ==========================================
# 辅助函数
# ==========================================
def get_current_user():
    """获取当前登录用户"""
    if st.session_state.get('is_logged_in') and st.session_state.get('user_id'):
        return st.session_state['user_id']
    return None


def format_time(dt: datetime) -> str:
    """格式化时间显示（北京时间）"""
    if not dt:
        return ""
    now = datetime.now(BEIJING_TZ)
    # 数据库存储UTC（MySQL NOW()），转换为北京时间再计算差值
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(BEIJING_TZ)
    delta = now - dt

    if delta.days == 0:
        if delta.seconds < 3600:
            mins = delta.seconds // 60
            return f"{mins} 分钟前" if mins > 0 else "刚刚"
        return f"{delta.seconds // 3600} 小时前"
    elif delta.days == 1:
        return "昨天 " + dt.strftime("%H:%M")
    elif delta.days < 7:
        return f"{delta.days} 天前"
    else:
        return dt.strftime("%m-%d %H:%M")


def toggle_expand(content_id: int):
    """切换内容展开/收起"""
    if content_id in st.session_state.expanded_content:
        st.session_state.expanded_content.remove(content_id)
    else:
        st.session_state.expanded_content.add(content_id)


# ==========================================
# 获取用户信息
# ==========================================
user = get_current_user()
unread_count = sub_svc.get_unread_count(user) if user else 0

# ==========================================
# 🎯 侧边栏（统一暗色风格，参考Home.py）
# ==========================================
with st.sidebar:
    # 用户状态 - 使用暗色卡片而非 st.success
    if user:
        st.markdown(f"""
        <div style="background-color: #1E2329; border: 1px solid #31333F; border-radius: 8px; padding: 12px 15px; margin-bottom: 12px;">
            <span style="color: #22c55e; font-weight: 600;">👤 欢迎回来，{user}</span>
        </div>
        """, unsafe_allow_html=True)

        # 订阅统计卡片
        user_subs = sub_svc.get_user_subscriptions(user)
        active_subs = [s for s in user_subs if s['is_active']]

        st.markdown(f"""
        <div style="background-color: #1E2329; border: 1px solid #31333F; border-radius: 8px; padding: 15px; margin-bottom: 12px;">
            <div style="font-size: 14px; font-weight: bold; color: #e6e6e6; margin-bottom: 12px;">📊 订阅统计</div>
            <div style="display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid rgba(255,255,255,0.05);">
                <span style="color: #8b949e; font-size: 13px;">已订阅频道</span>
                <span style="color: #fbbf24; font-size: 13px; font-weight: 600;">{len(active_subs)} 个</span>
            </div>
            <div style="display: flex; justify-content: space-between; padding: 8px 0;">
                <span style="color: #8b949e; font-size: 13px;">未读消息</span>
                <span style="color: #fbbf24; font-size: 13px; font-weight: 600;">{unread_count} 条</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # 快捷操作
        if unread_count > 0:
            if st.button(f"🔔 查看消息 ({unread_count})", use_container_width=True):
                st.session_state.show_notifications = True
                st.rerun()

        # 管理订阅
        with st.expander("⚙️ 管理订阅", expanded=False):
            channels = sub_svc.get_all_channels()
            user_sub_map = {s['channel_id']: s for s in user_subs}

            for channel in channels:
                sub_info = user_sub_map.get(channel['id'])
                # 判断当前用户是否拥有该频道的有效订阅
                is_subscribed = sub_info and sub_info.get('is_active')

                col1, col2 = st.columns([2.5, 1.2])  # 调整一下列宽比例

                with col1:
                    st.markdown(f"**{channel['icon']} {channel['name']}**")
                    if is_subscribed:
                        # 显示到期时间
                        expire_text = sub_svc.format_expire_time(sub_info['expire_at'])
                        st.caption(f"✅ {expire_text}")
                    elif not channel['is_premium']:
                        st.caption("🆓 免费")
                    else:
                        st.caption("🔒 未订阅")

                with col2:
                    # 🔥🔥🔥 【核心修改逻辑开始】 🔥🔥🔥

                    # 场景 1: 针对 "复盘晚报" (或你可以添加其他允许自助订阅的频道)
                    if channel['name'] in FREE_SELF_SUBSCRIBE_CHANNELS:
                        if is_subscribed:
                            # 已有权限 -> 显示“退订” (Secondary 灰色按钮)
                            if st.button("退订", key=f"unsub_{channel['id']}", type="secondary",
                                         use_container_width=True):
                                if sub_svc.cancel_subscription(user, channel['id']):
                                    st.success("已退订")
                                    st.rerun()
                        else:
                            # 无权限 -> 显示“订阅” (Primary 亮色按钮)
                            if st.button("订阅", key=f"sub_{channel['id']}", type="primary", use_container_width=True):
                                # 默认订阅 30 天，或者你可以改成 365 天
                                success, msg = sub_svc.add_subscription(user, channel['id'], days=100)
                                if success:
                                    st.balloons()
                                    st.rerun()
                                else:
                                    st.error(msg)

                    # 场景 2: 其他付费频道 -> 引导联系客服
                    elif channel['is_premium'] and not is_subscribed:
                        if st.button("开通", key=f"side_sub_{channel['id']}", type="secondary",
                                     use_container_width=True):
                            st.toast("此频道请联系客服开通", icon="💁")

    st.markdown("---")

    # 客服卡片
    st.markdown("""
    <div class="contact-card">
        <div class="contact-title">🤝 客服联系</div>
        <div class="contact-item">微信：<span class="wechat-highlight">trader-sec</span></div>
        <div class="contact-item">电话：<span class="wechat-highlight">17521591756</span></div>
        <div class="contact-item" style="font-size: 12px; margin-top: 8px;">
            沪ICP备2021018087号-2
        </div>
    </div>
    """, unsafe_allow_html=True)

# ==========================================
# 📡 主内容区
# ==========================================
render_quant_ops_header(
    "情报站",
    "",
    "AI结合市场数据和团队实战经验生成报告",
)

# ==========================================
# 未登录提示
# ==========================================
if not user:
    st.markdown("""
    <div class="locked-card">
        <div class="locked-icon">🔐</div>
        <div class="locked-title">请先登录</div>
        <div class="locked-desc">登录后即可查看订阅内容、管理订阅和接收消息通知</div>
    </div>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.page_link("Home.py", label="🏠 返回首页登录", use_container_width=True)
    st.stop()

# ==========================================
# 站内消息弹窗
# ==========================================
if st.session_state.get('show_notifications'):
    st.markdown(
        '<div class="section-header"><div class="section-icon"></div><h3 class="section-title">📬 站内消息</h3></div>',
        unsafe_allow_html=True)

    notifications = sub_svc.get_notifications(user, limit=20)

    if notifications:
        col1, col2 = st.columns([3, 1])
        with col2:
            if st.button("全部已读", key="mark_all_read", type="secondary"):
                sub_svc.mark_notification_read(user_id=user, mark_all=True)
                st.rerun()

        for notif in notifications:
            bg = "rgba(251,191,36,0.08)" if not notif['is_read'] else "rgba(30,41,59,0.4)"
            st.markdown(f"""
            <div style="background:{bg}; border-radius:10px; padding:14px; margin-bottom:8px; border-left:3px solid {'#fbbf24' if not notif['is_read'] else 'transparent'};">
                <div style="color:#e2e8f0; font-size:14px; font-weight:{'600' if not notif['is_read'] else '400'};">{notif['title']}</div>
                <div style="color:#64748b; font-size:12px; margin-top:4px;">{format_time(notif['created_at'])}</div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("📭 暂无消息")

    if st.button("关闭消息", key="close_notif", use_container_width=True):
        st.session_state.show_notifications = False
        st.rerun()

    st.markdown("---")

# ==========================================
# 📺 频道选择
# ==========================================
channels = sub_svc.get_all_channels()
user_subs = sub_svc.get_user_subscriptions(user)
user_sub_map = {s['channel_id']: s for s in user_subs}

# 频道按钮
cols = st.columns(len(channels) + 1)

with cols[0]:
    is_all_active = st.session_state.selected_channel == 'all'
    if st.button("📋 全部", key="ch_all", type="primary" if is_all_active else "secondary", use_container_width=True):
        st.session_state.selected_channel = 'all'
        st.rerun()

for i, channel in enumerate(channels):
    with cols[i + 1]:
        sub_info = user_sub_map.get(channel['id'])
        is_active = st.session_state.selected_channel == channel['code']

        # 状态判断
        if not channel['is_premium']:
            status = "免费"
            status_class = "free"
        elif sub_info and sub_info['is_active']:
            status = sub_svc.format_expire_time(sub_info['expire_at'])
            status_class = "active"
        elif sub_info and sub_info.get('is_expired'):
            status = "已过期"
            status_class = "expired"
        else:
            status = "未订阅"
            status_class = "none"

        if st.button(f"{channel['icon']} {channel['name']}", key=f"ch_{channel['code']}",
                     type="primary" if is_active else "secondary", use_container_width=True):
            st.session_state.selected_channel = channel['code']
            st.rerun()

        # 状态标签
        st.markdown(
            f'<div style="text-align:center;margin-top:-8px;"><span class="status-badge status-{status_class}">{status}</span></div>',
            unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ==========================================
# 📄 内容列表
# ==========================================
st.markdown(
    '<div class="section-header"><div class="section-icon"></div><h3 class="section-title">📄 最新内容</h3></div>',
    unsafe_allow_html=True)

# 获取内容
if st.session_state.selected_channel == 'all':
    contents = sub_svc.get_channel_contents(days=10, limit=30)
else:
    contents = sub_svc.get_channel_contents(channel_code=st.session_state.selected_channel, days=10, limit=20)

if not contents:
    st.markdown("""
    <div class="empty-state">
        <div class="empty-icon">📭</div>
        <div class="empty-text">暂无内容，请稍后再来查看</div>
    </div>
    """, unsafe_allow_html=True)
else:
    today = datetime.now(BEIJING_TZ).date()
    yesterday = today - timedelta(days=1)
    current_date = None

    for content in contents:
        # 日期分隔（publish_time 是 UTC，转北京时间取日期）
        pt = content['publish_time']
        if pt:
            if pt.tzinfo is None:
                pt = pt.replace(tzinfo=timezone.utc).astimezone(BEIJING_TZ)
            content_date = pt.date()
        else:
            content_date = None
        if content_date != current_date:
            current_date = content_date
            if content_date == today:
                date_text = "📅 今天"
            elif content_date == yesterday:
                date_text = "📅 昨天"
            else:
                date_text = f"📅 {content_date.strftime('%m月%d日')}" if content_date else "📅 未知"

            st.markdown(f'<div class="date-divider"><span class="date-label">{date_text}</span></div>',
                        unsafe_allow_html=True)

        # 检查权限
        access = sub_svc.check_subscription_access(user, content['channel_id'])

        if access['has_access']:
            # 有权限 - 显示内容卡片
            pub_time_str = format_time(content['publish_time'])
            summary_text = content['summary'][:120] + "..." if content['summary'] else "暂无摘要"

            # 1. 先显示一个漂亮的预览卡片 (Header)
            # 这里只显示标题、摘要和时间，不显示复杂内容，所以绝对不会崩
            st.markdown(f"""
                        <div style="background:rgba(30,41,59,0.5); border:1px solid rgba(255,255,255,0.06); border-radius:12px; padding:16px; margin-bottom:0px; border-bottom-left-radius:0; border-bottom-right-radius:0;">
                            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                                <div style="color:#f1f5f9; font-size:16px; font-weight:700; display:flex; align-items:center; gap:8px;">
                                    {content['channel_icon']} {content['title']}
                                </div>
                                <div style="color:#64748b; font-size:12px;">{pub_time_str}</div>
                            </div>
                            <div style="color:#94a3b8; font-size:13px; line-height:1.5;">
                                {summary_text}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

            # 2. 使用 Expander 包裹正式内容
            # 注意：st.expander 在新版 Streamlit 中也是客户端渲染，速度很快
            with st.expander("📄 点击查看完整复盘", expanded=False):
                # 3. 🔥🔥🔥 核心：使用 components.html 渲染 HTML 🔥🔥🔥
                # height=1000: 给一个足够的高度
                # scrolling=True: 内容太长可以滚动
                # 这样就是一个独立的网页沙箱，无论 HTML 多复杂都能完美显示！
                components.html(content['content'], height=1000, scrolling=True)

                # 🔥 新增：分享功能
                add_share_button(
                    content_title=content['title'],
                    content_summary=content['summary'],
                    content_html=content['content'],
                    channel_icon=content['channel_icon'],
                    pub_time=pub_time_str,
                    content_id=content['id']
                )

            # 加个间距
            st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)
        else:
            # 无权限 - 显示锁定卡片
            reason_map = {
                "not_subscribed": "订阅后即可查看完整内容",
                "expired": f"订阅已于 {access['expire_at'].strftime('%Y-%m-%d') if access['expire_at'] else ''} 过期",
                "subscription_inactive": "订阅已停用"
            }
            reason = reason_map.get(access['reason'], "需要订阅")

            st.markdown(f"""
            <div class="locked-card">
                <div class="locked-icon">🔒</div>
                <div class="locked-title">{content['title']}</div>
                <div class="locked-desc">{reason}</div>
            </div>
            """, unsafe_allow_html=True)

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                # 🔥🔥🔥 【修复】针对“复盘晚报”实现点击即订阅 🔥🔥🔥

                # 检查是否是复盘晚报 (或者其他你定义的免费频道)
                # 注意：这里需要确保 content 字典里有 'channel_name' 字段
                current_content_channel = content.get('channel_name')

                # 2. 判断是否为免费频道 (复盘晚报)
                is_free_channel = current_content_channel in FREE_SELF_SUBSCRIBE_CHANNELS

                if is_free_channel:
                    # 场景 1: 免费/自助频道 -> 显示绿色/亮色按钮直接开通
                    if st.button("🔓 免费订阅", key=f"lock_{content['id']}", type="primary", use_container_width=True):
                        # 调用订阅服务
                        success, msg = sub_svc.add_subscription(user, content['channel_id'], days=100)

                        if success:
                            st.balloons()  # 撒花
                            st.toast("✅ 订阅成功！正在刷新...", icon="🎉")
                            time.sleep(1)
                            st.rerun()  # 立即刷新页面
                        else:
                            st.error(f"订阅失败: {msg}")

                else:
                    # 场景 2: 其他付费频道 -> 引导联系客服
                    if st.button("🔓 立即订阅", key=f"lock_{content['id']}", type="primary", use_container_width=True):
                        st.info("此频道为高级服务，请联系客服开通：微信 trader-sec")

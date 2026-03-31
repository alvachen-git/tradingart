import streamlit as st
import pandas as pd
import os
import uuid
import markdown
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import plotly.express as px
import time
import sys
import auth_utils as auth
import extra_streamlit_components as stx
from html import escape
from textwrap import dedent
from ui_components import inject_sidebar_toggle_style

# 1. 环境初始化
load_dotenv(override=True)

st.set_page_config(
    page_title="爱波塔-私密",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)



# 🔥 添加统一的侧边栏导航
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation
with st.sidebar:
    show_navigation()

# 2. 样式注入 (已同步 Home.py 的去白和侧边栏样式)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

    :root {
        --bg-0: #060d1f;
        --bg-1: #0b1730;
        --card: #111f3e;
        --card-soft: rgba(17, 31, 62, 0.72);
        --line: rgba(120, 149, 204, 0.32);
        --text: #ecf3ff;
        --muted: #9fb0cd;
        --cyan: #3cc8ff;
        --green: #2ecb88;
    }

    .stApp {
        background:
            radial-gradient(1200px 600px at 72% -10%, rgba(51, 108, 201, 0.32), transparent 62%),
            radial-gradient(900px 500px at 10% 0%, rgba(24, 154, 127, 0.18), transparent 58%),
            linear-gradient(150deg, var(--bg-0), var(--bg-1));
        color: var(--text);
        font-family: "Rajdhani", "Noto Sans SC", sans-serif;
    }

    [data-testid="stMainBlockContainer"] {
        max-width: 95rem !important;
        padding-top: 0.9rem;
        padding-bottom: 1.5rem;
    }

    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    [data-testid="stDecoration"] {
        display: none;
    }

    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
        border-right: 1px solid #1e293b;
    }
    [data-testid="stSidebar"] p,
    [data-testid="stSidebar"] span,
    [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }

    h1, h2, h3, h4, p, label, .stCaption {
        color: var(--text) !important;
    }

    .hero-shell {
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 18px 14px;
        background: linear-gradient(120deg, rgba(12, 26, 54, 0.92), rgba(10, 22, 46, 0.78));
        box-shadow: 0 16px 40px rgba(0, 0, 0, 0.32), inset 0 1px 0 rgba(255, 255, 255, 0.04);
        margin-bottom: 12px;
        position: relative;
        overflow: hidden;
        animation: heroOpen 850ms cubic-bezier(.22,.9,.28,1) both;
    }
    .hero-shell::after {
        content: "";
        position: absolute;
        top: 0;
        left: -38%;
        width: 32%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(108, 171, 255, 0.18), transparent);
        transform: skewX(-16deg);
        animation: scanSweep 6.2s ease-in-out infinite;
    }
    .hero-title {
        font-size: clamp(28px, 3.8vw, 42px);
        line-height: 1.06;
        font-weight: 700;
        margin: 0;
        letter-spacing: 0.02em;
    }
    .hero-sub {
        margin-top: 6px;
        color: var(--muted);
        font-size: 14px;
    }
    .hero-kpi-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 10px;
        margin-top: 12px;
    }
    .hero-kpi-card {
        border: 1px solid var(--line);
        border-radius: 12px;
        background: var(--card-soft);
        padding: 10px 12px;
    }
    .hero-kpi-label {
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--muted);
        margin-bottom: 4px;
    }
    .hero-kpi-value {
        font-size: clamp(20px, 2.1vw, 30px);
        line-height: 1.1;
        color: #eff6ff;
        font-family: "IBM Plex Mono", monospace;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    .hero-kpi-value.phone {
        color: var(--cyan);
    }
    .hero-kpi-sub {
        margin-top: 3px;
        color: #8fa7ce;
        font-size: 12px;
        font-family: "IBM Plex Mono", monospace;
    }

    .section-divider {
        margin: 12px 0 14px 0;
        border: 0;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(124, 157, 212, 0.5), transparent);
    }
    .panel-title {
        margin: 6px 0 6px 0;
        font-size: clamp(21px, 2.0vw, 30px);
        letter-spacing: 0.03em;
        font-weight: 700;
        color: #ecf3ff;
    }
    .panel-sub {
        color: var(--muted);
        margin-bottom: 8px;
        font-size: 14px;
    }

    .streamlit-expanderHeader {
        background: rgba(13, 28, 58, 0.86) !important;
        border: 1px solid var(--line) !important;
        border-radius: 10px !important;
        color: #dce8ff !important;
        font-weight: 700 !important;
    }
    .streamlit-expanderContent {
        background: rgba(9, 21, 43, 0.76) !important;
        border: 1px solid var(--line) !important;
        border-top: none !important;
        border-bottom-left-radius: 10px !important;
        border-bottom-right-radius: 10px !important;
    }

    div[data-testid="stExpander"] {
        border-radius: 12px;
        background: rgba(9, 19, 39, 0.42);
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.03);
    }

    .stTextInput label, .stNumberInput label, .stSelectbox label {
        color: var(--text) !important;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        margin-bottom: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border: 1px solid var(--line);
        border-radius: 10px;
        background: rgba(10, 22, 46, 0.68);
        color: #c9d9f5;
        padding: 8px 14px;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(120deg, rgba(30, 64, 124, 0.68), rgba(16, 88, 135, 0.52));
        border-color: rgba(108, 171, 255, 0.65);
        color: #f4f8ff;
    }
    .stButton > button {
        border: 1px solid var(--line) !important;
        border-radius: 10px !important;
        background: rgba(12, 28, 58, 0.74) !important;
        color: #e8f1ff !important;
    }
    .stButton > button:hover {
        border-color: rgba(108, 171, 255, 0.7) !important;
        background: linear-gradient(120deg, rgba(22, 52, 103, 0.9), rgba(17, 73, 110, 0.72)) !important;
        color: #ffffff !important;
    }

    .achv-wrap {
        background: linear-gradient(145deg, rgba(11, 28, 58, 0.88), rgba(9, 22, 47, 0.72));
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 16px;
        margin: 10px 0 18px;
        box-shadow: 0 14px 32px rgba(0, 0, 0, 0.24);
    }
    .achv-head {
        display: flex;
        justify-content: space-between;
        align-items: baseline;
        margin-bottom: 12px;
    }
    .achv-title {
        font-size: 18px;
        font-weight: 700;
        color: #eff6ff;
    }
    .achv-progress {
        font-size: 13px;
        color: #9ac6ff;
    }
    .achv-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 10px;
    }
    .achv-badge {
        border: 1px solid var(--line);
        border-radius: 12px;
        padding: 10px;
        min-height: 100px;
        background: rgba(12, 28, 58, 0.62);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .achv-badge:hover {
        transform: translateY(-2px);
        border-color: rgba(108, 171, 255, 0.62);
    }
    .achv-badge.locked {
        opacity: 0.45;
        filter: grayscale(1);
    }
    .achv-icon {
        font-size: 20px;
        margin-bottom: 6px;
    }
    .achv-name {
        color: #e2e8f0;
        font-size: 14px;
        font-weight: 700;
        line-height: 1.25;
        margin-bottom: 4px;
    }
    .achv-desc {
        color: #94a3b8;
        font-size: 12px;
        line-height: 1.4;
    }
    .achv-time {
        margin-top: 6px;
        color: #60a5fa;
        font-size: 11px;
    }

    @keyframes heroOpen {
        from { opacity: 0; transform: translateY(10px); }
        to { opacity: 1; transform: translateY(0); }
    }
    @keyframes scanSweep {
        0% { left: -40%; }
        48% { left: 110%; }
        100% { left: 110%; }
    }

    @media (max-width: 1024px) {
        .achv-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 768px) {
        .achv-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
</style>
""", unsafe_allow_html=True)

# 统一侧边栏折叠/展开箭头样式（与 Home 页一致）
inject_sidebar_toggle_style(mode="high_contrast")

# 3. 引入依赖
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from memory_utils import get_vector_store
except ImportError:
    st.error("❌ 找不到 memory_utils.py，请确保文件在项目根目录下。")


    def get_vector_store():
        return None

# 导入邮箱和认证工具
try:
    from email_utils import send_bind_email_code, verify_bind_email_code, send_reset_password_code
    from auth_utils import get_masked_email, bind_email, change_password_with_old, reset_password_with_email

    EMAIL_ENABLED = True
except ImportError:
    EMAIL_ENABLED = False
    print("⚠️ 邮箱功能模块未找到")


# --- 分享函数 ---
def native_share_button(user_content, ai_content, key):
    unique_id = str(uuid.uuid4())[:8]
    container_id = f"share-container-{unique_id}"
    btn_id = f"btn-{unique_id}"

    # Markdown 轉 HTML
    html_content = markdown.markdown(
        ai_content,
        extensions=['tables', 'fenced_code', 'nl2br']
    )

    # 構建精美的分享卡片 HTML
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
            #{container_id} strong {{ color: #FFD700; }}
        </style>

        <div style="display: flex; align-items: center; margin-bottom: 20px; border-bottom: 1px solid rgba(255,255,255,0.1); padding-bottom: 15px;">
            <div style="font-size: 24px; margin-right: 10px;">🧠</div>
            <div>
                <div style="font-weight: 900; font-size: 16px; color: #fff;">愛波塔 - 交易記憶碎片</div>
                <div style="font-size: 11px; color: #94a3b8;">AI 深度復盤記錄</div>
            </div>
        </div>

        <div style="
            background: rgba(255,255,255,0.08); 
            border-left: 4px solid #3b82f6; 
            padding: 12px; 
            border-radius: 6px; 
            margin-bottom: 20px;
        ">
            <div style="font-size: 12px; color: #94a3b8; margin-bottom: 4px; font-weight:bold;">👤 當時你問:</div>
            <div style="font-size: 14px; color: #fff; font-weight: 500;">{user_content}</div>
        </div>

        <div style="margin-bottom: 20px;">
            <div style="font-size: 12px; color: #10b981; margin-bottom: 6px; font-weight:bold;">🤖 AI 回憶:</div>
            <div style="font-size: 13px; color: #cbd5e1;">{html_content}</div>
        </div>

        <div style="
            display: flex; justify-content: space-between; align-items: center;
            border-top: 1px dashed rgba(255,255,255,0.1); padding-top: 10px; margin-top: 15px;
        ">
            <div style="font-size: 11px; color: #64748b;">Generated by 愛波塔</div>
            <div style="font-size: 11px; color: #3b82f6;">www.aiprota.com</div>
        </div>
    </div>
    """

    # JS 邏輯：截圖並調用原生分享
    html_code = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <style>
        .share-btn {{
            background-color: transparent; border: 1px solid #4B5563; color: #9CA3AF;
            padding: 4px 10px; border-radius: 15px; font-size: 11px; cursor: pointer;
            display: inline-flex; align-items: center; transition: all 0.2s;
            margin-top: 10px;
        }}
        .share-btn:hover {{ background-color: #3b82f6; color: white; border-color: #3b82f6; }}
    </style>
    </head>
    <body>
        {styled_html}
        <button class="share-btn" id="{btn_id}" onclick="generateAndShare()">
            <i class="fas fa-share-alt" style="margin-right:5px;"></i> 分享此记忆
        </button>
        <script>
        function generateAndShare() {{
            const btn = document.getElementById('{btn_id}');
            const originalText = btn.innerHTML;
            const target = document.getElementById('{container_id}');
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 生成中...';

            html2canvas(target, {{ backgroundColor: null, scale: 2, logging: false, useCORS: true }}).then(canvas => {{
                canvas.toBlob(function(blob) {{
                    const file = new File([blob], "memory_card.png", {{ type: "image/png" }});
                    if (navigator.canShare && navigator.canShare({{ files: [file] }})) {{
                        navigator.share({{ files: [file], title: '愛波塔記憶卡片' }}).then(() => resetBtn(btn, originalText)).catch(() => resetBtn(btn, originalText));
                    }} else {{
                        alert("您的瀏覽器不支持直接分享，請截圖保存。");
                        resetBtn(btn, originalText);
                    }}
                }}, 'image/png');
            }});
        }}
        function resetBtn(btn, text) {{ btn.innerHTML = text; }}
        </script>
    </body>
    </html>
    """
    components.html(html_code, height=45)


# 4. 数据库连接
def get_db_engine():
    try:
        db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
        return create_engine(db_url)
    except:
        return None


# 5. 获取用户基本信息
def get_user_stats(user_id):
    engine = get_db_engine()
    default_stats = {
        "level": 1,
        "experience": 0,
        "capital": 0,
        "created_at": None,
        "phone": None,
    }

    if not engine:
        return default_stats

    try:
        sql = text("SELECT level, experience, capital, created_at, phone FROM users WHERE username = :user")
        with engine.connect() as conn:
            result = conn.execute(sql, {'user': user_id}).mappings().fetchone()

            if result:
                return dict(result)
            return default_stats
    except Exception:
        return default_stats


def mask_phone_number(phone):
    digits = "".join(ch for ch in str(phone or "") if ch.isdigit())
    if not digits:
        return "未绑定"

    if digits.startswith("86") and len(digits) >= 13:
        digits = digits[-11:]

    if len(digits) >= 7:
        return f"{digits[:3]}****{digits[-4:]}"

    return "***"

# 6. 从向量库读取回忆
def get_memory_fragments(user_id):
    try:
        vector_store = get_vector_store()
        if not vector_store: return pd.DataFrame()

        results = vector_store._collection.get(
            where={"user_id": str(user_id)},
            include=["metadatas", "documents"]
        )

        data = []
        if results and results['documents']:
            for doc, meta in zip(results['documents'], results['metadatas']):
                timestamp = meta.get('timestamp', '未知时间')
                data.append({
                    "content": doc,
                    "create_time": timestamp,
                    "type": "memory_block"
                })

        df = pd.DataFrame(data)
        if not df.empty and 'create_time' in df.columns:
            df = df.sort_values('create_time', ascending=False)

        return df

    except Exception as e:
        return pd.DataFrame()


def get_achievement_catalog():
    try:
        from kline_game import ACHIEVEMENTS
        return ACHIEVEMENTS
    except Exception:
        return {}


def get_user_achievements(user_id):
    engine = get_db_engine()
    if not engine:
        return []
    try:
        with engine.connect() as conn:
            sql = text("""
                SELECT achievement_code, achievement_name, unlocked_at, exp_reward
                FROM kline_game_achievements
                WHERE user_id = :uid
                ORDER BY unlocked_at DESC
            """)
            rows = conn.execute(sql, {"uid": user_id}).mappings().fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def render_achievement_badges(user_id):
    # 进入个人资料页时做一次成就补齐，避免历史局因旧版本逻辑漏发
    try:
        from kline_game import check_achievements
        check_achievements(
            user_id=user_id,
            game_profit=0,
            profit_rate=0,
            trade_count=0,
            max_drawdown=0,
            leverage=1,
        )
    except Exception:
        pass

    catalog = get_achievement_catalog() or {}
    unlocked_rows = get_user_achievements(user_id)

    if not catalog:
        st.info("成就系统资料暂不可用")
        return

    unlocked_map = {str(r.get("achievement_code")): r for r in unlocked_rows}
    total = len(catalog)
    done = len(unlocked_map)

    icon_map = {
        "dual_leverage": "⚖️", "games_20": "📚",
        "profit_100k": "💯", "profit_500k": "💸", "rate_50": "📈",
        "loss_100k": "⚠️", "loss_500k": "🧯",
        "no_drawdown_win": "🛡️", "streak_5": "🔥", "streak_10": "🏆",
        "trader_20": "⚡", "zen_1_trade_win": "🎯",
        "lev10_win": "🧠",
        "gross_profit_100k": "🥉", "gross_profit_1m": "🥈", "gross_profit_10m": "🥇",
        "gross_loss_500k": "🌊",
    }

    cards_html = []
    for code, meta in catalog.items():
        unlocked = code in unlocked_map
        row = unlocked_map.get(code, {})
        unlocked_at = row.get("unlocked_at")
        unlocked_text = "<div class='achv-time' style='color:#64748b;'>未解锁</div>"
        if unlocked and unlocked_at:
            unlocked_text = f"<div class='achv-time'>解锁：{escape(str(unlocked_at))[:16]}</div>"
        elif unlocked:
            unlocked_text = "<div class='achv-time'>已解锁</div>"

        cards_html.append(
            dedent(
                f"""
                <div class="achv-badge {'unlocked' if unlocked else 'locked'}">
                    <div class="achv-icon">{icon_map.get(code, '🏅')}</div>
                    <div class="achv-name">{escape(meta.get('name', code))}</div>
                    {unlocked_text}
                </div>
                """
            ).strip()
        )

    html = dedent(
        f"""
        <div class="achv-wrap">
            <div class="achv-head">
                <div class="achv-title">🏅 成就徽章</div>
                <div class="achv-progress">已解锁 {done}/{total}</div>
            </div>
            <div class="achv-grid">{''.join(cards_html)}</div>
        </div>
        """
    ).strip()

    st.markdown(html, unsafe_allow_html=True)


def get_achievement_progress(user_id):
    catalog = get_achievement_catalog() or {}
    unlocked_rows = get_user_achievements(user_id)
    return len(unlocked_rows), len(catalog)


# ================= 页面主逻辑 =================

# 登录态恢复（处理 Streamlit 会话超时）
if "is_logged_in" not in st.session_state:
    st.session_state.is_logged_in = False
    st.session_state.user_id = None
    st.session_state.token = None
if "profile_cookie_retry_once" not in st.session_state:
    st.session_state.profile_cookie_retry_once = False

cookie_manager = stx.CookieManager(key="profile_cookie_manager")
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
        st.session_state.profile_cookie_retry_once = False
    elif restore_state in ("empty", "partial", "error") and not st.session_state.get("profile_cookie_retry_once", False):
        st.session_state.profile_cookie_retry_once = True
        time.sleep(0.15)
        st.rerun()
    elif restore_state == "invalid":
        try:
            cookie_manager.delete("username", key="profile_del_user")
            cookie_manager.delete("token", key="profile_del_token")
        except:
            pass

if st.session_state.get("just_logged_out", False):
    st.session_state.just_logged_out = False

# 1. 权限检查
if not st.session_state.get('is_logged_in', False):
    st.warning("🔒 请先在首页登录后查看个人资料")
    st.stop()

username = st.session_state.get('user_id', 'Unknown')

# 获取数据
user_data = get_user_stats(username)
memory_df = get_memory_fragments(username)

# 2. 顶部：个人信息区（高科技风）
level = int(user_data.get('level', 1) or 1)
exp = int(user_data.get('experience', 0) or 0)
exp_pct = min(max(exp / 1000, 0), 1.0)
phone_display = mask_phone_number(user_data.get('phone'))

created_at = user_data.get('created_at')
join_date = str(created_at)[:10] if created_at else "未知"
capital = float(user_data.get('capital', 0) or 0)

hero_html = dedent(
    f"""
    <div class="hero-shell">
        <h1 class="hero-title">👤 交易员档案 · {escape(str(username))}</h1>
        <div class="hero-kpi-grid">
            <div class="hero-kpi-card">
                <div class="hero-kpi-label">等级</div>
                <div class="hero-kpi-value">LV.{level}</div>
                <div class="hero-kpi-sub">成长目标: LV.{max(level + 1, 2)}</div>
            </div>
            <div class="hero-kpi-card">
                <div class="hero-kpi-label">绑定手机号</div>
                <div class="hero-kpi-value phone">{escape(phone_display)}</div>
                <div class="hero-kpi-sub">仅显示脱敏信息</div>
            </div>
            <div class="hero-kpi-card">
                <div class="hero-kpi-label">爱波币</div>
                <div class="hero-kpi-value">¥ {capital:,.0f}</div>
                <div class="hero-kpi-sub">K线游戏资金</div>
            </div>
            <div class="hero-kpi-card">
                <div class="hero-kpi-label">注册时间</div>
                <div class="hero-kpi-value">{escape(join_date)}</div>
                <div class="hero-kpi-sub">账号创建日期</div>
            </div>
        </div>
    </div>
    """
).strip()
st.markdown(hero_html, unsafe_allow_html=True)

st.progress(exp_pct, text=f"EXP: {exp}/1000")
st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)
done_count, total_count = get_achievement_progress(username)
with st.expander(f"🏅 成就徽章（已解锁 {done_count}/{total_count}，点击展开）", expanded=False):
    render_achievement_badges(username)
st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)

# ============================================
# 账号安全设置（紧凑折叠版）
# ============================================
st.markdown('<div class="panel-title">账号安全中心</div>', unsafe_allow_html=True)
st.markdown('<div class="panel-sub">邮箱绑定与密码管理</div>', unsafe_allow_html=True)
if EMAIL_ENABLED:
    masked_email = get_masked_email(username)

    # 显示邮箱状态的简短文字
    email_status = f"📧 {masked_email}" if masked_email else "📧 未绑定邮箱"

    with st.expander(f"⚙️ 账号设置 | {email_status}", expanded=False):
        tab_email, tab_pwd = st.tabs(["绑定邮箱", "修改密码"])

        # ============ Tab1: 邮箱绑定 ============
        with tab_email:
            if masked_email:
                st.success(f"✅ 已绑定：{masked_email}")
                st.caption("如需换绑，请输入新邮箱")
            else:
                st.warning("⚠️ 未绑定邮箱，建议绑定以便找回密码")

            col1, col2 = st.columns([3, 1])
            with col1:
                bind_email_input = st.text_input("邮箱", placeholder="your@email.com", key="bind_email",
                                                 label_visibility="collapsed")
            with col2:
                if st.button("发送验证码", key="btn_send_bind", use_container_width=True):
                    if bind_email_input:
                        success, msg = send_bind_email_code(bind_email_input)
                        if success:
                            st.success("已发送")
                        else:
                            st.error(msg)
                    else:
                        st.warning("请输入邮箱")

            col1, col2 = st.columns([3, 1])
            with col1:
                bind_code = st.text_input("验证码", max_chars=6, key="bind_email_code", label_visibility="collapsed",
                                          placeholder="验证码")
            with col2:
                if st.button("绑定", type="primary", key="btn_bind_email", use_container_width=True):
                    if bind_email_input and bind_code:
                        success, msg = bind_email(username, bind_email_input, bind_code)
                        if success:
                            st.success(msg)
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(msg)
                    else:
                        st.warning("请填写完整")

        # ============ Tab2: 修改密码 ============
        with tab_pwd:
            old_pwd = st.text_input("当前密码", type="password", key="old_pwd")
            new_pwd = st.text_input("新密码", type="password", key="new_pwd", placeholder="至少6位")
            new_pwd2 = st.text_input("确认密码", type="password", key="new_pwd2")

            if st.button("确认修改", type="primary", key="btn_change_pwd", use_container_width=True):
                if not old_pwd:
                    st.warning("请输入当前密码")
                elif not new_pwd or len(new_pwd) < 6:
                    st.warning("新密码至少6位")
                elif new_pwd != new_pwd2:
                    st.error("两次密码不一致")
                else:
                    success, msg = change_password_with_old(username, old_pwd, new_pwd)
                    if success:
                        st.success(msg)
                        st.session_state.is_logged_in = False
                        time.sleep(1.5)
                        st.rerun()
                    else:
                        st.error(msg)

# 3. 底部：记忆碎片展示 (折叠版)
st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)
st.markdown('<div class="panel-title">大脑记忆</div>', unsafe_allow_html=True)
st.markdown('<div class="panel-sub">AI 记忆碎片与历史交互回放</div>', unsafe_allow_html=True)

if memory_df.empty:
    st.info("📭 暂无记忆数据。去首页多和 AI 聊聊，它就会记住你了！")
else:
    # A. 可视化活跃度
    if 'create_time' in memory_df.columns:
        try:
            memory_df['date'] = pd.to_datetime(memory_df['create_time']).dt.date
            daily_counts = memory_df['date'].value_counts().sort_index()

            fig = px.bar(x=daily_counts.index, y=daily_counts.values,
                         labels={'x': '', 'y': '记忆条数'},
                         template="plotly_dark", height=200)
            fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                              margin=dict(l=0, r=0, t=10, b=0))
            st.plotly_chart(fig, use_container_width=True)
        except:
            pass

    st.divider()

    # B. 遍历显示记忆卡片 (折叠样式)
    for index, row in memory_df.iterrows():
        raw_text = row['content']
        time_str = row['create_time']

        # 解析文本：尝试提取"用户问"作为标题
        q_preview = "无标题记忆"
        q_full = raw_text
        a_full = ""

        if "用户问:" in raw_text:
            try:
                parts = raw_text.split('AI回答:', 1)
                q_part = parts[0]
                if "用户问:" in q_part:
                    q_part = q_part.split('用户问:', 1)[1].strip()

                # 标题只取前30个字
                q_preview = q_part[:30] + "..." if len(q_part) > 30 else q_part
                q_full = q_part

                if len(parts) > 1:
                    a_full = parts[1].strip()
            except:
                pass

        expander_title = f"📅 {time_str} | 🗣️ {q_preview}"

        with st.expander(expander_title, expanded=False):
            st.markdown(f"**👤 用户提问:**\n\n{q_full}")

            st.markdown('<hr class="section-divider" />', unsafe_allow_html=True)

            st.markdown("🤖 **AI 回答:**")
            if a_full:
                import re as _re
                # 提取【回答片段】后面的内容，去掉内存标记噪声
                if "【回答片段】" in a_full:
                    a_full = a_full.split("【回答片段】", 1)[1].strip()
                elif "【结构化摘要】" in a_full:
                    a_full = a_full.split("【结构化摘要】", 1)[-1].strip()
                # 清除可能残留的 HTML 标签，避免 </div> 等裸露显示
                clean_answer = _re.sub(r'<[^>]+>', '', a_full).strip()
                st.markdown(clean_answer)
            else:
                st.caption("(未解析到回答内容)")
            native_share_button(q_full, a_full, key=f"share_mem_{index}")

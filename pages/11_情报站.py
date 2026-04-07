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


def is_mobile():
    """检测是否为移动设备访问。"""
    try:
        user_agent = st.context.headers.get("User-Agent", "")
        mobile_keywords = ["Mobile", "Android", "iPhone", "iPad", "Windows Phone"]
        return any(keyword in user_agent for keyword in mobile_keywords)
    except Exception:
        return False


# ==========================================
# 椤甸潰閰嶇疆
# ==========================================
st.set_page_config(
    page_title="情报站 - 爱波塔",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)



# 馃敟 娣诲姞缁熶竴鐨勪晶杈规爮瀵艰埅
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from sidebar_navigation import show_navigation

# ==========================================
# 馃敡 鑷姪璁㈤槄棰戦亾鐧藉悕鍗曪紙榛樿鍏抽棴锛涢€氳繃鐜鍙橀噺鐏板害寮€鍚級
# FREE_SELF_SUBSCRIBE_CHANNEL_CODES 绀轰緥: daily_report,expiry_option_radar
# ==========================================
FREE_SELF_SUBSCRIBE_CHANNEL_CODES = {
    item.strip().lower()
    for item in str(os.getenv("FREE_SELF_SUBSCRIBE_CHANNEL_CODES", "")).split(",")
    if item.strip()
}
FORCE_PAID_CHANNEL_CODES = {
    "daily_report",
    "expiry_option_radar",
    "broker_position_report",
    "fund_flow_report",
}
EFFECTIVE_FREE_CHANNEL_CODES = FREE_SELF_SUBSCRIBE_CHANNEL_CODES - FORCE_PAID_CHANNEL_CODES
with st.sidebar:
    show_navigation()

# ==========================================
# 馃帹 楂樼骇鏍峰紡娉ㄥ叆
# ==========================================
st.markdown("""
<style>
    /* ========== 鍏ㄥ眬鑳屾櫙 ========== */
    .stApp {
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
    }

    /* 鎷撳涓诲唴瀹瑰尯 */
    [data-testid="stMainBlockContainer"] {
        max-width: 88rem !important;
        padding-top: 0.8rem !important;
        padding-bottom: 1.2rem !important;
        padding-left: 1.2rem;
        padding-right: 1.2rem;
    }

    /* 鎯呮姤绔欐爣棰橀珮瀵规瘮淇 */
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

    /* 闅愯棌椤堕儴瑁呴グ */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    [data-testid="stDecoration"] {
        display: none;
    }

    /* ========== 椤甸潰鏍囬鏍峰紡 ========== */
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

    /* ========== 棰戦亾鍗＄墖缃戞牸 ========== */
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

    /* 璁㈤槄鐘舵€佸窘绔?*/
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

    /* ========== 鍐呭鍖哄煙 ========== */
    .section-header {
        display: flex;
        align-items: center;
        gap: 12px;
        margin-bottom: 20px;
        padding-bottom: 12px;
        border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .section-title {
        /* 馃敟 淇敼鐐癸細鏀逛负浜櫧鑹诧紝骞跺姞绮?*/
        color: #f1f5f9 !important; 
        font-size: 22px;  /* 瀛楀彿鍔犲ぇ涓€鐐?*/
        font-weight: 700; /* 鍔犵矖 */
        margin: 0;
        /* 澧炲姞鏂囧瓧闃村奖锛岃瀹冧粠鑳屾櫙涓诞鍑烘潵 */
        text-shadow: 0 2px 4px rgba(0,0,0,0.6); 
        letter-spacing: 1px; /* 澧炲姞瀛楅棿璺?*/
    }
    .section-icon {
        width: 5px; /* 绋嶅井鍔犲瑁呴グ鏉?*/
        height: 24px;
        background: linear-gradient(180deg, #fbbf24, #f59e0b);
        border-radius: 2px;
        box-shadow: 0 0 10px rgba(251, 191, 36, 0.5); /* 璁╂梺杈圭殑閲戞潯鍙戝厜 */
    }

    /* ========== 鍐呭鍗＄墖 ========== */
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

    /* ========== 閿佸畾閬僵 ========== */
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

    /* ========== 娑堟伅閫氱煡鎸夐挳 ========== */
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

    /* ========== 鏃ユ湡鍒嗛殧绾?========== */
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

    /* ========== 绌虹姸鎬?========== */
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

    /* ========== 绌虹姸鎬?========== */
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

    /* 馃敟馃敟馃敟 [鏂板] 寮哄埗渚ц竟鏍忚儗鏅笌 Home.py 涓€鑷?馃敟馃敟馃敟 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important; /* 娣辫摑榛戣儗鏅?*/
        border-right: 1px solid #1e293b;      /* 鍙€夛細鍙充晶鍒嗗壊绾?*/
    }

    /* 馃敟馃敟馃敟 [鏂板] 寮哄埗渚ц竟鏍忔枃瀛楀彉鐧?鐏?(瑕嗙洊榛樿榛戣壊) 馃敟馃敟馃敟 */
    [data-testid="stSidebar"] p, 
    [data-testid="stSidebar"] span, 
    [data-testid="stSidebar"] div,
    [data-testid="stSidebar"] label {
        color: #cbd5e1 !important;
    }

    /* ========== 渚ц竟鏍忕粺涓€鏍峰紡 ========== */
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

    /* 瀹㈡湇鍗＄墖 */
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

    /* ========== 鎸夐挳鏍峰紡瑕嗙洊 ========== */
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
    /* 1. 闅愯棌鍘熺敓涓戦檵鐨勪笁瑙掑舰 */
    details > summary {
        list-style: none;
        cursor: pointer;
        outline: none;
    }
    details > summary::-webkit-details-marker {
        display: none;
    }

    /* 2. 瀹氫箟 Summary (涔熷氨鏄爣棰樻爮) 鐨勫竷灞€ */
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

    /* 3. 瀹氫箟灞曞紑鍚庣殑鍐呭瀹瑰櫒 */
    .native-content {
        padding: 0 20px 20px 20px;
        margin-top: -10px; /* 璁╁唴瀹圭揣璐存爣棰?*/
        border-top: 1px solid rgba(255,255,255,0.06);
        padding-top: 20px;
        /* 馃敟 鍏抽敭锛氭坊鍔犺繘鍏ュ姩鐢?*/
        animation: slideDown 0.3s ease-out forwards;
    }

    /* 4. 瀹氫箟鎸夐挳鏂囧瓧鍙樺寲 (绾疌SS瀹炵幇 灞曞紑/鏀惰捣 鍒囨崲) */
    .toggle-text::after {
        content: "灞曞紑";
        display: inline-block;
        padding: 4px 12px;
        background: rgba(255, 255, 255, 0.1);
        border-radius: 6px;
        color: #e2e8f0;
        font-size: 13px;
        transition: all 0.2s;
    }

    /* 褰?details 澶勪簬 open 鐘舵€佹椂锛屾枃瀛楀彉鏇翠负 鏀惰捣 */
    details[open] .toggle-text::after {
        content: "鏀惰捣";
        background: rgba(251, 191, 36, 0.2);
        color: #fbbf24;
    }

    /* 5. 鍔ㄧ敾鍏抽敭甯?*/
    @keyframes slideDown {
        from { opacity: 0; transform: translateY(-10px); }
        to { opacity: 1; transform: translateY(0); }
    }

    /* 6. 瀹瑰櫒杈规 (鏇夸唬鍘熸潵鐨?col 甯冨眬) */
    .native-card-container {
        border-bottom: 1px solid rgba(255,255,255,0.1);
        margin-bottom: 16px;
    }

    /* =============================================
       馃敟 [淇] 鎶樺彔妗?(Expander) 鏍囬楂樹寒鏍峰紡
       ============================================= */

    /* 1. 寮哄埗鎶樺彔妗嗘爣棰樻爮 (summary) 鐨勬牱寮?*/
    [data-testid="stExpander"] details > summary {
        color: #fbbf24 !important;      /* 寮哄埗浜噾鑹插瓧浣?*/
        font-size: 12px;     /* 瀛楀彿閫備腑 */
        font-weight: 400 !important;    /* 鍔犵矖 */
        background-color: rgba(255, 255, 255, 0.05) !important; /* 寰井鍙戜寒鐨勮儗鏅?*/
        border: 1px solid rgba(255, 255, 255, 0.1) !important;  /* 鍔犱笂杈规鏇村儚涓寜閽?*/
        border-radius: 8px !important;
        padding: 10px 15px !important;  /* 澧炲姞鐐瑰嚮鍖哄煙 */
        transition: all 0.3s ease !important;
    }

    /* 2. 淇鎶樺彔妗嗗唴閮ㄧ殑鏂囧瓧鍏冪礌 (闃叉琚?span/p 瑕嗙洊) */
    [data-testid="stExpander"] details > summary p,
    [data-testid="stExpander"] details > summary span {
        color: #fbbf24 !important;
        font-weight: 600 !important;
    }

    /* 3. 寮哄埗绠ご鍥炬爣鍙樿壊 */
    [data-testid="stExpander"] details > summary svg {
        fill: #fbbf24 !important;       /* 绠ご鍙樻垚閲戣壊 */
        color: #fbbf24 !important;
    }

    /* 4. 榧犳爣鎮仠鏃剁殑浜や簰鏁堟灉 */
    [data-testid="stExpander"] details > summary:hover {
        background-color: rgba(251, 191, 36, 0.15) !important; /* 鎮仠鑳屾櫙鍙樹寒 */
        border-color: #fbbf24 !important;                      /* 杈规鍙樹寒 */
    }

    [data-testid="stExpander"] details > summary:hover p,
    [data-testid="stExpander"] details > summary:hover span {
        color: #ffffff !important;      /* 鎮仠鏃舵枃瀛楀彉鐧斤紝鎻愮ず鍙偣鍑?*/
    }

    [data-testid="stExpander"] details > summary:hover svg {
        fill: #ffffff !important;       /* 鎮仠鏃剁澶村彉鐧?*/
    }
</style>
""", unsafe_allow_html=True)
inject_sidebar_toggle_style(mode="high_contrast")
inject_quant_ops_header_style()

# ==========================================
# 鍒濆鍖?Session State
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
    杩斿洖:
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
# 杈呭姪鍑芥暟
# ==========================================
def get_current_user():
    """鑾峰彇褰撳墠鐧诲綍鐢ㄦ埛"""
    if st.session_state.get('is_logged_in') and st.session_state.get('user_id'):
        return st.session_state['user_id']
    return None


def format_time(dt: datetime) -> str:
    """格式化时间显示（北京时间）"""
    if not dt:
        return ""
    now = datetime.now(BEIJING_TZ)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc).astimezone(BEIJING_TZ)
    delta = now - dt

    if delta.days == 0:
        if delta.seconds < 3600:
            mins = delta.seconds // 60
            return f"{mins} 分钟前" if mins > 0 else "刚刚"
        return f"{delta.seconds // 3600} 小时前"
    if delta.days == 1:
        return "昨天 " + dt.strftime("%H:%M")
    if delta.days < 7:
        return f"{delta.days} 天前"
    return dt.strftime("%m-%d %H:%M")


def toggle_expand(content_id: int):
    """切换内容展开/收起"""
    if content_id in st.session_state.expanded_content:
        st.session_state.expanded_content.remove(content_id)
    else:
        st.session_state.expanded_content.add(content_id)


# ==========================================
# 鑾峰彇鐢ㄦ埛淇℃伅
# ==========================================
user = get_current_user()

# ==========================================
# 馃幆 渚ц竟鏍忥紙缁熶竴鏆楄壊椋庢牸锛屽弬鑰僅ome.py锛?
# ==========================================
with st.sidebar:
    # 鐢ㄦ埛鐘舵€?- 浣跨敤鏆楄壊鍗＄墖鑰岄潪 st.success
    if user:
        user_card_html = (
            '<div style="background-color:#1E2329;border:1px solid #31333F;'
            'border-radius:8px;padding:12px 15px;margin-bottom:12px;">'
            f'<span style="color:#22c55e;font-weight:600;">欢迎回来，{user}</span>'
            '</div>'
        )
        st.markdown(user_card_html, unsafe_allow_html=True)

        # 用户订阅信息
        user_subs = sub_svc.get_user_subscriptions(user)

        # 绠＄悊璁㈤槄
        with st.expander("⚙️ 管理订阅", expanded=False):
            channels = sub_svc.get_all_channels()
            user_sub_map = {s['channel_id']: s for s in user_subs}

            for channel in channels:
                sub_info = user_sub_map.get(channel['id'])
                # 鍒ゆ柇褰撳墠鐢ㄦ埛鏄惁鎷ユ湁璇ラ閬撶殑鏈夋晥璁㈤槄
                is_subscribed = sub_info and sub_info.get('is_active')

                col1, col2 = st.columns([2.5, 1.2])  # 璋冩暣涓€涓嬪垪瀹芥瘮渚?

                with col1:
                    st.markdown(f"**{channel['icon']} {channel['name']}**")
                    if is_subscribed:
                        # 显示到期时间
                        expire_text = sub_svc.format_expire_time(sub_info['expire_at'])
                        st.caption(f"✅ {expire_text}")
                    elif str(channel.get('code', '')).lower() in EFFECTIVE_FREE_CHANNEL_CODES:
                        st.caption("🆓 免费")
                    else:
                        st.caption("🔒 未订阅")

                with col2:
                    # 场景 1: 白名单频道允许自助订阅/退订
                    if str(channel.get('code', '')).lower() in EFFECTIVE_FREE_CHANNEL_CODES:
                        if is_subscribed:
                            if st.button("退订", key=f"unsub_{channel['id']}", type="secondary",
                                         use_container_width=True):
                                if sub_svc.cancel_subscription(user, channel['id']):
                                    st.success("已退订")
                                    st.rerun()
                        else:
                            if st.button("订阅", key=f"sub_{channel['id']}", type="primary", use_container_width=True):
                                # 默认订阅 100 天
                                success, msg = sub_svc.add_subscription(
                                    user,
                                    channel['id'],
                                    days=100,
                                    source_type="self_subscribe_whitelist",
                                    source_ref=f"streamlit:intel_sidebar:{str(channel.get('code', '')).lower()}",
                                    source_note="intel_sidebar_free_subscribe",
                                    operator="user_self_service",
                                )
                                if success:
                                    st.balloons()
                                    st.rerun()
                                else:
                                    st.error(msg)

                    # 场景 2: 其他付费频道引导到充值中心
                    elif channel['is_premium'] and not is_subscribed:
                        if st.button("开通", key=f"side_sub_{channel['id']}", type="secondary",
                                     use_container_width=True):
                            st.toast("请点击下方“付费开通”完成购买", icon="💳")
        st.page_link("pages/17_充值中心.py", label="💳 付费开通", use_container_width=True)

    st.markdown("---")

    # 客服卡片
    contact_html = (
        '<div class="contact-card">'
        '<div class="contact-title">客服联系</div>'
        '<div class="contact-item">微信：<span class="wechat-highlight">trader-sec</span></div>'
        '<div class="contact-item">电话：<span class="wechat-highlight">17521591756</span></div>'
        '<div class="contact-item" style="font-size: 12px; margin-top: 8px;">'
        '沪ICP备2021018087号-2'
        '</div>'
        '</div>'
    )
    st.markdown(contact_html, unsafe_allow_html=True)

# ==========================================
# 馃摗 涓诲唴瀹瑰尯
# ==========================================
render_quant_ops_header(
    "情报站",
    "",
    "AI结合市场数据和团队实战经验生成报告",
)

# ==========================================
# 鏈櫥褰曟彁绀?
# ==========================================
if not user:
    login_html = (
        '<div class="locked-card">'
        '<div class="locked-icon">LOCK</div>'
        '<div class="locked-title">请先登录</div>'
        '<div class="locked-desc">登录后即可查看订阅内容、管理订阅和接收消息通知</div>'
        '</div>'
    )
    st.markdown(login_html, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.page_link("Home.py", label="🏠 返回首页登录", use_container_width=True)
    st.stop()

# ==========================================
# ==========================================
# 馃摵 棰戦亾閫夋嫨
# ==========================================
channels = sub_svc.get_all_channels()
user_subs = sub_svc.get_user_subscriptions(user)
user_sub_map = {s['channel_id']: s for s in user_subs}

# 频道筛选：手机端下拉，桌面端保持原按钮样式
if is_mobile():
    channel_options = [('all', '📋 全部')] + [
        (channel['code'], f"{channel['icon']} {channel['name']}") for channel in channels
    ]
    option_codes = [code for code, _ in channel_options]
    option_labels = {code: label for code, label in channel_options}

    if st.session_state.selected_channel not in option_codes:
        st.session_state.selected_channel = 'all'

    selected_code = st.selectbox(
        "频道筛选",
        options=option_codes,
        index=option_codes.index(st.session_state.selected_channel),
        format_func=lambda code: option_labels.get(code, code),
        key="intel_channel_dropdown",
    )

    if selected_code != st.session_state.selected_channel:
        st.session_state.selected_channel = selected_code
        st.rerun()

    # 当前频道订阅状态（全部不展示）
    if st.session_state.selected_channel != 'all':
        selected_channel = next((ch for ch in channels if ch['code'] == st.session_state.selected_channel), None)
        if selected_channel:
            sub_info = user_sub_map.get(selected_channel['id'])
            channel_code = str(selected_channel.get("code", "")).lower()
            if channel_code in EFFECTIVE_FREE_CHANNEL_CODES:
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

            st.markdown(
                f'<div style="text-align:right;margin-top:-2px;"><span class="status-badge status-{status_class}">{status}</span></div>',
                unsafe_allow_html=True,
            )
else:
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

            channel_code = str(channel.get("code", "")).lower()
            if channel_code in EFFECTIVE_FREE_CHANNEL_CODES:
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

            st.markdown(
                f'<div style="text-align:center;margin-top:-8px;"><span class="status-badge status-{status_class}">{status}</span></div>',
                unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ==========================================
# 馃搫 鍐呭鍒楄〃
# ==========================================
st.markdown(
    '<div class="section-header"><div class="section-icon"></div><h3 class="section-title">📋 最新内容</h3></div>',
    unsafe_allow_html=True)

# 鑾峰彇鍐呭
if st.session_state.selected_channel == 'all':
    contents = sub_svc.get_channel_contents(days=10, limit=30)
else:
    contents = sub_svc.get_channel_contents(channel_code=st.session_state.selected_channel, days=10, limit=20)

if not contents:
    st.markdown("""
    <div class="empty-state">
        <div class="empty-icon">📭</div>
        <div class="empty-text">暂无内容，请稍后再来看</div>
    </div>
    """, unsafe_allow_html=True)
else:
    today = datetime.now(BEIJING_TZ).date()
    yesterday = today - timedelta(days=1)
    current_date = None

    for content in contents:
        # 鏃ユ湡鍒嗛殧锛坧ublish_time 鏄?UTC锛岃浆鍖椾含鏃堕棿鍙栨棩鏈燂級
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

        # 妫€鏌ユ潈闄?
        access = sub_svc.check_subscription_access(user, content['channel_id'])

        if access['has_access']:
            # 鏈夋潈闄?- 鏄剧ず鍐呭鍗＄墖
            pub_time_str = format_time(content['publish_time'])
            summary_text = content['summary'][:120] + "..." if content['summary'] else "暂无摘要"

            # 1. 鍏堟樉绀轰竴涓紓浜殑棰勮鍗＄墖 (Header)
            # 杩欓噷鍙樉绀烘爣棰樸€佹憳瑕佸拰鏃堕棿锛屼笉鏄剧ず澶嶆潅鍐呭锛屾墍浠ョ粷瀵逛笉浼氬穿
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

            # 2. 浣跨敤 Expander 鍖呰９姝ｅ紡鍐呭
            # 娉ㄦ剰锛歴t.expander 鍦ㄦ柊鐗?Streamlit 涓篃鏄鎴风娓叉煋锛岄€熷害寰堝揩
            with st.expander("📋 点击查看完整复盘", expanded=False):
                # 3. 馃敟馃敟馃敟 鏍稿績锛氫娇鐢?components.html 娓叉煋 HTML 馃敟馃敟馃敟
                # height=1000: 缁欎竴涓冻澶熺殑楂樺害
                # scrolling=True: 鍐呭澶暱鍙互婊氬姩
                # 杩欐牱灏辨槸涓€涓嫭绔嬬殑缃戦〉娌欑锛屾棤璁?HTML 澶氬鏉傞兘鑳藉畬缇庢樉绀猴紒
                components.html(content['content'], height=1000, scrolling=True)

                # 馃敟 鏂板锛氬垎浜姛鑳?
                add_share_button(
                    content_title=content['title'],
                    content_summary=content['summary'],
                    content_html=content['content'],
                    channel_icon=content['channel_icon'],
                    pub_time=pub_time_str,
                    content_id=content['id']
                )

            # 鍔犱釜闂磋窛
            st.markdown("<div style='margin-bottom: 20px;'></div>", unsafe_allow_html=True)
        else:
            # 无权限：显示锁定卡片
            reason_map = {
                "not_subscribed": "订阅后即可查看完整内容",
                "expired": f"订阅已于 {access['expire_at'].strftime('%Y-%m-%d') if access['expire_at'] else ''} 过期",
                "subscription_inactive": "订阅状态已失效",
            }
            reason = reason_map.get(access['reason'], "需要订阅后查看")

            locked_html = (
                '<div class="locked-card">'
                '<div class="locked-icon">🔒</div>'
                f"<div class='locked-title'>{content['title']}</div>"
                f"<div class='locked-desc'>{reason}</div>"
                '</div>'
            )
            st.markdown(locked_html, unsafe_allow_html=True)

            col1, col2, col3 = st.columns([1, 2, 1])
            with col2:
                # 馃敟馃敟馃敟 銆愪慨澶嶃€戦拡瀵光€滃鐩樻櫄鎶モ€濆疄鐜扮偣鍑诲嵆璁㈤槄 馃敟馃敟馃敟

                # 妫€鏌ユ槸鍚︽槸澶嶇洏鏅氭姤 (鎴栬€呭叾浠栦綘瀹氫箟鐨勫厤璐归閬?
                # 娉ㄦ剰锛氳繖閲岄渶瑕佺‘淇?content 瀛楀吀閲屾湁 'channel_name' 瀛楁
                current_content_channel_code = str(content.get('channel_code') or "").lower()

                # 2. 鍒ゆ柇鏄惁涓虹櫧鍚嶅崟棰戦亾锛堥粯璁ゅ叧闂級
                is_free_channel = current_content_channel_code in EFFECTIVE_FREE_CHANNEL_CODES

                if is_free_channel:
                    # 场景1：白名单频道，允许直接开通
                    if st.button("🔓 免费订阅", key=f"lock_{content['id']}", type="primary", use_container_width=True):
                        success, msg = sub_svc.add_subscription(
                            user,
                            content['channel_id'],
                            days=100,
                            source_type="self_subscribe_whitelist",
                            source_ref=f"streamlit:intel_locked:{current_content_channel_code}",
                            source_note="intel_locked_card_free_subscribe",
                            operator="user_self_service",
                        )

                        if success:
                            st.balloons()
                            st.toast("✅ 订阅成功，正在刷新页面…", icon="🎉")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"订阅失败: {msg}")

                else:
                    # 场景2：付费频道，跳转充值中心
                    if st.button("💳 付费开通", key=f"lock_{content['id']}", type="primary", use_container_width=True):
                        st.switch_page("pages/17_充值中心.py")



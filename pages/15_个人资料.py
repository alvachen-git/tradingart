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

# 1. 环境初始化
load_dotenv(override=True)

st.set_page_config(
    page_title="爱波塔-私密",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. 样式注入 (已同步 Home.py 的去白和侧边栏样式)
st.markdown("""
<style>
    /* 1. 全局背景 (同步 Home.py 的深空蓝黑渐变) */
    .stApp { 
        background-color: #0b1121 !important;
        background-image: radial-gradient(circle at 50% 0%, #1e293b 0%, #0b1121 70%);
        color: #e2e8f0;
        font-family: 'PingFang SC', sans-serif;
    }

    /* 2. 顶部去白核心代码 (同步 Home.py) */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
    }
    /* 隐藏顶部的彩虹装饰线条 */
    [data-testid="stDecoration"] {
        display: none;
    }
    /* 调整顶部空白区域的高度 */
    .block-container {
        padding-top: 2rem !important; 
    }

    /* 3. 侧边栏样式 (同步 Home.py 的深色) */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
        border-right: 1px solid #1e293b;
    }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }

    /* --- 以下是个人资料页特有的样式 (保持不变) --- */

    /* 卡片容器 */
    .profile-card {
        background-color: #1e293b;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 24px;
        margin-bottom: 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    }

    /* 等级徽章 */
    .level-badge {
        background: linear-gradient(135deg, #fbbf24 0%, #d97706 100%);
        color: #fff;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: bold;
        font-size: 14px;
        margin-left: 10px;
    }

    /* 统计数字 */
    .stat-value {
        font-size: 28px;
        font-weight: 700;
        color: #f8fafc;
        font-family: 'Roboto Mono', monospace;
    }
    .stat-label {
        font-size: 14px;
        color: #94a3b8;
    }

    /* 调整 Expander 样式，让它更有质感 */
    .streamlit-expanderHeader {
        background-color: #1e293b !important;
        border: 1px solid #334155 !important;
        border-radius: 8px !important;
        color: #e2e8f0 !important;
        font-weight: 600 !important;
    }
    .streamlit-expanderContent {
        background-color: #0f172a !important;
        border: 1px solid #334155 !important;
        border-top: none !important;
        border-bottom-left-radius: 8px !important;
        border-bottom-right-radius: 8px !important;
        padding: 15px !important;
    }

    /* 消除 Expander 内部的 Markdown 默认边距 */
    .streamlit-expanderContent p {
        margin-bottom: 0px !important;
    }
    
    /* 输入框标签文字颜色 */
    .stTextInput label, .stNumberInput label, .stSelectbox label {
        color: #e2e8f0 !important;
    }

    /* 邮箱显示样式 */
    .email-bound {
        background: linear-gradient(135deg, #10b981 0%, #059669 100%);
        color: white;
        padding: 8px 16px;
        border-radius: 8px;
        display: inline-block;
        font-size: 14px;
    }
</style>
""", unsafe_allow_html=True)

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
    # 默认值
    default_stats = {"level": 1, "experience": 0, "capital": 0, "join_date": "未知"}

    if not engine: return default_stats

    try:
        sql = text("SELECT level, experience, capital, created_at FROM users WHERE username = :user")
        with engine.connect() as conn:
            result = conn.execute(sql, {'user': user_id}).mappings().fetchone()

            if result:
                return dict(result)
            else:
                return default_stats
    except Exception as e:
        return default_stats


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


# ================= 页面主逻辑 =================

# 1. 权限检查
if not st.session_state.get('is_logged_in', False):
    st.warning("🔒 请先在首页登录后查看个人资料")
    st.stop()

username = st.session_state.get('user_id', 'Unknown')

# 获取数据
user_data = get_user_stats(username)
memory_df = get_memory_fragments(username)

# 2. 顶部：个人信息卡片
st.markdown(f"## 👤 交易员档案: {username}")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown('<div>', unsafe_allow_html=True)
    st.markdown(f"""
    <div class="stat-label">等级</div>
    <div style="display:flex; align-items:center;">
        <div class="stat-value">LV.{user_data.get('level', 1)}</div>
        <span class="level-badge">期权暴徒</span>
    </div>
    """, unsafe_allow_html=True)
    exp = user_data.get('experience', 0)
    exp_pct = min(exp / 1000, 1.0)
    st.progress(exp_pct, text=f"EXP: {exp}/1000")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div>', unsafe_allow_html=True)
    money = float(user_data.get('capital', 0))
    st.markdown(f"""
    <div class="stat-label">爱波币</div>
    <div class="stat-value" style="color: #fbbf24;">¥ {money:,.0f}</div>
    <div style="font-size:12px; color:#64748b; margin-top:5px;">钱有什么用？</div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div>', unsafe_allow_html=True)
    total_memories = len(memory_df)
    last_active = memory_df['create_time'].iloc[0] if not memory_df.empty else "暂无"

    st.markdown(f"""
    <div class="stat-label">AI 记忆深度</div>
    <div class="stat-value" style="color: #38bdf8;">{total_memories} 条</div>
    <div style="font-size:12px; color:#64748b; margin-top:5px;">最近交互: {last_active}</div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

st.markdown("---")

# ============================================
# 账号安全设置（紧凑折叠版）
# ============================================
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
st.subheader("🧠 大脑记忆")
st.caption("这里存储了 AI 对您的所有深度记忆，也可以分享。")

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

            st.markdown("---")

            st.markdown(
                f"""
                <div style="background-color: #161b22; border-left: 3px solid #10b981; padding: 10px; border-radius: 4px;">
                    <span style="color: #10b981; font-weight: bold;">🤖 AI 回答:</span>
                    <div style="margin-top: 5px; color: #cbd5e1; font-size: 14px; line-height: 1.6;">
                        {a_full if a_full else "(未解析到回答内容)"}
                    </div>
                </div>
                """,
                unsafe_allow_html=True
            )
            native_share_button(q_full, a_full, key=f"share_mem_{index}")
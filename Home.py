import streamlit as st
import pandas as pd
import data_engine as de
import os
import json
import random
import markdown
import sys
import auth_utils as auth
import memory_utils as mem
import threading
from datetime import datetime, timedelta
from streamlit_lottie import st_lottie
from kline_tools import analyze_kline_pattern
from screener_tool import search_top_stocks
from news_tools import get_financial_news
import time
import extra_streamlit_components as stx
import streamlit.components.v1 as components
import uuid #用于生成唯一ID
from market_tools import get_market_snapshot, get_price_statistics,tool_query_specific_option
from data_engine import get_commodity_iv_info, check_option_expiry_status,search_broker_holdings_on_date,tool_analyze_position_change
from captcha_utils import generate_captcha_image
from market_correlation import tool_stock_hedging_analysis, tool_futures_correlation_check,tool_stock_correlation_check
from sqlalchemy import text
from dotenv import load_dotenv
from beta_tool import calculate_hedging_beta
from knowledge_tools import search_investment_knowledge
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
# --- AI 相关导入 ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

try:
    from langgraph.prebuilt import create_react_agent
except ImportError:
    st.error("❌ 请先安装 LangGraph: `pip install langgraph`")
    st.stop()

# 1. 初始化环境
load_dotenv(override=True)

# --- 系统代理清理 ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

# ==========================================
#  1. 页面配置 (必须在第一行) [修改点：改为 centered 布局]
# ==========================================
st.set_page_config(
    page_title="爱波塔-懂期权的AI | K线分析+期权策略",
    page_icon="favicon.ico",
    layout="centered",
    initial_sidebar_state="expanded"
)

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
        background-color: #2b313e !important;
        color: #e6e6e6 !important;
        border: 1px solid #3b4252;
        border-radius: 4px;
        padding: 0.2rem 0.4rem;
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
    
    /* 1. 锁定按钮容器：给它加一个明显的背景色块 */
    button[data-testid="stSidebarCollapsedControl"] {
        background-color: #3b82f6 !important; /* 亮蓝色底 (如果不喜欢蓝色，可改为 #FFD700 金色) */
        border: 2px solid rgba(255, 255, 255, 0.6) !important; /* 半透明白边框 */
        border-radius: 12px !important;       /* 圆角矩形 */
        width: 40px !important;               /* 强制宽度 */
        height: 40px !important;              /* 强制高度 */
        box-shadow: 0 4px 12px rgba(0,0,0,0.5) !important; /* 明显的阴影，增加立体感 */
        
        /* 强制定位：防止被 Header 遮挡 */
        position: fixed !important;
        left: 15px !important;
        top: 15px !important;
        z-index: 999999 !important; /* 层级最高，浮在一切之上 */
        
        /* 确保内容居中 */
        display: flex !important;
        align-items: center !important;
        justify-content: center !important;
        opacity: 1 !important; /* 防止被系统默认的透明度影响 */
        transition: transform 0.2s ease !important;
    }

    /* 2. 鼠标悬停效果：点击欲望 */
    button[data-testid="stSidebarCollapsedControl"]:hover {
        background-color: #2563eb !important; /* 悬停深蓝 */
        transform: scale(1.1) !important;     /* 微微放大 */
        border-color: #ffffff !important;     /* 边框变亮白 */
    }

    /* 3. 核心修复：强制内部的箭头图标(SVG)变白 */
    button[data-testid="stSidebarCollapsedControl"] svg,
    button[data-testid="stSidebarCollapsedControl"] i {
        fill: #ffffff !important;    /* 填充纯白 */
        color: #ffffff !important;   /* 颜色纯白 */
        stroke: #ffffff !important;  /* 描边纯白 */
        width: 20px !important;      /* 强制图标大小 */
        height: 20px !important;
    }

    /* 调整底部输入框样式 */
    .stChatInput {
        padding-bottom: 20px;
    }
        /* 3. 侧边栏文字强制变白 */
    [data-testid="stSidebar"] {
        background-color: #0f172a !important;
    }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div {
        color: #cbd5e1 !important;
    }
</style>
""", unsafe_allow_html=True)


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
    unique_id = str(uuid.uuid4())[:8]
    container_id = f"share-container-{unique_id}"
    btn_id = f"btn-{unique_id}"

    # 1. 【核心修改】将 AI 的 Markdown 内容转换为 HTML
    # extensions=['tables'] 用于支持 | 表格 | 语法
    # extensions=['nl2br'] 用于支持换行
    html_content = markdown.markdown(
        ai_content,
        extensions=['tables', 'fenced_code', 'nl2br']
    )

    # 2. 构建包含样式的 HTML
    # 注意：我在 <style> 里增加了针对 table, th, td, h3, strong 的样式，让排版更漂亮
    styled_html = f"""
    <div id="{container_id}" style="
        background: linear-gradient(145deg, #1e293b 0%, #0f172a 100%);
        color: #e6e6e6;
        padding: 25px;
        border-radius: 16px;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        line-height: 1.6;
        width: 400px; /* 稍微加宽一点以容纳表格 */
        position: fixed; top: -9999px; left: -9999px;
        box-sizing: border-box;
    ">
        <style>
            #{container_id} table {{
                border-collapse: collapse;
                width: 100%;
                margin: 10px 0;
                font-size: 12px;
                color: #e6e6e6;
            }}
            #{container_id} th, #{container_id} td {{
                border: 1px solid #475569;
                padding: 6px 8px;
                text-align: left;
            }}
            #{container_id} th {{
                background-color: rgba(255, 255, 255, 0.1);
                color: #fff;
                font-weight: bold;
            }}
            #{container_id} h1, #{container_id} h2, #{container_id} h3, #{container_id} h4 {{
                color: #ffffff;
                margin-top: 15px;
                margin-bottom: 8px;
                font-weight: 700;
            }}
            #{container_id} strong {{
                color: #FFD700; /* 金黄色高亮重点 */
            }}
            #{container_id} ul, #{container_id} ol {{
                padding-left: 20px;
                margin: 5px 0;
            }}
            #{container_id} li {{
                margin-bottom: 4px;
            }}
            #{container_id} p {{
                margin-bottom: 8px;
            }}
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
            display: flex; justify-content: space-between; align-items: center;
            border-top: 1px dashed rgba(255,255,255,0.1); padding-top: 10px; margin-top: 15px;
        ">
            <div style="font-size: 11px; color: #64748b;">Generated by 爱波塔</div>
            <div style="font-size: 11px; color: #3b82f6;">www.aiprota.com</div>
        </div>
    </div>
    """

    # --- 3. JS 逻辑 (保持不变) ---
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
        .share-btn:hover, .share-btn:active {{ background-color: #3b82f6; color: white; border-color: #3b82f6; }}
        #modal-overlay {{ display: none; position: fixed; top: 0; left: 0; width: 100vw; height: 100vh; background: rgba(0,0,0,0.85); z-index: 9999; flex-direction: column; align-items: center; justify-content: center; }}
        #modal-img {{ max-width: 85%; border-radius: 10px; box-shadow: 0 0 20px rgba(0,0,0,0.5); }}
        #modal-tip {{ color: white; margin-top: 15px; font-size: 14px; background: #333; padding: 5px 15px; border-radius: 20px; }}
        #close-btn {{ position: absolute; top: 20px; right: 20px; color: white; font-size: 30px; cursor: pointer; }}
    </style>
    </head>
    <body>
        {styled_html}
        <button class="share-btn" id="{btn_id}" onclick="generateAndShare()">
            <i class="fas fa-share-square" style="margin-right:5px;"></i> 分享完整对话
        </button>
        <div id="modal-overlay" onclick="closeModal()">
            <div id="close-btn">&times;</div><img id="modal-img" src="" /><div id="modal-tip">👆 长按图片 -> 转发给朋友</div>
        </div>
        <script>
        function generateAndShare() {{
            const btn = document.getElementById('{btn_id}');
            const originalText = btn.innerHTML;
            const target = document.getElementById('{container_id}');
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 生成中...';

            html2canvas(target, {{ backgroundColor: null, scale: 2, logging: false, useCORS: true }}).then(canvas => {{
                canvas.toBlob(function(blob) {{
                    const file = new File([blob], "ai-analysis.png", {{ type: "image/png" }});
                    if (navigator.canShare && navigator.canShare({{ files: [file] }})) {{
                        navigator.share({{ files: [file], title: '爱波塔 AI 分析' }}).then(() => resetBtn(btn, originalText, true)).catch(() => resetBtn(btn, originalText, false));
                    }} else {{
                        showFallbackModal(canvas.toDataURL('image/png'));
                        resetBtn(btn, originalText, false);
                    }}
                }}, 'image/png');
            }});
        }}
        function showFallbackModal(imgData) {{ document.getElementById('modal-img').src = imgData; document.getElementById('modal-overlay').style.display = 'flex'; }}
        function closeModal() {{ document.getElementById('modal-overlay').style.display = 'none'; }}
        function resetBtn(btn, originalText, success) {{
            if(success) {{ btn.innerHTML = '<i class="fas fa-check"></i> 已调起'; btn.style.borderColor = '#10B981'; btn.style.color = '#10B981'; }} 
            else {{ btn.innerHTML = originalText; }}
            setTimeout(() => {{ btn.innerHTML = originalText; btn.style.borderColor = ''; btn.style.color = ''; }}, 3000);
        }}
        </script>
    </body>
    </html>
    """
    components.html(html_code, height=50)

# ==========================================
#  3. Auth & State 初始化 (保持不变)
# ==========================================
def get_manager(): return stx.CookieManager(key="master_cookie_manager")


cookie_manager = get_manager()
cookies = cookie_manager.get_all() or {}

# 尝试从 Cookie 恢复登录
# 【关键修复 1】增加 'just_logged_out' 判断，如果刚点了登出，绝不执行自动登录
should_auto_login = not st.session_state.get('is_logged_in', False) and not st.session_state.get('just_logged_out',
                                                                                                 False)

if should_auto_login and cookies:
    c_user = cookies.get("username")
    c_token = cookies.get("token")

    if c_user and c_token and str(c_user).strip() != "":
        if auth.check_token(str(c_user), c_token):
            st.session_state['is_logged_in'] = True
            st.session_state['user_id'] = str(c_user)
            st.toast(f"欢迎回来，{c_user} (自动登录)")
            time.sleep(0.3)  # 給一點 UI 反應時間
            st.rerun()

# 【关键修复 2】如果已经是登出后的重跑，现在可以重置标记了
# 这样下次用户刷新页面(F5)时，如果 Cookie 还在(虽然应该删了)，还能尝试登录，或者单纯重置状态
if st.session_state.get('just_logged_out', False):
    st.session_state['just_logged_out'] = False

# 只有第一次运行时才初始化，如果已经登录了，不要重置它
if 'is_logged_in' not in st.session_state:
    st.session_state['is_logged_in'] = False
    st.session_state['user_id'] = None
    st.session_state['username'] = None

# --- 1. 初始化验证码 (如果还没生成过) ---
if 'captcha_code' not in st.session_state:
    img, code = generate_captcha_image()
    st.session_state['captcha_img'] = img
    st.session_state['captcha_code'] = code


def refresh_captcha():
    """刷新验证码的回调函数"""
    img, code = generate_captcha_image()
    st.session_state['captcha_img'] = img
    st.session_state['captcha_code'] = code


# 初始化聊天记录
if "messages" not in st.session_state:
    st.session_state.messages = []


# ==========================================
#  4. AI Agent 定义 (完全保留您的核心逻辑)
# ==========================================
def get_agent(current_user="访客", user_query=""):  # 传入 current_user
    # ... (这里保留您原来的 prompt 和 tools) ...
    tools = [analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_commodity_iv_info,get_financial_news,search_broker_holdings_on_date,tool_analyze_position_change,tool_query_specific_option,
             get_price_statistics, check_option_expiry_status,tool_stock_hedging_analysis,tool_futures_correlation_check,tool_stock_correlation_check,search_top_stocks,calculate_hedging_beta]
    if not os.getenv("DASHSCOPE_API_KEY"):
        st.error("❌ 未配置 API KEY");
        return None

    llm = ChatTongyi(model="qwen-plus", temperature=0.2)

    # --- 🔥【核心修复 1】强制使用北京时间 (UTC+8) ---
    # 服务器通常是 UTC，直接 +8 小时修正
    utc_now = datetime.utcnow()
    china_now = utc_now + timedelta(hours=8)

    weekday_cn = ["一", "二", "三", "四", "五", "六", "日"][china_now.weekday()]

    # --- 🔥【核心修复 2】获取数据库里的“最新行情日期” ---
    # 如果今天是周六，db_latest_date 应该是周五
    db_latest_date = de.get_latest_data_date()

    # --- 日期计算逻辑 (基于北京时间) ---
    # 本周范围
    current_week_start = (china_now - timedelta(days=china_now.weekday())).strftime('%Y%m%d')
    # 上周范围
    last_week_start_dt = china_now - timedelta(days=china_now.weekday() + 7)
    last_week_end_dt = china_now - timedelta(days=china_now.weekday() + 1)
    last_week_start = last_week_start_dt.strftime('%Y%m%d')
    last_week_end = last_week_end_dt.strftime('%Y%m%d')

    # 上月范围
    first_day_this_month = china_now.replace(day=1)
    last_day_last_month = first_day_this_month - timedelta(days=1)
    first_day_last_month = last_day_last_month.replace(day=1)

    last_month_start = first_day_last_month.strftime('%Y%m%d')
    last_month_end = last_day_last_month.strftime('%Y%m%d')
    last_month_name = first_day_last_month.strftime('%Y年%m月')


    system_message = f"""
    你是拥有30年经验的资深交易员，擅长K线技术分析和期权，遵守顺势交易。 
    

    【当前时间基准】：
    1. **现实时间**：{china_now.strftime('%Y年%m月%d日 %H:%M')} (周{weekday_cn})。
    2. **数据最新日期**：【{db_latest_date}】。
       - 当用户问“今天”、“最新”的行情时，**必须**使用日期 `{db_latest_date}` 进行查询。
    【日期参考】
        上周: {last_week_start}-{last_week_end}
        上月: {last_month_start}-{last_month_end}
    

    【工具使用指南】：
    1. 当前/最新价格数据 -> 用 `get_market_snapshot`。
    2. 被问 **历史某一天** 或 **指定日期** 的价格-> 可以用 `get_price_statistics`。
    3. 股票或期货的技术面、K线形态和趋势-> 用 `analyze_kline_pattern`
    4. 期权知识、期权策略、K线交易-> 用 `search_investment_knowledge`
    5. 期权波动率数据 -> 用 `get_commodity_iv_info`。
    6.当客户问“推荐股票”、“选股”-> 用`search_top_stocks`（选分数最高的）
    7.查新闻、消息面-> 用 `get_financial_news` 
    8.查询某期货商当天的持仓 -> 用 `search_broker_holdings_on_date`
    9.查询期货商一段时间的持仓变化 ->用`tool_analyze_position_change`
    10.只要客户问保证金问题-> 必须用 `search_investment_knowledge`。
  

    【你的行为准则】
    1. 当用户询问某个标的时，如果没有指定K线分析，可以同时调用`analyze_kline_pattern`和`get_financial_news`，将消息面与技术面进行对比。
    2. 当用户问期权或交易问题，优先以知识库工具为信息参考。
    3. 股票没有期权，客户问股票时，不要给期权策略，除非是用ETF期权来对冲股票。
    4. 期权策略的建议，需要考虑波动率和距离到期日，使用工具`check_option_expiry_status`和知识库搭配回答
    5. 如果客户问最近某商品的技术面，可以把前面几天的K线都一起分析后给出总结
    6. 给出明确操作建议，根据用户风险偏好（激进/保守）给他喜欢的策略，如果是保守的，就不要给激进建议。
    
    【高级背离研判逻辑 (触发警报)】
     1.如果新闻显示重大利好，但技术面显示利空信号(收长上影线、跌破区间），要提醒利多不涨。
     2.如果新闻是坏消息，但 K 线竟然不跌反涨，或者在低位收出大阳线/下影线,，要提醒利空不跌。


    【回答格式】
   先给结论，然后解释理由。期权策略的使用要结合K线技术面和IV。
    """

    try:
        return create_react_agent(llm, tools, state_modifier=system_message)
    except TypeError:
        try:
            return create_react_agent(llm, tools, messages_modifier=system_message)
        except:
            return create_react_agent(llm, tools)


# 定义随机幽默加载文案
LOADING_JOKES = [
    "☕️ AI正在思考，这问题太简单，我该如何回答...",
    "⚡️ AI正在思考，回想Jack老师的教导...",
    "📈 AI正在思考，顺便用紫微斗数模拟未来 1000 种走势...",
    "📈 AI正在思考，默默拿出K线战法偷看...",
    "🧘‍♂️ AI正在思考，平复最近赚钱激动的心，保持客观...",
    "📞 AI正在思考，连线华尔街内幕人士...",
    "📞 AI正在思考，给主力资金打电话核实...",
    "📞 AI正在思考，准备求助游资大佬...",
    "🔮 AI正在思考，偷偷拿出水晶球...",
    "📉 AI正在思考，顺便检查这根 K 线是不是骗线...",
    "🐂 AI正在思考，还要忙喂养牛市的公牛...",
    "🐻 AI正在思考，尽力跳脱刚才亏钱的思绪里...",
    "🧠 AI正在思考，回想您上次亏损是不是因为没听我劝...",
    "⚡️ AI正在思考，准备请教陈老师..."
]


# ==========================================
#  5. 核心逻辑处理函数 [修改点：封装成函数以便复用]
# ==========================================
def process_user_input(prompt_text):
    """处理用户输入（无论是来自输入框还是快捷卡片）"""
    # 1. 显示用户消息
    st.session_state.messages.append({"role": "user", "content": prompt_text})
    with st.chat_message("user"):
        st.markdown(prompt_text)

    # 2. 生成 AI 回复
    current_user = st.session_state.get('user_id', "访客")
    # [修改] 调用 get_agent 时传入用户的 prompt_text，以便去搜记忆
    agent = get_agent(current_user, user_query=prompt_text)

    if agent:
        with st.chat_message("assistant"):
            # ===========================
            # 🔄 [修改部分开始] 替换原本的 st.spinner
            # ===========================

            # 2. 随机选一句骚话
            random_msg = random.choice(LOADING_JOKES)

            # 4. 使用随机文案作为 Spinner 文字，包裹后续的耗时操作
            try:
                with st.spinner(f"🤖 {random_msg}"):
                    # 3. 显示动画
                    # =================================================
                    # 🧠 [核心修改 1]：在这里检索记忆
                    # =================================================
                    # 【优化】定义触发词：只有涉及用户自身情况时，才加载画像和记忆
                    # 这样能节省大量 System Prompt 的 Token
                    personal_keywords = ["之前", "持仓", "账户", "买", "卖", "建议", "仓位", "风险", "风格" , "推荐"]
                    need_personal_context = any(k in prompt_text for k in personal_keywords)

                    memory_context = ""
                    # 确保这一行和上面的代码对齐
                    if current_user != "访客" and need_personal_context:
                        # 检索最近 3 条最相关的记忆
                        found = mem.retrieve_relevant_memory(current_user, prompt_text, k=2)
                        if found:
                            memory_context = f"""
                                                \n【🔍 必须参考的历史记忆】
                                                 {found}
                                                 """

                        # 获取用户画像
                        user_profile = de.get_user_profile(current_user)
                        risk = user_profile.get('risk_preference', '未知')

                        # 将 画像 + 记忆 组合成一条强力的 SystemMessage
                        super_system_prompt = f"""
                                                    【当前用户档案】
                                                     - 用户名：{current_user}
                                                     - 风险偏好：{risk}

                                                        {memory_context}

                                                     【回答指令】
                                                     请结合上述记忆和当前问题进行回答。如果记忆里有相关持仓信息，请主动提及。
                                                     """
                    else:
                        # ⚠️ 注意：这里的 else 必须和上面的 if 垂直对齐
                        # 如果只是查行情，给个最简单的空串，或者通用指令
                        super_system_prompt = ""

                    # 构建 LangChain 消息历史
                    max_history_rounds = 2
                    recent_messages = st.session_state.messages[-max_history_rounds:]

                    history = [
                        HumanMessage(content=m["content"]) if m["role"] == "user" else AIMessage(content=m["content"])
                        for m in recent_messages
                    ]

                    # ⚡ 暴力注入：把这个超级指令插在最前面
                    history.insert(0, SystemMessage(content=super_system_prompt))

                    # 1. 实例化回调对象
                    monitor_callback = TokenMonitorCallback(
                        username=current_user,
                        query_text=prompt_text
                    )

                    response = agent.invoke(
                        {"messages": history},
                        config={"recursion_limit": 100,
                                "callbacks": [monitor_callback]
                                }

                    )
                    ai_response = response["messages"][-1].content


                    # 打字机效果显示
                    placeholder = st.empty()
                    full_response = ""
                    #  for chunk in ai_response.split():  # 简单模拟流式 (由于 invoke 是同步的，这里只是为了视觉效果)
                    #   full_response += chunk + " "
                    #   placeholder.markdown(full_response + "▌")
                    #   time.sleep(0.01)
                    placeholder.markdown(full_response)

                    st.session_state.messages.append({"role": "ai", "content": ai_response})

                    # ==========================================
                    #  💾 [新增]：将对话存入向量数据库
                    # ==========================================
                    # 只有登录用户才存，或者是访客也可以存(看您需求)
                    if current_user != "访客":
                        mem.save_interaction(current_user, prompt_text, ai_response)
                        print(f"已存档记忆: {prompt_text[:10]}...")

                    # 更新记忆
                    if hasattr(de, 'update_user_memory_async'):
                        de.update_user_memory_async(current_user, prompt_text)
                    # [关键] 在这里直接渲染按钮，为了防止刷新前看不见
                    # 注意：这里不需要存入 session_state，因为上面的"历史消息循环"会负责存储后的渲染
                    native_share_button(ai_response, key=f"share_new_{int(time.time())}")


            except Exception as e:
                st.error(f"分析中断: {e}")


# ==========================================
#  6. 页面渲染：Welcome Screen (空状态) [修改点：新增]
# ==========================================
def show_welcome_screen():
    st.markdown("<br><br>", unsafe_allow_html=True)
    # --- 修复 1：优化标题排版 ---
    # white-space: nowrap -> 强制不换行
    # font-size: clamp(...) -> 智能字体大小（最小 1.8rem，最大 3rem，中间自适应）
    # margin-bottom: 0.5rem -> 调整下边距
    st.markdown("""
            <div style="text-align: center;">
                <h1 style='
                    color: #fff; 
                    white-space: nowrap; 
                    font-size: clamp(2rem, 5vw, 3rem); 
                    margin-bottom: 0.5rem;
                '>
                    🤓 嗨，我是爱波塔
                </h1>
                <p style='color: #8b949e; font-size: 1rem;'>
                    陪你在金融市场奋斗
                </p>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- 快捷指令卡片 ---
    col1, col2, col3 = st.columns(3)

    # 定义点击回调
    # --- 关键修改：定义回调函数 ---
    # 这个函数会在页面重新加载前优先执行，确保数据这就位
    def set_prompt_callback(text):
        st.session_state.pending_prompt = text

    with col1:
        st.button("📉 波动率分析-50ETF期权现在贵吗？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("50ETF期权现在的IV高吗？",)
         )

    with col2:
        st.button("📅 期权学习-什么是飞龙在天？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("期权的飞龙在天是什么策略？",)
         )

    with col3:
        st.button("🎯 K线分析-帮我看今天的白银",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("分析今天白银K线",)
         )

# ==========================================
#  7. 主程序入口
# ==========================================

# A. 侧边栏：登录/设置 (折叠起来保持清爽)
with st.sidebar:
    if not st.session_state['is_logged_in']:
        # --- A. 未登錄狀態 ---
        tab1, tab2 = st.tabs(["登录", "注册"])

        with tab1:
            with st.form("login_form_sidebar"):
                u = st.text_input("账号", key="login_user")
                p = st.text_input("密码", type="password", key="login_pass")
                if st.form_submit_button("登录", type="primary", use_container_width=True):
                    success, msg, token = auth.login_user(u, p)
                    if success:
                        st.session_state['is_logged_in'] = True
                        st.session_state['user_id'] = u

                        # 【關鍵修改】寫入 Cookie (設置 7 天過期)
                        # expires_at 是 datetime 對象
                        expires = datetime.now() + timedelta(days=7)

                        cookie_manager.set("username", u, expires_at=expires, key="set_user_cookie")
                        cookie_manager.set("token", token, expires_at=expires, key="set_token_cookie")

                        st.success("登录成功")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)

        with tab2:
            with st.form("reg_form_sidebar"):
                st.write("### 注册新账号")
                new_user = st.text_input("新账号", key="reg_user")
                new_pass = st.text_input("新密码", type="password", key="reg_pass")
                # --- 2. 显示验证码 ---
                col1, col2 = st.columns([1, 1])
                with col1:
                    # 显示图片
                    st.image(st.session_state['captcha_img'], caption="请输入右侧数字", width=120)
                with col2:
                    # 验证码输入框
                    captcha_input = st.text_input("验证码", placeholder="4位数字", label_visibility="collapsed")

                # 刷新按钮 (放在 form 外面或者用特殊处理，最简单是让用户输错自动刷新)
                # 这里我们直接放提交按钮
                submit_btn = st.form_submit_button("注册")
        # --- 3. 处理提交逻辑 ---
        if submit_btn:
            # 🛑 第一关：检查输入是否为空
            if not new_user or not new_pass or not captcha_input:
                st.warning("⚠️ 请填写完整的账号、密码和验证码！")

                # 🛑 第二关：检查验证码
                # 注意：这里判断是否不相等
            elif captcha_input != st.session_state.get('captcha_code'):
                st.error("❌ 验证码错误！已为您更换一张，请重新输入。")
                refresh_captcha()
                # 注意：这里不要加 st.rerun()，否则错误提示会瞬间消失！
                # 用户看到错误后，手动再次点击注册即可。


            # C. 执行注册 (调用之前的 register_user 函数)
            else:
                success, msg = auth.register_user(new_user, new_pass)
                if success:
                    # --- 🎉 注册成功后的自动登录逻辑 ---
                    st.success("注册成功！正在为您自动登录...")
                    # 1. 我们需要拿到刚注册的 user_id (为了后续功能使用)
                    try:
                        login_success, login_msg, token = auth.login_user(new_user, new_pass)

                        # 2. 关键步骤：修改 Session 状态
                        # 这几行代码就是“告诉 Streamlit 我已经登录了”
                        st.session_state['is_logged_in'] = True
                        st.session_state['user_id'] = new_user

                        # 設置 Cookie (保持登錄狀態)
                        expires = datetime.now() + timedelta(days=7)
                        cookie_manager.set("username", new_user, expires_at=expires, key="reg_set_user")
                        cookie_manager.set("token", token, expires_at=expires, key="reg_set_token")

                        time.sleep(0.3)
                        st.rerun()

                        # 3. 清理注册时用的验证码 (防止返回后还在)
                        if 'captcha_code' in st.session_state:
                            del st.session_state['captcha_code']

                        # 4. 强制刷新页面
                        # 刷新后，Streamlit 会重新运行，发现 logged_in=True，就会直接显示主页，而不是登录页
                        st.rerun()
                    except Exception as e:
                        st.error(f"自动登录失败，请尝试手动登录: {e}")
                else:
                    # === ❌ 注册失败 (比如密码太短、用户已存在) ===
                    # 这里直接显示 register_user 函数返回的错误消息
                    st.error(f"❌ {msg}")
                    # 同样，这里也不要 rerun，让用户看到错误并去修改
                    refresh_captcha()  # 为了安全，失败时最好也刷新验证码

        # 加一个小按钮允许用户手动刷新看不清的图片
        if st.button("🔄 看不清？换一张"):
            refresh_captcha()
            st.rerun()

    else:
        # --- B. 已登錄狀態 ---
        user = st.session_state['user_id']
        st.success(f"👤 欢迎回来，{user}")

        # 顯示資產 (模擬)
        try:
            info = pd.read_sql(f"SELECT level, capital FROM users WHERE username='{user}'", de.engine).iloc[0]
        except:
            pass

        if st.button("登出", type="primary"):
            st.session_state['is_logged_in'] = False
            st.session_state['user_id'] = None

            # 【關鍵修改 3】設置一個“剛登出”的標記，防止 Rerun 後立馬被自動登錄捕獲
            st.session_state['just_logged_out'] = True

            # 【關鍵修改】刪除 Cookie
            cookie_manager.delete("username", key="del_user_cookie")
            cookie_manager.delete("token", key="del_token_cookie")

            time.sleep(0.3)
            st.rerun()


    if st.button("🗑️ 清空对话历史", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    # 客服卡片 CSS 样式
    st.markdown("""
        <style>
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
                color: #00e676; /* 微信绿 */
                font-weight: bold;
            }
        </style>

        <div class="contact-card">
            <div class="contact-title">🤝 客服联系</div>
            <div class="contact-item">微信：<span class="wechat-highlight">trader-sec</span></div>
            <div class="contact-item">电话：<span class="wechat-highlight">17521591756</span></div>
            <div class="contact-item" style="font-size: 12px; margin-top: 8px;">
                沪ICP备2021018087号-2
            </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown("---")
    st.caption("""
        AI期权策略建议 | 股票K线形态选股 | 
        期权波动率分析 | 持仓对冲建议 |
        期权知识学习
        """)

# B. 处理卡片点击产生的 Pending Prompt [修改点：处理快捷指令]
if "pending_prompt" in st.session_state:
    prompt = st.session_state.pending_prompt
    del st.session_state.pending_prompt  # 消费掉，防止循环
    process_user_input(prompt)
    st.rerun()  # 重新加载以显示新消息

# ==========================================
#  B. 界面显示逻辑
# ==========================================
if not st.session_state.messages:
    show_welcome_screen()
else:
    st.markdown('<div style="height: 20px;"></div>', unsafe_allow_html=True)
    for i, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

            # [关键修改]
            if msg["role"] == "ai":
                # 尝试获取上一条消息作为“提问”
                user_question = "（上下文关联提问）"
                if i > 0 and st.session_state.messages[i - 1]["role"] == "user":
                    user_question = st.session_state.messages[i - 1]["content"]

                # 传入两个参数：问题 + 回答
                native_share_button(user_question, msg["content"], key=f"share_history_{i}")

# D. 底部输入框 (Sticky Footer) [修改点：使用 st.chat_input]
if prompt := st.chat_input("我受过交易汇训练，欢迎问我任何实战交易问题..."):
    if not st.session_state['is_logged_in']:
        st.warning("🔒 请先在左侧侧边栏登录")
    else:
        process_user_input(prompt)
        st.rerun()  # 确保界面更新
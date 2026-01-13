import streamlit as st
import pandas as pd
import data_engine as de
import re
import plotly.io as pio
import os
import json
import random
import markdown
import sys
import auth_utils as auth
import memory_utils as mem
import threading
from datetime import datetime, timedelta
from auth_ui import show_auth_dialog, sidebar_user_menu
from streamlit_lottie import st_lottie
from kline_tools import analyze_kline_pattern
from screener_tool import search_top_stocks,get_available_patterns
from news_tools import get_financial_news
from langgraph.errors import GraphRecursionError
from fund_flow_tools import tool_get_retail_money_flow
from plot_tools import draw_chart_tool
from futures_fund_flow_tools import get_futures_fund_flow,get_futures_fund_ranking
from vision_tools import analyze_financial_image
from volume_oi_tools import get_volume_oi, get_futures_oi_ranking, get_option_oi_ranking,get_option_volume_abnormal, get_option_oi_abnormal
import time
import extra_streamlit_components as stx
import streamlit.components.v1 as components
import uuid #用于生成唯一ID
from market_tools import get_market_snapshot, get_price_statistics,tool_query_specific_option,get_historical_price
from data_engine import get_commodity_iv_info, check_option_expiry_status,search_broker_holdings_on_date,tool_analyze_position_change,tool_compare_stocks,get_stock_valuation
from captcha_utils import generate_captcha_image
from search_tools import search_web
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
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHART_DIR = os.path.join(BASE_DIR, "static", "charts")

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


def clean_chart_tag(response_text):
    """清理 AI 乱加的图片链接和标记"""
    text = response_text

    # 1. 删掉所有 Markdown 图片语法: ![任意文字](任意链接)
    text = re.sub(r'!\[.*?\]\(.*?\)', '', text)

    # 2. 删掉 chart_xxx.json 相关链接
    text = re.sub(r'\[.*?\]\(.*?chart_[a-f0-9]+_[a-f0-9]+\.json.*?\)', '', text)

    # 3. 删掉旧标记
    text = re.sub(r'\[CHART_FILE:.*?\]', '', text)
    text = re.sub(r'\[CHART_JSON:.*?\]', '', text)

    # 4. 清理多余空行
    text = re.sub(r'\n{3,}', '\n\n', text)

    return text.strip()


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
should_auto_login = (
    not st.session_state.get('is_logged_in', False)
    and not st.session_state.get('just_logged_out', False)  # ← 这行很重要！
)

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




# 初始化聊天记录
if "messages" not in st.session_state:
    st.session_state.messages = []


# ==========================================
#  4. AI Agent 定义 (完全保留您的核心逻辑)
# ==========================================
def get_agent(current_user="访客", user_query=""):  # 传入 current_user
    # ... (这里保留您原来的 prompt 和 tools) ...
    tools = [analyze_kline_pattern, search_investment_knowledge, get_market_snapshot, get_commodity_iv_info,get_financial_news,search_broker_holdings_on_date,tool_analyze_position_change,tool_query_specific_option,get_historical_price,get_volume_oi,get_futures_oi_ranking,get_option_oi_ranking,get_option_volume_abnormal,get_option_oi_abnormal,
             get_price_statistics, check_option_expiry_status,tool_stock_hedging_analysis,tool_futures_correlation_check,tool_stock_correlation_check,search_top_stocks,calculate_hedging_beta,tool_get_retail_money_flow,draw_chart_tool,search_web,get_stock_valuation,tool_compare_stocks,get_futures_fund_flow,get_futures_fund_ranking,get_available_patterns]
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
    3. 当客户问“推荐股票”、“选股”-> 用`search_top_stocks`（选分数最高的）
    4. 只要客户问保证金问题-> 必须参考 `search_investment_knowledge`。
    5. 查新闻时，先用`get_financial_news`，如果没找到信息，再用`search_web`。
    

    【你的行为准则】
    1. 股票没有期权，客户问股票时，不要给期权策略，除非是用ETF期权来对冲股票。
    2. 期权策略的建议，需要考虑波动率和距离到期日，使用工具`check_option_expiry_status`和知识库搭配回答。
    3. 国内主流商品期货都有对应期权，先查数据库再回答。
    4. 如果思考步数过长，直接根据已知的信息做总结。
    5. 给出明确操作建议，根据用户风险偏好来给他喜欢的策略，如果是保守的，就不要给激进建议，如果是激进的，就给进攻很强的策略。
        

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
    "🔮 AI正在思考，应该说实话吗...",
    "📉 AI正在思考，顺便检查这根 K 线是不是骗线...",
    "🐂 AI正在思考，牛市里应该怎么做...",
    "🐻 AI正在思考，尽力跳脱刚才亏钱的思绪里...",
    "🧠 AI正在思考，回想您上次亏损是不是因为没听我劝...",
    "🧠 AI正在思考，感觉这个用户好像很贪心...",
    "🧠 AI正在思考，不知道这用户在害怕什么...",
    "🧠 AI正在思考，是不是应该劝你all in...",
    "⚡️ AI正在思考，准备请教陈老师..."
]


# ==========================================
#  5. 核心逻辑处理函数 [修改点：封装成函数以便复用]
# ==========================================
def process_user_input(prompt_text):
    """处理用户输入（无论是来自输入框还是快捷卡片）"""
    # 1. 显示用户消息
    # --- 🔥 [新增逻辑] 处理图片 ---
    # 检查是否有上传的文件
    image_context = ""
    if st.session_state.get("portfolio_uploader"):
        with st.status("📸 正在识别持仓截图...", expanded=True) as status:
            st.write("AI 正在观察图片...")
            # 调用视觉模型提取文字
            vision_result = analyze_financial_image(st.session_state.portfolio_uploader)
            status.update(label="✅ 图片识别完成", state="complete", expanded=False)

            # 将识别结果拼接到上下文中，但不直接展示给用户看，而是作为 AI 的“潜意识”
            image_context = f"\n\n【用户上传图，视觉模型提取的信息如下，请务必基于这信息，回答用户下面的问题】：\n{vision_result}\n----------------\n"

            # (可选) 可以在界面上显示识别到了什么，增强信任感
            with st.chat_message("ai"):
                st.caption(f"已识别图片内容：\n{vision_result[:100]}...")

     # 2. 显示用户提问
    st.session_state.messages.append({"role": "user", "content": prompt_text})
    with st.chat_message("user"):
        st.markdown(prompt_text)
        # 如果有图片，顺便显示一下缩略图
        if st.session_state.get("portfolio_uploader"):
            st.image(st.session_state.portfolio_uploader, width=200)

    # 2. 生成 AI 回复
    current_user = st.session_state.get('user_id', "访客")
    # 真正的 Prompt = 图片提取出的持仓数据 + 用户的问题
    final_prompt = image_context + prompt_text
    # [修改] 调用 get_agent 时传入用户的 prompt_text，以便去搜记忆
    agent = get_agent(current_user, user_query=final_prompt)

    if agent:
        with st.chat_message("assistant"):
            status_placeholder = st.empty()
            response_container = st.empty()
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
                    need_personal_context = any(k in final_prompt for k in personal_keywords)

                    memory_context = ""
                    # 确保这一行和上面的代码对齐
                    if current_user != "访客" and need_personal_context:
                        # 检索最近 3 条最相关的记忆
                        found = mem.retrieve_relevant_memory(current_user, final_prompt, k=2)
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

                    history = []
                    for m in recent_messages:
                        if m["role"] == "user":
                            history.append(HumanMessage(content=m["content"]))
                        else:
                            history.append(AIMessage(content=m["content"]))

                    # 🔥【核心修复】如果有图片识别信息，替换最后一条用户消息
                    if image_context and history:
                        for i in range(len(history) - 1, -1, -1):
                            if isinstance(history[i], HumanMessage):
                                history[i] = HumanMessage(content=final_prompt)
                                break

                    # ⚡ 暴力注入：把这个超级指令插在最前面
                    history.insert(0, SystemMessage(content=super_system_prompt))

                    # 1. 实例化回调对象
                    monitor_callback = TokenMonitorCallback(
                        username=current_user,
                        query_text=final_prompt
                    )

                    response = agent.invoke(
                        {"messages": history},
                        config={"recursion_limit": 80,
                                "callbacks": [monitor_callback]
                                }

                    )
                    ai_response = response["messages"][-1].content

                    # 🔥🔥【核心修改】手动检测死循环特征 🔥🔥
                    # 如果 AI 返回了这句话，说明它其实已经报错了，但被吞掉了。
                    # 我们手动 raise 一个错误，强行跳转到下面的 except 救援逻辑！
                    if "Sorry, need more steps" in ai_response or "Agent stopped" in ai_response:
                        raise GraphRecursionError("强制触发救援模式")
                    # ========================================================
                    # 🔥 [修改区域]：检票员逻辑 (Inspector Strategy)
                    # ========================================================

                    attached_chart = None

                    # 1. 倒序遍历消息记录，寻找工具的返回值 (ToolMessage)
                    # LangGraph 返回的 messages 列表里包含了：Human -> AI -> Tool -> AI(Final)
                    for msg in reversed(response["messages"]):
                        # 检查是否是工具消息 (type='tool') 且包含我们的暗号
                        if msg.type == 'tool' and "IMAGE_CREATED:" in msg.content:
                            # 提取文件名
                            attached_chart = msg.content.split("IMAGE_CREATED:")[1].strip()
                            print(f"🎫 检票成功，发现图表: {attached_chart}")
                            break  # 找到一张最新的就够了

                    # 2. 存入历史记录 (带 chart 字段)
                    message_data = {
                        "role": "ai",
                        "content": ai_response,
                        "chart": attached_chart
                    }
                    st.session_state.messages.append(message_data)

                    # 3. 界面即时显示
                    placeholder = st.empty()
                    # 彻底清理可能残留的标记 (双保险)
                    display_text = clean_chart_tag(ai_response).replace("IMAGE_CREATED:", "")
                    placeholder.markdown(display_text)

                    # 4. 渲染图表
                    if attached_chart:
                        render_chart_by_filename(attached_chart)


                    # ==========================================
                    #  💾 [新增]：将对话存入向量数据库
                    # ==========================================
                    # 只有登录用户才存，或者是访客也可以存(看您需求)
                    if current_user != "访客":
                        mem.save_interaction(current_user, final_prompt, ai_response)
                        print(f"已存档记忆: {final_prompt[:10]}...")

                    # 更新记忆
                    if hasattr(de, 'update_user_memory_async'):
                        de.update_user_memory_async(current_user, final_prompt)
                    # [关键] 在这里直接渲染按钮，为了防止刷新前看不见
                    # 注意：这里不需要存入 session_state，因为上面的"历史消息循环"会负责存储后的渲染
                    native_share_button(ai_response, key=f"share_new_{int(time.time())}")

            # 🔥🔥 [核心修改] 捕获"思考步数过多"的错误 🔥🔥
            except GraphRecursionError:
                status_placeholder.empty()
                # --- 启动救援模式 ---
                print("⚠️ [GraphRecursionError] 触发救援模式")

                try:
                    # 1. 定义一个救援用的 LLM（可以直接复用 ChatTongyi）
                    rescue_llm = ChatTongyi(model="qwen-plus", temperature=0.3)

                    # 2. 构建救援 Prompt
                    # 我们把之前的历史记录发给它，并强制它总结
                    rescue_system_prompt = SystemMessage(content="""
                                    系统检测到你之前的思考过程过长。
                                    请立刻停止工具调用！
                                    请根据你【已经知道的信息】和【上下文历史】，直接给用户一个总结性的回答。
                                    """)

                    # 组合消息：历史记录 + 强制总结指令
                    rescue_messages = history + [rescue_system_prompt]

                    # 3. 强制生成回答
                    rescue_response = rescue_llm.invoke(rescue_messages)
                    ai_response = rescue_response.content

                    # 4. 正常显示救援后的回答
                    message_data = {
                        "role": "ai",
                        "content": ai_response,
                        "chart": None
                    }
                    st.session_state.messages.append(message_data)
                    placeholder = st.empty()
                    placeholder.markdown(ai_response)

                    # 渲染按钮
                    native_share_button(ai_response, key=f"share_rescue_{int(time.time())}")

                except Exception as rescue_e:
                    # 如果连救援都失败了，再报错
                    response_container.error(f"🤯 AI 甚至在尝试救援时都失败了: {rescue_e}")

            # 捕获其他通用错误
            except Exception as e:
                status_placeholder.empty()
                # 如果是其他错误，也转成中文
                error_msg = str(e)
                if "recursion limit" in error_msg.lower(): # 双重保险，防止某些版本抛出 ValueError
                     response_container.warning("🤔 AI 思考步数超限，请尝试把问题描述得更具体一些。")
                else:
                    response_container.error(f"🤯 AI 大脑过载了: {error_msg}")

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
        st.button("📅 期权学习-什么是牛市价差？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("期权的牛市价差是什么策略？",)
         )

    with col3:
        st.button("🎯 K线分析-强势股票",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("最近有哪些K线强势的股票推荐",)
         )

# ==========================================
#  7. 主程序入口
# ==========================================

# A. 侧边栏：登录/设置 (折叠起来保持清爽)
with st.sidebar:
    if not st.session_state['is_logged_in']:
        # --- A. 未登录状态 ---
        tab1, tab2, tab3 = st.tabs(["🔐 登录", "📝 注册", "🔑 找回"])

        # ============ Tab1: 登录（仅密码方式）============
        with tab1:
            with st.form("login_form_sidebar"):
                u = st.text_input("用户名/邮箱", key="login_user", placeholder="输入用户名或邮箱")
                p = st.text_input("密码", type="password", key="login_pass")
                login_btn = st.form_submit_button("登录", type="primary", use_container_width=True)

            if login_btn:
                if u and p:
                    # 🔥 login_user 现在返回4个值：success, msg, token, username
                    success, msg, token, real_username = auth.login_user(u, p)
                    if success:
                        st.session_state['is_logged_in'] = True
                        st.session_state['user_id'] = real_username  # 🔥 使用真正的用户名

                        # 写入 Cookie（也用真正的用户名）
                        expires = datetime.now() + timedelta(days=7)
                        cookie_manager.set("username", real_username, expires_at=expires, key="set_user_cookie")
                        cookie_manager.set("token", token, expires_at=expires, key="set_token_cookie")

                        st.success("登录成功")
                        time.sleep(0.5)
                        st.rerun()
                    else:
                        st.error(msg)
                else:
                    st.warning("请输入账号和密码")

        # ============ Tab2: 注册（用户名必填，邮箱选填）============
        with tab2:
            st.caption("📝 创建新账号")

            reg_username = st.text_input("用户名（必填）", key="reg_username", placeholder="请输入用户名")
            reg_password = st.text_input("设置密码", type="password", key="reg_password", placeholder="至少6位")
            reg_password2 = st.text_input("确认密码", type="password", key="reg_password2")

            # 邮箱选填区域
            with st.expander("📧 绑定邮箱（选填）", expanded=False):
                st.caption("绑定后可用邮箱登录和找回密码，也可注册后在个人资料中绑定")
                reg_email = st.text_input("邮箱", key="reg_email", placeholder="your@email.com")

                col1, col2 = st.columns([2, 1])
                with col1:
                    reg_code = st.text_input("验证码", key="reg_code", max_chars=6)
                with col2:
                    st.write("")
                    if st.button("发送", key="btn_send_reg_code", use_container_width=True):
                        if reg_email:
                            from email_utils import send_register_code

                            success, msg = send_register_code(reg_email)
                            if success:
                                st.success("已发送")
                            else:
                                st.error(msg)
                        else:
                            st.warning("请输入邮箱")

            if st.button("注册", type="primary", use_container_width=True, key="btn_register"):
                if not reg_username:
                    st.warning("👤 用户名是必填项")
                elif len(reg_username) < 3:
                    st.warning("用户名至少3个字符")
                elif not reg_password:
                    st.warning("请设置密码")
                elif len(reg_password) < 6:
                    st.warning("密码至少6位")
                elif reg_password != reg_password2:
                    st.error("两次密码不一致")
                elif reg_email and not reg_code:
                    st.warning("填写了邮箱请输入验证码，或清空邮箱")
                else:
                    # 根据是否填写邮箱选择注册方式
                    if reg_email and reg_code:
                        # 带邮箱注册
                        success, msg = auth.register_with_email(
                            email=reg_email,
                            password=reg_password,
                            email_code=reg_code,
                            username=reg_username
                        )
                    else:
                        # 普通注册（不带邮箱）
                        success, msg = auth.register_user(reg_username, reg_password)

                    if success:
                        st.success(msg if msg else "注册成功！")
                        st.balloons()
                        # 自动登录
                        try:
                            login_success, login_msg, token, real_username = auth.login_user(reg_username, reg_password)
                            if login_success:
                                st.session_state['is_logged_in'] = True
                                st.session_state['user_id'] = real_username

                                expires = datetime.now() + timedelta(days=7)
                                cookie_manager.set("username", real_username, expires_at=expires, key="reg_set_user")
                                cookie_manager.set("token", token, expires_at=expires, key="reg_set_token")

                                time.sleep(0.5)
                                st.rerun()
                        except:
                            st.info("请切换到登录页登录")
                    else:
                        st.error(msg)

        # ============ Tab3: 找回密码 ============
        with tab3:
            st.caption("📧 通过邮箱重置密码")

            reset_email = st.text_input("注册邮箱", key="reset_email", placeholder="your@email.com")

            col1, col2 = st.columns([2, 1])
            with col1:
                reset_code = st.text_input("验证码", key="reset_code", max_chars=6)
            with col2:
                st.write("")
                if st.button("发送", key="btn_send_reset_code", use_container_width=True):
                    if reset_email:
                        from email_utils import send_reset_password_code

                        success, msg = send_reset_password_code(reset_email)
                        if success:
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
                    success, msg = auth.reset_password_with_email(reset_email, reset_code, new_pwd)
                    if success:
                        st.success(msg)
                        st.balloons()
                    else:
                        st.error(msg)

    else:
        # --- B. 已登录状态 ---
        user = st.session_state['user_id']
        st.success(f"👤 欢迎回来，{user}")

        # 显示邮箱绑定状态
        if user != "访客":
            masked_email = auth.get_masked_email(user)
            if masked_email:
                st.caption(f"📧 {masked_email}")
            else:
                st.caption("📧 未绑定邮箱 [去个人资料绑定]")


        # 🔥 登出回调函数
        def do_logout():
            # 1. 使数据库中的 token 失效（关键！）
            if user != "访客":
                auth.logout_user(user)

            # 2. 清除 session state
            st.session_state['is_logged_in'] = False
            st.session_state['user_id'] = None
            st.session_state['just_logged_out'] = True
            if 'token' in st.session_state:
                del st.session_state['token']


        # 登出按钮
        if st.button("🚪 登出", type="primary", use_container_width=True, on_click=do_logout):
            # 删除 Cookie
            try:
                cookie_manager.delete("username", key="logout_del_user")
                cookie_manager.delete("token", key="logout_del_token")
            except:
                pass
            time.sleep(0.3)
            st.rerun()

        # 清空对话历史（使用 popover 防误触）
        with st.popover("🗑️ 清空对话历史", use_container_width=True):
            st.markdown("⚠️ **确定要删除所有聊天记录吗？**\n\n此操作无法撤销。")
            if st.button("🚨 确认删除", type="primary", use_container_width=True, key="btn_clear_chat"):
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

            # 如果这条消息里有 "chart" 字段，且不为空，就把它画出来
            if msg.get("chart"):
                render_chart_by_filename(msg["chart"])

            # [关键修改]
            if msg["role"] == "ai":
                # 尝试获取上一条消息作为“提问”
                user_question = "（上下文关联提问）"
                if i > 0 and st.session_state.messages[i - 1]["role"] == "user":
                    user_question = st.session_state.messages[i - 1]["content"]

                # 传入两个参数：问题 + 回答
                native_share_button(user_question, msg["content"], key=f"share_history_{i}")

# ==========================================
#  E. 图片上传区 (新增)
# ==========================================
with st.container():
    # 使用 Expander 把上传控件收起来，避免占用太高空间
    with st.expander("📸 可以上传持仓、K线等图", expanded=False):
        uploaded_img = st.file_uploader("支持 JPG/PNG，截图越清晰越好", type=["jpg", "jpeg", "png"],
                                        key="portfolio_uploader")

        if uploaded_img:
            st.image(uploaded_img, caption="已加载截图", width=200)
            # 🔥 [修改點] 使用自定義 HTML 替代 st.info，解決看不清的問題
            st.markdown("""
                        <div style="
                            background-color: rgba(59, 130, 246, 0.2); /* 半透明亮藍底 */
                            border: 1px solid #3b82f6;               /* 亮藍色邊框 */
                            color: #ffffff !important;               /* 強制純白文字 */
                            padding: 12px;
                            border-radius: 8px;
                            margin-top: 10px;
                            line-height: 1.5;
                        ">
                            <strong style="color: #FFD700;">✅ 图片已就绪</strong><br>
                            请在下方输入框输入问题 <span style="color: #cbd5e1; font-size: 13px;">(例如：'帮分析持仓风险')</span>
                        </div>
                        """, unsafe_allow_html=True)

# D. 底部输入框 (Sticky Footer) [修改点：使用 st.chat_input]
if prompt := st.chat_input("我受过交易汇训练，欢迎问我任何实战交易问题..."):
    if not st.session_state['is_logged_in']:
        st.warning("🔒 请先在左侧侧边栏登录")
    else:
        process_user_input(prompt)
        st.rerun()  # 确保界面更新
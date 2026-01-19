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
from agent_core import build_trading_graph
from vision_tools import analyze_financial_image
from data_engine import get_commodity_iv_info
import time
import extra_streamlit_components as stx
import streamlit.components.v1 as components
import uuid #用于生成唯一ID
from market_tools import get_market_snapshot,tool_query_specific_option
from sqlalchemy import text
from dotenv import load_dotenv
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
# --- AI 相关导入 ---
from langchain_community.chat_models import ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage



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

def stream_text_generator(text, delay=0.01):
    """
    打字机效果生成器：将长文本拆分成字符逐个输出
    delay: 每个字符的延迟时间 (秒)，0.005-0.02 之间体感最佳
    """
    for char in text:
        yield char
        time.sleep(delay)
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

    # 🔥 [新增] 删掉 IMAGE_CREATED 标记
    text = re.sub(r'IMAGE_CREATED:chart_[a-zA-Z0-9_]+\.json', '', text)

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
        model="qwen-plus",
        temperature=0.2,  # 稍微增加一点创造性
        api_key=api_key
    )

    # 3. 🧠 大脑 (Max): 最贵、逻辑最强 -> 用于期权策略、王牌分析、CIO总结
    llm_max = ChatTongyi(
        model="qwen-max",
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

    # 1. 定义【绝对需要 Agent 思考】的复杂词 (负面清单 - 增强版)
    # 🔥 [修改点] 增加了 "概率", "几成", "可能", "战争", "局势" 等词，防止地缘政治问题被拦截
    complex_keywords = [
        "策略", "建议", "怎么做", "分析", "为何", "原因", "预测","K线","技术面","分析",
        "教学", "是什么", "含义", "解释", "持仓", "风险", "复盘", "总结","账户","资金",
        "止损", "止盈", "平仓", "割肉", "买入", "卖出", "加仓","相关性","相关度",
        "牛市价差", "熊市价差", "备兑", "跨式", "双卖","为什么","期权","距离",
        "高吗", "低吗", "合适吗", "能买吗","IV","波动率",
        "概率", "几成", "可能性", "胜率", "会打吗"
    ]

    # 2. 定义【简单查询】的触发词 (正面清单 - 收紧版)
    # 🔥 [修改点] 去掉了单独的 "多少"，改为 "多少钱", "多少点"
    price_keywords = [
        "价格", "现价", "收盘", "开盘", "最新价", "报价",
        "多少点", "多少钱", "几点"
    ]

    # 3. 状态判断
    has_complex = any(k in user_query for k in complex_keywords)
    has_price = any(k in user_query for k in price_keywords)

    # 4. 特殊补丁：如果用户只输入了 代码+多少 (例如 "茅台多少")
    # 虽然 "多少" 被删了，但我们要允许 "代码+多少" 的模糊匹配，前提是它不包含 complex 词
    # 简单的正则判断：是否有 "多少" 且没有 "概率/人口" 等
    is_fuzzy_price = "多少" in user_query and not has_complex

    # 过滤词 (有些词虽然像查询，但其实是期权链查询，不适合 snapshot)
    forbidden_terms = ["iv", "IV", "波动率", "期权", "认购", "认沽", "call", "put"]
    has_forbidden = any(k in user_query for k in forbidden_terms)

    # 5. 路由逻辑
    # 只有在 (命中精准价格词 OR 命中模糊多少) AND (没有复杂词) AND (没有禁用词) 时才触发
    is_fast_query = (has_price or is_fuzzy_price) and (not has_complex) and (not has_forbidden)

    if not is_fast_query:
        return False, None

    # --- ⚡ 走快速通道处理 (仅限 Snapshot) ---
    with st.chat_message("assistant", avatar="🤖"):
        with st.status("⚡ 正在连接交易所行情...", expanded=True) as status:
            try:
                # 确保顶部已导入: from market_tools import get_market_snapshot
                res = get_market_snapshot.invoke(user_query)

                status.write(res)
                status.update(label="✅ 报价完成", state="complete", expanded=True)

                return True, res

            except Exception as e:
                return False, None

    return False, None

# ==========================================
#  5. 核心逻辑处理函数 [修改点：封装成函数以便复用]
# ==========================================
def process_user_input(prompt_text):
    """处理用户输入（无论是来自输入框还是快捷卡片）"""

    # --- 1. 图片识别逻辑 (保留) ---
    image_context = ""
    if st.session_state.get("portfolio_uploader"):
        with st.status("📸 正在识别持仓截图...", expanded=True) as status:
            st.write("AI 正在观察图片...")
            vision_result = analyze_financial_image(st.session_state.portfolio_uploader)
            status.update(label="✅ 图片识别完成", state="complete", expanded=False)
            image_context = f"\n\n【用户上传图信息】：\n{vision_result}\n----------------\n"
            with st.chat_message("ai"):
                st.caption(f"已识别图片内容：\n{vision_result[:100]}...")

    # --- 2. 显示用户提问 (保留) ---
    st.session_state.messages.append({"role": "user", "content": prompt_text})
    with st.chat_message("user"):
        st.markdown(prompt_text)
        if st.session_state.get("portfolio_uploader"):
            st.image(st.session_state.portfolio_uploader, width=200)

    related_memories = "暂无相关历史记忆"
    try:
        # 获取当前用户ID (如果没登录就用 default)
        current_user = st.session_state.get("user_id", "guest")

        # 调用 memory_utils 里的函数
        # 注意：这里会自动去向量库找和 prompt 相似的历史对话
        found_mem = mem.retrieve_relevant_memory(
            user_id=current_user,
            query=prompt,
            k=3  # 只找最近3条最相关的
        )

        MAX_CHARS = 2000

        if found_mem:
            if len(found_mem) > MAX_CHARS:
                found_mem = found_mem[:MAX_CHARS] + "...(已截断)"
            related_memories = found_mem

    except Exception as e:
        print(f"❌ 记忆检索失败: {e}")

    # ---------------------------------------------------------
    # 构造输入 (注入 memory_context)
    # ---------------------------------------------------------
    inputs = {
        "user_query": prompt,
        "messages": [HumanMessage(content=prompt)],
        "is_complex_task": False,
        # 🔥 把搜到的记忆传给 Agent
        "memory_context": related_memories
    }

    # 构造最终 Prompt
    final_prompt = image_context + prompt_text

    # --- 3. 极速伪路由检查 (保留) ---
    is_hit, fast_response = fast_router_check(final_prompt)
    if is_hit:
        st.session_state.messages.append({"role": "assistant", "content": fast_response})
        st.rerun()
        return

    # ============================================================
    # 🔥🔥🔥 [修正区域]：LangGraph + 记忆检索 (RAG)
    # ============================================================

    current_user = st.session_state.get('user_id', "访客")

    # 🧠 [恢复功能 1]：检索历史记忆 & 用户画像
    # 只有涉及用户自身情况时，才加载画像和记忆，节省 Token
    personal_keywords = ["之前", "持仓", "账户", "买", "卖", "建议", "仓位", "风险", "风格", "推荐"]
    need_personal_context = any(k in final_prompt for k in personal_keywords)

    system_instruction = ""  # 初始化为空
    #[关键修复]：必须在这里先定义默认值，防止跳过 if 块后报错！
    risk = "稳健型"

    if current_user != "访客" and need_personal_context:
        try:
            # A. 检索向量库记忆
            found = mem.retrieve_relevant_memory(current_user, final_prompt, k=2)
            memory_context = f"\n【🔍 参考历史记忆】\n{found}" if found else ""

            # B. 获取用户画像 (风险偏好)
            user_profile = de.get_user_profile(current_user)
            risk = user_profile.get('risk_preference', '未知')

            # C. 组合成 System Prompt
            system_instruction = f"""
            【当前用户档案】
            - 用户名：{current_user}
            - 风险偏好：{risk}
            {memory_context}

            【回答要求】
            请结合上述记忆和当前问题进行回答。如果记忆里有相关持仓信息，请主动提及。
            """
            print(f"🧠 已注入记忆上下文 (风险偏好: {risk})")
        except Exception as e:
            print(f"记忆检索失败: {e}")

    # 初始化 Graph Agent
    app = get_agent(current_user, user_query=final_prompt)

    if app:
        with st.chat_message("assistant", avatar="🤖"):

            report_card = {
                "analyst": "", "monitor": "", "strategist": "",
                "researcher": "", "news": "", "generalist": "","screener": "",
                "chatter": "", "finalizer": "" # 👈 加上这俩
            }
            final_img_path = None

            with st.status("🚀 交易团队正在协作...", expanded=True) as status:

                # 🧠 [恢复功能 2]：构造输入消息列表 (含对话历史)
                # 🔥 [新增] 将最近的对话历史也传给 Agent，以便理解上下文
                input_messages = []

                # A. 先加入系统指令
                if system_instruction:
                    input_messages.append(SystemMessage(content=system_instruction))

                history_msgs = st.session_state.messages[:-1] if len(st.session_state.messages) > 1 else []
                recent_history = history_msgs[-2:] if len(history_msgs) > 2 else history_msgs

                for msg in recent_history:
                    if msg["role"] == "user":
                        input_messages.append(HumanMessage(content=msg["content"]))
                    elif msg["role"] in ["assistant", "ai"]:
                        # 截取 AI 回复的前 500 字，防止 Token 爆炸
                        content = msg["content"][:500] + "..." if len(msg["content"]) > 500 else msg["content"]
                        input_messages.append(AIMessage(content=content))

                # C. 最后加入当前问题
                input_messages.append(HumanMessage(content=final_prompt))

                inputs = {
                    "user_query": final_prompt,
                    "messages": input_messages,  # 👈 这里传入带记忆的消息列表
                    "risk_preference": risk
                }

                # 🔥 [修改] 使用 invoke 代替 stream，避免 GeneratorExit 问题
                input_message_count = len(input_messages)
                status.write("🔄 正在分析中，请稍候...")

                try:
                    # 使用 invoke 一次性执行完成
                    final_state = app.invoke(inputs, {"recursion_limit": 25})

                    # 🔥🔥🔥 [修复] 只处理新生成的消息，跳过历史消息
                    messages = final_state.get("messages", [])
                    new_messages = messages[input_message_count:]  # ← 关键修复：切片取新消息
                    seen_contents = set()
                    for msg in new_messages:
                        content = getattr(msg, 'content', str(msg))
                        # 跳过重复内容
                        content_hash = hash(content[:100])  # 用前100字符做哈希
                        if content_hash in seen_contents:
                            continue
                        seen_contents.add(content_hash)
                        print(f"📝 新消息: {content[:80]}...")

                        # 🔥 [修复] 按优先级顺序检查，带标签的优先
                        if "【技术分析】" in content:
                            report_card["analyst"] = content
                        elif "【数据监控】" in content:
                            report_card["monitor"] = content
                        elif "【王牌分析】" in content:
                            report_card["generalist"] = content
                        elif "【最终决策】" in content:
                            report_card["finalizer"] = content
                            print(f"✅ 找到最终决策")
                        elif "【情报与舆情】" in content:
                            report_card["researcher"] = content
                            report_card["news"] = content
                            print(f"✅ 找到情报与舆情")
                        elif "【闲聊】" in content or "【知识问答】" in content:
                            report_card["chatter"] = content
                        elif "【期权策略】" in content:
                            report_card["strategist"] = content
                        # 🔥 [修复] Fallback 放在最后，且排除系统消息
                        elif "【精选股票】" in content:
                            report_card["screener"] = content
                        elif (content.strip() and
                              "【" not in content and
                              "PASS" not in content and
                              "已制定计划" not in content and
                              len(content) > 30):
                            # 只有当没有其他内容时才使用
                            if not report_card.get("chatter"):
                                report_card["chatter"] = content

                    # 提取其他状态字段
                    if final_state.get("trend_signal"):
                        status.write(f"📈 **技术分析**: 趋势 {final_state.get('trend_signal')}")
                    if final_state.get("fund_data"):
                        report_card["monitor"] = final_state.get("fund_data")
                        status.write(f"💰 **资金监控**: 数据已更新")
                    if final_state.get("option_strategy"):
                        report_card["strategist"] = final_state.get("option_strategy")
                        status.write(f"⚖️ **期权策略**: 已生成")
                    if final_state.get("news_summary"):
                        report_card["news"] = final_state.get("news_summary")
                        status.write(f"📰 **情报**: 已检索")

                    # 提取图表路径
                    chart_img = final_state.get("chart_img", "")
                    if chart_img:
                        final_img_path = chart_img
                        print(f"🔍 从 final_state.chart_img 获取: {final_img_path}")

                    # 🔥 [新增] 如果 final_state 没有 chart_img，尝试从消息内容中提取
                    if not final_img_path:
                        print(f"🔍 开始遍历 {len(messages)} 条消息查找图表...")
                        for msg in reversed(messages):
                            content = getattr(msg, 'content', str(msg))
                            # 检查是否包含 IMAGE_CREATED 标记
                            if "IMAGE_CREATED:" in content:
                                # 使用正则提取文件名，更安全
                                chart_match = re.search(r'IMAGE_CREATED:(chart_[a-zA-Z0-9_]+\.json)', content)
                                if chart_match:
                                    final_img_path = chart_match.group(1)
                                    print(f"🎫 检票成功，发现图表: {final_img_path}")
                                    break

                    status.update(label="✅ 分析完成", state="complete", expanded=False)

                except Exception as e:
                    status.update(label="❌ 执行中断", state="error")
                    st.error(f"Agent Error: {e}")
                    import traceback
                    traceback.print_exc()
                    return

            # --- 拼接最终报告 ---
            final_response_md = ""

            # 获取各角色的输出
            chatter_txt = report_card.get("chatter", "")
            generalist_txt = report_card.get("generalist", "")
            finalizer_txt = report_card.get("finalizer", "")
            researcher_txt = report_card.get("researcher", "")
            screener_txt = report_card.get("screener", "")

            # === 场景 1: 闲聊/知识问答 ===
            if finalizer_txt and "PASS" not in finalizer_txt:
                final_response_md = finalizer_txt
                print("✅ 使用 finalizer 输出")

                # === 场景 1: 闲聊/知识问答 ===
            elif chatter_txt and "已制定计划" not in chatter_txt:
                final_response_md = chatter_txt
                print("✅ 使用 chatter 输出")

                # === 场景 2: 王牌分析师 (自带总结) ===
            elif generalist_txt:
                final_response_md = generalist_txt
                print("✅ 使用 generalist 输出")

                # === 场景 2.5: 情报研究员单独输出 ===
            elif researcher_txt:
                final_response_md = researcher_txt
                print("✅ 使用 researcher 输出")

            elif screener_txt:
                final_response_md = screener_txt
                status.update(label="✅ 选股策略已生成", state="complete")

            # === 场景 3: 交易团队协作 (核心修改逻辑) ===
            else:
                # 检查 CIO 是否发话了
                # 如果 CIO 内容不是 "PASS" 且不为空，说明触发了“多源整合模式”
                is_integrated_report = finalizer_txt and "PASS" not in finalizer_txt

                if is_integrated_report:
                    # 🔥 [整合模式]：只显示 CIO 的总结，隐藏前面分析师的零散报告
                    # 这样就解决了“信息重复”的问题
                    final_response_md = finalizer_txt

                else:
                    # 🔥 [单兵模式]：CIO 觉得没问题放行了 (PASS)
                    # 此时按顺序显示各个分析师的原话 (保留漂亮排版)

                    # 1. 技术分析
                    if report_card["analyst"]:
                        final_response_md += f"{report_card['analyst']}\n\n"

                    # 2. 资金监控
                    if report_card["monitor"] and report_card["monitor"] != "无数据":
                        final_response_md += f"### 💸 资金面监控\n{report_card['monitor']}\n\n"

                    # 3. 期权策略
                    if report_card["strategist"]:
                        final_response_md += f"### ⚖️ 衍生品策略建议\n{report_card['strategist']}\n\n"

                    # 🔥 [新增] 优先显示选股结果
                    if report_card["screener"]:
                        final_response_md += f"{report_card['screener']}\n\n"

                    # 4. 新闻
                    if report_card["news"]:
                        final_response_md += f"### 📰 相关情报\n{report_card['news']}\n"

                    # 如果 CIO 有修正意见 (即不是 PASS 但也不是完整报告，可能是纠错)，追加在最后
                    if finalizer_txt and "PASS" not in finalizer_txt:
                        final_response_md += f"\n\n---\n{finalizer_txt}"

            # 渲染图表和文字
            if final_img_path:
                render_chart_by_filename(final_img_path)
            # 清理文本中的 Markdown 图片语法，避免显示破图
            final_response_md = clean_chart_tag(final_response_md)

            if not final_response_md.strip() or "need more steps" in final_response_md.lower():
                print("⚠️ 报告为空或不完整，尝试 fallback...")
                messages = final_state.get("messages", [])

                # 策略1：尝试找工具返回的有用内容
                tool_results = []
                for msg in messages:
                    msg_type = getattr(msg, 'type', '')
                    content = getattr(msg, 'content', str(msg))
                    # 收集工具返回的内容（通常比较长且有用）
                    if msg_type == 'tool' and content and len(content) > 100:
                        tool_results.append(content)

                if tool_results:
                    # 取最近的工具结果
                    combined = "\n\n---\n\n".join(tool_results[-2:])
                    final_response_md = f"### 📊 已收集的信息\n\n{combined}"
                    print(f"✅ 从工具结果 fallback 成功")
                else:
                    # 策略2：找最后一条有意义的消息
                    for msg in reversed(messages):
                        content = getattr(msg, 'content', str(msg))
                        # 跳过系统消息、空消息、计划消息和错误消息
                        if (content.strip() and
                                "PASS" not in content and
                                "已制定计划" not in content and
                                "need more steps" not in content.lower() and
                                len(content) > 20):
                            final_response_md = content
                            print(f"✅ Fallback 成功: {content[:50]}...")
                            break

            with st.chat_message("assistant", avatar="🤖"):
                # 注意：如果文本太长，为了防止打字太慢，可以把 delay 调低，或者按“词”分割
                if len(final_response_md) > 1000:
                    # 如果内容很长，打字速度加快
                    st.write_stream(stream_text_generator(final_response_md, delay=0.005))
                else:
                    # 内容短，正常速度
                    st.write_stream(stream_text_generator(final_response_md, delay=0.015))

            # --- 存入 Session 历史 ---
            message_data = {
                "role": "ai",
                "content": final_response_md,
                "chart": final_img_path
            }
            st.session_state.messages.append(message_data)

            # 🧠 [恢复功能 3]：存入向量数据库 (长期记忆)
            if current_user != "访客":
                try:
                    # 存入向量库
                    mem.save_interaction(current_user, final_prompt, final_response_md)

                    # 触发后台画像更新 (如果你的 data_engine 有这个功能)
                    if hasattr(de, 'update_user_memory_async'):
                        de.update_user_memory_async(current_user, final_prompt)

                    print(f"💾 记忆已存档: {final_prompt[:10]}...")
                except Exception as e:
                    print(f"记忆存储失败: {e}")

            # 生成分享按钮
            native_share_button(prompt_text, final_response_md, key=f"share_new_{int(time.time())}")

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
        </style>
    """, unsafe_allow_html=True)

    # --- 2. 渲染标题 ---
    st.markdown("""
            <div style="padding: 20px 0;">
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
        st.button("创业板期权策略推荐什么？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("现在创业板适合什么期权策略",)
         )

    with col2:
        st.button("期权学习-什么是牛市价差？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("牛市价差策略是什么？",)
         )

    with col3:
        st.button("K线分析-强势股票",
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
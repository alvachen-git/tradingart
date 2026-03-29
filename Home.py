import streamlit as st
import hashlib
from task_manager import TaskManager
import time
import pandas as pd
import data_engine as de
import subscription_service as sub_svc
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
from vision_tools import analyze_financial_image, analyze_portfolio_image
from data_engine import get_commodity_iv_info
import time
import extra_streamlit_components as stx
import streamlit.components.v1 as components
import uuid #用于生成唯一ID
from market_tools import get_market_snapshot,tool_query_specific_option
from ui_components import inject_sidebar_toggle_style
from sqlalchemy import text
from dotenv import load_dotenv
from pathlib import Path
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.outputs import LLMResult
# --- AI 相关导入 ---
from llm_compat import ChatTongyiCompat as ChatTongyi
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

# Streamlit 运行时/第三方组件仍可能访问旧别名，提前映射以避免弃用日志噪音。
if hasattr(st, "user"):
    st.experimental_user = st.user



# 1. 初始化环境
_CURRENT_DIR = Path(__file__).resolve().parent
_ROOT_ENV = _CURRENT_DIR.parent / ".env"
if _ROOT_ENV.exists():
    load_dotenv(dotenv_path=_ROOT_ENV, override=True)
else:
    load_dotenv(override=True)

# --- 系统代理清理 ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

# ==================== 公告配置区 ====================
ENABLE_HOME_ANNOUNCEMENT = True  # 开启首页公告
# Fast router is disabled by default to avoid false positives on
# historical/list queries (for example: "最近两周每天价格").
FAST_ROUTER_ENABLED = os.getenv("AIBOTA_FAST_ROUTER_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}

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

def get_announcement_hash():
    """根据公告内容生成唯一hash"""
    content_str = str(ANNOUNCEMENT_CONTENT)
    return hashlib.md5(content_str.encode()).hexdigest()[:8]


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
            current_hash = get_announcement_hash()
            # ✅ 使用独立的 CookieManager（带唯一 key）
            cm = stx.CookieManager(key="announcement_cookie_setter")
            cm.set(
                "announcement_read",
                current_hash,
                expires_at=datetime.now() + timedelta(days=365)
            )
            # 同步更新会话状态，避免等待下一次 cookie 读取
            st.session_state.announcement_read_hash = current_hash
            st.session_state.announcement_acknowledged = True
            st.rerun()


def check_and_show_announcement():
    """检查是否需要显示公告 - 使用 session_state 中的 cookie 数据"""
    # 仅在用户明确点击“我知道了”后，本会话不再弹出
    if st.session_state.get('announcement_acknowledged', False):
        return

    # 🔥 关键2：如果刚刚手动登录，跳过这次显示，等下次用户刷新或操作时再显示
    # 避免登录后立即弹窗打扰用户，也避免闪现问题
    if st.session_state.get('just_manual_logged_in', False):
        st.session_state['just_manual_logged_in'] = False  # 重置标记
        return

    try:
        current_hash = get_announcement_hash()

        # 🔥 使用之前在初始化时读取的 cookie 状态（避免 rerun 冲突）
        read_hash = st.session_state.get('announcement_read_hash', None)

        # cookie 已确认当前版本：本会话不再弹出
        if read_hash == current_hash:
            st.session_state.announcement_acknowledged = True
            return

        # cookie 无记录或版本不匹配：继续显示公告
        show_announcement()
    except Exception as e:
        print(f"公告检查失败: {e}")
        # 出错时仍尝试显示，避免静默丢失公告
        if not st.session_state.get('announcement_acknowledged', False):
            show_announcement()

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
            display: flex; justify-content: space-between; align-items: center;
            border-top: 1px dashed rgba(255,255,255,0.1); padding-top: 10px; margin-top: 15px;
        ">
            <div style="font-size: 11px; color: #64748b;">Generated by 爱波塔</div>
            <div style="font-size: 11px; color: #3b82f6;">www.aiprota.com</div>
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
        function generateAndShare() {{
            const btn = document.getElementById('{btn_id}');
            const originalText = btn.innerHTML;
            const target = document.getElementById('{container_id}');
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> 生成中...';

            html2canvas(target, {{ backgroundColor: null, scale: 2, logging: false, useCORS: true }}).then(canvas => {{
                canvas.toBlob(function(blob) {{
                    const file = new File([blob], "aiprota_analysis.png", {{ type: "image/png" }});
                    if (navigator.canShare && navigator.canShare({{ files: [file] }})) {{
                        navigator.share({{ files: [file], title: '爱波塔 AI 分析' }}).then(() => resetBtn(btn, originalText)).catch(() => resetBtn(btn, originalText));
                    }} else {{
                        alert("您的浏览器不支持直接分享，请截图保存。");
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
    components.html(html_code, height=50)


# ==========================================
#  3. Auth & State 初始化 (保持不变)
# ==========================================
cookie_manager = stx.CookieManager(key="main_cookie_manager")
cookies = cookie_manager.get_all() or {}
if "home_cookie_retry_once" not in st.session_state:
    st.session_state.home_cookie_retry_once = False


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

# 🔥 [关键修复] 在任何 rerun 之前，先读取公告状态并保存到 session_state
# 这样即使后面触发 rerun，公告状态也不会丢失
if ENABLE_HOME_ANNOUNCEMENT and 'announcement_cookie_loaded' not in st.session_state:
    try:
        cm = stx.CookieManager(key="early_announcement_reader")
        announcement_cookies = cm.get_all() or {}
        st.session_state.announcement_read_hash = announcement_cookies.get("announcement_read", None)
        st.session_state.announcement_cookie_loaded = True
    except:
        st.session_state.announcement_read_hash = None
        st.session_state.announcement_cookie_loaded = True

# 初始化待处理任务状态
if "pending_task" not in st.session_state:
    st.session_state.pending_task = None
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
    restored, restore_state = _restore_login_with_cookie_state(cookies)

    if restored:
        st.session_state.home_cookie_retry_once = False
        c_user = st.session_state.get("user_id")

        # 🔥 [新增] 自动登录后，尝试从 Redis 恢复待处理任务
        from task_manager import TaskManager

        task_manager = TaskManager()
        pending_task_data = task_manager.get_user_pending_task(str(c_user))
        pending_portfolio_data = task_manager.get_user_pending_portfolio_task(str(c_user))

        if pending_task_data:
            # 恢复任务信息到 Session State
            st.session_state.pending_task = {
                "task_id": pending_task_data["task_id"],
                "prompt": pending_task_data["prompt"],
                "image_context": pending_task_data.get("image_context", ""),
                "risk": pending_task_data.get("risk_preference", "稳健型"),
                "start_time": pending_task_data["start_time"]
            }

            # 恢复用户消息到历史（如果 messages 为空）
            if not st.session_state.get("messages"):
                st.session_state.messages = [
                    {"role": "user", "content": pending_task_data["prompt"]}
                ]

            st.toast(f"欢迎回来，{c_user} (已恢复您的任务)")
            print(f"✅ 自动登录后恢复任务: {pending_task_data['task_id']}")
        else:
            st.toast(f"欢迎回来，{c_user} (自动登录)")

        if pending_portfolio_data:
            st.session_state.pending_portfolio_task = {
                "task_id": pending_portfolio_data["task_id"],
                "start_time": pending_portfolio_data["start_time"],
                "screenshot_hash": pending_portfolio_data.get("screenshot_hash", ""),
                "positions_count": pending_portfolio_data.get("positions_count", 0),
            }
            print(f"✅ 自动登录后恢复持仓任务: {pending_portfolio_data['task_id']}")

        time.sleep(0.3)
        st.rerun()

    # 某些浏览器首次载入时 Cookie 组件还没就绪，或只读到部分字段：重跑一次再读
    elif restore_state in ("empty", "partial", "error") and not st.session_state.get("home_cookie_retry_once", False):
        st.session_state.home_cookie_retry_once = True
        time.sleep(0.15)
        st.rerun()
    elif restore_state == "invalid":
        # Token 失效时清除 Cookie，避免反复失败
        c_user = cookies.get("username")
        c_token = cookies.get("token")
        if c_user or c_token:
            try:
                cookie_manager.delete("username", key="auto_clean_user")
                cookie_manager.delete("token", key="auto_clean_token")
            except:
                pass

# 【关键修复 2】如果已经是登出后的重跑，现在可以重置标记了
# 这样下次用户刷新页面(F5)时，如果 Cookie 还在(虽然应该删了)，还能尝试登录，或者单纯重置状态
if st.session_state.get('just_logged_out', False):
    st.session_state['just_logged_out'] = False

# 只有第一次运行时才初始化，如果已经登录了，不要重置它
if 'is_logged_in' not in st.session_state:
    st.session_state['is_logged_in'] = False
    st.session_state['user_id'] = None
    st.session_state['username'] = None
    st.session_state['token'] = None




# 初始化聊天记录
if "messages" not in st.session_state:
    st.session_state.messages = []
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = str(uuid.uuid4())

# 🔥 [新增] 图片上传器的动态 key，用于清除图片
if "uploader_key" not in st.session_state:
    st.session_state.uploader_key = 0

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
        model="qwen3.5-plus",
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
                # 🔥🔥🔥 [核心修复 1] 字符串清洗 (脱水处理)
                # 目的：把 "长江电力价格多少" 变成 "长江电力"
                clean_query = user_query

                # 按长度降序排，优先删长词 (防止删了"价格"剩下"多少")
                target_kws = sorted(price_keywords + ["多少"], key=len, reverse=True)

                for kw in target_kws:
                    clean_query = clean_query.replace(kw, "")

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
    "继续", "接着", "承接", "基于刚才", "刚聊到", "上一轮"
)


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
    current_tokens = _extract_similarity_tokens(prompt_text)
    if not current_tokens:
        return False

    best_score = 0.0
    for turn in recent_turns:
        turn_tokens = _extract_similarity_tokens(turn.get("content", ""))
        if not turn_tokens:
            continue
        union = current_tokens | turn_tokens
        if not union:
            continue
        score = len(current_tokens & turn_tokens) / len(union)
        best_score = max(best_score, score)
    return best_score >= threshold


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


def build_context_payload(prompt_text: str, current_user: str):
    """构建连续对话上下文载荷（会话+长期记忆）"""
    all_messages = list(st.session_state.get("messages", []))
    recent_turns = [
        {"role": msg.get("role", ""), "content": str(msg.get("content", ""))}
        for msg in all_messages[-4:]  # 最近两轮（user+ai）
    ]
    recent_context = _build_recent_context_text(recent_turns)

    is_followup = any(kw in prompt_text for kw in FOLLOWUP_KEYWORDS)
    semantic_related = _is_semantically_related(prompt_text, recent_turns)
    should_load_long_memory = is_followup or semantic_related

    memory_context = ""
    if current_user != "访客" and should_load_long_memory:
        try:
            found = mem.retrieve_relevant_memory(
                user_id=current_user,
                query=prompt_text,
                k=2
            )
            if found:
                memory_context = found[:1500]
        except Exception as e:
            print(f"❌ 长期记忆检索失败: {e}")

    conversation_id = st.session_state.get("conversation_id")
    if not conversation_id:
        conversation_id = str(uuid.uuid4())
        st.session_state["conversation_id"] = conversation_id

    return {
        "is_followup": bool(is_followup),
        "recent_turns": recent_turns,
        "recent_context": recent_context,
        "memory_context": memory_context,
        "semantic_related": bool(semantic_related),
        "conversation_id": conversation_id
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


def auto_submit_portfolio_task(uploaded_img):
    """上传截图后自动触发持仓分析任务（仅登录用户）。"""
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

    with st.status("📊 正在识别持仓截图并自动启动体检...", expanded=True) as status:
        vision_struct = analyze_portfolio_image(uploaded_img)
        if not vision_struct.get("ok"):
            status.update(label="❌ 持仓识别失败", state="error", expanded=False)
            err_msg = vision_struct.get("error", "未识别到有效持仓")
            st.warning(f"持仓识别失败：{err_msg}")
            return

        positions = vision_struct.get("positions", [])
        if not positions:
            status.update(label="❌ 未识别到有效持仓", state="error", expanded=False)
            st.warning("未识别到有效持仓数据，请换一张更清晰的截图后重试。")
            return

        task_manager = TaskManager()
        try:
            task_id = task_manager.create_portfolio_task(
                user_id=current_user,
                positions=positions,
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
            "positions_count": len(positions),
        }
        status.update(label="✅ 已自动提交持仓体检任务", state="complete", expanded=False)
        st.toast(f"持仓体检任务已启动（识别到 {len(positions)} 只股票）")
        st.rerun()


# ==========================================
#  5. 核心逻辑处理函数 [修改点：封装成函数以便复用]
# ==========================================
def process_user_input(prompt_text):
    """处理用户输入（无论是来自输入框还是快捷卡片）"""

    # --- 1. 图片识别逻辑 (保留) ---
    image_context = ""
    current_uploader_key = f"portfolio_uploader_{st.session_state.uploader_key}"
    uploaded_image = st.session_state.get(current_uploader_key)

    if uploaded_image:
        with st.status("📸 正在识别持仓截图...", expanded=True) as status:
            st.write("AI 正在观察图片...")
            vision_result = analyze_financial_image(uploaded_image)
            status.update(label="✅ 图片识别完成", state="complete", expanded=False)
            image_context = f"\n\n【用户上传图信息】：\n{vision_result}\n----------------\n"
            with st.chat_message("ai"):
                st.caption(f"已识别图片内容：\n{vision_result[:100]}...")

    current_user = st.session_state.get('user_id', "访客")
    # 在追加当前问题前构造上下文，避免本轮内容混进历史
    context_payload = build_context_payload(prompt_text=prompt_text, current_user=current_user)

    # --- 2. 显示用户提问 (保留) ---
    st.session_state.messages.append({"role": "user", "content": prompt_text})

    # 构造最终 Prompt
    final_prompt = image_context + prompt_text

    # --- 3. 极速伪路由检查（默认关闭，可通过环境变量显式开启） ---
    if FAST_ROUTER_ENABLED:
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
    task_manager = TaskManager()

    # 准备历史消息
    history_msgs = st.session_state.messages[:-1] if len(st.session_state.messages) > 1 else []
    recent_history = history_msgs[-4:] if len(history_msgs) > 4 else history_msgs
    history_for_task = [{"role": msg["role"], "content": msg["content"]} for msg in recent_history]

    # 提交后台任务
    task_id = task_manager.create_task(
        user_id=current_user,
        prompt=final_prompt,
        image_context=image_context,
        risk_preference=risk,
        history_messages=history_for_task,
        context_payload=context_payload,
        has_portfolio=has_portfolio
    )

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
    st.session_state.pending_task = {
        "task_id": task_id,
        "prompt": final_prompt,
        "image_context": image_context,
        "risk": risk,
        "context_payload": context_payload,
        "start_time": time.time()
    }


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
        st.button("创业板技术面如何？",
                     use_container_width=True,
                     on_click=set_prompt_callback,
                     args=("创业板技术面分析下",)
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
                                    st.success("验证码已发送，请注意查收")
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
                        success, msg = auth.register_with_username_phone(
                            final_username,
                            final_password,
                            final_phone,
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
            # 1. 使数据库中的 token 失效（只删当前设备，不影响其他设备）
            if user != "访客":
                auth.logout_user(user, st.session_state.get("token"))
                try:
                    tm = TaskManager()
                    tm.clear_user_pending_task(user)
                    tm.clear_user_pending_portfolio_task(user)
                except Exception as e:
                    print(f"清理待处理任务失败: {e}")

            # 2. 清除 session state
            st.session_state['is_logged_in'] = False
            st.session_state['user_id'] = None
            st.session_state['just_logged_out'] = True
            st.session_state['pending_task'] = None
            st.session_state['pending_portfolio_task'] = None
            st.session_state['portfolio_last_attempt_hash'] = None
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
                st.session_state.conversation_id = str(uuid.uuid4())
                # 🔥 [修改] 清空上传的图片，通过增加 key 计数器实现
                st.session_state.uploader_key += 1
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
# 只在用户登录后显示公告
if st.session_state.get('is_logged_in', False) and ENABLE_HOME_ANNOUNCEMENT:
    check_and_show_announcement()

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
                    mem.save_interaction(current_user, "自动持仓体检", retrieval_text or summary)
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
# 🔥 [新增] 任务恢复机制（正确位置）
# ==========================================
if "pending_task" in st.session_state and st.session_state.pending_task:
    task_info = st.session_state.pending_task
    task_id = task_info["task_id"]
    task_start = task_info["start_time"]

    # 获取当前用户
    current_user = st.session_state.get('user_id', "访客")

    # 检查任务是否超时（30分钟）
    if time.time() - task_start < 1800:
        with st.container():
            status_placeholder = st.empty()
            content_placeholder = st.empty()

            # 查询任务状态
            task_manager = TaskManager()
            task_status = task_manager.get_task_status(task_id)
            current_status = task_status["status"]

            if current_status in ["pending", "processing"]:
                progress_msg = task_status.get("progress", "正在处理...")
                elapsed_sec = int(max(0, time.time() - task_start))
                phase_steps = [
                    ("🛰️ 正在检索市场数据", "读取行情、新闻与历史上下文"),
                    ("🧠 正在进行策略推理", "多模型协作评估方向与风险"),
                    ("🧪 正在校验关键结论", "交叉检查数据一致性与边界条件"),
                    ("📝 正在整理最终回答", "生成结构化结论与可执行建议"),
                ]
                phase_idx = (elapsed_sec // 6) % len(phase_steps)
                phase_title, phase_desc = phase_steps[phase_idx]
                status_placeholder.markdown(f"""
                <style>
                .thinking-wrap {{
                    background: linear-gradient(135deg, rgba(15, 23, 42, 0.92), rgba(30, 58, 138, 0.92));
                    border: 1px solid rgba(148, 163, 184, 0.28);
                    border-radius: 12px;
                    padding: 14px 16px;
                    color: #e2e8f0;
                    margin-bottom: 8px;
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
                    color: #cbd5e1;
                }}
                .thinking-phase {{
                    margin-top: 8px;
                    color: #bfdbfe;
                    font-size: 13px;
                    font-weight: 600;
                }}
                .thinking-meta {{
                    margin-top: 4px;
                    color: #94a3b8;
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
                    background: #93c5fd;
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
                        🚀 团队正在协作分析
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
                """, unsafe_allow_html=True)

                with content_placeholder.container():
                    st.caption("正在后台持续处理，完成后会自动返回结果。")

                # 自动轮询任务状态，避免用户手动点击刷新
                time.sleep(1.5)
                st.rerun()

            elif current_status == "success":
                # 任务完成，显示结果
                status_placeholder.empty()
                result = task_status.get("result")

                if result and isinstance(result, dict):
                    final_response_md = result.get("response", "")
                    final_img_path = result.get("chart", "")
                    attachments = result.get("attachments", [])

                    if not final_response_md:
                        final_response_md = "抱歉，AI 分析未返回有效结果。"

                    # 渲染图表
                    if final_img_path:
                        try:
                            render_chart_by_filename(final_img_path)
                        except Exception as e:
                            print(f"图表渲染失败: {e}")

                    # 清理图片标签
                    final_response_md = clean_chart_tag(final_response_md)

                    # 打字机效果（优化：批量更新）
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

                            # 🔥 [修复] 批量更新，减少 WebSocket 消息
                            if len(final_response_md) > 800:
                                # 长文本：每 50 个字符更新一次
                                update_interval = 100
                                chars = list(final_response_md)

                                for i in range(0, len(chars), update_interval):
                                    chunk = ''.join(chars[i:i+update_interval])
                                    full_response += chunk
                                    placeholder.markdown(full_response + "▌", unsafe_allow_html=True)
                                    time.sleep(0.05)  # 每批次延迟 50ms

                                placeholder.markdown(full_response, unsafe_allow_html=True)
                            else:
                                # 短文本：正常打字机效果
                                delay_time = 0.01

                                for char in stream_text_generator(final_response_md, delay=delay_time):
                                    full_response += char
                                    placeholder.markdown(full_response + "▌", unsafe_allow_html=True)

                                placeholder.markdown(full_response, unsafe_allow_html=True)

                    if attachments:
                        with content_placeholder.container():
                            render_knowledge_attachments(attachments, exclude_indices=inline_state["used_indices"])

                    # 存入历史
                    message_data = {
                        "role": "ai",
                        "content": final_response_md,
                        "chart": final_img_path,
                        "attachments": attachments,
                    }
                    st.session_state.messages.append(message_data)

                    # 保存记忆
                    if current_user != "访客":
                        try:
                            memory_record = _build_memory_record(final_response_md)
                            mem.save_interaction(current_user, task_info["prompt"], memory_record)
                        except Exception as e:
                            print(f"记忆存储失败: {e}")

                # 清除待处理任务
                del st.session_state.pending_task

                # 🔥 [新增] 同时清除 Redis 中的待处理任务
                task_manager.clear_user_pending_task(current_user)

                time.sleep(1)
                st.rerun()  # 刷新页面

            elif current_status == "error":
                # 任务失败
                status_placeholder.error("❌ 分析失败")
                error_msg = task_status.get("error", "未知错误")
                content_placeholder.error(f"抱歉，分析过程出现问题：{error_msg[:100]}")

                # 清除待处理任务
                del st.session_state.pending_task

                # 🔥 [新增] 同时清除 Redis 中的待处理任务
                task_manager.clear_user_pending_task(current_user)

                time.sleep(2)
                st.rerun()
    else:
        # 超时，清除任务
        st.warning("⏱️ 任务超时，请重新提问")
        del st.session_state.pending_task

        # 🔥 [新增] 同时清除 Redis 中的待处理任务
        current_user = st.session_state.get('user_id', "访客")
        if current_user != "访客":
            task_manager = TaskManager()
            task_manager.clear_user_pending_task(current_user)

        st.rerun()



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
                                系统将自动识别持仓并启动体检任务，完成后可在“持仓体检”页面查看图像化结果。
                            </div>
                            """, unsafe_allow_html=True)
                auto_submit_portfolio_task(uploaded_img)
        else:
            st.session_state.portfolio_last_attempt_hash = None

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
    font-weight: 800 !important;
    text-shadow: 0 1px 2px rgba(0,0,0,0.55) !important;
}
</style>
""", unsafe_allow_html=True)

# D. 底部输入框 (Sticky Footer) [修改点：使用 st.chat_input]
if prompt := st.chat_input("我受过交易汇训练，欢迎问我任何实战交易问题..."):
    if not st.session_state['is_logged_in']:
        st.warning("🔒 请先在左侧侧边栏登录")
    else:
        process_user_input(prompt)
        st.rerun()  # 确保界面更新

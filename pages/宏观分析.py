import streamlit as st
import plotly.graph_objects as go
from macro_data import get_china_us_spread, get_gold_silver_ratio, get_cpi_ppi_data
import pandas as pd
from fed_data import get_fed_probabilities
import plotly.express as px
import os
import sys
from macro_data import get_dashboard_metrics
st.set_page_config(page_title="宏观全景", layout="wide")

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)

# 加载 CSS (注意路径)
css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

st.markdown("""
<style>
    /* 全局纯黑背景 */
    .stApp {
        background-color: #0b1121 !important;
        font-family: 'JetBrains Mono', 'Courier New', monospace;
    }

    /* --- Header & 侧边栏按钮交互修复 --- */

    /* 1. Header 背景透明 */
    header[data-testid="stHeader"] {
        background-color: transparent !important;
        border-bottom: none !important;
        pointer-events: none;
    }

    /* 2. 让按钮区域可点击 */
    header[data-testid="stHeader"] button,
    [data-testid="stSidebarCollapsedControl"] {
        pointer-events: auto !important;
    }

    /* 3. 【核心修复】左上角展开箭头的交互样式 */

    /* === 默认状态 === */
    [data-testid="stSidebarCollapsedControl"] {
        background-color: rgba(255, 255, 255, 0.1) !important; /* 微弱背景 */
        border: 1px solid rgba(255, 255, 255, 0.3) !important; /* 可见边框 */
        border-radius: 8px !important;
        color: #ffffff !important;
        transition: all 0.3s ease !important; /* 添加平滑过渡动画 */
        z-index: 999999 !important;
    }
    /* 默认尝试让箭头变白 */
    [data-testid="stSidebarCollapsedControl"] svg {
        fill: #ffffff !important;
        color: #ffffff !important;
    }

    /* === 鼠标悬停状态 (Hover) === */
    /* 当鼠标移上去时，背景变白，箭头变黑 */
    [data-testid="stSidebarCollapsedControl"]:hover {
        background-color: #ffffff !important; /* 背景亮白 */
        box-shadow: 0 0 15px rgba(255,255,255, 0.8) !important; /* 发光效果 */
        border-color: #ffffff !important;
        transform: scale(1.1); /* 微微放大 */
    }

    /* 悬停时强制箭头变黑 (利用滤镜反转，或者直接改 fill) */
    [data-testid="stSidebarCollapsedControl"]:hover svg {
        fill: #000000 !important; 
        color: #000000 !important;
        filter: brightness(0) !important; /* 强制变全黑 */
    }

    /* 右上角的菜单按钮也加同样的特效 */
    header[data-testid="stHeader"] button[data-testid="baseButton-headerNoPadding"]:hover {
        background-color: rgba(255,255,255,0.2) !important;
        border-radius: 50%;
    }

    /* 隐藏装饰条 */
    header[data-testid="stHeader"] .stAppDeployButton { display: none; }

    /* --- 折叠栏 (Expander) --- */
    div[data-testid="stExpander"] details summary p {
        color: #ffffff !important;
        font-size: 16px !important;
        font-weight: bold !important;
    }
    div[data-testid="stExpander"] details {
        border: 1px solid #333;
        border-radius: 5px;
        background-color: #111; 
        margin-bottom: 10px;
    }
    div[data-testid="stExpander"] svg {
        fill: #ffffff !important;
        color: #ffffff !important;
    }
    div[data-testid="stExpander"] div[role="group"] {
        background-color: #000000 !important;
        padding: 10px;
    }

    /* 狙击手卡片 */
    .sniper-card {
        background-color: #111111;
        border: 1px solid #333;
        border-radius: 8px;
        padding: 20px;
        margin-bottom: 15px;
        text-align: center;
    }
    .sniper-label { color: #888; font-size: 12px; margin-bottom: 5px; }
    .sniper-value { color: #fff; font-size: 32px; font-weight: 900; }
    .sniper-delta { font-size: 14px; font-weight: bold; margin-top: 5px; padding: 2px 8px; border-radius: 4px; display: inline-block; }

    /* 状态条 */
    .status-bar {
        padding: 15px; text-align: center; font-weight: 900; font-size: 24px; color: black;
        margin-bottom: 30px; margin-top: -50px; margin-left: -5rem; margin-right: -5rem;
    }
    @media (max-width: 640px) { .sniper-value { font-size: 28px; } .status-bar { font-size: 18px; margin-left: -1rem; margin-right: -1rem; } }
</style>
""", unsafe_allow_html=True)

# --- 数据获取 ---
metrics = get_dashboard_metrics()
default_val = {'value': '-', 'delta': '0'}
m_spread = metrics.get('spread', default_val)
m_gs = metrics.get('gs_ratio', default_val)
m_ppi = metrics.get('ppi', default_val)
m_dxy = metrics.get('dxy', default_val)

# --- 市场状态判断 ---
try:
    spread_val = float(m_spread['value'].replace(' BP', ''))
    dxy_val = float(m_dxy['value'])
    if spread_val > -150 or dxy_val < 100:
        market_status = "⚠️ 美元指数弱 (利多宏观商品)"
        status_color = "#ff4d4d"
    else:
        market_status = "🚀 美元指数强 (利空宏观商品)"
        status_color = "#00e676"
except:
    market_status = "🔍 ANALYZING..."
    status_color = "#333"

st.markdown(f"""<div class="status-bar" style="background-color: {status_color};">{market_status}</div>""",
            unsafe_allow_html=True)


# --- 卡片渲染函数 ---
def render_sniper_card(label, value, delta, inverse=False):
    try:
        delta_val = float(delta.replace('%', '').replace(' BP', ''))
    except:
        delta_val = 0
    if delta_val > 0:
        color = "#ff4d4d" if inverse else "#00e676"
        bg = "rgba(255, 77, 77, 0.1)" if inverse else "rgba(0, 230, 118, 0.1)"
        arrow = "▲"
    elif delta_val < 0:
        color = "#00e676" if inverse else "#ff4d4d"
        bg = "rgba(0, 230, 118, 0.1)" if inverse else "rgba(255, 77, 77, 0.1)"
        arrow = "▼"
    else:
        color = "#888";
        bg = "#222";
        arrow = "-"
    return f"""<div class="sniper-card"><div class="sniper-label">{label}</div><div class="sniper-value">{value}</div><div class="sniper-delta" style="color: {color}; background: {bg};">{arrow} {delta}</div></div>"""


c1, c2, c3, c4 = st.columns(4)
with c1: st.markdown(render_sniper_card("中美国债利差", m_spread['value'], m_spread['delta'], inverse=False),
                     unsafe_allow_html=True)
with c2: st.markdown(render_sniper_card("美元指数", m_dxy['value'], m_dxy['delta'], inverse=True),
                     unsafe_allow_html=True)
with c3: st.markdown(render_sniper_card("金银比", m_gs['value'], m_gs['delta'], inverse=True),
                     unsafe_allow_html=True)
with c4: st.markdown(render_sniper_card("PPI同比", m_ppi['value'], m_ppi['delta'], inverse=False),
                     unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# --- 图表区 (V2.4 修复版) ---

# 1. 中美利差 (双轴 + 三条线)
with st.expander("📉 中美利率与利差 (点击展开)", expanded=True):
    df_spread = get_china_us_spread()
    if not df_spread.empty:
        fig = go.Figure()

        # 1. 阴影背景：利差 (右轴)
        fig.add_trace(go.Scatter(
            x=df_spread['date'], y=df_spread['spread'],
            name='利差(BP)', yaxis='y2',
            fill='tozeroy', line=dict(width=0), opacity=0.3
        ))

        # 2. 红线：中国国债 (左轴)
        fig.add_trace(go.Scatter(
            x=df_spread['date'], y=df_spread['cn_10y'],
            name='中国10Y', line=dict(color='#ff4d4d', width=2), yaxis='y1'
        ))

        # 3. 蓝线：美国国债 (左轴)
        fig.add_trace(go.Scatter(
            x=df_spread['date'], y=df_spread['us_10y'],
            name='美国10Y', line=dict(color='#2962ff', width=2), yaxis='y1'
        ))

        fig.update_layout(
            title=dict(text="中美10年期国债收益率 & 利差", font=dict(color='white')),
            paper_bgcolor='black', plot_bgcolor='black',
            margin=dict(l=10, r=10, t=40, b=10),
            height=300,
            xaxis=dict(showgrid=False, gridcolor='#333'),

            # 🟢 修复：正确的 Axis Title 写法 (解决 Bad property path)
            yaxis=dict(
                title=dict(text="收益率(%)", font=dict(color='white')),
                showgrid=True, gridcolor='#222',
                tickfont=dict(color='white')
            ),

            # 🟢 修复：右轴写法
            yaxis2=dict(
                title=dict(text="利差(BP)", font=dict(color='gray')),
                overlaying='y', side='right', showgrid=False,
                tickfont=dict(color='gray')
            ),

            legend=dict(orientation="h", y=1.1, font=dict(color='white')),
            font=dict(color='white')
        )
        # 🟢 修复：st.plotly 改回 st.plotly_chart
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})

# 2. 金银比
with st.expander("⚖️ 金银比 (点击展开)", expanded=False):
    df_gs = get_gold_silver_ratio()
    if not df_gs.empty and 'date' in df_gs.columns:
        df_gs['date'] = pd.to_datetime(df_gs['date'].astype(str))
        fig_gs = go.Figure()
        fig_gs.add_trace(go.Scatter(x=df_gs['date'], y=df_gs['ratio'], line=dict(color='#FFD700', width=2)))
        fig_gs.update_layout(
            title=dict(text="Gold/Silver Ratio", font=dict(color='white')),
            paper_bgcolor='black', plot_bgcolor='black',
            margin=dict(l=0, r=0, t=30, b=0), height=250,
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor='#222'),
            font=dict(color='white')
        )
        st.plotly_chart(fig_gs, use_container_width=True, config={'displayModeBar': False})

# 3. 通胀剪刀差
with st.expander("🏭 通胀剪刀差 (点击展开)", expanded=False):
    df_cpi = get_cpi_ppi_data()
    if not df_cpi.empty:
        fig_inf = go.Figure()
        fig_inf.add_trace(go.Scatter(x=df_cpi['date'], y=df_cpi['ppi_yoy'], name='PPI', line=dict(color='#2962ff')))
        fig_inf.add_trace(go.Scatter(x=df_cpi['date'], y=df_cpi['cpi_yoy'], name='CPI', line=dict(color='#ff0055')))
        fig_inf.update_layout(
            title=dict(text="CPI vs PPI", font=dict(color='white')),
            paper_bgcolor='black', plot_bgcolor='black',
            margin=dict(l=0, r=0, t=30, b=0), height=250,
            xaxis=dict(showgrid=False), yaxis=dict(gridcolor='#222'),
            font=dict(color='white'),
            legend=dict(orientation="h", y=1.1, font=dict(color='white'))
        )
        st.plotly_chart(fig_inf, use_container_width=True, config={'displayModeBar': False})

# 4. 美联储预测
with st.expander("🏦 美联储降息预测 (CME)", expanded=False):
    df_fed = get_fed_probabilities()
    if df_fed is not None and not df_fed.empty:
        next_meeting = df_fed['会议日期'].iloc[0]
        df_next = df_fed[df_fed['会议日期'] == next_meeting]
        fig_fed = px.bar(df_next, x='目标利率', y='概率(%)', text='概率(%)',
                         color_discrete_sequence=['#00e676'])
        fig_fed.update_layout(
            title=dict(text=f"Next Meeting: {next_meeting}", font=dict(color='white')),
            paper_bgcolor='black', plot_bgcolor='black',
            font=dict(color='white'),
            yaxis=dict(showgrid=False), xaxis=dict(showgrid=False),
            height=250, margin=dict(l=0, r=0, t=30, b=0)
        )
        st.plotly_chart(fig_fed, use_container_width=True, config={'displayModeBar': False})

st.markdown(
    "<div style='text-align: center; color: #444; font-size: 12px; margin-top: 50px;'>QUANTLAB MACRO SYSTEM v2.4</div>",
    unsafe_allow_html=True)
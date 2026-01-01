import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import fund_flow_tools as fft
import numpy as np

# 1. 配置与样式
st.set_page_config(
    page_title="爱波塔-股票资金分析",
    page_icon="favicon.ico",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    [data-testid="stDecoration"] { display: none; }
    header[data-testid="stHeader"] { background-color: transparent !important; }
    .block-container { padding-top: 1.5rem !important; }
    .stApp { background-color: #0b1121 !important; color: white !important; }
    .stApp p, .stApp span, .stApp div, .stApp li, .stApp h1, .stApp h2, .stApp h3 { color: #e6e6e6 !important; }
    /* 侧边栏复原 */
    [data-testid="stSidebar"] { background-color: #0f172a !important; border-right: 1px solid #1e293b; }
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] div { color: #cbd5e1 !important; }
    /* 控件颜色强制修复 */
    div[data-baseweb="select"] > div, div.stButton > button { background-color: #1E2329 !important; color: white !important; border: 1px solid #31333F !important; }
    div[data-baseweb="popover"], div[data-baseweb="menu"] { background-color: #1E2329 !important; border: 1px solid #31333F !important; }
    div[data-baseweb="menu"] li div { color: #e6e6e6 !important; }
    div[data-baseweb="menu"] li:hover { background-color: #3b82f6 !important; }
</style>
""", unsafe_allow_html=True)

# 2. 顶部控制区
col_title, col_type, col_sel, col_time = st.columns([1.5, 1, 1.2, 0.8])

with col_title:
    st.title("资金流监控")

with col_type:
    # --- 🔥 新增：板块类型切换 ---
    # 默认选"行业"，因为它更清爽，符合您的需求
    sector_type = st.radio("板块视角", ["行业 (清爽)", "概念 (细分)"], horizontal=True, index=0)
    # 提取真实值 '行业' 或 '概念'
    real_sector_type = sector_type.split(" ")[0]

with col_sel:
    FLOW_MAP = {
        "🔴 大单资金": "main_net_inflow",
        "🟡 中单资金": "medium_net_inflow",
        "🟢 小单资金": "small_net_inflow",
        "🔵 全市场资金": "main_net_inflow + medium_net_inflow + small_net_inflow"
    }
    selected_flow_label = st.selectbox("资金类型", list(FLOW_MAP.keys()), index=0)
    selected_flow_col = FLOW_MAP[selected_flow_label]

# 获取数据 (传入 sector_type)
df_in, df_out, update_time = fft.get_sector_ranking(10, flow_col=selected_flow_col, sector_type=real_sector_type)

with col_time:
    if update_time:
        st.markdown(f'<div style="text-align: right; padding-top: 25px; color: #3b82f6;">📅 {update_time}</div>',
                    unsafe_allow_html=True)

st.divider()

# 3. 轮动四象限
st.subheader(f"🎯 {real_sector_type}轮动 - {selected_flow_label}")

with st.container():
    st.markdown(
        '<div style="background: rgba(255,255,255,0.03); padding: 15px; border-radius: 10px; margin-bottom: 20px;">',
        unsafe_allow_html=True)
    c1, c2, c3, c4, c5 = st.columns([1.2, 1.2, 2, 2, 1.5])
    with c1: trend_days = st.selectbox("📅 沉淀周期", [5, 10, 20, 30], index=1)
    with c2: attack_days = st.selectbox("🚀 攻击周期", [1, 3, 5, 10], index=0)
    with c3: filter_status = st.multiselect("🔍 筛选", ["双红 (共识买入)", "反转 (底部承接)", "分歧 (获利了结)",
                                                       "双绿 (加速卖出)"],
                                            default=["双红 (共识买入)", "反转 (底部承接)"])
    with c4: top_n_slider = st.slider("📉 密度", 10, 200, 50)
    with c5:
        st.markdown("<br>", unsafe_allow_html=True)
        show_all_labels = st.checkbox("显示标签", value=False)
    st.markdown('</div>', unsafe_allow_html=True)

df_bubble = fft.get_sector_rotation_data(trend_days, attack_days, flow_col=selected_flow_col,
                                         sector_type=real_sector_type)

if not df_bubble.empty:
    threshold_size = df_bubble['bubble_size'].quantile(0.8)
    threshold_y = df_bubble['attack_rate'].quantile(0.9)
    if filter_status:
        df_display = df_bubble[df_bubble['status'].isin(filter_status)]
    else:
        df_display = df_bubble.copy()
    df_display = df_display.sort_values('bubble_size', ascending=False).head(top_n_slider)


    def smart_label(row):
        if show_all_labels: return row['industry']
        if row['bubble_size'] > threshold_size or row['attack_rate'] > threshold_y: return row['industry']
        return None


    df_display['label'] = df_display.apply(smart_label, axis=1)

    fig = px.scatter(
        df_display, x="period_net_inflow", y="attack_rate", size="bubble_size",
        color="avg_pct_change", text="label", hover_name="industry",
        color_continuous_scale="RdYlGn_r",
        labels={"period_net_inflow": f"{trend_days}日净流入(万)", "attack_rate": f"{attack_days}日攻击度(%)",
                "bubble_size": "成交额"},
        title=None
    )
    fig.update_layout(template="plotly_dark", height=600, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)')
    fig.add_hline(y=0, line_dash="dash", line_color="gray")
    fig.add_vline(x=0, line_dash="dash", line_color="gray")
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info(f"⏳ 暂无【{real_sector_type}】数据，请运行 update_sector_flow.py 补全数据。")

# 4. 趋势透视
st.divider()
all_sectors = fft.get_all_sectors(sector_type=real_sector_type)
default_idx = 0
if not df_in.empty and df_in.iloc[0]['industry'] in all_sectors:
    default_idx = all_sectors.index(df_in.iloc[0]['industry'])

header_placeholder = st.empty()
col_sel, col_chart = st.columns([1, 4])
with col_sel: target_sector = st.selectbox(f"选择{real_sector_type}", options=all_sectors, index=default_idx)
header_placeholder.subheader(f"📈 趋势透视 - {target_sector}")

with col_chart:
    if target_sector:
        df_trend = fft.get_sector_trend_data(target_sector, days=60)
        if not df_trend.empty:
            df_trend['date_display'] = df_trend['trade_date'].dt.strftime('%Y-%m-%d')
            if "+" in selected_flow_col:
                y_data = df_trend['main_net_inflow'] + df_trend['medium_net_inflow'] + df_trend['small_net_inflow']
            else:
                y_data = df_trend[selected_flow_col]
            y_cumsum = y_data.cumsum()
            colors = ['#ff4d4d' if x > 0 else '#2ecc71' for x in y_data]

            fig_trend = go.Figure()
            fig_trend.add_trace(go.Bar(x=df_trend['date_display'], y=y_data, marker_color=colors, name='每日净流入'))
            fig_trend.add_trace(go.Scatter(x=df_trend['date_display'], y=y_cumsum, mode='lines', name='累计趋势',
                                           line=dict(color='#ffd700', width=3), yaxis='y2'))
            fig_trend.update_layout(
                template="plotly_dark", paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                yaxis=dict(title="净流入(万)", showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
                yaxis2=dict(title="累计趋势", overlaying='y', side='right', showgrid=False, title_font_color="#ffd700",
                            tickfont_color="#ffd700"),
                legend=dict(orientation="h", y=1.1, font=dict(color="white")), height=450
            )
            st.plotly_chart(fig_trend, use_container_width=True)


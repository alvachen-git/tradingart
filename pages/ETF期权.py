import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# 路径修复
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
sys.path.append(root_dir)
import data_engine as de

# 加载 CSS
css_path = os.path.join(root_dir, 'style.css')
with open(css_path, encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# --- 页面逻辑 ---
st.markdown('<div class="mobile-top-container">', unsafe_allow_html=True)
c1, c2 = st.columns([1, 2])
with c1:
    st.markdown("### 💹 **ETF 期权**")
with c2:
    target = st.selectbox("选择标的", ["510050 (50ETF)", "510300 (300ETF)", "510500 (500ETF)", "588000 (科创50ETF)",
                                       "159915 (创业板ETF)"], label_visibility="collapsed")
st.markdown('</div>', unsafe_allow_html=True)

etf_code = target.split(' ')[0]

# 数据获取
with st.spinner(f"正在扫描 {etf_code} 全市场持仓数据..."):
    df = de.get_etf_option_analysis(etf_code, days=20)

if df is None or df.empty:
    st.error("暂无数据，可能是非交易时间或 Tushare 接口受限。")
    st.stop()

# --- 【关键修复】健壮的日期处理 ---
try:
    # 1. 先统一转为字符串，去除可能的空格
    df['date'] = df['date'].astype(str).str.strip()

    # 2. 智能解析日期 (兼容 '20251121', '2025-11-21' 等多种格式)
    df['date_obj'] = pd.to_datetime(df['date'], errors='coerce')

    # 3. 再次格式化为我们想要的字符串 (用于表格显示)
    df['date_str'] = df['date_obj'].dt.strftime('%Y-%m-%d')

    # 4. 按时间排序
    df = df.sort_values('date_obj')

    # 5. 去除解析失败的坏数据
    df = df.dropna(subset=['date_obj'])

except Exception as e:
    st.error(f"日期处理出错: {e}")
    st.stop()

# --- 1. 核心洞察卡片 ---
latest_date = df['date_str'].max()  # 使用格式化后的字符串取最大值
latest_data = df[df['date_str'] == latest_date]

try:
    # 容错：如果某天只有认购或只有认沽，避免报错
    call_row = latest_data[latest_data['type'].str.contains('认购')]
    put_row = latest_data[latest_data['type'].str.contains('认沽')]

    call_strike = f"{call_row.iloc[0]['strike']:.3f}" if not call_row.empty else "N/A"
    call_oi = int(call_row.iloc[0]['oi']) if not call_row.empty else 0

    put_strike = f"{put_row.iloc[0]['strike']:.3f}" if not put_row.empty else "N/A"
    put_oi = int(put_row.iloc[0]['oi']) if not put_row.empty else 0

    st.info(f"📅 分析日期：**{latest_date}**")

    # 计算区间
    spread = f"{put_strike} ~ {call_strike}"

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">上方压力 (认购最大OI)</div>
            <div class="metric-value" style="color:#d32f2f">{call_strike}</div>
            <div class="metric-delta">持仓 {call_oi:,} 手</div>
        </div>
        """, unsafe_allow_html=True)

    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">下方支撑 (认沽最大OI)</div>
            <div class="metric-value" style="color:#2e7d32">{put_strike}</div>
            <div class="metric-delta">持仓 {put_oi:,} 手</div>
        </div>
        """, unsafe_allow_html=True)

    with col3:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">主力博弈区间</div>
            <div class="metric-value" style="font-size:1.2rem">{spread}</div>
            <div class="metric-delta delta-neu">多空分界</div>
        </div>
        """, unsafe_allow_html=True)

except Exception as e:
    st.warning(f"今日数据解析不完整: {e}")

st.markdown("---")

# --- 2. 趋势图表：防线移动 ---
st.subheader("📊 主力持仓防线移动 (近20日)")
st.caption(
    "观察期权最大持仓合约的移动。若红色线上移，代表压力上移（卖方看多）；若绿色线下移，代表支撑下移（卖方看空）。")

# 【关键修改】使用 date_obj (时间对象) 作为 X 轴，而不是字符串
# 这样 Plotly 会自动处理时间刻度，不会出现 2011 这种解析错误
fig = px.line(
    df,
    x='date_obj',
    y='strike',
    color='type',
    markers=True,
    symbol='type',
    title=f"{target} 压力与支撑位变动",
    color_discrete_map={"认购 (压力)": "#d32f2f", "认沽 (支撑)": "#2e7d32"}
)

# 优化图表布局
fig.update_layout(
    plot_bgcolor='white',
    xaxis_title="日期",
    yaxis_title="行权价",
    hovermode="x unified",
    height=400,
    xaxis=dict(
        showgrid=False,
        tickformat="%Y-%m-%d"  # 强制显示为 YYYY-MM-DD 格式
    ),
    yaxis=dict(showgrid=True, gridcolor='#eee')
)
st.plotly_chart(fig, use_container_width=True)

# --- 3. 详细数据 ---
with st.expander("查看详细数据表"):
    # 增加一列：持仓金额估算 (价格 * 持仓 * 10000)
    df['amt_est'] = df['price'] * df['oi'] * 10000 / 100000000  # 亿

    st.dataframe(
        df[['date_str', 'type', 'strike', 'oi', 'price', 'code']],
        column_config={
            "date_str": "日期",
            "type": "类型",
            "strike": st.column_config.NumberColumn("行权价", format="%.3f"),
            "oi": st.column_config.NumberColumn("持仓量(张)", format="%d"),
            "price": st.column_config.NumberColumn("期权价", format="%.4f"),
            "code": "合约代码"
        },
        use_container_width=True
    )
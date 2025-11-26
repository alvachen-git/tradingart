import streamlit as st
import pandas as pd
import os
import sys
import plotly.express as px


current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
import data_engine as de


# 1. 页面配置
st.set_page_config(
    page_title="Alpha 智能期货终端",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 加载 CSS
with open('style.css', encoding='utf-8') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# --- 首页内容 ---

st.title("📱 交易汇-情报局")
st.markdown("---")

# --- 外资动向卡片 ---
st.markdown("### 🌍 外资动向 (摩根/瑞银/乾坤)")

# 读库
try:
    # 获取最新日期
    latest_f_date = pd.read_sql("SELECT MAX(trade_date) FROM foreign_capital_analysis", de.engine).iloc[0, 0]

    if latest_f_date:
        df_foreign = pd.read_sql(f"SELECT * FROM foreign_capital_analysis WHERE trade_date='{latest_f_date}'",
                                 de.engine)

        if not df_foreign.empty:
            # 使用列布局展示卡片
            cols = st.columns(4)
            for i, row in df_foreign.iterrows():
                # 循环使用列
                with cols[i % 4]:
                    # --- 【新增】清洗機構名稱 ---
                    # 去除 (代客)、（代客）等後綴
                    cleaned_brokers = row['brokers'].replace('（代客）', '').replace('(代客)', '')

                    color = "#d32f2f" if row['direction'] == "做多" else "#2e7d32"

                    st.markdown(f"""
                                        <div class="metric-card" style="border-top: 3px solid {color};">
                                            <div class="metric-label">{row['symbol'].upper()}</div>
                                            <div class="metric-value" style="color:{color}">{row['direction']}</div>
                                            <div class="metric-delta" style="font-size:0.8rem; color:#888;">
                                               {cleaned_brokers} </div>
                                            <div style="font-size:0.8rem; margin-top:5px;">
                                               淨量: {int(row['total_net_vol']):,}
                                            </div>
                                        </div>
                                        """, unsafe_allow_html=True)
        else:
            st.info("今日外资无明显共振操作。")
    else:
        st.info("暂无外资分析数据，请运行 calc_foreign_capital.py。")

except Exception as e:
    st.error(f"读取外资数据失败: {e}")

st.markdown("---")

# --- 新增：多空巔峰對決 (Smart vs Dumb) ---
st.markdown("### ⚔️ 多空巅峰对决")
st.caption("筛选逻辑：机构与散户差异最大的持仓对比")

# 1. 獲取數據 (直接讀表)
try:
    # 檢查表裡是否有數據
    latest_c_date = pd.read_sql("SELECT MAX(trade_date) FROM market_conflict_daily", de.engine).iloc[0, 0]

    if latest_c_date:
        df_conflict = pd.read_sql(f"SELECT * FROM market_conflict_daily WHERE trade_date='{latest_c_date}'", de.engine)

        if not df_conflict.empty:
            # 創建 4 列佈局
            cols = st.columns(4)
            for i, row in df_conflict.iterrows():
                with cols[i % 4]:  # 防止超過4個報錯
                    # 顏色邏輯
                    direction = row['action']
                    color = "#d32f2f" if direction == "看漲" else "#2e7d32"  # 紅漲綠跌

                    # HTML 結構 (引用上面定義好的 CSS 類名)
                    card_html = f"""
        <div class="conflict-card" style="border-top: 4px solid {color};">
        <div class="conflict-header">
        <div class="conflict-symbol">{row['symbol'].upper()}</div>
        <div class="conflict-direction" style="color: {color};">{direction}</div>
        </div>
        <div class="conflict-body">
        <div class="conflict-item-left">
        <div class="conflict-label">反指(散户)</div>
        <div class="conflict-value" style="color: #333;">{int(row['dumb_net']):,}</div>
        </div>
        <div style="width: 1px; height: 20px; background-color: #ddd;"></div>
        <div class="conflict-item-right">
        <div class="conflict-label">正指(主力)</div>
        <div class="conflict-value" style="color: {color};">{int(row['smart_net']):,}</div>
        </div>
        </div>
        </div>
        """
                    st.markdown(card_html, unsafe_allow_html=True)
        else:
            st.info("今日市場平靜，無明顯正反博弈信號。")
    else:
        st.info("暫無對決數據，請運行後台計算腳本。")

except Exception as e:
    st.error(f"讀取對決數據失敗: {e}")

st.markdown("---")





# 2. 【新增】全市场风云榜
st.markdown("### 🏆 全品种盈亏排行榜")
st.caption("统计范围：近200天, (部分期货商亏损是因为做套保)")

# 获取数据
with st.spinner("正在扫描全市场数据..."):
    df_win, df_lose = de.get_cross_market_ranking(days=150, top_n=5)

if not df_win.empty:
    col_win, col_lose = st.columns(2)

    with col_win:

        st.markdown("**👑 盈利王 (Top 5)**")

        # 绘制条形图
        fig_win = px.bar(
            df_win.sort_values('score', ascending=True),  # 升序是为了让最大的在上面
            x='score', y='broker',
            orientation='h',
            text_auto='.0f',
            color='score',
            color_continuous_scale='Reds'
        )
        fig_win.update_layout(
            plot_bgcolor='white',
            margin=dict(l=0, r=0, t=0, b=0),
            height=200,
            xaxis=dict(showgrid=False, title=None),
            yaxis=dict(title=None),
            coloraxis_showscale=False  # 隐藏色条
        )
        st.plotly_chart(fig_win, use_container_width=True)


    with col_lose:

        st.markdown("**💸 亏损王 (Top 5)**")

        # 绘制条形图
        fig_lose = px.bar(
            df_lose.sort_values('score', ascending=False),  # 降序是为了让负分最大的在上面
            x='score', y='broker',
            orientation='h',
            text_auto='.0f',
            color='score',
            color_continuous_scale='Teal_r'  # 绿色系倒序
        )
        fig_lose.update_layout(
            plot_bgcolor='white',
            margin=dict(l=0, r=0, t=0, b=0),
            height=200,
            xaxis=dict(showgrid=False, title=None),
            yaxis=dict(title=None),
            coloraxis_showscale=False
        )
        st.plotly_chart(fig_lose, use_container_width=True)


else:
    st.warning("暂无足够数据进行全市场排名。")


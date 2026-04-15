import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import time
from datetime import datetime

# --- 配置 ---
st.set_page_config(page_title="行情判断录入后台", page_icon="👨‍⚖️")
st.title("👨‍⚖️ 专家观点录入系统")

DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'


# 连接数据库
@st.cache_resource
def get_engine():
    return create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


engine = get_engine()

# --- 1. 录入区域 ---
with st.container(border=True):
    st.subheader("📝 新增/修改 判断")

    col1, col2 = st.columns(2)
    with col1:
        # 日期选择 (默认今天)
        input_date = st.date_input("日期")
        date_str = input_date.strftime('%Y%m%d')

    with col2:
        # 品种选择 (扩充了列表)
        symbol = st.selectbox("品种",
                              ["lc (碳酸锂)", "si (工业硅)", "ps(多晶硅)","ih (上证50)", "if (沪深300)", "ic (中证500)","im (中证1000)", "au (黄金)",
            "ag (白银)","铜 (cu)","铝 (al)","锌 (zn)", "lh (生猪)", "i (铁矿石)","fg (玻璃)", "sa (纯碱)","p (棕榈油)",
            "ma (甲醇)", "cf (棉花)", "sr (白糖)"])
        ts_code = symbol.split(" ")[0]

    # 分数选择
    score_map = {
        2: "🔥🔥 看大涨 (+2)",
        1: "🔥 看小涨 (+1)",
        0: "👀 方向不明 (0)",
        -1: "💧 看小跌 (-1)",
        -2: "🌊 看大跌 (-2)"
    }
    # 默认选0
    score_val = st.select_slider("您对后市的判断：", options=[2, 1, 0, -1, -2], value=0,
                                 format_func=lambda x: score_map[x])

    # 理由输入 (提示选填)
    reason = st.text_area("判断理由 (选填，给客户看的话术)", placeholder="例如：主力大幅加仓多单...（不填则默认为空）")

    # 提交按钮
    if st.button("💾 保存/更新判断", type="primary"):
        # --- 修改点：移除了 'if not reason' 的阻断检查 ---

        # 如果没填，给一个默认空字符串，防止 SQL 报错或逻辑问题
        final_reason = reason if reason else "（专家未提供详细理由）"

        try:
            with engine.connect() as conn:
                # 1. 先删旧的
                del_sql = text(
                    "DELETE FROM market_sentiment WHERE trade_date=:trade_date AND ts_code=:ts_code")
                conn.execute(del_sql, {"trade_date": date_str, "ts_code": ts_code})

                # 2. 插入新的 (使用 final_reason)
                insert_sql = text("""
                    INSERT INTO market_sentiment (trade_date, ts_code, score, reason)
                    VALUES (:trade_date, :ts_code, :score, :reason)
                """)
                conn.execute(insert_sql, {
                    "trade_date": date_str,
                    "ts_code": ts_code,
                    "score": score_val,
                    "reason": final_reason
                })
                conn.commit()

            st.success(f"成功保存！{date_str} {ts_code} -> {score_map[score_val]}")
            time.sleep(1)  # 刷新一下
            st.rerun()

        except Exception as e:
            st.error(f"保存失败: {e}")

# --- 2. 历史数据查看 ---
st.divider()
st.subheader("📚 历史判断记录")

try:
    # 读取最近 20 条记录
    df = pd.read_sql("SELECT * FROM market_sentiment ORDER BY trade_date DESC LIMIT 20", engine)
    if not df.empty:
        # 美化显示
        def color_score(val):
            if val > 0:
                return 'color: red'
            elif val < 0:
                return 'color: green'
            return 'color: gray'


        st.dataframe(
            df.style.map(color_score, subset=['score']),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("暂无记录，请在上方录入。")
except Exception as e:
    st.error(f"读取历史失败: {e}")

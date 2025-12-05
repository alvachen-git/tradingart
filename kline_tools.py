import pandas as pd
import os
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
import symbol_map

# --- 1. 独立初始化 ---
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

@st.cache_resource
def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()




# --- 2. 核心工具定义 ---

@tool
def analyze_kline_pattern(query: str):
    """
    【K线形态计算器】
    根据用户输入的商品，分析其最近的 K 线形态，判断多空方向。
    包含：单根K线形状（大阳/大阴/影线）、吞噬形态、趋势强弱、是否有转折K线。
    当用户询问“技术面怎么看”、“走势如何”、“K线形态”时，**必须**调用此工具。
    """
    if engine is None: return "数据库连接失败"
    if not query: return "请输入有效的品种名称或代码。"  # 【新增】空值拦截

    # 1. 智能解析名称
    # 【核心修复】安全拆包
    result = symbol_map.resolve_symbol(query)
    if not result or result[0] is None:
        return f"未找到与'{query}'相关的品种，请尝试输入全名。"

    symbol, asset_type = result


    # 1. 容错处理
    clean_symbol = ''.join([i for i in symbol if not i.isdigit()])
    target_code_1 = f"{clean_symbol}0"
    target_code_2 = clean_symbol

    try:
        # 2. 获取数据
        if asset_type == 'stock':
            sql = f"""
                SELECT trade_date, open_price, high_price, low_price, close_price 
                FROM stock_price
                WHERE ts_code='{symbol}' 
                ORDER BY trade_date DESC LIMIT 60
            """
            df = pd.read_sql(sql, engine)

        elif asset_type == 'future':
            sql = f"""
                SELECT trade_date, open_price, high_price, low_price, close_price 
                FROM futures_price
                WHERE ts_code='{target_code_1}' OR ts_code='{target_code_2}'
                ORDER BY trade_date DESC LIMIT 60
            """
            df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到品种 {symbol} 的历史价格数据，无法进行技术分析。"

        # 3. 数据预处理
        df = df.sort_values('trade_date').reset_index(drop=True)
        for col in ['open_price', 'high_price', 'low_price', 'close_price']:
            df[col] = pd.to_numeric(df[col])

        # 4. 计算均线
        df['MA5'] = df['close_price'].rolling(window=5).mean()
        df['MA10'] = df['close_price'].rolling(window=10).mean()
        df['MA20'] = df['close_price'].rolling(window=20).mean()
        df['MA60'] = df['close_price'].rolling(window=60).mean()

        if len(df) < 2: return "数据不足，无法分析趋势。"

        # 提取今日和昨日数据
        curr = df.iloc[-1]
        prev = df.iloc[-2]
        pprev = df.iloc[-3]

        date = curr['trade_date']
        close = curr['close_price']
        open_p = curr['open_price']
        high = curr['high_price']
        low = curr['low_price']

        # 昨日和前日数据
        prev_close = prev['close_price']
        pprev_close = pprev['close_price']
        prev_open = prev['open_price']
        pprev_open = pprev['open_price']

        # --- 5. 形态识别逻辑 (核心算法) ---

        # A. 基础计算
        body_size = abs(close - open_p)
        upper_shadow = high - max(close, open_p)
        lower_shadow = min(close, open_p) - low
        total_range = high - low if high != low else 0.01

        body_pct = body_size / total_range
        upper_pct = upper_shadow / body_size
        lower_pct = lower_shadow / body_size

        # 今日涨跌幅
        chg_pct = (close - prev_close) / prev_close

        # 昨日实体涨跌幅 (用于判断大阴/大阳)
        prev_chg_pct = (prev_close - prev_open) / prev_open

        # B. K线形态判断列表
        patterns = []

        # --- 【新增】吞噬形态 (Engulfing Pattern) ---

        # 1. 多头吞噬 (Bullish Engulfing)
        # 条件：空头趋势(5<20) + 昨日大跌(>2%) + 今日大涨 + 包住昨日
        if (curr['MA5'] < curr['MA20']) and \
                (prev_chg_pct < -0.02) and \
                (chg_pct > 0) and \
                (open_p < prev_close) and \
                (close > prev_open):
            patterns.append("【多头吞噬】(空头趋势末端，一阳吞一阴，强力反转信号！)")

        # 2. 空头吞噬 (Bearish Engulfing)
        # 条件：多头趋势(5>20) + 昨日大涨(>2%) + 今日大跌 + 包住昨日
        if (curr['MA5'] > curr['MA20']) and \
                (prev_chg_pct > 0.02) and \
                (chg_pct < 0) and \
                (open_p > prev_close) and \
                (close < prev_open):
            patterns.append("【空头吞噬】(多头趋势末端，一阴吞一阳，强力见顶信号！)")

        # --- 基础形态 ---

        # 3. 大阳/大阴
        if body_pct > 0.8 and abs(chg_pct) > 0.03:
            if close > open_p:
                patterns.append("【大阳线】(买盘强劲)")
            else:
                patterns.append("【大阴线】(抛压沉重)")

        # 4. 长下影
        if lower_pct > 2 and body_pct < 0.3 and curr['MA5'] < curr['MA20']:
            patterns.append("【锤子】(下方有强支撑)")

        # 5. 长上影
        if upper_pct > 2 and body_pct < 0.3 and curr['MA5'] > curr['MA20'] and close> prev_close and prev_close > pprev_close:
            patterns.append("【倒状锤子】(上方压力巨大)")

        # 6. 十字星
        if body_pct < 0.1:
            patterns.append("【十字星】(变盘前兆)")

        # C. 趋势位置判断
        trends = []
        if close > curr['MA5']:
            trends.append("站上5日线(短强)")
        else:
            trends.append("跌破5日线(短弱)")

        if close > curr['MA20']:
            if curr['MA20'] > prev['MA20']:
                trends.append("站稳20日线且向上(中多)")
            else:
                trends.append("20日线上方震荡")
        else:
            trends.append("跌破20日线(中空)")

        if curr['MA5'] > curr['MA10'] > curr['MA20']:
            trends.append("均线多头排列")
        elif curr['MA5'] < curr['MA10'] < curr['MA20']:
            trends.append("均线空头排列")

        # --- 6. 输出报告 ---
        report = f"""
        📊 **{symbol} K线技术面诊断** ({date})

        1. **形态信号**：{' 🔥 '.join(patterns) if patterns else '普通震荡K线。'}
        2. **趋势结构**：{'，'.join(trends)}。
        3. **关键数据**：
           - 收盘：{close} (涨跌 {chg_pct * 100:.2f}%)
        """

        return report

    except Exception as e:
        return f"K线分析出错: {e}"


if __name__ == "__main__":
    # 工具必须用 .invoke 并在字典里传参
    print(analyze_kline_pattern.invoke({"symbol": "im"}))
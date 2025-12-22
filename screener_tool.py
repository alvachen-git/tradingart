import pandas as pd
import os
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool

# 1. 初始化数据库连接
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


# ==========================================
# 🛠️ 智能选股工具
# ==========================================

@tool
def search_top_stocks(condition: str = "综合评分"):
    """
    【智能选股器】
    当用户询问“推荐股票”、“哪些股票形态好”、“选强势股”时使用此工具。

    参数:
    - condition: 用户感兴趣的条件，例如 "综合评分"（默认）、"红三兵"、"金针探底"、"上升通道"。
                 如果用户没说具体形态，就传 "综合评分"。
    """
    if engine is None:
        return "数据库连接失败，无法获取选股数据。"

    try:
        # 1. 自动获取数据库里最新的交易日期 (防止周末查不到数据)
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return "选股数据库为空，请先运行盘后更新程序。"

        # 2. 构建 SQL 查询
        # 默认逻辑：查最新日期，按分数倒序，取前 10 名
        base_sql = f"""
            SELECT ts_code, name, industry, close, pct_chg, pattern, ma_trend, score, ai_summary
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
        """

        # 3. 简单的关键词过滤 (让 AI 支持稍微复杂点的筛选)
        # 如果用户指明要找某种形态
        params = {}
        if condition and condition not in ["综合评分", "推荐", "股票"]:
            # 模糊匹配形态或趋势
            base_sql += f" AND (pattern LIKE :cond OR ma_trend LIKE :cond)"
            params['cond'] = f"%{condition}%"

        # 4. 按分数排序，取前 8 个
        base_sql += " ORDER BY score DESC LIMIT 8"

        # 5. 执行查询
        df = pd.read_sql(base_sql, engine, params=params)

        if df.empty:
            return f"在 {max_date} 未找到符合 '{condition}' 条件的股票，建议查看综合评分较高的股票。"

        # 6. 格式化输出 (转成 Markdown 文本给 AI 读)
        # AI 读这种格式非常快
        result_text = f"📅 **最新选股结果 ({max_date})**\n\n"

        for idx, row in df.iterrows():
            # 图标装饰
            score_icon = "🌟" if row['score'] >= 80 else "📈"

            result_text += f"**{idx + 1}. {row['name']} ({row['ts_code']})** | {row['industry']}\n"
            result_text += f"   - 📊 评分：{score_icon} **{row['score']}分**\n"
            result_text += f"   - 💰 现价：{row['close']} ({row['pct_chg']}%) \n"

            # 如果有形态就显示形态，没形态显示趋势
            features = row['pattern'] if row['pattern'] else row['ma_trend']
            # 截取太长的文本防止 token 爆炸
            if len(features) > 50: features = features[:50] + "..."

            result_text += f"   - ⚡ 亮点：{features}\n"
            result_text += f"   - 📝 AI点评：{row['ai_summary']}\n\n"

        return result_text

    except Exception as e:
        return f"选股工具运行出错: {e}"
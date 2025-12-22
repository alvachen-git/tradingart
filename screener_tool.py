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
def search_top_stocks(condition: str = "综合评分", industry: str = None):
    """
    【智能选股器】
    根据技术形态、评分和行业板块筛选股票。

    参数:
    - condition: 形态条件，如 "红三兵"、"金针探底"、"上升通道"。默认为 "综合评分"。
    - industry: (可选) 用户指定的行业或板块名称，例如 "银行"、"房地产"、"半导体"、"酿酒"。
                如果用户没有指定行业，不要传此参数。
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


        params = {}
        # 3.1 处理行业过滤 (新增逻辑)
        # 使用模糊匹配，这样用户说 "地产" 也能匹配到 "全国地产"
        if industry and industry not in ["全市场", "所有"]:
            # 注意：这里使用 :industry_param 作为占位符
            base_sql += " AND industry LIKE :industry_param"
            params['industry_param'] = f"%{industry}%"

        # 3.2 处理形态/趋势过滤
        if condition and condition not in ["综合评分", "推荐", "股票", "好的"]:
            base_sql += " AND (pattern LIKE :cond OR ma_trend LIKE :cond)"
            params['cond'] = f"%{condition}%"

        # 4. 按分数排序，取前 8 个
        base_sql += " ORDER BY score DESC LIMIT 8"

        # 5. 【关键修复】使用 text() 包装 SQL 字符串
        # pd.read_sql 接受 text(sql) 对象，这样参数绑定就稳了
        stmt = text(base_sql)

        # 执行查询 (使用 engine.connect() 这种更现代的写法)
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn, params=params)

        # 6. 处理空结果
        if df.empty:
            msg = f"在 {max_date} "
            if industry: msg += f"[{industry}] 板块中 "
            if condition: msg += f"未找到符合 '{condition}' 的股票。"
            return msg + " 建议尝试其他板块或放宽条件。"

        # 7. 格式化输出
        title_suffix = f" - {industry}板块" if industry else ""
        result_text = f"📅 **选股结果 ({max_date}){title_suffix}**\n\n"

        for idx, row in df.iterrows():
            score_icon = "🌟" if row['score'] >= 80 else "📈"
            ind_str = f"[{row['industry']}]"

            result_text += f"**{idx + 1}. {row['name']} ({row['ts_code']})** {ind_str}\n"
            result_text += f"   - 📊 评分：{score_icon} **{row['score']}分**\n"
            result_text += f"   - 💰 现价：{row['close']} (涨幅 {row['pct_chg']}%) \n"

            # 如果有形态就显示形态，没形态显示趋势
            features = row['pattern'] if row['pattern'] else row['ma_trend']
            # 截取太长的文本防止 token 爆炸
            if len(features) > 50: features = features[:50] + "..."

            result_text += f"   - ⚡ 亮点：{features}\n"
            result_text += f"   - 📝 AI点评：{row['ai_summary']}\n\n"

        return result_text

    except Exception as e:
        return f"选股工具运行出错: {e}"
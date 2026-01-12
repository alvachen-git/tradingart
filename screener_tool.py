"""
智能选股工具
===================
功能：
1. 根据K线形态全市场选股（红三兵、金针探底、吞噬形态等）
2. 按行业板块筛选
3. 按综合评分排名
"""

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
#  K线形态名称映射表
#  用户可能说的各种名称 -> 数据库中实际存储的关键词
# ==========================================
PATTERN_ALIAS = {
    # === 看涨形态 ===
    "红三兵": ["红三兵"],
    "三红兵": ["红三兵"],
    "连续阳线": ["连续", "阳"],
    "连阳": ["连续", "阳"],
    "三连阳": ["连续3阳", "红三兵"],

    "多头吞噬": ["多头吞噬"],
    "看涨吞噬": ["多头吞噬"],
    "阳包阴": ["多头吞噬"],

    "锤子": ["锤子"],
    "锤子线": ["锤子"],
    "金针探底": ["锤子", "金针"],
    "下影线": ["锤子"],

    "早晨之星": ["早晨之星", "晨星"],
    "启明星": ["早晨之星", "启明星"],

    "V型反转": ["V型反转"],
    "V形反转": ["V型反转"],
    "反转": ["反转", "吞噬"],
    "假突破": ["假突破"],
    "假跌破": ["假跌破"],
    "诱空": ["诱空"],
    "诱多": ["诱多"],

    "多头反击": ["多头反击"],
    "放量突破": ["放量突破"],
    "突破": ["突破"],

    "大阳线": ["大阳线", "大阳"],
    "大阳": ["大阳线", "大阳"],
    "涨停": ["涨停"],

    # === 看跌形态 ===
    "三只乌鸦": ["三只乌鸦"],
    "黑三兵": ["三只乌鸦"],
    "连续阴线": ["连续", "阴"],
    "连阴": ["连续", "阴"],
    "三连阴": ["连续3阴", "三只乌鸦"],

    "空头吞噬": ["空头吞噬"],
    "看跌吞噬": ["空头吞噬"],
    "阴包阳": ["空头吞噬"],

    "吊人线": ["吊人线", "吊人"],
    "上吊线": ["吊人线"],

    "倒锤子": ["倒锤子", "倒状锤子"],
    "倒锤头": ["倒锤子", "倒状锤子"],
    "射击之星": ["倒锤子", "射击"],
    "流星": ["倒锤子", "流星"],

    "黄昏之星": ["黄昏之星"],
    "倒V": ["倒V"],
    "见顶": ["见顶", "倒V"],

    "空头反击": ["空头反击"],
    "放量下跌": ["放量下跌"],

    "大阴线": ["大阴线", "大阴"],
    "大阴": ["大阴线", "大阴"],
    "跌停": ["跌停"],

    # === 中性形态 ===
    "十字星": ["十字星"],
    "十字线": ["十字星"],
    "doji": ["十字星"],

    "波动收窄": ["波动收窄", "收窄"],
    "整理": ["整理", "收窄", "横盘"],
    "蓄势": ["蓄势", "收窄"],

    # === 趋势类 ===
    "均线多头": ["多头排列"],
    "多头排列": ["多头排列"],
    "均线空头": ["空头排列"],
    "空头排列": ["空头排列"],

    "站上5日线": ["站上5日线", "短强"],
    "站稳20日线": ["站稳20日线", "中多"],
    "跌破20日线": ["跌破20日线", "中空"],

    "上升通道": ["多头排列", "上涨"],
    "下降通道": ["空头排列", "下跌"],
}


def resolve_pattern_keywords(user_input: str) -> list:
    """
    将用户输入的形态名称解析为数据库查询关键词列表

    示例:
    - "红三兵" -> ["红三兵"]
    - "金针探底" -> ["锤子", "金针"]
    - "吞噬" -> ["吞噬"]  (直接模糊匹配)
    """
    user_input = user_input.strip()

    # 1. 先尝试精确匹配别名表
    if user_input in PATTERN_ALIAS:
        return PATTERN_ALIAS[user_input]

    # 2. 尝试部分匹配别名表
    for alias, keywords in PATTERN_ALIAS.items():
        if user_input in alias or alias in user_input:
            return keywords

    # 3. 如果没有匹配到，直接返回用户输入作为模糊搜索词
    return [user_input]


# ==========================================
# 🛠️ 智能选股工具
# ==========================================

@tool
def search_top_stocks(condition: str = "综合评分", industry: str = None, limit: int = 10):
    """
    【智能选股器】
    根据K线形态、技术形态、评分和行业板块筛选股票。

    参数:
    - condition: 形态条件，支持以下类型：
        看涨形态: "红三兵"、"金针探底"、"多头吞噬"、"大阳线"、"V型反转"、"突破"、"多头反击"、"晨星"、"假跌破"
        看跌形态: "三只乌鸦"、"空头吞噬"、"吊人线"、"大阴线"、"空头反击"、"夜星"、"假突破"
        中性形态: "十字星"、"波动收窄"、"锤子"
        趋势类: "均线多头"、"均线空头"、"上升通道"
        默认: "综合评分" (按分数排名)
    - industry: (可选) 行业或板块名称，如 "银行"、"半导体"、"酿酒"
    - limit: 返回结果数量，默认10条
    """
    if engine is None:
        return "数据库连接失败，无法获取选股数据。"

    try:
        # 1. 获取最新交易日期
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return "选股数据库为空，请先运行盘后更新程序。"

        # 2. 解析用户输入的形态条件
        pattern_keywords = []
        is_pattern_search = False

        if condition and condition not in ["综合评分", "推荐", "股票", "好的", "全部", ""]:
            pattern_keywords = resolve_pattern_keywords(condition)
            is_pattern_search = True

        # 3. 构建 SQL 查询
        base_sql = f"""
            SELECT ts_code, name, industry, close, pct_chg, pattern, ma_trend, score, ai_summary
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
        """

        params = {}

        # 3.1 处理行业过滤
        if industry and industry not in ["全市场", "所有", "全部", ""]:
            base_sql += " AND industry LIKE :industry_param"
            params['industry_param'] = f"%{industry}%"

        # 3.2 处理形态过滤 (核心逻辑)
        if is_pattern_search and pattern_keywords:
            # 构建多关键词 OR 查询
            pattern_conditions = []
            for i, keyword in enumerate(pattern_keywords):
                param_name = f"pattern_{i}"
                # 同时搜索 pattern 和 ma_trend 字段
                pattern_conditions.append(f"(pattern LIKE :{param_name} OR ma_trend LIKE :{param_name})")
                params[param_name] = f"%{keyword}%"

            base_sql += " AND (" + " OR ".join(pattern_conditions) + ")"
            # 形态搜索时，优先按形态匹配度排序，再按分数
            base_sql += " ORDER BY score DESC"
        else:
            # 综合评分模式
            base_sql += " ORDER BY score DESC"

        base_sql += f" LIMIT {limit}"

        # 4. 执行查询
        stmt = text(base_sql)
        with engine.connect() as conn:
            df = pd.read_sql(stmt, conn, params=params)

        # 5. 处理空结果
        if df.empty:
            msg = f"📅 {max_date} "
            if industry:
                msg += f"【{industry}】板块中 "
            if is_pattern_search:
                msg += f"未找到符合【{condition}】形态的股票。\n\n"
                msg += "💡 建议：\n"
                msg += "- 尝试其他形态，如：红三兵、金针探底、多头吞噬\n"
                msg += "- 放宽行业限制，搜索全市场\n"
                msg += "- 该形态可能在今日市场中较少出现"
            else:
                msg += "暂无数据。"
            return msg

        # 6. 格式化输出
        title_suffix = f" - {industry}板块" if industry else ""
        search_desc = f"【{condition}】形态" if is_pattern_search else "综合评分"

        result_text = f"📅 **选股结果 ({max_date}){title_suffix}**\n"
        result_text += f"🔍 筛选条件: {search_desc}\n"
        result_text += f"📊 共找到 {len(df)} 只符合条件的股票\n\n"

        for idx, row in df.iterrows():
            score_icon = "🌟" if row['score'] >= 80 else "📈" if row['score'] >= 60 else "📊"
            pct_icon = "🔴" if row['pct_chg'] > 0 else "🟢" if row['pct_chg'] < 0 else "⚪"

            result_text += f"**{idx + 1}. {row['name']} ({row['ts_code']})** [{row['industry']}]\n"
            result_text += f"   {score_icon} 评分: **{row['score']}分** | {pct_icon} 现价: {row['close']} ({row['pct_chg']:+.2f}%)\n"

            # 显示匹配到的形态
            features = []
            if row['pattern']:
                features.append(f"形态: {row['pattern'][:60]}")
            if row['ma_trend']:
                features.append(f"趋势: {row['ma_trend'][:40]}")

            if features:
                result_text += f"   ⚡ {' | '.join(features)}\n"

            if row['ai_summary']:
                summary = row['ai_summary'][:80] + "..." if len(str(row['ai_summary'])) > 80 else row['ai_summary']
                result_text += f"   💡 {summary}\n"

            result_text += "\n"

        # 7. 添加提示信息
        if is_pattern_search:
            result_text += "---\n"
            result_text += "💡 **其他可搜索形态**: 红三兵、三只乌鸦、金针探底、多头吞噬、空头吞噬、十字星、大阳线、放量突破、均线多头"

        return result_text

    except Exception as e:
        return f"选股工具运行出错: {e}"


@tool
def get_available_patterns():
    """
    【形态查询助手】
    查询数据库中今日出现的所有K线形态及其数量统计。
    用于帮助用户了解当前市场有哪些形态可供筛选。
    """
    if engine is None:
        return "数据库连接失败"

    try:
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return "数据库为空"

        # 统计各形态出现次数
        sql = f"""
            SELECT pattern, COUNT(*) as cnt
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
              AND pattern IS NOT NULL 
              AND pattern != ''
            GROUP BY pattern
            ORDER BY cnt DESC
            LIMIT 5
        """

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)

        if df.empty:
            return f"📅 {max_date} 暂无形态数据"

        result = f"📅 **{max_date} 市场形态统计**\n\n"
        result += "| 形态 | 出现次数 |\n"
        result += "|------|----------|\n"

        for _, row in df.iterrows():
            pattern = row['pattern'][:30] + "..." if len(str(row['pattern'])) > 30 else row['pattern']
            result += f"| {pattern} | {row['cnt']} |\n"

        result += "\n💡 使用示例: `帮我找红三兵的股票` 或 `半导体板块有哪些金针探底`"

        return result

    except Exception as e:
        return f"查询形态统计出错: {e}"


# ==========================================
#  辅助函数：获取选股数据供页面使用
# ==========================================
def get_screener_data(condition: str = None, industry: str = None, limit: int = 50) -> pd.DataFrame:
    """
    获取选股数据，供 Streamlit 页面可视化使用
    """
    if engine is None:
        return pd.DataFrame()

    try:
        with engine.connect() as conn:
            max_date = conn.execute(text("SELECT MAX(trade_date) FROM daily_stock_screener")).scalar()

        if not max_date:
            return pd.DataFrame()

        sql = f"""
            SELECT ts_code, name, industry, close, pct_chg, pattern, ma_trend, score, ai_summary
            FROM daily_stock_screener
            WHERE trade_date = '{max_date}'
        """

        params = {}

        if industry and industry not in ["全市场", "所有", ""]:
            sql += " AND industry LIKE :industry"
            params['industry'] = f"%{industry}%"

        if condition and condition not in ["综合评分", ""]:
            keywords = resolve_pattern_keywords(condition)
            conditions = []
            for i, kw in enumerate(keywords):
                param = f"p{i}"
                conditions.append(f"(pattern LIKE :{param} OR ma_trend LIKE :{param})")
                params[param] = f"%{kw}%"
            sql += " AND (" + " OR ".join(conditions) + ")"

        sql += f" ORDER BY score DESC LIMIT {limit}"

        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)

        return df

    except Exception as e:
        print(f"Error in get_screener_data: {e}")
        return pd.DataFrame()


def get_industries_list() -> list:
    """获取所有行业列表"""
    if engine is None:
        return []

    try:
        sql = """
              SELECT DISTINCT industry
              FROM daily_stock_screener
              WHERE industry IS NOT NULL \
                AND industry != ''
              ORDER BY industry \
              """
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return df['industry'].tolist()
    except:
        return []


# ==========================================
#  测试入口
# ==========================================
if __name__ == "__main__":
    print("=" * 60)
    print("测试1: 搜索红三兵形态")
    print("=" * 60)
    result = search_top_stocks.invoke({"condition": "红三兵"})
    print(result)

    print("\n" + "=" * 60)
    print("测试2: 银行板块综合评分")
    print("=" * 60)
    result = search_top_stocks.invoke({"condition": "综合评分", "industry": "银行"})
    print(result)

    print("\n" + "=" * 60)
    print("测试3: 查看今日市场形态统计")
    print("=" * 60)
    result = get_available_patterns.invoke({})
    print(result)
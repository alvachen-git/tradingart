import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool

load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]): return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()


# ==========================================
#  1. 排行榜 (支持类型过滤)
# ==========================================
def get_sector_ranking(limit=10, flow_col='main_net_inflow', sector_type='行业'):
    if engine is None: return pd.DataFrame(), pd.DataFrame(), "无数据"
    try:
        with engine.connect() as conn:
            date_sql = text("SELECT MAX(trade_date) FROM sector_moneyflow")
            latest_date = conn.execute(date_sql).scalar()

        if not latest_date: return pd.DataFrame(), pd.DataFrame(), "暂无数据"

        # 增加 sector_type 筛选
        sql = f"""
            SELECT industry, {flow_col} as net_inflow 
            FROM sector_moneyflow 
            WHERE trade_date='{latest_date}' AND sector_type='{sector_type}'
        """
        df = pd.read_sql(sql, engine)

        if df.empty: return pd.DataFrame(), pd.DataFrame(), latest_date

        df_in = df.sort_values('net_inflow', ascending=False).head(limit)
        df_out = df.sort_values('net_inflow', ascending=True).head(limit)
        return df_in, df_out, latest_date
    except Exception as e:
        return pd.DataFrame(), pd.DataFrame(), "查询出错"


# ==========================================
#  2. 气泡图 (支持类型过滤)
# ==========================================
def get_sector_rotation_data(trend_days=10, attack_days=1, flow_col='main_net_inflow', sector_type='行业'):
    if engine is None: return pd.DataFrame()
    try:
        max_days = max(trend_days, attack_days) + 5
        dates_df = pd.read_sql(
            f"SELECT DISTINCT trade_date FROM sector_moneyflow ORDER BY trade_date DESC LIMIT {max_days}", engine)
        if dates_df.empty: return pd.DataFrame()

        all_dates = dates_df['trade_date'].tolist()
        attack_str = "'" + "','".join(all_dates[:attack_days]) + "'"
        trend_str = "'" + "','".join(all_dates[:trend_days]) + "'"

        # 增加 sector_type 筛选
        sql = f"""
            SELECT 
                industry,
                SUM(CASE WHEN trade_date IN ({attack_str}) THEN {flow_col} ELSE 0 END) as attack_net_inflow,
                SUM(CASE WHEN trade_date IN ({attack_str}) THEN total_turnover ELSE 0 END) as attack_turnover,
                SUM(CASE WHEN trade_date IN ({trend_str}) THEN {flow_col} ELSE 0 END) as period_net_inflow,
                SUM(CASE WHEN trade_date IN ({trend_str}) THEN total_turnover ELSE 0 END) as period_turnover,
                AVG(CASE WHEN trade_date IN ({attack_str}) THEN pct_change ELSE NULL END) as avg_pct_change
            FROM sector_moneyflow 
            WHERE trade_date IN ({trend_str}) AND sector_type='{sector_type}'
            GROUP BY industry
        """
        df = pd.read_sql(sql, engine)

        df['attack_rate'] = df.apply(
            lambda x: (x['attack_net_inflow'] / x['attack_turnover'] * 100) if x['attack_turnover'] > 0 else 0, axis=1)
        df['bubble_size'] = df['period_turnover']

        def classify(row):
            x = row['period_net_inflow']
            y = row['attack_rate']
            if x > 0 and y > 0: return "双红 (共识买入)"
            if x < 0 and y > 0: return "反转 (底部承接)"
            if x > 0 and y < 0: return "分歧 (获利了结)"
            return "双绿 (加速卖出)"

        df['status'] = df.apply(classify, axis=1)
        return df.round(2)
    except:
        return pd.DataFrame()


# ==========================================
#  3. 单板块趋势 (无需过滤类型，因为板块名唯一)
# ==========================================
def get_sector_trend_data(industry, days=60):
    # 这里不需要改，因为 industry 名字本身就能区分
    # 如果行业和概念同名（很少见），可以在这里也加过滤，但为了简单暂时不加
    # 建议在 get_all_sectors 里加上类型筛选
    if engine is None: return pd.DataFrame()
    try:
        clean_ind = industry.replace("'", "")
        sql = f"""
            SELECT trade_date, main_net_inflow, medium_net_inflow, small_net_inflow, total_turnover
            FROM sector_moneyflow 
            WHERE industry='{clean_ind}' 
            ORDER BY trade_date DESC LIMIT {days}
        """
        df = pd.read_sql(sql, engine)
        if df.empty: return pd.DataFrame()

        df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
        df = df.dropna(subset=['trade_date'])
        df = df.groupby('trade_date', as_index=False).sum().sort_values('trade_date', ascending=True).reset_index(
            drop=True)
        return df
    except:
        return pd.DataFrame()


# ==========================================
#  4. 获取所有板块 (支持类型过滤)
# ==========================================
def get_all_sectors(sector_type='行业'):
    if engine is None: return []
    try:
        sql = f"SELECT DISTINCT industry FROM sector_moneyflow WHERE sector_type='{sector_type}' ORDER BY industry"
        return pd.read_sql(sql, engine)['industry'].tolist()
    except:
        return []


# ==========================================
#  🔥 AI 专用工具: 量化/散户合力资金分析 (加上了 @tool)
# ==========================================
@tool
def tool_get_retail_money_flow(days: int = 1):
    """
    查询股票的行业资金流向。
    用于回答“股票资金最近在哪些行业流动”、“量化资金去哪了”、“散户在买什么”等问题。

    Args:
        days (int): 统计天数。1代表当日，3代表最近3天，5代表最近5天。
    """
    if engine is None: return "数据库连接失败"

    try:
        # 1. 确定日期范围
        dates_df = pd.read_sql(
            f"SELECT DISTINCT trade_date FROM sector_moneyflow ORDER BY trade_date DESC LIMIT {days + 5}", engine)
        if dates_df.empty: return "暂无资金数据，请先运行数据更新脚本。"

        # 截取最近 days 天的日期
        target_dates = dates_df.head(days)['trade_date'].tolist()
        date_str = "'" + "','".join(target_dates) + "'"
        date_range_info = f"{target_dates[-1]} ~ {target_dates[0]}"

        # 2. 核心查询：只算 Medium + Small
        sql = f"""
            SELECT 
                industry,
                SUM(medium_net_inflow + small_net_inflow) as hidden_flow,
                SUM(main_net_inflow) as main_flow,  -- 查出来仅作对比参考
                AVG(pct_change) as avg_pct
            FROM sector_moneyflow 
            WHERE trade_date IN ({date_str}) AND sector_type='行业' -- 默认只看行业
            GROUP BY industry
            ORDER BY hidden_flow DESC
        """

        df = pd.read_sql(sql, engine)

        if df.empty: return "该时间段内无数据。"

        # 3. 生成 AI 可读的分析报告
        report = f"📊 **【资金流向分析】**\n"
        report += f"📅 统计区间：{date_range_info} (近{days}个交易日)\n"

        # 取前 10 名 (流入)
        top_10 = df.head(3)
        report += "🚀 **净流入 Top 3 :**\n"
        for _, row in top_10.iterrows():
            report += f"- **{row['industry']}**: +{row['hidden_flow']:.0f}万 (均涨: {row['avg_pct']:.2f}%)"

        report += "\n"

        # 取后 3 名 (流出)
        bottom_5 = df.tail(3).sort_values('hidden_flow', ascending=True)
        report += "🧊 **净流出 Top 3 :**\n"
        for _, row in bottom_5.iterrows():
            report += f"- **{row['industry']}**: {row['hidden_flow']:.0f}万\n"

        return report

    except Exception as e:
        return f"查询出错: {e}"
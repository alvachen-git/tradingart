import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine
import streamlit as st


# 1. 初始化
load_dotenv(override=True)

# --- 【安全修正】从环境变量读取数据库配置 ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 检查配置是否读取成功 (防止 .env 没配好报错)
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 数据库配置缺失！请检查 .env 文件中是否包含 DB_HOST, DB_USER, DB_PASSWORD 等信息。")

# 【修改点】加上这个装饰器
@st.cache_resource
def get_db_engine():
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    # 增加连接池配置，防止连接断开
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)

engine = get_db_engine()


def get_macro_indicator(indicator_code: str, days: int = 30) -> str:
    """
    【宏观指标查询】
    查询指定宏观指标的最近数据。

    参数:
    - indicator_code: 指标代码，可选值：
        国债利率: US2Y, US10Y, US30Y, CN2Y, CN10Y, CN30Y, JP2Y, JP10Y, JP30Y, USHY
        汇率: DXY(美元指数), USDJPY(美元兑日元), USDEUR(美元兑欧元), USDCNH(美元兑离岸人民币)
        航运: BDI(波罗的海干散货指数)
    - days: 查询最近多少天的数据，默认30天

    返回: 该指标的最新值和趋势分析
    """
    try:
        sql = text("""
                   SELECT trade_date, indicator_name, close_value, change_value, change_pct
                   FROM macro_daily
                   WHERE indicator_code = :code
                   ORDER BY trade_date DESC LIMIT :days
                   """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"code": indicator_code.upper(), "days": days})

        if df.empty:
            return f"未找到指标 {indicator_code} 的数据"

        # 最新数据
        latest = df.iloc[0]
        name = latest["indicator_name"]
        value = latest["close_value"]
        change = latest["change_value"]
        change_pct = latest["change_pct"]
        date = latest["trade_date"].strftime("%Y-%m-%d") if hasattr(latest["trade_date"], "strftime") else str(
            latest["trade_date"])

        # 计算趋势
        if len(df) >= 5:
            recent_5d = df.head(5)["close_value"].mean()
            older_5d = df.iloc[5:10]["close_value"].mean() if len(df) >= 10 else df.tail(5)["close_value"].mean()
            trend = "上升" if recent_5d > older_5d else "下降"
        else:
            trend = "数据不足"

        # 计算区间
        high_30d = df["close_value"].max()
        low_30d = df["close_value"].min()

        result = f"""📊 **{name}** ({indicator_code})
- 最新值: {value:.4f} ({date})
- 日涨跌: {change:+.4f} ({change_pct:+.2f}%)
- 近30日趋势: {trend}
- 30日区间: {low_30d:.4f} ~ {high_30d:.4f}"""

        return result

    except Exception as e:
        return f"查询失败: {str(e)}"


def get_macro_overview(category: str = "all") -> str:
    """
    【宏观环境总览】
    获取当前宏观环境的综合概览。

    参数:
    - category: 类别筛选
        'all': 全部指标
        'bond': 仅国债利率
        'fx': 仅汇率
        'shipping': 仅航运指数

    返回: 各指标最新值的汇总表格
    """
    try:
        if category == "all":
            where_clause = "1=1"
        else:
            where_clause = f"category = '{category}'"

        sql = text(f"""
            SELECT m1.indicator_code, m1.indicator_name, m1.category,
                   m1.close_value, m1.change_pct, m1.trade_date
            FROM macro_daily m1
            INNER JOIN (
                SELECT indicator_code, MAX(trade_date) as max_date
                FROM macro_daily
                WHERE {where_clause}
                GROUP BY indicator_code
            ) m2 ON m1.indicator_code = m2.indicator_code AND m1.trade_date = m2.max_date
            ORDER BY m1.category, m1.indicator_code
        """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            return "暂无宏观数据"

        # 按类别分组输出
        result_lines = ["📊 **宏观环境总览**\n"]

        category_names = {"bond": "🏦 国债利率", "fx": "💱 汇率", "shipping": "🚢 航运"}

        for cat in df["category"].unique():
            cat_df = df[df["category"] == cat]
            result_lines.append(f"\n**{category_names.get(cat, cat)}**")

            for _, row in cat_df.iterrows():
                code = row["indicator_code"]
                name = row["indicator_name"]
                value = row["close_value"]
                change = row["change_pct"]

                # 格式化显示
                if cat == "bond":
                    value_str = f"{value:.2f}%"
                elif code == "DXY":
                    value_str = f"{value:.2f}"
                elif code == "BDI":
                    value_str = f"{value:.0f}"
                else:
                    value_str = f"{value:.4f}"

                change_str = f"{change:+.2f}%" if pd.notna(change) else "-"
                result_lines.append(f"- {name}: {value_str} ({change_str})")

        # 添加数据日期
        latest_date = df["trade_date"].max()
        date_str = latest_date.strftime("%Y-%m-%d") if hasattr(latest_date, "strftime") else str(latest_date)
        result_lines.append(f"\n📅 数据截至: {date_str}")

        return "\n".join(result_lines)

    except Exception as e:
        return f"查询失败: {str(e)}"


def analyze_yield_curve() -> str:
    """
    【收益率曲线分析】
    分析中美日国债收益率曲线形态，判断经济预期。

    返回: 收益率曲线分析报告
    """
    try:
        # 查询最新的国债利率
        sql = text("""
                   SELECT m1.indicator_code, m1.close_value
                   FROM macro_daily m1
                            INNER JOIN (SELECT indicator_code, MAX(trade_date) as max_date
                                        FROM macro_daily
                                        WHERE indicator_code IN
                                              ('US2Y', 'US10Y', 'US30Y', 'CN2Y', 'CN10Y', 'CN30Y', 'JP2Y', 'JP10Y',
                                               'JP30Y')
                                        GROUP BY indicator_code) m2
                                       ON m1.indicator_code = m2.indicator_code AND m1.trade_date = m2.max_date
                   """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn)

        if df.empty:
            return "暂无国债利率数据"

        # 转换为字典
        rates = dict(zip(df["indicator_code"], df["close_value"]))

        result_lines = ["📈 **收益率曲线分析**\n"]

        # 美国
        if all(k in rates for k in ["US2Y", "US10Y", "US30Y"]):
            us_2y, us_10y, us_30y = rates["US2Y"], rates["US10Y"], rates["US30Y"]
            us_spread_10_2 = us_10y - us_2y
            us_spread_30_10 = us_30y - us_10y

            result_lines.append("**🇺🇸 美国国债**")
            result_lines.append(f"- 2Y: {us_2y:.2f}% | 10Y: {us_10y:.2f}% | 30Y: {us_30y:.2f}%")
            result_lines.append(
                f"- 10Y-2Y利差: {us_spread_10_2:+.2f}% {'⚠️ 倒挂(衰退预警)' if us_spread_10_2 < 0 else '✅ 正常'}")

        # 中国
        if all(k in rates for k in ["CN2Y", "CN10Y", "CN30Y"]):
            cn_2y, cn_10y, cn_30y = rates["CN2Y"], rates["CN10Y"], rates["CN30Y"]
            cn_spread_10_2 = cn_10y - cn_2y

            result_lines.append("\n**🇨🇳 中国国债**")
            result_lines.append(f"- 2Y: {cn_2y:.2f}% | 10Y: {cn_10y:.2f}% | 30Y: {cn_30y:.2f}%")
            result_lines.append(f"- 10Y-2Y利差: {cn_spread_10_2:+.2f}%")

            # 中美利差
            if "US10Y" in rates:
                cn_us_spread = cn_10y - rates["US10Y"]
                result_lines.append(
                    f"- 中美10Y利差: {cn_us_spread:+.2f}% {'(中国更高)' if cn_us_spread > 0 else '(美国更高)'}")

        # 日本
        if all(k in rates for k in ["JP2Y", "JP10Y", "JP30Y"]):
            jp_2y, jp_10y, jp_30y = rates["JP2Y"], rates["JP10Y"], rates["JP30Y"]

            result_lines.append("\n**🇯🇵 日本国债**")
            result_lines.append(f"- 2Y: {jp_2y:.2f}% | 10Y: {jp_10y:.2f}% | 30Y: {jp_30y:.2f}%")

        return "\n".join(result_lines)

    except Exception as e:
        return f"分析失败: {str(e)}"


def get_macro_history(indicator_code: str, start_date: str = None, end_date: str = None) -> str:
    """
    【宏观指标历史数据】
    查询指定宏观指标的历史数据。

    参数:
    - indicator_code: 指标代码
    - start_date: 开始日期 (YYYYMMDD)，默认90天前
    - end_date: 结束日期 (YYYYMMDD)，默认今天

    返回: 历史数据表格
    """
    try:
        if not end_date:
            end_date = datetime.now().strftime("%Y%m%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y%m%d")

        # 格式化日期
        start_dt = f"{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}"
        end_dt = f"{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}"

        sql = text("""
                   SELECT trade_date, close_value, change_pct
                   FROM macro_daily
                   WHERE indicator_code = :code
                     AND trade_date BETWEEN :start_date AND :end_date
                   ORDER BY trade_date DESC LIMIT 30
                   """)

        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={
                "code": indicator_code.upper(),
                "start_date": start_dt,
                "end_date": end_dt
            })

        if df.empty:
            return f"未找到 {indicator_code} 在该时间段的数据"

        # 统计信息
        latest = df.iloc[0]["close_value"]
        high = df["close_value"].max()
        low = df["close_value"].min()
        avg = df["close_value"].mean()

        result = f"""📊 **{indicator_code} 历史数据** ({start_dt} ~ {end_dt})

| 统计项 | 数值 |
|--------|------|
| 最新值 | {latest:.4f} |
| 最高值 | {high:.4f} |
| 最低值 | {low:.4f} |
| 平均值 | {avg:.4f} |
| 数据条数 | {len(df)} |
"""
        return result

    except Exception as e:
        return f"查询失败: {str(e)}"
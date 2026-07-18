import pandas as pd
import os
import re
from sqlalchemy import bindparam, create_engine, text
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

SECTOR_TYPE_INDUSTRY = "行业"
SECTOR_HIERARCHY_SUFFIX_RE = re.compile(r"[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩ]+$")


class SectorMoneyFlowNotReadyError(RuntimeError):
    """The requested industry money-flow snapshot is missing or stale."""


def canonical_sector_name(value: str) -> str:
    """Remove only the trailing DC hierarchy marker, preserving semantic names."""
    name = str(value or "").strip()
    return SECTOR_HIERARCHY_SUFFIX_RE.sub("", name).strip()


def _numeric_or_zero(value) -> float:
    numeric = pd.to_numeric(value, errors="coerce")
    return 0.0 if pd.isna(numeric) else float(numeric)


def build_sector_money_flow_snapshot(
    days: int = 1,
    as_of_date: str = None,
    db_engine=None,
) -> dict:
    """Return one canonical, unique industry record per name and ranked Top3 lists."""
    active_engine = db_engine if db_engine is not None else engine
    if active_engine is None:
        raise SectorMoneyFlowNotReadyError("数据库连接失败")

    safe_days = max(1, min(int(days or 1), 60))
    normalized_as_of = _normalize_trade_date(as_of_date)
    date_where = ""
    date_params = {"sector_type": SECTOR_TYPE_INDUSTRY}
    if normalized_as_of:
        date_where = "AND REPLACE(trade_date, '-', '') <= :as_of_date"
        date_params["as_of_date"] = normalized_as_of

    dates_sql = text(f"""
        SELECT DISTINCT REPLACE(trade_date, '-', '') AS trade_date
        FROM sector_moneyflow
        WHERE sector_type = :sector_type
          {date_where}
        ORDER BY trade_date DESC
        LIMIT {safe_days + 5}
    """)
    dates_df = pd.read_sql(dates_sql, active_engine, params=date_params)
    if dates_df.empty:
        raise SectorMoneyFlowNotReadyError("暂无资金数据，请先运行数据更新脚本。")

    target_dates = [
        _normalize_trade_date(value)
        for value in dates_df.head(safe_days)["trade_date"].tolist()
    ]
    target_dates = [value for value in target_dates if value]
    if not target_dates:
        raise SectorMoneyFlowNotReadyError("板块资金交易日格式无效。")
    if normalized_as_of and target_dates[0] != normalized_as_of:
        raise SectorMoneyFlowNotReadyError(
            f"板块资金数据未就绪：报告日 {normalized_as_of}，"
            f"数据库最新仅到 {target_dates[0]}。禁止将旧数据写成当日资金流。"
        )

    rows_sql = text("""
        SELECT REPLACE(trade_date, '-', '') AS trade_date,
               industry, main_net_inflow, pct_change, net_rate
        FROM sector_moneyflow
        WHERE sector_type = :sector_type
          AND REPLACE(trade_date, '-', '') IN :target_dates
    """).bindparams(bindparam("target_dates", expanding=True))
    rows_df = pd.read_sql(
        rows_sql,
        active_engine,
        params={"sector_type": SECTOR_TYPE_INDUSTRY, "target_dates": target_dates},
    )
    if rows_df.empty:
        raise SectorMoneyFlowNotReadyError("该时间段内无行业资金数据。")

    # DC can publish the same semantic industry at multiple hierarchy levels.
    # Select one row per date + canonical name before any multi-day aggregation.
    selected_by_date = {}
    for _, row in rows_df.iterrows():
        trade_date = _normalize_trade_date(row.get("trade_date"))
        display_name = canonical_sector_name(row.get("industry"))
        if not trade_date or not display_name:
            continue
        record = {
            "display_name": display_name,
            "raw_name": str(row.get("industry") or "").strip(),
            "trade_date": trade_date,
            "main_flow_yi": _numeric_or_zero(row.get("main_net_inflow")) / 10000.0,
            "pct_change": _numeric_or_zero(row.get("pct_change")),
            "net_rate": _numeric_or_zero(row.get("net_rate")),
        }
        key = (trade_date, display_name)
        current = selected_by_date.get(key)
        if current is None or abs(record["main_flow_yi"]) > abs(current["main_flow_yi"]):
            selected_by_date[key] = record

    if not selected_by_date:
        raise SectorMoneyFlowNotReadyError("行业资金记录无法规范化。")

    aggregated = {}
    date_priority = {value: index for index, value in enumerate(target_dates)}
    for record in selected_by_date.values():
        name = record["display_name"]
        item = aggregated.setdefault(
            name,
            {
                "display_name": name,
                "raw_name": record["raw_name"],
                "trade_date": target_dates[0],
                "main_flow_yi": 0.0,
                "pct_values": [],
                "net_rate_values": [],
                "raw_name_priority": len(target_dates),
            },
        )
        item["main_flow_yi"] += record["main_flow_yi"]
        item["pct_values"].append(record["pct_change"])
        item["net_rate_values"].append(record["net_rate"])
        priority = date_priority.get(record["trade_date"], len(target_dates))
        if priority < item["raw_name_priority"]:
            item["raw_name"] = record["raw_name"]
            item["raw_name_priority"] = priority

    sectors = {}
    for name, item in aggregated.items():
        pct_values = item.pop("pct_values")
        net_rate_values = item.pop("net_rate_values")
        item.pop("raw_name_priority", None)
        item["main_flow_yi"] = round(item["main_flow_yi"], 4)
        item["pct_change"] = round(sum(pct_values) / len(pct_values), 4)
        item["net_rate"] = round(sum(net_rate_values) / len(net_rate_values), 4)
        sectors[name] = item

    ranked = list(sectors.values())
    top_in = sorted(
        (row for row in ranked if row["main_flow_yi"] > 0),
        key=lambda row: row["main_flow_yi"],
        reverse=True,
    )[:3]
    top_out = sorted(
        (row for row in ranked if row["main_flow_yi"] < 0),
        key=lambda row: row["main_flow_yi"],
    )[:3]
    return {
        "report_date": target_dates[0],
        "target_dates": target_dates,
        "date_range": f"{target_dates[-1]} ~ {target_dates[0]}",
        "sectors": sectors,
        "sector_top_in": top_in,
        "sector_top_out": top_out,
        "raw_row_count": int(len(rows_df)),
        "selected_row_count": int(len(selected_by_date)),
        "collapsed_duplicate_count": int(len(rows_df) - len(selected_by_date)),
    }


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
#  🔥 AI 专用工具: 量化/机构合力资金分析 (加上了 @tool)
# ==========================================
def _normalize_trade_date(value):
    """Normalize a date-like value to YYYYMMDD for report freshness checks."""
    cleaned = re.sub(r"\D", "", str(value or ""))[:8]
    return cleaned if len(cleaned) == 8 else ""


@tool
def tool_get_retail_money_flow(days: int = 1, as_of_date: str = None):
    """
    查询股票的机构资金流向。
    用于回答“股票资金最近在哪些行业流动”、“量化资金去哪了”、“机构在买什么”等问题。

    Args:
        days (int): 统计天数。1代表当日，3代表最近3天，5代表最近5天。
        as_of_date (str): 可选，报告对应交易日（YYYYMMDD）。传入后若该日
            数据尚未入库，会明确返回“数据未就绪”，禁止用上一交易日冒充当日。
    """
    try:
        snapshot = build_sector_money_flow_snapshot(days=days, as_of_date=as_of_date)
        report = f"📊 **【主力资金流向分析】**\n"
        report += f"📅 统计区间：{snapshot['date_range']} (近{len(snapshot['target_dates'])}个交易日)\n"

        report += "🚀 **主力净流入 Top 3：**\n"
        for row in snapshot["sector_top_in"]:
            report += (
                f"- **{row['display_name']}**: {row['main_flow_yi']:+.1f}亿 "
                f"(板块涨跌: {row['pct_change']:+.2f}%)\n"
            )

        report += "🧊 **主力净流出 Top 3：**\n"
        for row in snapshot["sector_top_out"]:
            report += (
                f"- **{row['display_name']}**: {row['main_flow_yi']:+.1f}亿 "
                f"(板块涨跌: {row['pct_change']:+.2f}%)\n"
            )

        return report

    except Exception as e:
        return f"查询出错: {e}"

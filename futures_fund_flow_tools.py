import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
import streamlit as st
import re

# 1. 初始化数据库连接
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


engine = get_db_engine()

# ==========================================
#  品种名称映射表（简称 -> 数据库代码）
# ==========================================
FUTURES_NAME_MAP = {
    # 贵金属
    "工业硅": "SI", "多晶硅": "PS", "碳酸锂": "LC", "钯金": "PD", "铂金": "PT",
    "黄金": "AU", "沪金": "AU", "金": "AU",
    "白银": "AG", "沪银": "AG", "银": "AG",
    # 有色金属
    "铜": "CU", "沪铜": "CU",
    "铝": "AL", "沪铝": "AL",
    "锌": "ZN", "沪锌": "ZN",
    "镍": "NI", "沪镍": "NI",
    "锡": "SN", "沪锡": "SN",
    "铅": "PB", "沪铅": "PB",
    "氧化铝": "AO",
    # 黑色系
    "螺纹钢": "RB", "螺纹": "RB",
    "热卷": "HC", "热轧卷板": "HC",
    "铁矿石": "I", "铁矿": "I",
    "焦煤": "JM",
    "焦炭": "J",
    "锰硅": "SM",
    "硅铁": "SF",
    "不锈钢": "SS",
    "线材": "WR",
    # 能源化工
    "原油": "SC", "原油期货": "SC",
    "燃油": "FU", "燃料油": "FU",
    "低硫燃油": "LU",
    "沥青": "BU", "石油沥青": "BU",
    "PTA": "TA",
    "甲醇": "MA", "郑醇": "MA",
    "乙二醇": "EG",
    "塑料": "L", "聚乙烯": "L", "LLDPE": "L",
    "PP": "PP", "聚丙烯": "PP",
    "PVC": "V",
    "橡胶": "RU", "天然橡胶": "RU",
    "20号胶": "NR",
    "纯碱": "SA",
    "玻璃": "FG",
    "尿素": "UR",
    "液化气": "PG", "LPG": "PG",
    "苯乙烯": "EB",
    "短纤": "PF",
    "纸浆": "SP",
    # 农产品
    "豆一": "A", "大豆": "A", "黄大豆1号": "A",
    "豆二": "B", "黄大豆2号": "B",
    "豆粕": "M",
    "豆油": "Y",
    "棕榈油": "P", "棕油": "P",
    "菜油": "OI", "菜籽油": "OI",
    "菜粕": "RM",
    "玉米": "C",
    "淀粉": "CS", "玉米淀粉": "CS",
    "小麦": "WH", "强麦": "WH",
    "棉花": "CF", "郑棉": "CF",
    "白糖": "SR", "郑糖": "SR",
    "苹果": "AP",
    "红枣": "CJ",
    "花生": "PK",
    "生猪": "LH",
    "鸡蛋": "JD",
    "粳米": "RR",
    # 股指期货
    "沪深300": "IF", "IF": "IF", "股指": "IF",
    "上证50": "IH", "IH": "IH",
    "中证500": "IC", "IC": "IC",
    "中证1000": "IM", "IM": "IM",
    # 国债期货
    "十年国债": "T", "十债": "T",
    "五年国债": "TF", "五债": "TF",
    "两年国债": "TS", "二债": "TS",
    "三十年国债": "TL",
}

# ==========================================
#  【核心】合约乘数映射表（交易单位）
#  数据来源：各交易所官网合约规格
# ==========================================
CONTRACT_MULTIPLIER = {
    # === 上海期货交易所 (SHFE) ===
    "AU": 1000,  # 黄金：1000克/手
    "AG": 15,  # 白银：15千克/手
    "SI": 5,  # 工业硅：5吨/手
    "PS": 3,  # 多晶硅：3吨/手
    "LC": 1,  # 碳酸锂：1吨/手
    "PD": 1000,  # 钯金：1000克/手
    "PT": 1000,  # 铂金：1000克/手
    "CU": 5,  # 铜：5吨/手
    "AL": 5,  # 铝：5吨/手
    "ZN": 5,  # 锌：5吨/手
    "PB": 5,  # 铅：5吨/手
    "NI": 1,  # 镍：1吨/手
    "SN": 1,  # 锡：1吨/手
    "AO": 20,  # 氧化铝：20吨/手
    "RB": 10,  # 螺纹钢：10吨/手
    "HC": 10,  # 热轧卷板：10吨/手
    "SS": 5,  # 不锈钢：5吨/手
    "WR": 10,  # 线材：10吨/手
    "RU": 10,  # 天然橡胶：10吨/手
    "FU": 10,  # 燃料油：10吨/手
    "BU": 10,  # 石油沥青：10吨/手
    "SP": 10,  # 纸浆：10吨/手
    # === 上海国际能源交易中心 (INE) ===
    "SC": 1000,  # 原油：1000桶/手
    "NR": 10,  # 20号胶：10吨/手
    "LU": 10,  # 低硫燃料油：10吨/手
    "BC": 5,  # 国际铜：5吨/手
    # === 大连商品交易所 (DCE) ===
    "A": 10,  # 黄大豆1号：10吨/手
    "B": 10,  # 黄大豆2号：10吨/手
    "M": 10,  # 豆粕：10吨/手
    "Y": 10,  # 豆油：10吨/手
    "P": 10,  # 棕榈油：10吨/手
    "C": 10,  # 玉米：10吨/手
    "CS": 10,  # 玉米淀粉：10吨/手
    "RR": 10,  # 粳米：10吨/手
    "JD": 10,  # 鸡蛋：10吨/手
    "LH": 16,  # 生猪：16吨/手
    "I": 100,  # 铁矿石：100吨/手
    "J": 100,  # 焦炭：100吨/手
    "JM": 60,  # 焦煤：60吨/手
    "L": 5,  # 聚乙烯：5吨/手
    "V": 5,  # PVC：5吨/手
    "PP": 5,  # 聚丙烯：5吨/手
    "EG": 10,  # 乙二醇：10吨/手
    "EB": 5,  # 苯乙烯：5吨/手
    "PG": 20,  # 液化石油气：20吨/手
    # === 郑州商品交易所 (CZCE) ===
    "SR": 10,  # 白糖：10吨/手
    "CF": 5,  # 棉花：5吨/手
    "OI": 10,  # 菜籽油：10吨/手
    "RM": 10,  # 菜籽粕：10吨/手
    "AP": 10,  # 苹果：10吨/手
    "CJ": 5,  # 红枣：5吨/手
    "PK": 5,  # 花生：5吨/手
    "WH": 20,  # 强麦：20吨/手
    "TA": 5,  # PTA：5吨/手
    "MA": 10,  # 甲醇：10吨/手
    "FG": 20,  # 玻璃：20吨/手
    "SA": 20,  # 纯碱：20吨/手
    "UR": 20,  # 尿素：20吨/手
    "SF": 5,  # 硅铁：5吨/手
    "SM": 5,  # 锰硅：5吨/手
    "PF": 5,  # 短纤：5吨/手
    # === 中国金融期货交易所 (CFFEX) ===
    "IF": 300,  # 沪深300：300元/点
    "IH": 300,  # 上证50：300元/点
    "IC": 200,  # 中证500：200元/点
    "IM": 200,  # 中证1000：200元/点
    "T": 10000,  # 10年期国债：面值100万元
    "TF": 10000,  # 5年期国债：面值100万元
    "TS": 20000,  # 2年期国债：面值200万元
    "TL": 10000,  # 30年期国债：面值100万元
}

# ==========================================
#  【核心】保证金比例映射表
#  数据来源：各交易所官网（交易所标准，期货公司会上浮）
#  注意：保证金比例会动态调整，这里用常规值
# ==========================================
MARGIN_RATE = {
    # === 贵金属 ===
    "AU": 0.08,  # 黄金 8%
    "AG": 0.09,  # 白银 9%
    "SI": 0.12,  # 工业硅 12%
    "PS": 0.12,  # 多晶硅 12%
    "LC": 0.12,  # 碳酸锂 17%（波动大）
    "PD": 0.10,  # 钯金 10%
    "PT": 0.10,  # 铂金 10%
    # === 有色金属 ===
    "CU": 0.09,  # 铜 9%
    "AL": 0.10,  # 铝 10%
    "ZN": 0.10,  # 锌 10%
    "PB": 0.09,  # 铅 9%
    "NI": 0.14,  # 镍 14%（波动大）
    "SN": 0.12,  # 锡 12%
    "AO": 0.10,  # 氧化铝 10%
    # === 黑色金属 ===
    "RB": 0.10,  # 螺纹钢 10%
    "HC": 0.10,  # 热轧卷板 10%
    "SS": 0.10,  # 不锈钢 10%
    "WR": 0.09,  # 线材 9%
    "I": 0.13,  # 铁矿石 13%
    "J": 0.15,  # 焦炭 15%
    "JM": 0.15,  # 焦煤 15%
    "SF": 0.12,  # 硅铁 12%
    "SM": 0.12,  # 锰硅 12%
    # === 能源化工 ===
    "SC": 0.11,  # 原油 11%
    "FU": 0.10,  # 燃料油 10%
    "LU": 0.10,  # 低硫燃料油 10%
    "BU": 0.10,  # 石油沥青 10%
    "RU": 0.10,  # 天然橡胶 10%
    "NR": 0.10,  # 20号胶 10%
    "SP": 0.08,  # 纸浆 8%
    "TA": 0.08,  # PTA 8%
    "MA": 0.09,  # 甲醇 9%
    "EG": 0.09,  # 乙二醇 9%
    "L": 0.08,  # 聚乙烯 8%
    "V": 0.08,  # PVC 8%
    "PP": 0.08,  # 聚丙烯 8%
    "EB": 0.10,  # 苯乙烯 10%
    "PG": 0.10,  # 液化石油气 10%
    "SA": 0.09,  # 纯碱 9%
    "FG": 0.09,  # 玻璃 9%
    "UR": 0.08,  # 尿素 8%
    "PF": 0.08,  # 短纤 8%
    # === 农产品 ===
    "A": 0.08,  # 黄大豆1号 8%
    "B": 0.08,  # 黄大豆2号 8%
    "M": 0.08,  # 豆粕 8%
    "Y": 0.08,  # 豆油 8%
    "P": 0.09,  # 棕榈油 9%
    "C": 0.08,  # 玉米 8%
    "CS": 0.07,  # 玉米淀粉 7%
    "RR": 0.07,  # 粳米 7%
    "JD": 0.09,  # 鸡蛋 9%
    "LH": 0.12,  # 生猪 12%
    "SR": 0.08,  # 白糖 8%
    "CF": 0.08,  # 棉花 8%
    "OI": 0.08,  # 菜籽油 8%
    "RM": 0.08,  # 菜籽粕 8%
    "AP": 0.10,  # 苹果 10%
    "CJ": 0.10,  # 红枣 10%
    "PK": 0.10,  # 花生 10%
    "WH": 0.08,  # 强麦 8%
    # === 股指期货 ===
    "IF": 0.12,  # 沪深300 12%
    "IH": 0.12,  # 上证50 12%
    "IC": 0.14,  # 中证500 14%
    "IM": 0.15,  # 中证1000 15%
    # === 国债期货 ===
    "T": 0.025,  # 10年期国债 2.5%
    "TF": 0.015,  # 5年期国债 1.5%
    "TS": 0.01,  # 2年期国债 1%
    "TL": 0.035,  # 30年期国债 3.5%
}

# 默认值
DEFAULT_MULTIPLIER = 10
DEFAULT_MARGIN_RATE = 0.10


def extract_contract_month(ts_code: str) -> str:
    """
    从 ts_code 中提取合约月份

    示例:
    - "RB2510" -> "2510"
    - "RB2510.SHF" -> "2510"
    - "IM2506.CFX" -> "2506"
    """
    if not ts_code:
        return ""
    # 匹配品种代码后的4位数字月份
    match = re.search(r'[A-Z]+(\d{4})', ts_code.upper())
    if match:
        return match.group(1)
    # 如果没有4位，尝试匹配2位或3位
    match = re.search(r'[A-Z]+(\d{2,3})', ts_code.upper())
    if match:
        return match.group(1)
    return ""


def get_contract_multiplier(symbol: str) -> float:
    """获取合约乘数"""
    clean_symbol = re.sub(r'[0-9]', '', symbol.upper().strip())
    return CONTRACT_MULTIPLIER.get(clean_symbol, DEFAULT_MULTIPLIER)


def get_margin_rate(symbol: str) -> float:
    """获取保证金比例"""
    clean_symbol = re.sub(r'[0-9]', '', symbol.upper().strip())
    return MARGIN_RATE.get(clean_symbol, DEFAULT_MARGIN_RATE)


def resolve_futures_symbol(query: str) -> str:
    """将用户输入的品种名称解析为数据库代码（不含月份）"""
    query = query.strip().upper()
    # 移除数字部分
    query_clean = re.sub(r'[0-9]', '', query)
    if query_clean in FUTURES_NAME_MAP.values():
        return query_clean
    query_lower = query.lower()
    for name, code in FUTURES_NAME_MAP.items():
        if name in query_lower or query_lower in name:
            return code
    return query_clean


def parse_contract_input(user_input: str) -> tuple:
    """
    解析用户输入，返回 (品种代码, 合约月份)

    示例:
    - "螺纹钢" -> ("RB", None)  主力合约
    - "螺纹钢2506" -> ("RB", "2506")
    - "螺纹钢06月" -> ("RB", "06")
    - "RB2506" -> ("RB", "2506")
    - "RB06" -> ("RB", "06")
    - "螺纹钢主力" -> ("RB", None)
    """
    user_input = user_input.strip()

    # 移除"主力"、"主力合约"等关键词
    for keyword in ["主力合约", "主力", "当月", "近月"]:
        user_input = user_input.replace(keyword, "")

    # 尝试提取月份（4位如2506，或2位如06）
    month_match = re.search(r'(\d{4}|\d{2})(?:月)?$', user_input)

    if month_match:
        month = month_match.group(1)
        # 去除月份部分，得到品种名
        symbol_part = user_input[:month_match.start()].strip()
        code = resolve_futures_symbol(symbol_part)
        return (code, month)
    else:
        code = resolve_futures_symbol(user_input)
        return (code, None)


def get_dominant_contract(symbol_code: str, trade_date: str = None) -> str:
    """
    获取某品种持仓量最大的合约（主力合约）

    参数:
    - symbol_code: 品种代码如 RB, AU
    - trade_date: 交易日期，默认最新

    返回: 主力合约的 ts_code，如 RB2510.SHF
    """
    if engine is None:
        return None

    try:
        # 获取最新交易日
        if trade_date is None:
            date_sql = f"""
                SELECT MAX(trade_date) as latest_date 
                FROM futures_price 
                WHERE ts_code LIKE '{symbol_code}%%'
            """
            date_df = pd.read_sql(date_sql, engine)
            if date_df.empty or date_df.iloc[0]['latest_date'] is None:
                return None
            trade_date = date_df.iloc[0]['latest_date']

        # 查询该品种所有合约，按持仓量降序
        sql = f"""
            SELECT ts_code, oi
            FROM futures_price
            WHERE ts_code LIKE '{symbol_code}%%'
              AND trade_date = '{trade_date}'
            ORDER BY oi DESC
            LIMIT 1
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return None

        return df.iloc[0]['ts_code']

    except Exception as e:
        print(f"Error getting dominant contract: {e}")
        return None


def get_contract_by_month(symbol_code: str, month: str, trade_date: str = None) -> str:
    """
    根据月份获取合约代码

    参数:
    - symbol_code: 品种代码如 RB
    - month: 月份，如 "2506" 或 "06"
    - trade_date: 交易日期

    返回: 合约的 ts_code
    """
    if engine is None:
        return None

    try:
        # 获取最新交易日
        if trade_date is None:
            date_sql = f"SELECT MAX(trade_date) as latest_date FROM futures_price WHERE ts_code LIKE '{symbol_code}%%'"
            date_df = pd.read_sql(date_sql, engine)
            if date_df.empty:
                return None
            trade_date = date_df.iloc[0]['latest_date']

        # 如果月份是2位，尝试匹配
        if len(month) == 2:
            pattern = f"{symbol_code}%%{month}%%"
        else:
            pattern = f"{symbol_code}{month}%%"

        sql = f"""
            SELECT ts_code, oi
            FROM futures_price
            WHERE ts_code LIKE '{pattern}'
              AND trade_date = '{trade_date}'
            ORDER BY oi DESC
            LIMIT 1
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return None

        return df.iloc[0]['ts_code']

    except Exception as e:
        print(f"Error getting contract by month: {e}")
        return None


# ==========================================
#  AI Agent 工具 1: 查询单个商品期货资金流动趋势
# ==========================================
@tool
def get_futures_fund_flow(symbol: str, days: int = 20):
    """
    【期货资金流分析器】
    查询某个商品期货最近N天的资金流动情况，包括每日资金净流入、累计沉淀、持仓变化等。

    参数:
    - symbol: 品种名称或代码，支持以下格式：
        - "白银" / "螺纹钢" / "AU" / "RB" → 自动查询主力合约（持仓量最大）
        - "螺纹钢2506" / "RB2506" → 查询指定2506合约
        - "螺纹钢06月" / "RB06" → 查询06月合约
        - "螺纹钢主力" → 查询主力合约
    - days: 查询天数，默认20天

    返回: 该品种的资金流动趋势分析报告
    """
    if engine is None:
        return "数据库连接失败"

    # 解析用户输入：品种代码 + 月份
    code, month = parse_contract_input(symbol)

    # 获取合约乘数和保证金
    multiplier = get_contract_multiplier(code)
    margin_rate = get_margin_rate(code)

    try:
        # 根据是否指定月份，确定查询的合约
        if month:
            # 用户指定了月份
            target_contract = get_contract_by_month(code, month)
            contract_desc = f"{month}合约"
        else:
            # 未指定月份，查询主力合约（持仓量最大）
            target_contract = get_dominant_contract(code)
            contract_desc = "主力合约"

        if not target_contract:
            return f"未找到品种 '{symbol}' (代码: {code}) 的{contract_desc}数据，请检查品种名称或月份是否正确。"

        # 查询该合约的历史数据
        sql = f"""
            SELECT 
                trade_date,
                ts_code,
                close_price,
                settle_price,
                vol,
                oi,
                LAG(oi) OVER (ORDER BY trade_date) as prev_oi
            FROM futures_price
            WHERE ts_code = '{target_contract}'
            ORDER BY trade_date DESC
            LIMIT {days + 1}
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return f"未找到合约 '{target_contract}' 的数据"

        df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
        df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
        df['price'] = df['settle_price'].fillna(df['close_price'])
        df['oi_change'] = df['oi'] - df['prev_oi']

        # 【核心公式】资金流 = 持仓变化 × 价格 × 乘数 × 保证金比例 / 10000 (转万元)
        df['fund_flow'] = df['oi_change'] * df['price'] * multiplier * margin_rate / 10000
        df['liquid_fund'] = df['vol'] * df['price'] * multiplier * margin_rate / 10000

        # 总沉淀资金 = 持仓量 × 价格 × 乘数 × 保证金比例 / 10000
        df['total_margin'] = df['oi'] * df['price'] * multiplier * margin_rate / 10000

        df['cumsum_flow'] = df['fund_flow'].cumsum()
        df = df.dropna(subset=['oi_change'])

        if df.empty:
            return f"合约 '{target_contract}' 数据不足"

        latest = df.iloc[-1]
        latest_date = latest['trade_date'].strftime('%Y-%m-%d')
        ts_code = latest['ts_code']

        total_inflow = df['fund_flow'].sum()
        total_liquid = df['liquid_fund'].sum()
        avg_daily_flow = df['fund_flow'].mean()
        current_margin = latest['total_margin']

        recent_3d = df.tail(3)['fund_flow'].sum()
        recent_5d = df.tail(5)['fund_flow'].sum()

        if recent_3d > 0 and recent_5d > 0:
            trend = "📈 资金持续流入，多头主导"
        elif recent_3d < 0 and recent_5d < 0:
            trend = "📉 资金持续流出，空头主导"
        elif recent_3d > 0 and recent_5d < 0:
            trend = "🔄 短期资金回流，关注反转信号"
        elif recent_3d < 0 and recent_5d > 0:
            trend = "⚠️ 短期资金外流，注意回调风险"
        else:
            trend = "↔️ 资金进出平衡，横盘震荡"

        recent_detail = []
        for _, row in df.tail(5).iterrows():
            date_str = row['trade_date'].strftime('%m-%d')
            flow = row['fund_flow']
            oi_chg = row['oi_change']
            direction = "+" if flow > 0 else ""
            recent_detail.append(
                f"  {date_str}: {direction}{flow:,.0f}万 (持仓{'+' if oi_chg > 0 else ''}{oi_chg:,.0f}手)")

        # 提取合约月份用于显示
        contract_month = extract_contract_month(ts_code)

        report = f"""
📊 **{code} {contract_month} ({ts_code}) 资金流分析** (截至 {latest_date})
📌 合约类型: {contract_desc}

**一、合约参数**
- 合约乘数: {multiplier}
- 保证金比例: {margin_rate:.1%}

**二、资金概览**
- 当前总沉淀资金: {current_margin:,.0f} 万元
- 近{len(df)}日累计净流入: {total_inflow:+,.0f} 万元
- 日均资金流: {avg_daily_flow:+,.0f} 万元

**三、资金趋势**
{trend}
- 近3日净流入: {recent_3d:+,.0f} 万元
- 近5日净流入: {recent_5d:+,.0f} 万元

**四、近5日资金流明细**
{chr(10).join(recent_detail)}

**五、最新持仓**
- 今日持仓: {latest['oi']:,.0f} 手
- 今日成交: {latest['vol']:,.0f} 手

        """
        return report

    except Exception as e:
        return f"查询期货资金流出错: {e}"


# ==========================================
#  AI Agent 工具 2: 全市场期货资金排行榜
# ==========================================
@tool
def get_futures_fund_ranking(rank_type: str = "total", days: int = 5, top_n: int = 5):
    """
    【期货资金排行榜】
    查询全市场商品期货的资金沉淀排行。

    参数:
    - rank_type: 排行类型
        - "total" 或 "总沉淀": 总沉淀资金排行（持仓量×价格×乘数×保证金）
        - "static" 或 "净流入": 近N日资金净流入排行
        - "liquid" 或 "流动": 流动资金排行（市场活跃度）
    - days: 统计周期（天数），默认5天
    - top_n: 显示前N名，默认10

    返回: 期货资金排行榜报告（基于各品种主力合约）
    """
    if engine is None:
        return "数据库连接失败"

    rank_type = rank_type.lower()
    if rank_type in ["total", "总沉淀", "沉淀", "存量"]:
        rank_mode = "total"
        rank_name = "总沉淀资金"
    elif rank_type in ["static", "净流入", "流入", "增量"]:
        rank_mode = "static"
        rank_name = "资金净流入"
    elif rank_type in ["liquid", "流动", "活跃", "成交"]:
        rank_mode = "liquid"
        rank_name = "流动资金"
    else:
        rank_mode = "total"
        rank_name = "总沉淀资金"

    try:
        # 获取最近交易日期
        date_sql = "SELECT DISTINCT trade_date FROM futures_price ORDER BY trade_date DESC LIMIT 30"
        dates_df = pd.read_sql(date_sql, engine)

        if dates_df.empty:
            return "暂无期货数据"

        latest_date = dates_df.iloc[0]['trade_date']
        period_dates = dates_df.head(days + 1)['trade_date'].tolist()
        period_str = "'" + "','".join([str(d) for d in period_dates]) + "'"

        # 第一步：找到每个品种在最新交易日持仓量最大的合约（主力合约）
        dominant_sql = f"""
            SELECT ts_code, oi,
                   REGEXP_REPLACE(ts_code, '[0-9]', '') as symbol
            FROM futures_price
            WHERE trade_date = '{latest_date}'
        """
        dominant_df = pd.read_sql(dominant_sql, engine)

        if dominant_df.empty:
            return "查询期货数据为空"

        # 按品种分组，取持仓量最大的合约
        dominant_contracts = dominant_df.loc[dominant_df.groupby('symbol')['oi'].idxmax()]['ts_code'].tolist()

        if not dominant_contracts:
            return "未找到主力合约"

        contracts_str = "'" + "','".join(dominant_contracts) + "'"

        # 第二步：查询这些主力合约在统计周期内的数据
        sql = f"""
            SELECT 
                trade_date,
                ts_code,
                close_price,
                settle_price,
                vol,
                oi
            FROM futures_price
            WHERE trade_date IN ({period_str})
              AND ts_code IN ({contracts_str})
            ORDER BY ts_code, trade_date
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return "查询期货数据为空"

        df['symbol'] = df['ts_code'].apply(lambda x: re.sub(r'[0-9]', '', x))
        df['price'] = df['settle_price'].fillna(df['close_price'])

        results = []
        for ts_code in df['ts_code'].unique():
            contract_df = df[df['ts_code'] == ts_code].sort_values('trade_date')
            if len(contract_df) < 2:
                continue

            symbol = contract_df.iloc[0]['symbol']
            multiplier = get_contract_multiplier(symbol)
            margin_rate = get_margin_rate(symbol)

            contract_df = contract_df.copy()
            contract_df['oi_change'] = contract_df['oi'].diff()
            contract_df['fund_flow'] = contract_df['oi_change'] * contract_df[
                'price'] * multiplier * margin_rate / 10000
            contract_df['liquid_fund'] = contract_df['vol'] * contract_df['price'] * multiplier * margin_rate / 10000

            latest_row = contract_df.iloc[-1]

            # 总沉淀资金 = 持仓量 × 价格 × 乘数 × 保证金比例
            total_margin = latest_row['oi'] * latest_row['price'] * multiplier * margin_rate / 10000

            # 提取合约月份
            contract_month = extract_contract_month(ts_code)

            results.append({
                'symbol': symbol,
                'ts_code': ts_code,
                'contract_month': contract_month,
                'multiplier': multiplier,
                'margin_rate': margin_rate,
                'total_margin': total_margin,
                'net_flow': contract_df['fund_flow'].sum(),
                'liquid_fund': contract_df['liquid_fund'].sum(),
                'price': latest_row['price'],
                'oi': latest_row['oi'],
                'vol': latest_row['vol']
            })

        if not results:
            return "计算期货资金流失败"

        result_df = pd.DataFrame(results)

        # 根据排行类型排序
        if rank_mode == "total":
            result_df = result_df.sort_values('total_margin', ascending=False)
            value_col = 'total_margin'
        elif rank_mode == "static":
            result_df = result_df.sort_values('net_flow', ascending=False)
            value_col = 'net_flow'
        else:
            result_df = result_df.sort_values('liquid_fund', ascending=False)
            value_col = 'liquid_fund'

        # 生成排行列表
        top_list = []
        for i, (_, row) in enumerate(result_df.head(top_n).iterrows(), 1):
            val = row[value_col]
            margin_pct = row['margin_rate'] * 100
            if rank_mode == "total":
                top_list.append(
                    f"  {i}. {row['symbol']}({row['contract_month']}): {val:,.0f}万 (保证金{margin_pct:.0f}%, 持仓{row['oi']:,.0f}手)")
            else:
                sign = "+" if val > 0 else ""
                top_list.append(
                    f"  {i}. {row['symbol']}({row['contract_month']}): {sign}{val:,.0f}万 (保证金{margin_pct:.0f}%)")

        # 对于净流入，还显示流出排行
        bottom_list = []
        if rank_mode == "static":
            bottom_df = result_df.tail(top_n).iloc[::-1]
            for i, (_, row) in enumerate(bottom_df.iterrows(), 1):
                val = row[value_col]
                if val < 0:
                    bottom_list.append(
                        f"  {i}. {row['symbol']}({row['contract_month']}): {val:,.0f}万 (保证金{row['margin_rate'] * 100:.0f}%)")

        # 统计
        total_market_margin = result_df['total_margin'].sum()
        net_inflow_count = len(result_df[result_df['net_flow'] > 0])
        net_outflow_count = len(result_df[result_df['net_flow'] < 0])

        report = f"""
📊 **全市场期货{rank_name}排行榜** (近{days}日)
📌 统计口径: 各品种主力合约

**🏆 {rank_name} Top {min(len(top_list), top_n)}**
{chr(10).join(top_list) if top_list else "  暂无数据"}
"""

        if rank_mode == "static" and bottom_list:
            report += f"""
**📉 资金流出 Top {min(len(bottom_list), top_n)}**
{chr(10).join(bottom_list)}
"""

        report += f"""
**📈 市场概览**
- 统计品种数: {len(result_df)} 个
- 全市场总沉淀资金: {total_market_margin:,.0f} 万元 ({total_market_margin / 10000:,.1f} 亿元)
- 资金净流入品种: {net_inflow_count} 个
- 资金净流出品种: {net_outflow_count} 个
- 近{days}日全市场净流入: {result_df['net_flow'].sum():+,.0f} 万元

        """
        return report

    except Exception as e:
        return f"查询期货资金排行出错: {e}"


# ==========================================
#  页面辅助函数
# ==========================================
def get_futures_fund_flow_data(days: int = 20) -> pd.DataFrame:
    """获取全市场期货资金流数据，供页面可视化使用（使用主力合约）"""
    if engine is None:
        return pd.DataFrame()

    try:
        date_sql = "SELECT DISTINCT trade_date FROM futures_price ORDER BY trade_date DESC LIMIT 30"
        dates_df = pd.read_sql(date_sql, engine)

        if dates_df.empty:
            return pd.DataFrame()

        latest_date = dates_df.iloc[0]['trade_date']
        period_dates = dates_df.head(days + 1)['trade_date'].tolist()
        period_str = "'" + "','".join([str(d) for d in period_dates]) + "'"

        # 找到每个品种的主力合约（持仓量最大）
        dominant_sql = f"""
            SELECT ts_code, oi,
                   REGEXP_REPLACE(ts_code, '[0-9]', '') as symbol
            FROM futures_price
            WHERE trade_date = '{latest_date}'
        """
        dominant_df = pd.read_sql(dominant_sql, engine)

        if dominant_df.empty:
            return pd.DataFrame()

        dominant_contracts = dominant_df.loc[dominant_df.groupby('symbol')['oi'].idxmax()]['ts_code'].tolist()
        contracts_str = "'" + "','".join(dominant_contracts) + "'"

        sql = f"""
            SELECT trade_date, ts_code, close_price, settle_price, vol, oi
            FROM futures_price
            WHERE trade_date IN ({period_str})
              AND ts_code IN ({contracts_str})
            ORDER BY ts_code, trade_date
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return pd.DataFrame()

        df['symbol'] = df['ts_code'].apply(lambda x: re.sub(r'[0-9]', '', x))
        df['price'] = df['settle_price'].fillna(df['close_price'])

        results = []
        for ts_code in df['ts_code'].unique():
            contract_df = df[df['ts_code'] == ts_code].sort_values('trade_date')
            if len(contract_df) < 2:
                continue

            symbol = contract_df.iloc[0]['symbol']
            multiplier = get_contract_multiplier(symbol)
            margin_rate = get_margin_rate(symbol)

            contract_df = contract_df.copy()
            contract_df['oi_change'] = contract_df['oi'].diff()
            contract_df['fund_flow'] = contract_df['oi_change'] * contract_df[
                'price'] * multiplier * margin_rate / 10000
            contract_df['liquid_fund'] = contract_df['vol'] * contract_df['price'] * multiplier * margin_rate / 10000

            latest_row = contract_df.iloc[-1]
            total_margin = latest_row['oi'] * latest_row['price'] * multiplier * margin_rate / 10000

            # 提取合约月份
            contract_month = extract_contract_month(ts_code)

            results.append({
                'symbol': symbol,
                'ts_code': ts_code,
                'contract_month': contract_month,
                'multiplier': multiplier,
                'margin_rate': margin_rate,
                'total_margin': total_margin,
                'net_flow': contract_df['fund_flow'].sum(),
                'liquid_fund': contract_df['liquid_fund'].sum(),
                'today_flow': contract_df.iloc[-1]['fund_flow'] if pd.notna(
                    contract_df.iloc[-1].get('fund_flow')) else 0,
                'price': latest_row['price'],
                'oi': latest_row['oi'],
                'vol': latest_row['vol'],
                'pct_change': (contract_df.iloc[-1]['price'] / contract_df.iloc[0]['price'] - 1) * 100 if
                contract_df.iloc[0]['price'] > 0 else 0
            })

        return pd.DataFrame(results)

    except Exception as e:
        print(f"Error in get_futures_fund_flow_data: {e}")
        return pd.DataFrame()


def get_single_futures_trend(symbol: str, days: int = 60) -> pd.DataFrame:
    """
    获取单个期货品种的历史资金流趋势数据
    支持指定月份，如 "RB2506"，否则使用主力合约
    """
    if engine is None:
        return pd.DataFrame()

    # 解析用户输入
    code, month = parse_contract_input(symbol)
    multiplier = get_contract_multiplier(code)
    margin_rate = get_margin_rate(code)

    try:
        # 确定目标合约
        if month:
            target_contract = get_contract_by_month(code, month)
        else:
            target_contract = get_dominant_contract(code)

        if not target_contract:
            return pd.DataFrame()

        sql = f"""
            SELECT trade_date, ts_code, close_price, settle_price, vol, oi
            FROM futures_price
            WHERE ts_code = '{target_contract}'
            ORDER BY trade_date DESC
            LIMIT {days + 1}
        """
        df = pd.read_sql(sql, engine)

        if df.empty:
            return pd.DataFrame()

        df = df.sort_values('trade_date', ascending=True).reset_index(drop=True)
        df['trade_date'] = pd.to_datetime(df['trade_date'].astype(str), format='%Y%m%d', errors='coerce')
        df['price'] = df['settle_price'].fillna(df['close_price'])
        df['oi_change'] = df['oi'].diff()

        # 统一公式
        df['fund_flow'] = df['oi_change'] * df['price'] * multiplier * margin_rate / 10000
        df['liquid_fund'] = df['vol'] * df['price'] * multiplier * margin_rate / 10000
        df['total_margin'] = df['oi'] * df['price'] * multiplier * margin_rate / 10000
        df['cumsum_flow'] = df['fund_flow'].cumsum()
        df['multiplier'] = multiplier
        df['margin_rate'] = margin_rate

        df = df.dropna(subset=['oi_change'])
        return df

    except Exception as e:
        print(f"Error in get_single_futures_trend: {e}")
        return pd.DataFrame()


def get_all_futures_symbols() -> list:
    """获取所有期货品种代码列表"""
    if engine is None:
        return []
    try:
        sql = "SELECT DISTINCT ts_code FROM futures_price WHERE ts_code REGEXP '[A-Z]+[0-9]*0$' ORDER BY ts_code"
        df = pd.read_sql(sql, engine)
        symbols = df['ts_code'].apply(lambda x: re.sub(r'[0-9]', '', x)).unique().tolist()
        return sorted(symbols)
    except:
        return []


# ==========================================
#  测试入口
# ==========================================
if __name__ == "__main__":
    print("=" * 60)
    print("测试1: 白银主力合约资金流分析（默认）")
    print("=" * 60)
    result = get_futures_fund_flow.invoke({"symbol": "白银", "days": 10})
    print(result)

    print("\n" + "=" * 60)
    print("测试2: 螺纹钢指定月份合约（2610）")
    print("=" * 60)
    result = get_futures_fund_flow.invoke({"symbol": "螺纹钢2610", "days": 10})
    print(result)

    print("\n" + "=" * 60)
    print("测试3: 期货总沉淀资金排行榜（主力合约）")
    print("=" * 60)
    result = get_futures_fund_ranking.invoke({"rank_type": "total", "days": 5, "top_n": 10})
    print(result)
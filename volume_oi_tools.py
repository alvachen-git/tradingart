# ==========================================
#  volume_oi_tools.py - 成交量/持仓量工具（优化版）
#  优化点：精简描述、合并工具、提取公共变量
# ==========================================

import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from langchain_core.tools import tool
from pydantic import BaseModel, Field
import symbol_map
import traceback
import re

# 初始化
load_dotenv(override=True)
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")


def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]): return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(
        db_url,
        pool_pre_ping=True,      # 每次查询前检查连接是否有效
        pool_recycle=7200,       # 1小时回收连接
        pool_size=5,             # 连接池大小
        max_overflow=10          # 最大溢出连接数
    )


engine = get_db_engine()

# ==========================================
#  全局映射表（只定义一次）
# ==========================================
COMMODITY_MAP = {
    '豆粕': 'M', '白糖': 'SR', '棉花': 'CF', '玉米': 'C', '铁矿': 'I', '铁矿石': 'I',
    '黄金': 'AU', '白银': 'AG', '铜': 'CU', '铝': 'AL', '锌': 'ZN',
    "铅": "pb", "沪铅": "pb", "镍": "ni", "沪镍": "ni", "锡": "sn", "沪锡": "sn",
    "氧化铝": "ao", "铝合金": "ad",
    "br橡胶": "br", "苯乙烯": "eb", "沪深300": "IF", "上证50": "IH", "中证500": "IC", "中证1000": "IM",
    '橡胶': 'RU', '原油': 'SC', '棕榈油': 'P', '菜油': 'OI', '菜粕': 'RM',
    'PTA': 'TA', "PX": "px", "瓶片": "pr", '甲醇': 'MA', '聚丙烯': 'PP', '塑料': 'L', '乙二醇': 'EG',
    '螺纹': 'RB', '螺纹钢': 'RB', '热卷': 'HC', '焦炭': 'J', '焦煤': 'JM',
    '豆油': 'Y', '豆一': 'A', '豆二': 'B', '花生': 'PK', '生猪': 'LH', '苹果': 'ap', '红枣': 'cj',
    '纯碱': 'SA', '玻璃': 'FG', '尿素': 'UR', '锰硅': 'SM', '硅铁': 'SF',
    '液化气': 'PG', 'LPG': 'PG', '燃油': 'FU', '沥青': 'BU',
    "碳酸锂": "lc", "工业硅": "si", "多晶硅": "PS", "PS": "PS", "钯金": "pd", "铂金": "pt",
    "纸浆": "sp", "双胶纸": "op", "原木": "lg",
}

ETF_MAP = {
    '50ETF': ('510050.SH', '50ETF'), '上证50': ('510050.SH', '50ETF'),
    '300ETF': ('510300.SH', '300ETF'), '沪深300': ('510300.SH', '300ETF'),
    '500ETF': ('510500.SH', '500ETF'), '中证500': ('510500.SH', '500ETF'),
    '创业板': ('159915.SZ', '创业板ETF'), '创业板ETF': ('159915.SZ', '创业板ETF'),
    '科创50': ('588000.SH', '科创50ETF'), '科创板': ('588000.SH', '科创50ETF'),
}

CODE_TO_NAME_MAP = {
    # 广期所
    "LC": "碳酸锂", "SI": "工业硅", "LH": "生猪", "PS": "多晶硅",
    # 上期所
    "CU": "沪铜", "AL": "沪铝", "ZN": "沪锌", "PB": "沪铅", "SN": "沪锡", "NI": "沪镍",
    "AU": "黄金", "AG": "白银", "RU": "橡胶", "RB": "螺纹钢", "HC": "热轧卷板",
    "FU": "燃料油", "BU": "沥青", "SC": "原油", "LU": "低硫燃料油", "NR": "20号胶",
    "SP": "纸浆", "SS": "不锈钢", "BR": "丁二烯橡胶", "AO": "氧化铝",
    # 大商所
    "M": "豆粕", "Y": "豆油", "P": "棕榈油", "C": "玉米", "I": "铁矿石",
    "J": "焦炭", "JM": "焦煤", "A": "黄大豆1号", "B": "黄大豆2号",
    "V": "PVC", "PP": "聚丙烯", "L": "LLDPE", "EB": "苯乙烯", "EG": "乙二醇",
    "PG": "液化石油气", "RR": "粳米", "JD": "鸡蛋", "CS": "玉米淀粉",
    # 郑商所
    "SR": "白糖", "CF": "棉花", "TA": "PTA", "MA": "甲醇", "RM": "菜粕",
    "OI": "菜籽油", "FG": "玻璃", "ZC": "动力煤", "SF": "硅铁", "SM": "锰硅",
    "AP": "苹果", "CJ": "红枣", "UR": "尿素", "SA": "纯碱", "PK": "花生",
}


def parse_commodity_contract(ts_code: str) -> dict:
    """解析商品期权合约代码，提取品种名称"""
    ts_upper = ts_code.upper()

    match = re.match(r'^([A-Z]{1,2})', ts_upper)
    if not match:
        return {'code': '', 'name': '未知品种', 'option_type_cn': '', 'full_name': '未知品种'}

    symbol_code = match.group(1)
    commodity_name = CODE_TO_NAME_MAP.get(symbol_code, symbol_code)

    # 提取年月
    year_month_match = re.search(r'(\d{4})', ts_code)
    year_month = year_month_match.group(1) if year_month_match else ''

    # 提取行权价
    strike_match = re.search(r'[-](\d+)[.\w]*$', ts_code)
    if not strike_match:
        strike_match = re.search(r'[CP](\d+)', ts_code, re.IGNORECASE)
    strike = strike_match.group(1) if strike_match else ''

    # 期权类型
    if re.search(r'-P-|\dP\d', ts_upper):
        option_type_cn = '认沽'
    elif re.search(r'-C-|\dC\d', ts_upper):
        option_type_cn = '认购'
    else:
        option_type_cn = '未知'

    # ✅ 新增：完整中文名称
    full_name = f"{commodity_name}{year_month}{option_type_cn}{strike}"

    return {
        'code': symbol_code,
        'name': commodity_name,
        'option_type_cn': option_type_cn,
        'full_name': full_name  # ✅ 关键新增字段
    }
# ==========================================
#  辅助函数
# ==========================================
def fmt_vol(vol):
    """格式化数量"""
    if vol is None: return "-"
    if vol >= 1e8: return f"{vol / 1e8:.1f}亿"
    if vol >= 1e4: return f"{vol / 1e4:.1f}万"
    return f"{vol:.0f}"


def fmt_amt(amt):
    """格式化金额（千元->亿元）"""
    if amt is None: return "-"
    v = amt * 1000
    if v >= 1e8: return f"{v / 1e8:.1f}亿"
    if v >= 1e4: return f"{v / 1e4:.1f}万"
    return f"{v:.0f}"


def get_commodity_prefix(query):
    """从查询中提取商品代码前缀"""
    for name, prefix in COMMODITY_MAP.items():
        if name in query: return prefix, name
    return None, None


def get_etf_underlying(query):
    """从查询中提取ETF标的"""
    for name, (code, display) in ETF_MAP.items():
        if name in query.upper(): return code, display
    return None, None


# ==========================================
#  工具1：成交量/持仓量查询（通用）
# ==========================================
@tool
def get_volume_oi(query: str):
    """查询期货/股票/期权的成交量和持仓量。如"白银持仓量"、"茅台成交量"、"AG2602持仓"。"""
    if engine is None: return "❌ 数据库未连接"
    q = query.strip()

    try:
        # 解析品种
        symbol, asset_type = symbol_map.resolve_symbol(q)
        if not symbol: return f"⚠️ 未找到: {q}"
        code = symbol.upper()

        if asset_type == 'future':
            has_month = bool(re.search(r'\d{3,4}', code))
            if has_month:
                sql = text(
                    "SELECT trade_date,ts_code,vol,oi FROM futures_price WHERE ts_code=:c ORDER BY trade_date DESC LIMIT 1")
                df = pd.read_sql(sql, engine, params={"c": code})
            else:
                clean = ''.join([i for i in code if not i.isdigit()])
                sql = text(
                    f"SELECT trade_date,ts_code,vol,oi FROM futures_price WHERE ts_code IN ('{clean}0','{clean}') ORDER BY trade_date DESC LIMIT 1")
                df = pd.read_sql(sql, engine)
            if df.empty: return f"⚠️ 无数据: {q}"
            r = df.iloc[0]
            return f"{r['ts_code']} ({r['trade_date']}): 成交{fmt_vol(r['vol'])}手, 持仓{fmt_vol(r['oi'])}手"

        elif asset_type == 'stock':
            is_hk = code.endswith('.HK')
            if is_hk:
                sql = text(
                    "SELECT trade_date,ts_code,name,vol,amount FROM stock_price WHERE ts_code=:c ORDER BY trade_date DESC LIMIT 1")
                df = pd.read_sql(sql, engine, params={"c": code})
            else:
                codes = [code] + ([f"{code}.SZ", f"{code}.SH"] if "." not in code else [])
                sql = text(
                    f"SELECT trade_date,ts_code,name,vol,amount FROM stock_price WHERE ts_code IN ({','.join([repr(c) for c in codes])}) ORDER BY trade_date DESC LIMIT 1")
                df = pd.read_sql(sql, engine)
            if df.empty: return f"⚠️ 无数据: {q}"
            r = df.iloc[0]
            return f"{r.get('name', q)} ({r['trade_date']}): 成交{fmt_vol(r['vol'])}股, 成交额{fmt_amt(r['amount'])}"

        return f"⚠️ 不支持: {q}"
    except Exception as e:
        return f"❌ 错误: {e}"


# ==========================================
#  工具2：期货各月份持仓排名
# ==========================================
@tool
def get_futures_oi_ranking(query: str):
    """查询期货各月份合约持仓排名。如"白银哪个月份持仓最大"、"螺纹主力是哪个月"。"""
    if engine is None: return "❌ 数据库未连接"

    try:
        symbol, asset_type = symbol_map.resolve_symbol(query)
        if not symbol or asset_type != 'future': return f"⚠️ {query}不是期货品种"

        clean = ''.join([i for i in symbol.upper() if not i.isdigit()])
        sql = text(f"""
            SELECT t1.ts_code,t1.trade_date,t1.vol,t1.oi FROM futures_price t1
            INNER JOIN (SELECT ts_code,MAX(trade_date) md FROM futures_price 
                WHERE ts_code LIKE '{clean}%' AND ts_code NOT LIKE '%0' AND ts_code REGEXP '^{clean}[0-9]{{3,4}}$' GROUP BY ts_code) t2 
            ON t1.ts_code=t2.ts_code AND t1.trade_date=t2.md ORDER BY t1.oi DESC LIMIT 4
        """)
        df = pd.read_sql(sql, engine)
        if df.empty: return f"⚠️ 无数据: {query}"

        result = f"📊 {query}({clean})持仓排名 ({df.iloc[0]['trade_date']})\n"
        for i, r in df.iterrows():
            mark = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i + 1}."
            result += f"{mark} {r['ts_code']}: 持仓{fmt_vol(r['oi'])}手\n"
        return result
    except Exception as e:
        return f"❌ 错误: {e}"


# ==========================================
#  工具3：期权持仓排名（ETF+商品合并）
# ==========================================
@tool
def get_option_oi_ranking(query: str):
    """查询期权合约持仓排名。如"50ETF期权持仓最大"、"豆粕期权持仓排名"、"300ETF认购持仓排名"。"""
    if engine is None: return "❌ 数据库未连接"
    q = query.strip()

    # 判断方向
    direction = ""
    dir_name = "全部"
    if any(k in q for k in ['认购', '购', '看涨']):
        direction, dir_name = 'C', '认购'
    elif any(k in q for k in ['认沽', '沽', '看跌']):
        direction, dir_name = 'P', '认沽'

    try:
        # ETF期权
        underlying, etf_name = get_etf_underlying(q)
        if underlying:
            dir_filter = f"AND ob.call_put='{direction}'" if direction else ""
            sql = text(f"""
                SELECT od.ts_code,od.trade_date,od.vol,od.oi,ob.call_put,ob.exercise_price
                FROM option_daily od
                INNER JOIN option_basic ob ON od.ts_code=ob.ts_code
                INNER JOIN (SELECT ts_code,MAX(trade_date) md FROM option_daily GROUP BY ts_code) t ON od.ts_code=t.ts_code AND od.trade_date=t.md
                WHERE ob.underlying=:u AND od.oi>0 AND ob.delist_date>=DATE_FORMAT(NOW(),'%Y%m%d') {dir_filter}
                ORDER BY od.oi DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"u": underlying})
            if df.empty: return f"⚠️ {etf_name}期权无数据"

            result = f"📊 {etf_name}期权持仓排名({dir_name}) {df.iloc[0]['trade_date']}\n"
            for i, r in df.iterrows():
                cp = "购" if r['call_put'] == 'C' else "沽"
                mark = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i + 1}."
                result += f"{mark} {r['ts_code']}({cp}{r['exercise_price']}): {fmt_vol(r['oi'])}手\n"
            return result

        # 商品期权
        prefix, com_name = get_commodity_prefix(q)
        if prefix:
            dir_filter = f"AND ts_code LIKE '%{direction}%'" if direction else ""
            sql = text(f"""
                SELECT t1.ts_code,t1.trade_date,t1.vol,t1.oi FROM commodity_opt_daily t1
                INNER JOIN (SELECT ts_code,MAX(trade_date) md FROM commodity_opt_daily WHERE ts_code LIKE '{prefix}%' GROUP BY ts_code) t2 
                ON t1.ts_code=t2.ts_code AND t1.trade_date=t2.md
                WHERE t1.oi>0 {dir_filter} ORDER BY t1.oi DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine)
            if df.empty: return f"⚠️ {com_name}期权无数据"

            result = f"📊 {com_name}期权持仓排名({dir_name}) {df.iloc[0]['trade_date']}\n"
            for i, r in df.iterrows():
                parsed = parse_commodity_contract(r['ts_code'])
                mark = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i + 1}."
                # ✅ 新格式
                result += f"{mark} {r['ts_code']}（{parsed['full_name']}）: {fmt_vol(r['oi'])}手\n"
            return result

        return "⚠️ 请指定ETF(如50ETF)或商品(如豆粕)期权"
    except Exception as e:
        return f"❌ 错误: {e}"


# ==========================================
#  工具4：期权成交量异常（ETF+商品合并）
# ==========================================
@tool
def get_option_volume_abnormal(query: str = "全部"):
    """查询成交量异常放大的期权合约。如"哪个期权成交量异常"、"50ETF期权成交放大"、"豆粕期权成交异常"。"""
    if engine is None: return "❌ 数据库未连接"
    q = query.strip()

    try:
        # 判断是ETF还是商品期权
        underlying, etf_name = get_etf_underlying(q)
        prefix, com_name = get_commodity_prefix(q)

        if underlying:
            # ETF期权
            date_sql = text("SELECT DISTINCT trade_date FROM option_daily ORDER BY trade_date DESC LIMIT 2")
            dates = pd.read_sql(date_sql, engine)
            if len(dates) < 2: return "⚠️ 数据不足"
            latest, prev = dates.iloc[0]['trade_date'], dates.iloc[1]['trade_date']

            sql = text(f"""
                SELECT t1.ts_code,t1.vol today_vol,t2.vol prev_vol,ROUND(t1.vol/t2.vol,1) ratio,ob.call_put,ob.exercise_price
                FROM option_daily t1
                INNER JOIN option_daily t2 ON t1.ts_code=t2.ts_code AND t2.trade_date=:p
                INNER JOIN option_basic ob ON t1.ts_code=ob.ts_code
                WHERE t1.trade_date=:l AND t2.vol>200 AND t1.vol>t2.vol*2 AND ob.underlying=:u
                ORDER BY ratio DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"l": latest, "p": prev, "u": underlying})
            name = etf_name
        elif prefix:
            # 商品期权
            date_sql = text("SELECT DISTINCT trade_date FROM commodity_opt_daily ORDER BY trade_date DESC LIMIT 2")
            dates = pd.read_sql(date_sql, engine)
            if len(dates) < 2: return "⚠️ 数据不足"
            latest, prev = dates.iloc[0]['trade_date'], dates.iloc[1]['trade_date']

            sql = text(f"""
                SELECT t1.ts_code,t1.vol today_vol,t2.vol prev_vol,ROUND(t1.vol/t2.vol,1) ratio
                FROM commodity_opt_daily t1
                INNER JOIN commodity_opt_daily t2 ON t1.ts_code=t2.ts_code AND t2.trade_date=:p
                WHERE t1.trade_date=:l AND t2.vol>200 AND t1.vol>t2.vol*2 AND t1.ts_code LIKE '{prefix}%'
                ORDER BY ratio DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"l": latest, "p": prev})
            name = com_name
        else:
            # 全部商品期权
            date_sql = text("SELECT DISTINCT trade_date FROM commodity_opt_daily ORDER BY trade_date DESC LIMIT 2")
            dates = pd.read_sql(date_sql, engine)
            if len(dates) < 2: return "⚠️ 数据不足"
            latest, prev = dates.iloc[0]['trade_date'], dates.iloc[1]['trade_date']

            sql = text(f"""
                SELECT t1.ts_code,t1.vol today_vol,t2.vol prev_vol,ROUND(t1.vol/t2.vol,1) ratio
                FROM commodity_opt_daily t1
                INNER JOIN commodity_opt_daily t2 ON t1.ts_code=t2.ts_code AND t2.trade_date=:p
                WHERE t1.trade_date=:l AND t2.vol>200 AND t1.vol>t2.vol*2
                ORDER BY ratio DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"l": latest, "p": prev})
            name = "全部商品"

        if df.empty: return f"✅ {name}期权({latest})无成交量异常"

        result = f"🔥 {name}期权成交量异常 ({latest})\n"
        for _, r in df.iterrows():
            parsed = parse_commodity_contract(r['ts_code'])
            alert = "🔴" if r['ratio'] >= 5 else "🟠" if r['ratio'] >= 3 else "🟡"
            # ✅ 新格式
            result += f"{alert} {r['ts_code']}（{parsed['full_name']}）: {fmt_vol(r['prev_vol'])}→{fmt_vol(r['today_vol'])}手 **{r['ratio']}倍**\n"
        return result
    except Exception as e:
        return f"❌ 错误: {e}"


# ==========================================
#  工具5：期权持仓量异常（ETF+商品合并）
# ==========================================
@tool
def get_option_oi_abnormal(query: str = "全部"):
    """查询持仓量异常增加的期权合约。如"哪个期权持仓异常增加"、"50ETF期权持仓变化大"、"白银期权持仓异常"。"""
    if engine is None: return "❌ 数据库未连接"
    q = query.strip()

    try:
        underlying, etf_name = get_etf_underlying(q)
        prefix, com_name = get_commodity_prefix(q)

        if underlying:
            # ETF期权
            date_sql = text("SELECT DISTINCT trade_date FROM option_daily ORDER BY trade_date DESC LIMIT 2")
            dates = pd.read_sql(date_sql, engine)
            if len(dates) < 2: return "⚠️ 数据不足"
            latest, prev = dates.iloc[0]['trade_date'], dates.iloc[1]['trade_date']

            sql = text(f"""
                SELECT t1.ts_code,t1.oi today_oi,t2.oi prev_oi,(t1.oi-t2.oi) chg,ROUND((t1.oi-t2.oi)/t2.oi*100,1) pct,ob.call_put,ob.exercise_price
                FROM option_daily t1
                INNER JOIN option_daily t2 ON t1.ts_code=t2.ts_code AND t2.trade_date=:p
                INNER JOIN option_basic ob ON t1.ts_code=ob.ts_code
                WHERE t1.trade_date=:l AND t2.oi>500 AND t1.oi>t2.oi*1.3 AND ob.underlying=:u AND ob.delist_date>=DATE_FORMAT(NOW(),'%Y%m%d')
                ORDER BY pct DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"l": latest, "p": prev, "u": underlying})
            name = etf_name
        elif prefix:
            # 商品期权
            date_sql = text("SELECT DISTINCT trade_date FROM commodity_opt_daily ORDER BY trade_date DESC LIMIT 2")
            dates = pd.read_sql(date_sql, engine)
            if len(dates) < 2: return "⚠️ 数据不足"
            latest, prev = dates.iloc[0]['trade_date'], dates.iloc[1]['trade_date']

            sql = text(f"""
                SELECT t1.ts_code,t1.oi today_oi,t2.oi prev_oi,(t1.oi-t2.oi) chg,ROUND((t1.oi-t2.oi)/t2.oi*100,1) pct
                FROM commodity_opt_daily t1
                INNER JOIN commodity_opt_daily t2 ON t1.ts_code=t2.ts_code AND t2.trade_date=:p
                WHERE t1.trade_date=:l AND t2.oi>100 AND t1.oi>t2.oi*1.5 AND t1.ts_code LIKE '{prefix}%'
                ORDER BY pct DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"l": latest, "p": prev})
            name = com_name
        else:
            # 全部商品
            date_sql = text("SELECT DISTINCT trade_date FROM commodity_opt_daily ORDER BY trade_date DESC LIMIT 2")
            dates = pd.read_sql(date_sql, engine)
            if len(dates) < 2: return "⚠️ 数据不足"
            latest, prev = dates.iloc[0]['trade_date'], dates.iloc[1]['trade_date']

            sql = text(f"""
                SELECT t1.ts_code,t1.oi today_oi,t2.oi prev_oi,(t1.oi-t2.oi) chg,ROUND((t1.oi-t2.oi)/t2.oi*100,1) pct
                FROM commodity_opt_daily t1
                INNER JOIN commodity_opt_daily t2 ON t1.ts_code=t2.ts_code AND t2.trade_date=:p
                WHERE t1.trade_date=:l AND t2.oi>100 AND t1.oi>t2.oi*1.5
                ORDER BY pct DESC LIMIT 5
            """)
            df = pd.read_sql(sql, engine, params={"l": latest, "p": prev})
            name = "全部商品"

        if df.empty: return f"✅ {name}期权({latest})无持仓异常增加"

        result = f"🔥 {name}期权持仓异常 ({prev}→{latest})\n"
        for _, r in df.iterrows():
            ts_code = r['ts_code']
            alert = "🔴" if r['pct'] >= 100 else "🟠" if r['pct'] >= 50 else "🟡"

            # ✅ 硬编码：直接构造完整说明
            if ts_code.startswith('LC'):
                # LC = 碳酸锂
                year_month = ts_code[2:6]  # 2603
                strike = ts_code.split('-')[2].split('.')[0]  # 140000
                option_type = '认购' if '-C-' in ts_code else '认沽'
                full_desc = f"碳酸锂{year_month}{option_type}{strike}"

            elif ts_code.startswith('AG'):
                # AG = 白银
                year_month = ts_code[2:6]
                if 'C' in ts_code[6:]:
                    strike = ts_code.split('C')[1].split('.')[0]
                    option_type = '认购'
                else:
                    strike = ts_code.split('P')[1].split('.')[0]
                    option_type = '认沽'
                full_desc = f"白银{year_month}{option_type}{strike}"

            elif ts_code.startswith('SI'):
                # SI = 工业硅
                year_month = ts_code[2:6]
                strike = ts_code.split('-')[2].split('.')[0]
                option_type = '认购' if '-C-' in ts_code else '认沽'
                full_desc = f"工业硅{year_month}{option_type}{strike}"

            elif ts_code.startswith('AL'):
                # AL = 沪铝
                year_month = ts_code[2:6]
                if '-C-' in ts_code:
                    strike = ts_code.split('-')[2].split('.')[0]
                    option_type = '认购'
                else:
                    strike = ts_code.split('-')[2].split('.')[0] if '-P-' in ts_code else ''
                    option_type = '认沽'
                full_desc = f"沪铝{year_month}{option_type}{strike}"

            else:
                # 其他品种
                full_desc = f"{ts_code}合约"

            result += f"{alert} {ts_code}（{full_desc}）: {fmt_vol(r['prev_oi'])}→{fmt_vol(r['today_oi'])}手 **+{r['pct']}%**\n"

        return result
    except Exception as e:
        return f"❌ 错误: {e}"


# ==========================================
#  在 volume_oi_tools.py 末尾添加这个新工具
#  修正版：正确关联 option_basic 和 option_daily
# ==========================================

@tool
def analyze_etf_option_sentiment(query: str = "50ETF"):
    """
    分析ETF期权的持仓变化，解读资金对后市的看法。

    分析维度：
    1. 认购/认沽总持仓变化 → 判断多空情绪
    2. 最大持仓合约行权价移动 → 判断压力位/支撑位变化

    输入：ETF名称，如"50ETF"、"300ETF"、"创业板ETF"
    """
    if engine is None: return "❌ 数据库未连接"

    try:
        # 1. 解析ETF标的
        underlying, etf_name = get_etf_underlying(query)
        if not underlying:
            return "⚠️ 请指定ETF，如：50ETF、300ETF、500ETF、创业板ETF、科创50ETF"

        # 2. 获取最近两个交易日
        date_sql = text("SELECT DISTINCT trade_date FROM option_daily ORDER BY trade_date DESC LIMIT 2")
        dates = pd.read_sql(date_sql, engine)
        if len(dates) < 2:
            return "⚠️ 数据不足，需要至少2个交易日的数据"

        today = dates.iloc[0]['trade_date']
        yesterday = dates.iloc[1]['trade_date']

        # 3. 查询今日和昨日的持仓数据
        # 关键：通过 ts_code 关联 option_basic 和 option_daily
        sql = text("""
                   SELECT od.trade_date,
                          ob.call_put,
                          ob.exercise_price,
                          od.oi,
                          od.ts_code
                   FROM option_daily od
                            INNER JOIN option_basic ob ON od.ts_code = ob.ts_code
                   WHERE od.trade_date IN (:today, :yesterday)
                     AND ob.underlying = :underlying
                     AND ob.delist_date >= :today
                   ORDER BY od.trade_date, ob.call_put, od.oi DESC
                   """)
        df = pd.read_sql(sql, engine, params={
            "today": today,
            "yesterday": yesterday,
            "underlying": underlying
        })

        if df.empty:
            return f"⚠️ {etf_name}期权暂无数据"

        # 4. 分离今日和昨日数据
        df_today = df[df['trade_date'] == today]
        df_yesterday = df[df['trade_date'] == yesterday]

        if df_today.empty or df_yesterday.empty:
            return f"⚠️ {etf_name}期权数据不完整，需要连续两个交易日的数据"

        # 5. 计算认购/认沽总持仓变化
        call_today = df_today[df_today['call_put'] == 'C']['oi'].sum()
        call_yesterday = df_yesterday[df_yesterday['call_put'] == 'C']['oi'].sum()
        put_today = df_today[df_today['call_put'] == 'P']['oi'].sum()
        put_yesterday = df_yesterday[df_yesterday['call_put'] == 'P']['oi'].sum()

        call_chg = call_today - call_yesterday
        put_chg = put_today - put_yesterday
        call_chg_pct = (call_chg / call_yesterday * 100) if call_yesterday > 0 else 0
        put_chg_pct = (put_chg / put_yesterday * 100) if put_yesterday > 0 else 0

        # 6. 找出最大持仓合约（认购/认沽分别找）
        call_today_df = df_today[df_today['call_put'] == 'C'].copy()
        call_yesterday_df = df_yesterday[df_yesterday['call_put'] == 'C'].copy()
        put_today_df = df_today[df_today['call_put'] == 'P'].copy()
        put_yesterday_df = df_yesterday[df_yesterday['call_put'] == 'P'].copy()

        # 最大持仓的行权价（需要重置索引避免 KeyError）
        if not call_today_df.empty:
            call_today_df = call_today_df.reset_index(drop=True)
            call_strike_today = call_today_df.loc[call_today_df['oi'].idxmax(), 'exercise_price']
            call_max_oi_today = call_today_df['oi'].max()
        else:
            call_strike_today, call_max_oi_today = 0, 0

        if not call_yesterday_df.empty:
            call_yesterday_df = call_yesterday_df.reset_index(drop=True)
            call_strike_yesterday = call_yesterday_df.loc[call_yesterday_df['oi'].idxmax(), 'exercise_price']
        else:
            call_strike_yesterday = 0

        if not put_today_df.empty:
            put_today_df = put_today_df.reset_index(drop=True)
            put_strike_today = put_today_df.loc[put_today_df['oi'].idxmax(), 'exercise_price']
            put_max_oi_today = put_today_df['oi'].max()
        else:
            put_strike_today, put_max_oi_today = 0, 0

        if not put_yesterday_df.empty:
            put_yesterday_df = put_yesterday_df.reset_index(drop=True)
            put_strike_yesterday = put_yesterday_df.loc[put_yesterday_df['oi'].idxmax(), 'exercise_price']
        else:
            put_strike_yesterday = 0

        call_strike_move = call_strike_today - call_strike_yesterday
        put_strike_move = put_strike_today - put_strike_yesterday

        # ==========================================
        # 7. 核心解读逻辑
        # ==========================================
        signals = []
        bullish_score = 0  # 看多得分
        bearish_score = 0  # 看空得分

        # 7.1 认购持仓变化解读
        # 期权市场以卖方为主导（机构），认购持仓增加 = 机构卖认购 = 看空
        if call_chg_pct > 10:
            signals.append(f"🔴 认购持仓大增 {call_chg_pct:+.1f}%，卖方（机构）加码压制上方，**偏空信号**")
            bearish_score += 2
        elif call_chg_pct > 5:
            signals.append(f"🟠 认购持仓增加 {call_chg_pct:+.1f}%，卖方略偏谨慎")
            bearish_score += 1
        elif call_chg_pct < -10:
            signals.append(f"🟢 认购持仓大减 {call_chg_pct:+.1f}%，卖方平仓撤退，**上方压力减轻**")
            bullish_score += 1
        elif call_chg_pct < -5:
            signals.append(f"🟢 认购持仓减少 {call_chg_pct:+.1f}%，上方压力略减")

        # 7.2 认沽持仓变化解读
        # 认沽持仓增加 = 机构卖认沽 = 看多（愿意接货）
        if put_chg_pct > 10:
            signals.append(f"🟢 认沽持仓大增 {put_chg_pct:+.1f}%，卖方（机构）愿意接货，**偏多信号**")
            bullish_score += 2
        elif put_chg_pct > 5:
            signals.append(f"🟢 认沽持仓增加 {put_chg_pct:+.1f}%，卖方看好下方支撑")
            bullish_score += 1
        elif put_chg_pct < -10:
            signals.append(f"🔴 认沽持仓大减 {put_chg_pct:+.1f}%，卖方平仓撤离，**下方支撑减弱**")
            bearish_score += 1
        elif put_chg_pct < -5:
            signals.append(f"🟠 认沽持仓减少 {put_chg_pct:+.1f}%，支撑略减弱")

        # 7.3 认购最大持仓行权价移动解读
        # 认购最大持仓 = 压力位，上移 = 压力位上移 = 看多
        if call_strike_move > 0:
            signals.append(f"🟢 认购最大持仓行权价上移 {call_strike_yesterday}→{call_strike_today}，**压力位上移，偏多**")
            bullish_score += 2
        elif call_strike_move < 0:
            signals.append(f"🔴 认购最大持仓行权价下移 {call_strike_yesterday}→{call_strike_today}，**压力位下移，偏空**")
            bearish_score += 2
        else:
            signals.append(f"⚪ 认购最大持仓行权价不变 {call_strike_today}，压力位稳定")

        # 7.4 认沽最大持仓行权价移动解读
        # 认沽最大持仓 = 支撑位，上移 = 支撑位上移 = 看多
        if put_strike_move > 0:
            signals.append(f"🟢 认沽最大持仓行权价上移 {put_strike_yesterday}→{put_strike_today}，**支撑位上移，偏多**")
            bullish_score += 2
        elif put_strike_move < 0:
            signals.append(f"🔴 认沽最大持仓行权价下移 {put_strike_yesterday}→{put_strike_today}，**支撑位下移，偏空**")
            bearish_score += 2
        else:
            signals.append(f"⚪ 认沽最大持仓行权价不变 {put_strike_today}，支撑位稳定")

        # 8. 综合判断
        if bullish_score > bearish_score + 2:
            overall = "📈 **综合研判：机构偏多**"
            overall_detail = "多个信号指向看涨，资金布局偏乐观"
        elif bearish_score > bullish_score + 2:
            overall = "📉 **综合研判：机构偏空**"
            overall_detail = "多个信号指向看跌，资金布局偏谨慎"
        elif bullish_score > bearish_score:
            overall = "📊 **综合研判：中性偏多**"
            overall_detail = "信号略偏乐观，但不够强烈"
        elif bearish_score > bullish_score:
            overall = "📊 **综合研判：中性偏空**"
            overall_detail = "信号略偏谨慎，但不够强烈"
        else:
            overall = "📊 **综合研判：多空均衡**"
            overall_detail = "暂无明显方向，观望为主"

        # 9. 组装报告
        result = f"""📊 **{etf_name}期权持仓分析** ({yesterday} → {today})

**一、持仓量变化**
| 类型 | 昨日持仓 | 今日持仓 | 变化 |
|------|----------|----------|------|
| 认购 | {fmt_vol(call_yesterday)} | {fmt_vol(call_today)} | {call_chg_pct:+.1f}% |
| 认沽 | {fmt_vol(put_yesterday)} | {fmt_vol(put_today)} | {put_chg_pct:+.1f}% |

**二、最大持仓合约（压力/支撑位）**
| 类型 | 昨日行权价 | 今日行权价 | 移动 | 今日持仓 |
|------|------------|------------|------|----------|
| 认购(压力位) | {call_strike_yesterday} | {call_strike_today} | {'↑上移' if call_strike_move > 0 else '↓下移' if call_strike_move < 0 else '→不变'} | {fmt_vol(call_max_oi_today)} |
| 认沽(支撑位) | {put_strike_yesterday} | {put_strike_today} | {'↑上移' if put_strike_move > 0 else '↓下移' if put_strike_move < 0 else '→不变'} | {fmt_vol(put_max_oi_today)} |

**三、信号解读**
{chr(10).join(signals)}

**四、{overall}**
{overall_detail}

---
**解读原理**：
- ETF期权市场以机构卖方为主导
- 认购持仓上升 = 机构卖Call压制 = 偏空
- 认沽持上升 = 机构卖Put接货 = 偏多
- 最大持仓行权价 = 市场认可的压力/支撑位
"""
        return result

    except Exception as e:
        import traceback
        return f"❌ 分析出错: {e}\n{traceback.format_exc()}"


# ==========================================
#  新增工具：查询ETF期权可用行权价
#  添加到 volume_oi_tools.py 末尾
# ==========================================

@tool
def get_etf_option_strikes(query: str = "50ETF"):
    """
    查询指定ETF期权当前可用的行权价列表，用于制定交易策略。

    返回：当月/次月合约的所有行权价，标注平值附近位置。
    输入：ETF名称，如"50ETF"、"300ETF"、"创业板ETF"
    """
    if engine is None: return "❌ 数据库未连接"

    # 解析ETF名称
    etf_code, asset_type = symbol_map.resolve_symbol(query)

    if not etf_code:
        return f"无法识别ETF: {query}"

    # 确保代码格式正确(带交易所后缀)
    if "." not in etf_code:
        if etf_code.startswith("15") or etf_code.startswith("16"):
            etf_code += ".SZ"
        else:
            etf_code += ".SH"

    print(f"[DEBUG] {query} -> {etf_code}")

    try:
        # ✅ 直接使用已解析的代码
        underlying = etf_code
        etf_name = query

        # 2. 获取ETF当前价格
        etf_price_sql = text("""
                             SELECT close_price
                             FROM stock_price
                             WHERE ts_code = :underlying
                             ORDER BY trade_date DESC LIMIT 1
                             """)
        etf_df = pd.read_sql(etf_price_sql, engine, params={"underlying": underlying})
        etf_price = etf_df.iloc[0]['close_price'] if not etf_df.empty else None

        # 3. 获取最新交易日
        date_sql = text("SELECT MAX(trade_date) as latest FROM option_daily")
        latest_date = pd.read_sql(date_sql, engine).iloc[0]['latest']

        # 4. 查询当前可用的行权价（未到期合约）
        sql = text("""
                   SELECT DISTINCT ob.exercise_price,
                                   ob.call_put,
                                   SUBSTRING(ob.name, LOCATE('期权', ob.name) + 2, 4) as exp_month,
                                   od.oi,
                                   od.vol
                   FROM option_basic ob
                            INNER JOIN option_daily od ON ob.ts_code = od.ts_code
                   WHERE ob.underlying = :underlying
                     AND ob.delist_date >= :latest_date
                     AND od.trade_date = :latest_date
                   ORDER BY ob.exercise_price
                   """)
        df = pd.read_sql(sql, engine, params={
            "underlying": underlying,
            "latest_date": latest_date
        })

        if df.empty:
            return f"⚠️ {etf_name}期权暂无可用合约"

        # 5. 获取所有唯一行权价
        strikes = sorted(df['exercise_price'].unique())

        # 6. 找出平值附近的行权价
        if etf_price:
            # 找最接近ETF价格的行权价作为平值
            atm_strike = min(strikes, key=lambda x: abs(x - etf_price))
            atm_idx = strikes.index(atm_strike)
        else:
            atm_idx = len(strikes) // 2
            atm_strike = strikes[atm_idx]

        # 7. 统计每个行权价的持仓量
        call_oi = df[df['call_put'] == 'C'].groupby('exercise_price')['oi'].sum().to_dict()
        put_oi = df[df['call_put'] == 'P'].groupby('exercise_price')['oi'].sum().to_dict()

        # 8. 获取到期月份
        months = df['exp_month'].unique()
        month_str = '/'.join(sorted(set(months)))

        # 9. 组装结果
        etf_price_str = f"{etf_price:.3f}" if etf_price else "未知"
        result = f"""📋 **{etf_name}期权可用行权价** ({latest_date})

**ETF现价**: {etf_price_str}
**平值(ATM)**: {atm_strike}
**到期月份**: {month_str}

**行权价列表** (共{len(strikes)}个):
"""

        # 显示平值附近的行权价（上下各5档）
        start_idx = max(0, atm_idx - 5)
        end_idx = min(len(strikes), atm_idx + 6)

        result += "\n| 行权价 | 位置 | 认购持仓 | 认沽持仓 |\n"
        result += "|--------|------|----------|----------|\n"

        for i in range(start_idx, end_idx):
            strike = strikes[i]
            if strike == atm_strike:
                pos = "**⭐平值**"
            elif strike > atm_strike:
                pos = f"虚值+{i - atm_idx}"
            else:
                pos = f"实值{i - atm_idx}"

            c_oi = call_oi.get(strike, 0)
            p_oi = put_oi.get(strike, 0)
            result += f"| {strike} | {pos} | {fmt_vol(c_oi)} | {fmt_vol(p_oi)} |\n"

        # 10. 添加完整行权价列表（供AI参考）
        result += f"\n**全部行权价**: {', '.join([str(s) for s in strikes])}\n"

        # 11. 添加策略参考提示
        result += f"""
---
💡 **参考行权价**（基于当前平值 {atm_strike}）:
- 保守牛市价差: 买入 购{strikes[max(atm_idx - 1, 0)]}(实值) + 卖出 购{strikes[min(atm_idx + 1, len(strikes) - 1)]}(浅虚值)
- 积极牛市价差: 买入 购{strikes[min(atm_idx + 1, len(strikes) - 1)]}(虚值) + 卖出 购{strikes[min(atm_idx + 2, len(strikes) - 1)]}(更虚值)
- 反比例认购: 卖出 购{atm_strike}(平值)1张 + 买入 购{strikes[min(atm_idx + 1, len(strikes) - 1)]}(虚值)3张
- 备兑开仓(Covered Call): 卖出 购{strikes[min(atm_idx + 1, len(strikes) - 1)]} 或 购{strikes[min(atm_idx + 2, len(strikes) - 1)]}
- 多头保险策略: 买入 沽{strikes[max(atm_idx - 1, 0)]} 或 沽{strikes[max(atm_idx - 2, 0)]}
"""
        return result

    except Exception as e:
        import traceback
        return f"❌ 查询出错: {e}\n{traceback.format_exc()}"
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
    return create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")


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
                cp = "购" if 'C' in r['ts_code'] else "沽"
                mark = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else f"{i + 1}."
                result += f"{mark} {r['ts_code']}({cp}): {fmt_vol(r['oi'])}手\n"
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
            cp = "购" if 'C' in r['ts_code'] else "沽"
            alert = "🔴" if r['ratio'] >= 5 else "🟠" if r['ratio'] >= 3 else "🟡"
            result += f"{alert} {r['ts_code']}({cp}): {fmt_vol(r['prev_vol'])}→{fmt_vol(r['today_vol'])}手 **{r['ratio']}倍**\n"
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
            cp = "购" if 'C' in r['ts_code'] else "沽"
            alert = "🔴" if r['pct'] >= 100 else "🟠" if r['pct'] >= 50 else "🟡"
            result += f"{alert} {r['ts_code']}({cp}): {fmt_vol(r['prev_oi'])}→{fmt_vol(r['today_oi'])}手 **+{r['pct']}%**\n"
        return result
    except Exception as e:
        return f"❌ 错误: {e}"
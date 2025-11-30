import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv
from functools import lru_cache

# 1. 初始化
load_dotenv(override=True)
ts_token = os.getenv("TUSHARE_TOKEN")

# 2. 期貨字典 (手動維護常用)
FUTURES_DICT = {
    "碳酸锂": "lc", "工业硅": "si","多晶硅": "ps",
    "螺纹": "rb", "螺纹刚": "rb", "热卷": "hc",
    "铁矿": "i", "铁矿石": "i", "焦煤": "jm", "焦炭": "j",
    "豆粕": "m", "豆油": "y", "棕榈": "p", "棕榈油": "p",
    "玻璃": "fg", "纯碱": "sa", "甲醇": "ma", "尿素": "ur","棉花": "cf",
    "沪深300": "IF", "上证50": "IH", "中证500": "IC", "中证1000": "IM",
    "黃金": "au", "白银": "ag", "原油": "sc", "燃油": "fu", "铜": "cu", "铝": "al", "锌": "zn"
}


@lru_cache(maxsize=1)
def get_stock_map():
    if not ts_token: return {}
    try:
        ts.set_token(ts_token)
        pro = ts.pro_api()
        df_s = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        df_f = pro.fund_basic(market='E', status='L', fields='ts_code,name')
        df = pd.concat([df_s, df_f])
        return dict(zip(df['name'], df['ts_code']))
    except:
        return {}


def resolve_symbol(query: str):
    # 【修复】输入为空时直接返回双 None，防止报错
    if not query or not isinstance(query, str) or query.strip() == "":
        return None, None

    query = query.strip().upper()

    # 1. 期货代码
    if query.lower() in FUTURES_DICT.values():
        return query.lower(), 'future'

    # 2. 期货名称
    for name, code in FUTURES_DICT.items():
        if query in name or name in query:
            return code, 'future'

    # 3. 股票/ETF
    stock_map = get_stock_map()
    if not stock_map: return None, None  # 防止字典为空

    if query in stock_map:
        return stock_map[query], 'stock'

    for name, code in stock_map.items():
        if query in name: return code, 'stock'

    if query in stock_map.values(): return query, 'stock'

    for code in stock_map.values():
        if query == code.split('.')[0]: return code, 'stock'

    return None, None
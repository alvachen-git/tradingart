import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv
from functools import lru_cache

# 1. 初始化
load_dotenv(override=True)
ts_token = os.getenv("TUSHARE_TOKEN")

# 2. 期货字典 (手动维护常用)
FUTURES_DICT = {
    "碳酸锂": "lc", "工业硅": "si",
    "螺纹": "rb", "螺纹钢": "rb", "热卷": "hc",
    "铁矿": "i", "铁矿石": "i", "焦煤": "jm", "焦炭": "j",
    "豆粕": "m", "豆油": "y", "棕榈": "p", "棕榈油": "p",
    "玻璃": "fg", "纯碱": "sa", "甲醇": "ma", "尿素": "ur",
    "沪深300": "IF", "上证50": "IH", "中证500": "IC", "中证1000": "IM",
    "黄金": "au", "白银": "ag", "原油": "sc", "燃油": "fu"
}


@lru_cache(maxsize=1)
def get_stock_map():
    """
    获取全A股名称映射表 (带缓存)
    返回: {'平安银行': '000001.SZ', '贵州茅台': '600519.SH', ...}
    """
    if not ts_token: return {}

    try:
        ts.set_token(ts_token)
        pro = ts.pro_api()
        # 获取上市股票列表 (只取 name 和 ts_code)
        df = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        # 转为字典
        return dict(zip(df['name'], df['ts_code']))
    except:
        return {}


def resolve_symbol(query: str):
    """
    核心函数：输入中文/代码，返回 (标准代码, 类型)
    类型: 'future' 或 'stock'
    """
    query = query.strip().upper()  # 转大写，去空格

    # 1. 尝试直接匹配期货代码 (如 LC, RB)
    if query.lower() in FUTURES_DICT.values():
        return query.lower(), 'future'

    # 2. 尝试匹配期货中文名 (如 碳酸锂)
    # 模糊匹配：如果 query 在 key 里 (比如 "螺纹" 在 "螺纹钢" 里)
    for name, code in FUTURES_DICT.items():
        if query in name or name in query:
            return code, 'future'

    # 3. 尝试匹配股票 (需要联网或缓存)
    stock_map = get_stock_map()

    # A. 精确匹配中文名
    if query in stock_map:
        return stock_map[query], 'stock'

    # B. 模糊匹配中文名 (如 "茅台" -> "贵州茅台")
    # 简单遍历 (性能一般，但够用)
    for name, code in stock_map.items():
        if query in name:
            return code, 'stock'

    # C. 直接匹配股票代码 (如 600519)
    # 检查 value 里有没有这个代码
    if query in stock_map.values():
        return query, 'stock'
    # 处理不带后缀的情况 (600519 -> 600519.SH)
    for code in stock_map.values():
        if query == code.split('.')[0]:
            return code, 'stock'

    return None, None


# 测试
if __name__ == "__main__":
    print(resolve_symbol("茅台"))  # ('600519.SH', 'stock')
    print(resolve_symbol("碳酸锂"))  # ('lc', 'future')
    print(resolve_symbol("300750"))  # ('300750.SZ', 'stock')
    print(resolve_symbol("螺纹"))  # ('rb', 'future')
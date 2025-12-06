import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv
from functools import lru_cache

# 1. 初始化
load_dotenv(override=True)

# --- 【关键修复】强制清除代理 (防止 Tushare 连接失败) ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

ts_token = os.getenv("TUSHARE_TOKEN")

COMMON_ALIASES = {
    # 黑色系
    "螺纹": "rb", "螺纹钢": "rb", "热卷": "hc", "铁矿": "i", "铁矿石": "i",
    "焦煤": "jm", "焦炭": "j", "硅铁": "sf", "锰硅": "sm","不锈钢": "ss",

    # 有色金属
    "铜": "cu", "沪铜": "cu", "铝": "al", "沪铝": "al", "锌": "zn", "沪锌": "zn",
    "铅": "pb", "沪铅": "pb", "镍": "ni", "沪镍": "ni", "锡": "sn", "沪锡": "sn", "氧化铝": "ao","铝合金": "ad",
    "黄金": "au", "沪金": "au", "金": "au","白银": "ag", "沪银": "ag","银": "ag",

    # 能源化工
    "原油": "sc", "燃油": "fu", "沥青": "bu", "橡胶": "ru", "天胶": "ru", "天然橡胶": "ru","液化气": "pg","lpg": "pg",
    "20号胶": "nr", "br橡胶": "br","塑料": "l", "PVC": "v", "PP": "pp", "聚丙烯": "pp",
    "乙二醇": "eg", "苯乙烯": "eb", "纯苯": "bz","甲醇": "ma", "尿素": "ur","烧碱": "sh",
    "纯碱": "sa", "玻璃": "fg", "短纤": "pf", "PTA": "ta","PX": "px","对二甲苯": "px","瓶片": "pr",

    # 农产品
    "豆粕": "m", "豆油": "y", "棕榈": "p", "棕榈油": "p", "菜油": "oi", "菜粕": "rm","豆一": "a","豆二": "b",
    "玉米": "c", "淀粉": "cs", "鸡蛋": "jd", "生猪": "lh", "棉花": "cf", "白糖": "sr","花生": "pk","苹果": "ap","红枣": "cj",

    # 新能源
    "碳酸锂": "lc", "工业硅": "si","多晶硅": "ps","钯金": "pd","铂金": "pt","纸浆": "sp","双胶纸": "op","原木": "lg",

    # 股指/国债
    "沪深300": "IF", "上证50": "IH", "中证500": "IC", "中证1000": "IM",
    "十债": "T", "10年国债": "T", "五年债": "TF", "5年国债": "TF", "二债": "TS","2年国债": "TS", "三十债": "TL", "30年国债": "TL",

    # --- ETF (官方名太长，这里加简称) ---
    "50ETF": "510050.SH", "上证50ETF": "510050.SH",
    "300ETF": "510300.SH", "沪深300ETF": "510300.SH",
    "500ETF": "510500.SH", "中证500ETF": "510500.SH",
    "创业板ETF": "159915.SZ", "创业板": "159915.SZ",
    "科创50": "588000.SH", "科创50ETF": "588000.SH",
    "科创板": "588000.SH"
}


@lru_cache(maxsize=1)
def get_all_market_map():
    """
    获取【全市场】名称映射表 (股票 + ETF)
    """
    if not ts_token: return {}

    try:
        print("[*] 正在加载全市场品种列表 (股票 + ETF)...")
        ts.set_token(ts_token)
        pro = ts.pro_api()

        # 1. 获取股票列表
        df_stock = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')

        # 2. 获取 ETF 列表 (关键！)
        # market='E' 代表场内基金(Exchange)
        df_fund = pro.fund_basic(market='E', status='L', fields='ts_code,name')

        # 3. 合并
        df_all = pd.concat([df_stock, df_fund])

        # 转字典 {名称: 代码}
        # 注意：如果有重名，后面的会覆盖前面的，通常 ETF 名字比较独特，问题不大
        mapping = dict(zip(df_all['name'], df_all['ts_code']))

        print(f" [√] 加载完成，共 {len(mapping)} 个品种")
        return mapping
    except Exception as e:
        print(f" [!] 加载品种列表失败: {e}")
        return {}


def resolve_symbol(query: str):
    """
    核心解析函数
    返回: (标准代码, 类型)
    类型: 'future' 或 'stock' (ETF也算stock类型，因为存在stock_price表)
    """
    if not query or not isinstance(query, str) or query.strip() == "":
        return None, None

    query = query.strip().upper()

    # --- 1. 查常用字典 (优先级最高，支持简称) ---
    # 检查 Key (名称)
    if query in COMMON_ALIASES:
        code = COMMON_ALIASES[query]
        # 判断是期货还是股票/ETF
        # 期货代码通常没有点 (lc, rb)，股票/ETF有点 (510050.SH)
        asset_type = 'stock' if '.' in code else 'future'
        return code, asset_type

    # 检查 Value (代码) - 比如用户直接输入 'lc'
    if query.lower() in COMMON_ALIASES.values():
        return query.lower(), 'future'

    # --- 2. 查全市场字典 (Tushare) ---
    market_map = get_all_market_map()

    # A. 精确匹配名称 (如 "贵州茅台", "华夏上证50ETF")
    if query in market_map:
        return market_map[query], 'stock'

    # B. 模糊匹配名称 (如 "茅台" -> "贵州茅台")
    # 遍历查找 (性能稍低，但对聊天机器人够用)
    for name, code in market_map.items():
        if query in name:
            return code, 'stock'

    # C. 匹配代码 (如 510050, 600519)
    # 检查是否在 values 里
    # 为了快一点，我们直接判断格式
    if query[0].isdigit():
        # 尝试直接匹配
        if query in market_map.values():
            return query, 'stock'
        # 尝试不带后缀匹配 (输入 510050 -> 找 510050.SH)
        for code in market_map.values():
            if query == code.split('.')[0]:
                return code, 'stock'

    return None, None


# 测试
if __name__ == "__main__":
    print(f"50ETF -> {resolve_symbol('50ETF')}")
    print(f"茅台 -> {resolve_symbol('茅台')}")
    print(f"螺纹 -> {resolve_symbol('螺纹')}")
    print(f"橡胶 -> {resolve_symbol('橡胶')}")
import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv
from functools import lru_cache
import re

# 1. 初始化
load_dotenv(override=True)

# --- 强制清除代理 ---
for key in ["HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"]:
    if key in os.environ:
        del os.environ[key]

ts_token = os.getenv("TUSHARE_TOKEN")

# 基础映射表：中文名称 -> 主力连续代码 (不带数字)
COMMON_ALIASES = {
# === 🟢 A股核心资产 (新增) ===
    "宁德时代": "300750.SZ", "宁德": "300750.SZ",
    "贵州茅台": "600519.SH", "茅台": "600519.SH",
    "五粮液": "000858.SZ",
    "比亚迪": "002594.SZ",
    "东方财富": "300059.SZ", "东财": "300059.SZ",
    "招商银行": "600036.SH", "招行": "600036.SH",
    "中国平安": "601318.SH", "平安": "601318.SH",
    "隆基绿能": "601012.SH", "隆基": "601012.SH",
    "中国神华": "601088.SH", "神华": "601088.SH",
    "长江电力": "600900.SH", "长电": "600900.SH",
    "迈瑞医疗": "300760.SZ", "迈瑞": "300760.SZ",
    "恒瑞医药": "600276.SH", "恒瑞": "600276.SH",
    "万华化学": "600309.SH", "万华": "600309.SH",
    "立讯精密": "002475.SZ", "立讯": "002475.SZ",
    "美的集团": "000333.SZ", "美的": "000333.SZ",
    "格力电器": "000651.SZ", "格力": "000651.SZ",
    "紫金矿业": "601899.SH", "紫金": "601899.SH",
    "海康威视": "002415.SZ", "海康": "002415.SZ",
    "中信证券": "600030.SH", "中信": "600030.SH",
    "中国铝业": "601600.SH", "中铝": "601600.SH",
    "京东方A": "000725.SZ", "京东方": "000725.SZ",
    "中芯国际": "688981.SH", "中芯": "688981.SH",
    "中国石油": "601857.SH", "中石油": "601857.SH",
    "阳光电源": "300274.SZ", "赣锋锂业": "002460.SZ",
    "新易盛": "300502.SZ", "工業富聯": "601138.SH",

    # 黑色系
    "螺纹": "rb", "螺纹钢": "rb", "热卷": "hc", "铁矿": "i", "铁矿石": "i",
    "焦煤": "jm", "焦炭": "j", "硅铁": "sf", "锰硅": "sm", "不锈钢": "ss",

    # 有色金属
    "铜": "cu", "沪铜": "cu", "铝": "al", "沪铝": "al", "锌": "zn", "沪锌": "zn",
    "铅": "pb", "沪铅": "pb", "镍": "ni", "沪镍": "ni", "锡": "sn", "沪锡": "sn",
    "氧化铝": "ao", "铝合金": "ad",
    "黄金": "au", "沪金": "au", "金": "au", "白银": "ag", "沪银": "ag", "银": "ag",

    # 能源化工
    "原油": "sc", "燃油": "fu","燃料油": "fu", "沥青": "bu", "橡胶": "ru", "天胶": "ru", "天然橡胶": "ru",
    "液化气": "pg", "lpg": "pg", "20号胶": "nr", "br橡胶": "br", "塑料": "l",
    "PVC": "v", "PP": "pp", "聚丙烯": "pp", "乙二醇": "eg", "苯乙烯": "eb",
    "纯苯": "bz", "甲醇": "ma", "尿素": "ur", "烧碱": "sh", "纯碱": "sa",
    "玻璃": "fg", "短纤": "pf", "PTA": "ta", "PX": "px", "对二甲苯": "px", "瓶片": "pr",

    # 农产品
    "豆粕": "m", "豆油": "y", "棕榈": "p", "棕榈油": "p", "菜油": "oi", "菜粕": "rm",
    "豆一": "a", "豆二": "b", "玉米": "c", "淀粉": "cs", "鸡蛋": "jd", "生猪": "lh",
    "棉花": "cf", "白糖": "sr", "花生": "pk", "苹果": "ap", "红枣": "cj",

    # 新能源
    "碳酸锂": "lc", "工业硅": "si", "多晶硅": "ps", "钯金": "pd", "铂金": "pt",
    "纸浆": "sp", "双胶纸": "op", "原木": "lg",

    # 股指/国债
    "沪深300": "IF", "上证50": "IH", "中证500": "IC", "中证1000": "IM",
    "十债": "T", "10年国债": "T", "五年债": "TF", "5年国债": "TF",
    "二债": "TS", "2年国债": "TS", "三十债": "TL", "30年国债": "TL",

    # --- ETF ---
    "50ETF": "510050.SH", "上证50ETF": "510050.SH",
    "300ETF": "510300.SH", "沪深300ETF": "510300.SH",
    "500ETF": "510500.SH", "中证500ETF": "510500.SH",
    "创业板ETF": "159915.SZ", "创业板": "159915.SZ",
    "科创50": "588000.SH", "科创50ETF": "588000.SH"
}


@lru_cache(maxsize=1)
def get_all_market_map():
    """获取全市场品种列表 (股票 + ETF)"""
    if not ts_token: return {}
    try:
        ts.set_token(ts_token)
        pro = ts.pro_api()
        df_stock = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        df_fund = pro.fund_basic(market='E', status='L', fields='ts_code,name')
        df_all = pd.concat([df_stock, df_fund])
        return dict(zip(df_all['name'], df_all['ts_code']))
    except Exception as e:
        print(f" [!] 加载品种列表失败: {e}")
        return {}


def resolve_symbol(query: str):
    """
    核心解析函数：智能识别 主力连续 vs 具体合约
    """
    if not query or not isinstance(query, str) or query.strip() == "":
        return None, None

    query = query.strip().upper()

    # --- 1. 尝试匹配具体期货合约 (如 "豆粕2505", "rb2510", "M2509") ---
    # 规则：中文/英文前缀 + 3-4位数字

    # 提取数字部分
    digit_match = re.search(r"(\d{3,4})$", query)
    if digit_match:
        number_part = digit_match.group(1)
        # 提取前缀 (去掉数字剩下的部分)
        prefix_part = query[:digit_match.start()].strip()

        # 尝试解析前缀
        # 情况 A: 前缀是中文 (如 "豆粕") -> 查字典转代码 (M)
        if prefix_part in COMMON_ALIASES:
            base_code = COMMON_ALIASES[prefix_part]
            # 拼接成 M2505，直接返回
            # 注意：如果 base_code 是 stock 类型 (带点)，这里不适用，跳过
            if '.' not in base_code:
                return f"{base_code.upper()}{number_part}", 'future'

        # 情况 B: 前缀本身就是代码 (如 "RB", "M")
        # 检查是否在字典的值里
        if prefix_part.lower() in COMMON_ALIASES.values():
            return f"{prefix_part.upper()}{number_part}", 'future'

    # --- 2. 查常用字典 (主力连续 / ETF / 股票简称) ---
    if query in COMMON_ALIASES:
        code = COMMON_ALIASES[query]
        # 期货代码通常没有点，股票/ETF有点
        asset_type = 'stock' if '.' in code else 'future'
        # 如果是期货，这里返回的是基础代码 (如 'rb', 'M')，代表主力连续
        return code, asset_type

    # 检查是否直接输入了代码 (如 'M', 'RB')
    if query.lower() in COMMON_ALIASES.values():
        return query.upper(), 'future'

    # --- 3. 查全市场字典 (股票/ETF) ---
    market_map = get_all_market_map()
    if query in market_map:
        return market_map[query], 'stock'
    for name, code in market_map.items():
        if query in name:
            return code, 'stock'

    # 4. 匹配纯数字代码 (股票/ETF)
    if query[0].isdigit():
        for code in market_map.values():
            if query == code.split('.')[0]:
                return code, 'stock'

    return None, None


# 测试
if __name__ == "__main__":
    print(f"豆粕 -> {resolve_symbol('豆粕')}")  # ('m', 'future') -> 主连
    print(f"豆粕2505 -> {resolve_symbol('豆粕2505')}")  # ('M2505', 'future') -> 分合约
    print(f"rb2510 -> {resolve_symbol('rb2510')}")  # ('RB2510', 'future')
    print(f"螺纹 -> {resolve_symbol('螺纹')}")  # ('rb', 'future')
    print(f"50ETF -> {resolve_symbol('50ETF')}")  # ('510050.SH', 'stock')
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from langchain_core.tools import tool
import symbol_map

# 1. 初始化数据库连接
load_dotenv(override=True)

db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)


# ==========================================
#  核心工具函数：获取价格序列
# ==========================================

def get_price_series(code: str, asset_type: str, start_date: str) -> pd.Series:
    """
    通用函数：从数据库获取指定品种的收盘价序列
    """
    if asset_type == 'stock':
        table_name = 'stock_price'
    elif asset_type == 'index':
        table_name = 'index_price'
    elif asset_type == 'future':
        table_name = 'futures_price'
    else:
        return pd.Series()

    # --- 优化点：期货支持无后缀查询 (如 'AU' 匹配 'AU.SHF') ---
    if asset_type == 'future':
        sql = f"""
            SELECT trade_date, close_price 
            FROM {table_name} 
            WHERE (ts_code = :code OR ts_code LIKE CONCAT(:code, '.%')) 
            AND trade_date >= :start 
            ORDER BY trade_date
        """
    else:
        # 股票和指数通常建议精确匹配
        sql = f"""
            SELECT trade_date, close_price 
            FROM {table_name} 
            WHERE ts_code = :code AND trade_date >= :start 
            ORDER BY trade_date
        """

    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params={"code": code, "start": start_date})

        if df.empty:
            return pd.Series()

        df['trade_date'] = pd.to_datetime(df['trade_date'])
        df.set_index('trade_date', inplace=True)

        # 简单去重：防止同一天有两条数据导致计算报错
        if df.index.duplicated().any():
            df = df[~df.index.duplicated(keep='first')]

        return df['close_price'].astype(float)

    except Exception as e:
        print(f"Error fetching data for {code}: {e}")
        return pd.Series()


# ==========================================
#  业务逻辑 A：个股 vs 大盘指数 相关性
# ==========================================

def analyze_stock_market_correlation(stock_code: str, lookback_days=120):
    """
    计算个股与六大宽基指数的相关性
    """
    end_date = datetime.now()
    start_date = (end_date - timedelta(days=lookback_days * 1.5)).strftime('%Y%m%d')

    # 基准指数代码
    indices = {
        '上证指数（大盘）': '000001.SH',
        '上证50 (权重)': '000016.SH',
        '沪深300 (蓝筹)': '000300.SH',
        '中证500 (中盘)': '000905.SH',
        '中证1000 (小盘)': '000852.SH',
        '中证2000 (小小盘)': '932000.CSI',
        '微盘股': '8841458.WI',
        '创业板 (成长)': '399006.SZ',
        '科创50 (科技)': '000688.SH'
    }

    stock_series = get_price_series(stock_code, 'stock', start_date)

    # 简单的错误检查
    if stock_series.empty:
        return None
    if len(stock_series) < 20:
        print(f"数据不足: {stock_code} 只有 {len(stock_series)} 条数据")
        return None

    results = []

    for name, idx_code in indices.items():
        idx_series = get_price_series(idx_code, 'index', start_date)

        if not idx_series.empty:
            # 取交集日期
            df = pd.concat([stock_series, idx_series], axis=1, join='inner')
            # 计算涨跌幅
            df_pct = df.pct_change().dropna()

            if len(df_pct) > 10:
                corr = df_pct.iloc[:, 0].corr(df_pct.iloc[:, 1])

                desc = "无相关"
                if corr > 0.7:
                    desc = "强正相关 (随涨随跌)"
                elif corr > 0.4:
                    desc = "中度正相关"
                elif corr < -0.4:
                    desc = "负相关 (对冲效应)"

                results.append({
                    "指数名称": name,
                    "相关系数": round(corr, 3),
                    "关系解读": desc
                })

    if not results:
        return None

    return pd.DataFrame(results)


# ==========================================
#  业务逻辑 B：商品期货互相关性
# ==========================================

def analyze_futures_correlation(futures_codes: list, lookback_days=120):
    """
    计算一组期货品种的互相关矩阵
    """
    end_date = datetime.now()
    start_date = (end_date - timedelta(days=lookback_days * 1.5)).strftime('%Y%m%d')

    data_dict = {}

    for code in futures_codes:
        s = get_price_series(code, 'future', start_date)
        if not s.empty and len(s) > 20:
            name = code.split('.')[0]
            data_dict[name] = s

    if len(data_dict) < 2:
        return pd.DataFrame()

    df = pd.DataFrame(data_dict)
    df_pct = df.pct_change().dropna()

    if not df_pct.empty:
        return df_pct.corr().round(2)
    else:
        return pd.DataFrame()


def analyze_stock_correlation(stock_codes: list, lookback_days=120):
    """
    [新增] 计算一组股票之间的互相关矩阵
    """
    end_date = datetime.now()
    start_date = (end_date - timedelta(days=lookback_days * 1.5)).strftime('%Y%m%d')
    data_dict = {}

    for code in stock_codes:
        # 获取股票数据
        s = get_price_series(code, 'stock', start_date)
        if not s.empty and len(s) > 20:
            # 尝试用中文名做列名(如果能查到)，否则用代码
            # 这里简单处理，直接用代码，或者如果你有 stock_basic 表可以去查中文名
            # 为了简单，这里直接用代码
            data_dict[code] = s

    if len(data_dict) < 2: return pd.DataFrame()

    df = pd.DataFrame(data_dict)
    df_pct = df.pct_change().dropna()
    return df_pct.corr().round(2) if not df_pct.empty else pd.DataFrame()

# ==========================================
#  🤖 AI 工具封装层 (Wrapper)
#  这里定义的函数是专门给 AI Agent 调用的
# ==========================================

@tool  # <--- 关键修改：加上这个装饰器
def tool_stock_hedging_analysis(stock_name_or_code: str):
    """
    [股票对冲诊断工具]
    当用户询问"某股票如何用期权对冲保护"、"个股与大盘的关系"时使用。
    该工具会计算股票与上证50、沪深300、中证500、中证1000、创业板、科创50的相关性。

    :param stock_name_or_code: 股票名称或代码 (如 '紫金矿业', '601899', '600519.SH')
    """
    # 1. 使用 symbol_map 解析中文名称
    resolved_code, asset_type = symbol_map.resolve_symbol(stock_name_or_code)

    if not resolved_code:
        return f"抱歉，未找到 '{stock_name_or_code}' 对应的股票代码。请尝试输入准确的名称或代码。"

    if asset_type != 'stock':
        # 兼容一下，如果用户输了个期货让算股票对冲，也可以尝试，但最好提示一下
        # 这里直接继续，因为 analyze_stock_market_correlation 内部会处理
        pass

    # 2. 调用计算逻辑
    df = analyze_stock_market_correlation(resolved_code)

    if df is None or df.empty:
        return f"无法获取 {stock_name_or_code} ({resolved_code}) 的相关性数据。可能是新股或数据缺失。"

    # 转换为 Markdown 表格，这是 LLM 最容易理解的格式
    return f"【{stock_name_or_code} ({resolved_code}) 与各大指数的各相关性分析】\n" + df.to_markdown(index=False)


@tool  # <--- 关键修改：加上这个装饰器
def tool_futures_correlation_check(futures_list_str: str):
    """
    [期货/商品相关性分析工具]
    当用户询问"黄金和白银的相关性"、"持仓品种是否分散"、"这两个品种能对冲吗"时使用。

    :param futures_list_str: 期货品种列表字符串，用逗号分隔 (如 '黄金,白银', 'AU,AG,CU')。
    """
    # 1. 解析参数：AI 有时会传 list 有时传 string，做个兼容
    if isinstance(futures_list_str, list):
        raw_list = futures_list_str
    else:
        # 去掉可能存在的引号和空格
        raw_list = [c.strip() for c in futures_list_str.replace("'", "").replace('"', '').split(',') if c.strip()]

    if len(raw_list) < 2:
        return "请至少提供两个品种代码才能计算相关性。"

    # 2. 批量解析代码 (支持中文 -> 代码)
    resolved_codes = []
    for item in raw_list:
        code, _ = symbol_map.resolve_symbol(item)
        if code:
            resolved_codes.append(code)
        else:
            # 如果解析不了，可能是用户随口说的词，或者我们字典没收录，暂时忽略或保留原值尝试
            # 这里选择保留原值，万一用户输的是代码呢
            resolved_codes.append(item)

    # 去重
    resolved_codes = list(set(resolved_codes))

    if len(resolved_codes) < 2:
        return "解析有效品种少于2个，无法计算。请检查输入是否正确。"

    # 3. 调用核心逻辑
    df = analyze_futures_correlation(resolved_codes)

    if df.empty:
        return f"计算失败。请确保输入了正确的期货品种名称或代码。解析结果: {resolved_codes}"

    # 4. 返回 Markdown 矩阵
    return "【品种相关性矩阵 (1.0为完全正相关，负数为负相关)】\n" + df.to_markdown()


@tool
def tool_stock_correlation_check(stock_list_str: str):
    """
    [股票间相关性分析工具]
    当用户询问"茅台和宁德时代的相关性"、"买茅台和工商银行能分散风险吗"、"持仓股票分散度"时使用。
    :param stock_list_str: 股票名称或代码列表，逗号分隔 (如 '茅台,五粮液', '600519,000858')。
    """
    # 1. 解析输入
    if isinstance(stock_list_str, list):
        raw_list = stock_list_str
    else:
        raw_list = [c.strip() for c in stock_list_str.replace("'", "").replace('"', '').split(',') if c.strip()]

    # 2. 解析代码
    resolved_codes = []
    name_map = {}  # 用于最后显示中文名

    for item in raw_list:
        code, asset_type = symbol_map.resolve_symbol(item)
        if code and asset_type == 'stock':
            resolved_codes.append(code)
            name_map[code] = item  # 记录用户输入的名称
        elif code and asset_type != 'stock':
            # 如果用户混入了期货，也可以算，但尽量专注股票
            pass

    resolved_codes = list(set(resolved_codes))
    if len(resolved_codes) < 2:
        return "解析有效股票少于2个，无法计算。请确保输入正确的股票名称。"

    # 3. 计算
    df = analyze_stock_correlation(resolved_codes)

    if df.empty:
        return "计算失败，数据缺失。"

    # 4. 优化显示：尝试把列名换回用户输入的名称，方便阅读
    new_columns = [name_map.get(c, c) for c in df.columns]
    df.columns = new_columns
    df.index = new_columns

    return "【股票相关性 (1.0为完全正相关)】\n" + df.to_markdown()

# ==========================================
#  测试区域
# ==========================================
if __name__ == "__main__":
    # 请确保您先运行了 fetch_indices.py 填充数据！

    print("=== 测试 1: 股票 vs 指数 (需要 index_price 表有数据) ===")
    # 假设您的库里有 600519.SH
    df_stock = analyze_stock_market_correlation('600519.SH')
    if df_stock is not None:
        print(df_stock)
    else:
        print("未获取到股票相关性结果")

    print("\n=== 测试 2: 期货互相关 (需要 futures_price 表有数据) ===")
    # 请换成您库里实际存在的期货代码
    test_futures = ['AU', 'AG', 'CU']
    df_fut = analyze_futures_correlation(test_futures)
    if not df_fut.empty:
        print(df_fut)
    else:
        print("未获取到期货相关性结果")
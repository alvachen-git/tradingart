import requests
import pandas as pd
import re
import os
import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from langchain_core.tools import tool

load_dotenv()


# ==========================================
#  1. 核心：从数据库找主力合约代码
# ==========================================
def get_local_db():
    try:
        user = os.getenv("DB_USER")
        pwd = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST")
        port = os.getenv("DB_PORT")
        name = os.getenv("DB_NAME")
        if not all([user, pwd, host, name]): return None
        db_url = f"mysql+pymysql://{user}:{pwd}@{host}:{port}/{name}"
        return create_engine(db_url, pool_recycle=3600)
    except:
        return None


def _get_real_contract_code(symbol: str):
    """
    从数据库查当前持仓最大的合约代码 (如 IH2512)
    """
    raw_code = symbol.replace('nf_', '').strip()

    # 如果已经是带数字的，直接用
    if re.search(r'\d{3,}', raw_code):
        return raw_code

    engine = get_local_db()
    if engine:
        try:
            sql = text("""
                       SELECT ts_code
                       FROM futures_price
                       WHERE (ts_code LIKE :p1 OR ts_code LIKE :p2)
                         AND trade_date = (SELECT MAX(trade_date) FROM futures_price)
                       ORDER BY oi DESC LIMIT 5
                       """)

            with engine.connect() as conn:
                results = conn.execute(sql, {
                    "p1": f"{raw_code.lower()}%",
                    "p2": f"{raw_code.upper()}%"
                }).fetchall()

            for row in results:
                db_code = row[0]
                # 清洗后缀 (比如 IH2512.CFX -> IH2512)
                clean_code = db_code.split('.')[0]
                if re.search(r'\d+', clean_code):
                    print(f"DEBUG: [本地库] 锁定主力 -> {clean_code}")
                    return clean_code
        except Exception as e:
            print(f"DEBUG: 查库失败 {e}")

    return raw_code


# ==========================================
#  2. 新浪 K线接口 获取数据
# ==========================================
def fetch_sina_minute_trend(symbol: str):
    """
    【核心修改】
    不再使用 getInnerFuturesTrends (已死)，
    改用 getInnerFuturesMiniKLine5m (5分钟K线) 来模拟走势。
    """
    # 1. 获取基础代码 (如 IH2512)
    real_code = _get_real_contract_code(symbol)

    # 2. 穿马甲 (Sina 专用格式)
    # 规则：
    # - 金融期货 (IH/IF/IC/IM/T/TL): 必须加 CFF_RE_ 前缀，且大写。
    # - 商品期货 (rb/m/au...): 必须小写，无前缀。

    alpha = "".join(filter(str.isalpha, real_code))
    digits = "".join(filter(str.isdigit, real_code))

    # 金融期货列表
    CFFEX_LIST = ['IH', 'IF', 'IC', 'IM', 'T', 'TF', 'TS', 'TL']

    if alpha.upper() in CFFEX_LIST:
        sina_symbol = f"CFF_RE_{alpha.upper()}{digits}"
    else:
        sina_symbol = f"{alpha.lower()}{digits}"

    # 3. 请求新浪 5分钟 K线接口 (这个接口活着！)
    url = f"http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol={sina_symbol}"
    print(f"DEBUG: 请求新浪K线: {url}")

    try:
        # 使用 verify=False 防止 SSL 报错
        response = requests.get(url, timeout=3, verify=False)
        data = response.json()

        # 4. 容错处理
        if not data:
            print(f"DEBUG: 新浪返回空数据 ({sina_symbol})")
            return pd.DataFrame()

        # 5. 解析数据
        # 返回可能是 list of lists: [["2025-12-15 14:55:00", "2600", ...], ...]
        # 或者 list of dicts
        df = pd.DataFrame()

        if isinstance(data, list) and len(data) > 0:
            row0 = data[0]

            # 格式 A: List [Date, Open, High, Low, Close, Vol]
            if isinstance(row0, list):
                temp_df = pd.DataFrame(data)
                # 我们只需要 时间(0) 和 收盘价(4) 来画线
                df = temp_df.iloc[:, [0, 4]].copy()
                df.columns = ["date", "close"]

            # 格式 B: Dict {'d':..., 'c':...}
            elif isinstance(row0, dict):
                temp_df = pd.DataFrame(data)
                # 尝试找常用键名
                if 'd' in temp_df.columns and 'c' in temp_df.columns:
                    df = temp_df[['d', 'c']].rename(columns={'d': 'date', 'c': 'close'})
                elif 'date' in temp_df.columns and 'close' in temp_df.columns:
                    df = temp_df[['date', 'close']]

        if df.empty: return pd.DataFrame()

        # 6. 数据清洗
        # 确保价格是 float
        df['close'] = df['close'].astype(float)
        # 确保时间是字符串
        df['date'] = df['date'].astype(str)

        # 只取最近的 100 个点，画出来更像分时图
        return df.tail(100)

    except Exception as e:
        print(f"新浪K线接口异常: {e}")
        return pd.DataFrame()


# ==========================================
#  AI 工具 (Snapshot 保持原样，因为它本来就是通的)
# ==========================================
@tool
def get_sina_realtime_price(symbol: str):
    """获取实时报价"""
    real_code = _get_real_contract_code(symbol)
    alpha = "".join(filter(str.isalpha, real_code))
    digits = "".join(filter(str.isdigit, real_code))

    CFFEX_LIST = ['IH', 'IF', 'IC', 'IM', 'T', 'TF', 'TS', 'TL']
    if alpha.upper() in CFFEX_LIST:
        sina_code = f"CFF_RE_{alpha.upper()}{digits}"
    else:
        sina_code = f"nf_{alpha.lower()}{digits}"

    url = f"http://hq.sinajs.cn/list={sina_code}"
    try:
        resp = requests.get(url, timeout=2)
        if '="' in resp.text:
            data = resp.text.split('="')[1]
            parts = data.split(',')
            # 金融期货价格在 index 3, 商品在 index 8
            if "CFF_RE_" in sina_code:
                price = parts[3]
                name = parts[0]
            else:
                price = parts[8]
                name = parts[0]
            return f"【实时报价】{name} ({real_code}) 现价: {price}"
    except:
        pass
    return "查询失败"


@tool
def get_sina_kline_tool(symbol: str):
    """AI 查看趋势专用"""
    df = fetch_sina_minute_trend(symbol)
    if df.empty: return "暂无数据"
    start = float(df.iloc[0]['close'])
    end = float(df.iloc[-1]['close'])
    trend = "上涨" if end > start else "下跌"
    return f"参考最近K线走势：从 {start} 到 {end}，整体{trend}。"
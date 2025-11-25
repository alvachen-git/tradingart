import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text, types
from datetime import datetime, timedelta
import time
import warnings

# 忽略警告
warnings.filterwarnings('ignore')

# --- 1. 配置数据库 ---
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


# --- 2. 交易所映射 (保持不变) ---
def get_exchange(symbol):
    s = symbol.upper()
    if s in ['LC', 'SI','PS']: return 'GFEX'
    if s in ['M', 'I', 'P', 'Y', 'C', 'A', 'JD', 'JM', 'L', 'PP', 'V', 'EG', 'LH', 'EB']: return 'DCE'
    if s in ['FG', 'SA', 'MA', 'TA', 'SR', 'CF', 'RM', 'OI', 'AP', 'UR', 'SH']: return 'CZCE'
    if s in ['RB', 'HC', 'CU', 'AL', 'ZN', 'AU', 'AG', 'BU', 'SP', 'NI', 'ao', 'ru']: return 'SHFE'
    if s in ['IF', 'IC', 'IH', 'IM', 'T', 'TF', 'TS']: return 'CFFEX'
    return 'UNKNOWN'


# --- 3. 智能获取函数 (核心复用 save_all_futures_to_db.py 的逻辑) ---
def smart_fetch(exchange, date_str, symbol):
    ex_lower = exchange.lower()
    func_candidates = [
        f"futures_{ex_lower}_position_rank",
        f"get_{ex_lower}_rank_table",
        f"futures_{ex_lower}_rank_table"
    ]

    for func_name in func_candidates:
        if not hasattr(ak, func_name): continue
        method = getattr(ak, func_name)
        try:
            return method(date=date_str, vars_list=[symbol]), func_name
        except:
            pass
        try:
            return method(date=date_str, symbol=symbol.upper()), func_name
        except:
            pass
        try:
            return method(date=date_str, symbol=symbol), func_name
        except:
            pass
        try:
            return method(trade_date=date_str, vars_list=[symbol]), func_name
        except:
            pass
    return None, None


def fetch_and_process_data(date_str, symbol):
    exchange = get_exchange(symbol)
    print(f"[*] 获取 {date_str} [{exchange}] {symbol} ...", end="")

    raw_data, _ = smart_fetch(exchange, date_str, symbol)

    df_raw = pd.DataFrame()
    if isinstance(raw_data, dict):
        dfs = [v for k, v in raw_data.items() if v is not None and not v.empty]
        if dfs: df_raw = pd.concat(dfs, ignore_index=True)
    elif isinstance(raw_data, pd.DataFrame):
        df_raw = raw_data

    if df_raw.empty:
        print(" [-] 无数据")
        return None

    try:
        cols = [str(c).lower() for c in df_raw.columns.tolist()]

        # 智能列名识别 (英文/中文双轨)
        if any('long_party' in c for c in cols):
            c_broker = next(c for c in df_raw.columns if 'long_party' in str(c).lower())
            c_long_vol = next(
                c for c in df_raw.columns if 'long_open_interest' in str(c).lower() and 'chg' not in str(c).lower())
            c_long_chg = next(c for c in df_raw.columns if 'long_open_interest_chg' in str(c).lower())
            c_broker_short = next(c for c in df_raw.columns if 'short_party' in str(c).lower())
            c_short_vol = next(
                c for c in df_raw.columns if 'short_open_interest' in str(c).lower() and 'chg' not in str(c).lower())
            c_short_chg = next(c for c in df_raw.columns if 'short_open_interest_chg' in str(c).lower())

            df_long = df_raw[[c_broker, c_long_vol, c_long_chg]].copy()
            df_long.columns = ['broker', 'long_vol', 'long_chg']
            df_short = df_raw[[c_broker_short, c_short_vol, c_short_chg]].copy()
            df_short.columns = ['broker', 'short_vol', 'short_chg']
        else:
            # 中文列名匹配
            broker_indices = [i for i, c in enumerate(cols) if ('会员' in c or '名称' in c) and '1' not in c]
            if not broker_indices: broker_indices = [1]

            idx_long = broker_indices[-2] if len(broker_indices) >= 2 else broker_indices[0]
            idx_short = broker_indices[-1] if len(broker_indices) >= 2 else 5

            df_long = df_raw.iloc[:, [idx_long, idx_long + 1, idx_long + 2]].copy()
            df_long.columns = ['broker', 'long_vol', 'long_chg']
            df_short = df_raw.iloc[:, [idx_short, idx_short + 1, idx_short + 2]].copy()
            df_short.columns = ['broker', 'short_vol', 'short_chg']

        # 清洗
        filter_pat = '合计|共计|总计'
        df_long = df_long[df_long['broker'].notna() & (~df_long['broker'].astype(str).str.contains(filter_pat))]
        df_short = df_short[df_short['broker'].notna() & (~df_short['broker'].astype(str).str.contains(filter_pat))]

        def to_num(x):
            return pd.to_numeric(str(x).replace(',', ''), errors='coerce')

        for c in ['long_vol', 'long_chg']: df_long[c] = df_long[c].apply(to_num).fillna(0)
        for c in ['short_vol', 'short_chg']: df_short[c] = df_short[c].apply(to_num).fillna(0)

        # 合并
        df_final = pd.merge(df_long, df_short, on='broker', how='outer').fillna(0)
        df_final = df_final.groupby('broker').sum().reset_index()

        # 添加元数据
        df_final['trade_date'] = date_str
        df_final['ts_code'] = symbol.lower()
        df_final['net_vol'] = df_final['long_vol'] - df_final['short_vol']

        print(f" [√] 成功 ({len(df_final)}条)")
        return df_final

    except Exception as e:
        print(f" [!] 清洗异常: {e}")
        return None


# --- 4. 数据库写入 (先删后写) ---
def save_to_database(df, date_str, symbol):
    if df is None or df.empty: return
    try:
        with engine.connect() as conn:
            conn.execute(
                text(f"DELETE FROM futures_holding WHERE trade_date='{date_str}' AND ts_code='{symbol.lower()}'"))
            conn.commit()

        df.to_sql('futures_holding', engine, if_exists='append', index=False,
                  dtype={
                      'trade_date': types.VARCHAR(8), 'ts_code': types.VARCHAR(10), 'broker': types.VARCHAR(50),
                      'long_vol': types.Integer(), 'long_chg': types.Integer(),
                      'short_vol': types.Integer(), 'short_chg': types.Integer(), 'net_vol': types.Integer()
                  })
    except Exception as e:
        print(f"    [X] 入库失败: {e}")


# --- 5. 每日更新主程序 ---
def run_daily_update():
    # 1. 获取今天的日期
    today_str = datetime.now().strftime('%Y%m%d')
    weekday = datetime.now().weekday()  # 0=周一, 6=周日

    # 2. 如果是周末，直接跳过
    if weekday >= 5:
        print(f"今天是周末 ({today_str})，无需更新数据。")
        return

    print(f"=== 开始每日数据更新: {today_str} ===")

    # 3. 定义要更新的品种
    ALL_SYMBOLS = [
        'lc', 'si', 'ps', # 广期所
        'rb', 'hc', 'au', 'ag', 'al', 'zn', 'ao', 'ru', 'sp', 'ni',  # 上期所
        'm', 'i', 'lh', 'p', 'y',  'c', 'jd', 'jm', 'eb', 'eg', 'pvc', 'l', 'pp', # 大商所
        'fg', 'sa', 'cf', 'sr', 'ma', 'ta', 'ap','ur','sh', 'oi', 'rm', # 郑商所
        'IF', 'IM' 'IC', 'IH', 'T', 'TF', 'TS',  # 中金所
    ]

    # 4. 循环抓取
    for symbol in ALL_SYMBOLS:
        # 尝试抓取今天的数据
        df = fetch_and_process_data(today_str, symbol)

        if df is not None:
            save_to_database(df, today_str, symbol)
        else:
            # 如果今天的数据还没出(比如早上运行)，尝试抓取昨天的作为兜底
            # print(f"    (尝试补抓昨天数据...)")
            # yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            # df_yes = fetch_and_process_data(yesterday_str, symbol)
            # if df_yes is not None:
            #    save_to_database(df_yes, yesterday_str, symbol)
            pass

        # 礼貌延时
        time.sleep(1)

    print(f"\n=== 每日更新完成: {today_str} ===")


if __name__ == "__main__":
    run_daily_update()
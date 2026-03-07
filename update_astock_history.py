import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text, types
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import time

# 1. 初始化
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)

ts_token = os.getenv("TUSHARE_TOKEN")
ts.set_token(ts_token)
pro = ts.pro_api()


# --- 獲取全市場名稱字典 (用於填補 name) ---
def get_name_map():
    print("[*] 正在加載名稱字典...")
    try:
        # 股票
        df_s = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')
        # 基金/ETF
        df_f = pro.fund_basic(market='E', status='L', fields='ts_code,name')
        df_all = pd.concat([df_s, df_f])
        return dict(zip(df_all['ts_code'], df_all['name']))
    except:
        return {}


NAME_MAP = get_name_map()


# --- 3. 核心抓取邏輯 ---
def fetch_and_save_data(ts_code, start_date, end_date, asset_type='E'):
    """
    asset_type: 'E' = ETF, 'S' = Stock
    """
    # 嘗試從字典獲取名稱，如果沒有則用代碼
    code_name = NAME_MAP.get(ts_code, ts_code)
    print(f"[*] 正在獲取 {code_name} ({ts_code})...", end="")

    try:
        if asset_type == 'S':
            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        else:
            df = pro.fund_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

        if df.empty:
            print(" [-] 無數據")
            return

        # --- 【關鍵修改】統一列名 ---
        # 把 Tushare 的 open/high/low/close 改成 open_price/high_price...
        df = df.rename(columns={
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price'
            })


        # --- 【核心修復 1】填入名稱 ---
        df['name'] = code_name

        # --- 【核心修復 2】嚴格過濾字段 (防止 Unknown column 報錯) ---
        # 只保留資料庫中肯定存在的字段
        target_cols = [
            'trade_date', 'ts_code', 'name',
            'open_price', 'high_price', 'low_price', 'close_price',  # 新列名
            'vol', 'amount', 'pct_chg'
        ]

        # 確保 DataFrame 裡有這些列，沒有的填 0
        for c in target_cols:
            if c not in df.columns:
                df[c] = 0

        # 只取這些列，剔除 pre_close 等多餘列
        df_save = df[target_cols].copy()

        # 入庫 (先刪後寫)
        with engine.connect() as conn:
            del_sql = f"DELETE FROM stock_price WHERE ts_code='{ts_code}' AND trade_date >= '{start_date}' AND trade_date <= '{end_date}'"
            conn.execute(text(del_sql))
            conn.commit()

        df_save.to_sql('stock_price', engine, if_exists='append', index=False, dtype={
            'trade_date': types.VARCHAR(8),
            'ts_code': types.VARCHAR(10),
            'name': types.VARCHAR(50),
            'open_price': types.Float(), 'high_price': types.Float(), 'low_price': types.Float(), 'close_price': types.Float(),
            'vol': types.Float(), 'amount': types.Float(), 'pct_chg': types.Float()
        })
        print(f" [√] 成功入庫 {len(df)} 條")

    except Exception as e:
        print(f" [!] 錯誤: {e}")


# --- 4. 批量運行 ---
if __name__ == "__main__":
    today = datetime.now().strftime('%Y%m%d')
    start = (datetime.now() - timedelta(days=600)).strftime('%Y%m%d')

    print(f"=== 開始抓取 ({start} - {today}) ===")

    # 1. ETF
    # ETF_TARGETS = ["510050.SH", "159915.SZ", "588000.SH", "510300.SH", "510500.SH"]
    #for code in ETF_TARGETS:
    # fetch_and_save_data(code, start, today, asset_type='E')
    # time.sleep(0.3)

    # 2. 個股 (茅台在這裡！)
    STOCK_TARGETS = [
        "300417.SZ",

    ]
    for code in STOCK_TARGETS:
        fetch_and_save_data(code, start, today, asset_type='S')
        time.sleep(0.5)

    print("=== 全部完成 ===")
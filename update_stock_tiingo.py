import os
import pandas as pd
from tiingo import TiingoClient
from sqlalchemy import create_engine, text
import datetime
from dotenv import load_dotenv
import time  # <--- 1. 引入 time
import gc  # <--- 2. 引入 gc (好习惯)

load_dotenv(override=True)

TIINGO_KEY = "ddb0de2f922b0e2e02c6b50516b2b87cb9dc1bda"

# 数据库配置
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

if not TIINGO_KEY:
    raise ValueError("❌ 错误：未找到 TIINGO_API_KEY，请检查 .env 文件！")

SYMBOLS = ['NVDA', 'TSLA', 'GOOG', 'AAPL', 'MSFT', 'AVGO', 'AMD', 'META', 'AMZN', 'TSM', 'INTC']

config = {
    'session': True,
    'api_key': TIINGO_KEY
}
client = TiingoClient(config)
# --- 3. 优化：增加 pool_recycle ---
engine = create_engine(db_url, pool_recycle=3600)


def get_last_date_from_db(symbol):
    try:
        query = text(f"SELECT MAX(date) FROM stock_prices WHERE symbol = '{symbol}'")
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            if result and result[0]:
                return pd.to_datetime(result[0])
    except Exception:
        pass
    return None


def update_stock_data():
    print(f"🚀 开始通过 Tiingo 更新 {len(SYMBOLS)} 只股票数据...")

    for symbol in SYMBOLS:
        try:
            print(f"\n处理: {symbol}")

            last_date = get_last_date_from_db(symbol)

            if last_date:
                start_date = last_date + datetime.timedelta(days=1)
                print(f"   -> 数据库已有数据，更新起始日: {start_date.date()}")

                if start_date > datetime.datetime.now():
                    print("   -> 已经是最新数据，无需更新。")
                    continue
            else:
                start_date = datetime.datetime.now() - datetime.timedelta(days=365 * 3)
                print(f"   -> 首次抓取，下载近 3 年数据...")

            history_data = client.get_ticker_price(
                symbol,
                fmt='json',
                startDate=start_date.strftime('%Y-%m-%d'),
                frequency='daily'
            )

            if not history_data:
                print("   -> Tiingo 未返回新数据。")
                continue

            df = pd.DataFrame(history_data)

            # 数据清洗
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['symbol'] = symbol

            save_df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'adjClose']].copy()

            # 入库
            save_df.to_sql('stock_prices', con=engine, if_exists='append', index=False)
            print(f"   ✅ 成功保存 {len(save_df)} 条记录！")

            # --- 4. 优化：手动清理内存 (虽然数据小，但这是好习惯) ---
            del df
            del save_df
            gc.collect()

        except Exception as e:
            print(f"   ❌ 出错: {e}")

        # --- 5. 优化：防止 API 频率过快被封，且保护 CPU ---
        time.sleep(1.5)

    print("\n🏁 全部更新完成！")


if __name__ == "__main__":
    update_stock_data()
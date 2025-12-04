import os
import pandas as pd
from tiingo import TiingoClient
from sqlalchemy import create_engine, text
import datetime
from dotenv import load_dotenv  # 引入库

# 1. 加载 .env 文件里的变量到系统中
# (如果在服务器上没这个文件，这行代码也不会报错，只是什么都不做)
load_dotenv(override=True)

# 2. 安全地读取变量
# 如果读取不到，会返回 None，你可以加个判断防止报错
TIINGO_KEY = os.getenv("TIINGO_API_KEY")

# 数据库配置
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


# 简单检查一下
if not TIINGO_KEY:
    raise ValueError("❌ 错误：未找到 TIINGO_API_KEY，请检查 .env 文件！")

# 3. 你想抓取的股票列表
SYMBOLS = ['NVDA', 'TSLA', 'GOOG','AAPL', 'MSFT', 'AVGO', 'AMD', 'META', 'AMZN', 'TSM', 'INTC']

# --- 初始化 ---
config = {
    'session': True,
    'api_key': TIINGO_KEY
}
client = TiingoClient(config)
engine = create_engine(db_url)


def get_last_date_from_db(symbol):
    """
    检查数据库，看看这只股票我们要从哪天开始更新
    """
    try:
        # 尝试查询数据库里该股票最大的日期
        query = text(f"SELECT MAX(date) FROM stock_prices WHERE symbol = '{symbol}'")
        with engine.connect() as conn:
            result = conn.execute(query).fetchone()
            if result and result[0]:
                return pd.to_datetime(result[0])
    except Exception:
        # 如果表不存在或报错，说明是第一次存，返回 None
        pass
    return None


def update_stock_data():
    print(f"🚀 开始通过 Tiingo 更新 {len(SYMBOLS)} 只股票数据...")

    for symbol in SYMBOLS:
        try:
            print(f"\n处理: {symbol}")

            # 1. 确定开始日期
            last_date = get_last_date_from_db(symbol)

            if last_date:
                # 如果库里有数据，从库里最新日期的"明天"开始抓
                start_date = last_date + datetime.timedelta(days=1)
                print(f"   -> 数据库已有数据，更新起始日: {start_date.date()}")

                # 如果今天是周末或还没收盘，可能没有新数据
                if start_date > datetime.datetime.now():
                    print("   -> 已经是最新数据，无需更新。")
                    continue
            else:
                # 如果是第一次，抓过去 3 年的数据 (可以自己改)
                start_date = datetime.datetime.now() - datetime.timedelta(days=365 * 3)
                print(f"   -> 首次抓取，下载近 3 年数据...")

            # 2. 调用 Tiingo API
            # output_format='json' 会返回列表，pandas 很好处理
            # 这里的 columns 包含: date, close, high, low, open, volume, adjClose...
            history_data = client.get_ticker_price(
                symbol,
                fmt='json',
                startDate=start_date.strftime('%Y-%m-%d'),
                frequency='daily'
            )

            if not history_data:
                print("   -> Tiingo 未返回新数据。")
                continue

            # 3. 转为 DataFrame
            df = pd.DataFrame(history_data)

            # 4. 数据清洗
            # Tiingo 返回的日期是 UTC 时间，通常带有 T00:00:00.000Z
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['symbol'] = symbol  # 加上股票代码列

            # 只保留我们需要存的列 (Tiingo 提供了调整后价格 adjClose，建议存这个)
            # 列名映射：Tiingo 的 key -> 你的数据库字段名
            save_df = df[['date', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'adjClose']].copy()

            # 5. 存入数据库
            # if_exists='append': 如果表存在就追加，不存在就创建
            # index=False: 不要把 pandas 的索引(0,1,2...)存进去
            save_df.to_sql('stock_prices', con=engine, if_exists='append', index=False)

            print(f"   ✅ 成功保存 {len(save_df)} 条记录！")

        except Exception as e:
            print(f"   ❌ 出错: {e}")

    print("\n🏁 全部更新完成！")


if __name__ == "__main__":
    update_stock_data()
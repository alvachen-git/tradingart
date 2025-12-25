import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta

# 1. 初始化
load_dotenv(override=True)
# 确保 .env 配置正确
db_url = f"mysql+pymysql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(db_url)
ts.set_token(os.getenv("TUSHARE_TOKEN"))
pro = ts.pro_api()


def init_index_table():
    """初始化指数价格表 (包含 OHLC 数据)"""
    with engine.connect() as conn:
        # 💡 如果您之前已经有这个表但字段不够，建议先手动 DROP TABLE index_price;
        # 下面的 SQL 定义了包含开高低收的完整结构
        sql = """
              CREATE TABLE IF NOT EXISTS index_price \
              ( \
                  trade_date \
                  VARCHAR \
              ( \
                  20 \
              ),
                  ts_code VARCHAR \
              ( \
                  20 \
              ),
                  open_price FLOAT, -- 新增: 开盘价
                  high_price FLOAT, -- 新增: 最高价
                  low_price FLOAT, -- 新增: 最低价
                  close_price FLOAT, -- 收盘价
                  pct_chg FLOAT, -- 涨跌幅
                  vol FLOAT, -- 新增: 成交量 (手)
                  amount FLOAT, -- 新增: 成交额 (千元)
                  PRIMARY KEY \
              ( \
                  trade_date, \
                  ts_code \
              )
                  ) DEFAULT CHARSET=utf8mb4;
              """
        conn.execute(text(sql))
        print("✅ 表结构检查完成: index_price")


def fetch_and_save_indices():
    # === 1. 扩展指数列表 ===
    # 包含了市场核心宽基指数
    indices = {
        '000001.SH': '上证指数',  # 大盘核心
        '399001.SZ': '深证成指',
        '000300.SH': '沪深300',  # 核心蓝筹
        '000905.SH': '中证500',  # 中盘股
        '000852.SH': '中证1000',  # 小盘股
        '000688.SH': '科创50',  # 科创板
        '399006.SZ': '创业板指',  # 创业板
        '000016.SH': '上证50',  # 超大盘
        '399005.SZ': '中小100',  # 中小板
        '932000.CSI': '中证2000',
    }

    # 拉取过去 3 年的数据 (保证 MA60, MA250 等长周期均线能计算)
    start_date = (datetime.now() - timedelta(days=3000)).strftime('%Y%m%d')
    end_date = datetime.now().strftime('%Y%m%d')

    print(f"🚀 开始拉取指数 OHLC 数据 ({start_date} - {end_date})...")

    for code, name in indices.items():
        try:
            # === 2. 调用 Tushare 接口 ===
            # index_daily 默认包含: trade_date, ts_code, close, open, high, low, pct_chg, vol, amount
            df = pro.index_daily(ts_code=code, start_date=start_date, end_date=end_date)

            if not df.empty:
                # === 3. 字段重命名 ===
                # 将 Tushare 的简写 (open, high) 映射为数据库字段 (open_price, high_price)
                rename_map = {
                    'close': 'close_price',
                    'open': 'open_price',
                    'high': 'high_price',
                    'low': 'low_price',
                    # pct_chg, vol, amount 保持原名即可，或者根据需要修改
                }
                df = df.rename(columns=rename_map)

                # 确保只保留数据库需要的列，防止列名不匹配报错
                cols_to_save = ['trade_date', 'ts_code', 'open_price', 'high_price',
                                'low_price', 'close_price', 'pct_chg', 'vol', 'amount']

                # 过滤掉 dataframe 中没有的列 (防止接口变动)
                final_cols = [c for c in cols_to_save if c in df.columns]
                save_df = df[final_cols]

                # === 4. 存入数据库 ===
                # 使用 replace 模式不太好，因为会把整个表删了。
                # 建议使用 append，配合 try-except 处理主键冲突(已存在的数据)
                # 或者在 URL 参数里加 rewriteBatchedStatements=true 提速
                save_df.to_sql('index_price', engine, if_exists='append', index=False, chunksize=1000)

                print(f"   [√] {name} ({code}) 更新成功: {len(df)} 条")
            else:
                print(f"   [!] {name} 无数据返回")

            # 稍微停顿，防止触发频率限制
            time.sleep(0.3)

        except Exception as e:
            # 忽略主键重复错误 (Duplicate entry)，说明数据已经有了
            if "Duplicate entry" in str(e):
                print(f"   [-] {name} 数据已存在 (跳过)")
            else:
                print(f"   [x] {name} 入库失败: {e}")


if __name__ == "__main__":
    # ⚠️ 重要: 第一次运行新代码前，请确保数据库里旧表已删除，或者使用 ALTER TABLE 添加列
    # 建议操作: 在数据库工具里执行 DROP TABLE index_price; 然后再运行此脚本
    init_index_table()
    fetch_and_save_indices()
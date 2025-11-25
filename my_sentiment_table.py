from sqlalchemy import create_engine, text

# --- 配置 ---
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def create_sentiment_table():
    with engine.connect() as conn:
        print("[*] 正在创建/重置 'market_sentiment' 表...")

        # 1. 如果存在则删除 (慎用，初次运行没问题)
        # conn.execute(text("DROP TABLE IF EXISTS market_sentiment"))

        # 2. 创建表
        # 设置联合主键 (trade_date, ts_code)，确保同一天同一个品种只有一个判断
        sql = """
              CREATE TABLE IF NOT EXISTS market_sentiment \
              ( \
                  trade_date \
                  VARCHAR \
              ( \
                  8 \
              ) NOT NULL COMMENT '交易日期',
                  ts_code VARCHAR \
              ( \
                  10 \
              ) NOT NULL COMMENT '品种代码',
                  score INT NOT NULL COMMENT '2大涨 1小涨 0不明 -1小跌 -2大跌',
                  reason TEXT COMMENT '判断理由',
                  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                  PRIMARY KEY \
              ( \
                  trade_date, \
                  ts_code \
              )
                  ) \
              """
        conn.execute(text(sql))
        print("[√] 表 'market_sentiment' 创建成功！")


if __name__ == "__main__":
    create_sentiment_table()
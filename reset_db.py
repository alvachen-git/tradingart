from sqlalchemy import create_engine, text

# --- 配置 ---
DB_USER = 'root'
DB_PASSWORD = 'alva13557941'
DB_HOST = '39.102.215.198'
DB_PORT = '3306'
DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def reset_tables():
    with engine.connect() as conn:
        print("[*] 正在重置数据库表结构...")

        # 1. 删除旧表 (如果有)
        conn.execute(text("DROP TABLE IF EXISTS futures_holding"))
        conn.execute(text("DROP TABLE IF EXISTS futures_price"))

        # 2. 创建 futures_holding 表 (设置联合主键)
        # 主键: (trade_date, ts_code, broker) -> 同一天、同一品种、同一家期货商只能有一条记录
        sql_holding = """
                      CREATE TABLE futures_holding \
                      ( \
                          trade_date VARCHAR(8)  NOT NULL, \
                          ts_code    VARCHAR(10) NOT NULL, \
                          broker     VARCHAR(50) NOT NULL, \
                          long_vol   INT, \
                          long_chg   INT, \
                          short_vol  INT, \
                          short_chg  INT, \
                          net_vol    INT, \
                          PRIMARY KEY (trade_date, ts_code, broker)
                      ) \
                      """
        conn.execute(text(sql_holding))
        print("[+] futures_holding 表重建完成 (已添加唯一约束)。")

        # 3. 创建 futures_price 表 (设置联合主键)
        # 主键: (trade_date, ts_code) -> 同一天、同一品种只能有一条价格
        sql_price = """
                    CREATE TABLE futures_price \
                    ( \
                        trade_date  VARCHAR(8)  NOT NULL, \
                        ts_code     VARCHAR(10) NOT NULL, \
                        name        VARCHAR(50), \
                        open_price  FLOAT, \
                        high_price  FLOAT, \
                        low_price   FLOAT, \
                        close_price FLOAT, \
                        vol         BIGINT, \
                        oi          BIGINT, \
                        PRIMARY KEY (trade_date, ts_code)
                    ) \
                    """
        conn.execute(text(sql_price))
        print("[+] futures_price 表重建完成 (已添加唯一约束)。")

        conn.commit()


if __name__ == "__main__":
    reset_tables()
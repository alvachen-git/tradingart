import tushare as ts
import pandas as pd
from sqlalchemy import create_engine, text, types
import os
from dotenv import load_dotenv
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


# 交易所列表 (大商所 DCE, 郑商所 CZCE, 上期所 SHFE, 广期所 GFEX, 上能源 INE)
EXCHANGES = ['DCE', 'CZCE', 'SHFE', 'GFEX', 'INE','CFFEX']


def save_option_basic():
    print("=== 开始更新商品期权合约表 (Tushare) ===")

    for ex in EXCHANGES:
        print(f"[*] 正在抓取 {ex} 期权合约列表...")
        try:
            # 获取该交易所所有上市期权
            # exchange: 交易所代码
            # fields: ts_code, name, exercise_price, maturity_date, call_put
            df = pro.opt_basic(exchange=ex, fields='ts_code,name,exercise_price,maturity_date,call_put')

            if df.empty:
                print(f" [-] {ex} 無數據")
                continue
            # 强制按 ts_code 去重，保留第一条，防止 Tushare 返回重复数据导致报错
            df.drop_duplicates(subset=['ts_code'], inplace=True)

            # 简单清洗
            # 提取标的代码 (如 rb2501-C-3000 -> rb)
            # Tushare 格式通常是 rb2401-C-3000.SHFE
            # 我们只取字母部分作为 underlying
            import re
            def get_underlying(code):
                m = re.match(r'([a-zA-Z]+)', code)
                return m.group(1).lower() if m else 'unknown'

            df['underlying'] = df['ts_code'].apply(get_underlying)

            # 定義後綴映射 (手動修正)
            suffix_map = {
                'DCE': '.DCE',
                'CZCE': '.ZCE',  # <--- 修正這裡
                'SHFE': '.SHF',  # <--- 修正這裡 (有時是 SHFE，有時是 SHF)
                'GFEX': '.GFE',  # <--- 修正這裡
                'INE': '.INE',
                'CFFEX': '.CFFEX'
            }
            # 獲取正確的後綴進行刪除
            # 如果不確定，我們用更通用的邏輯：根據抓下來的數據特徵刪除
            sample_code = df['ts_code'].iloc[0]
            if '.' in sample_code:
                actual_suffix = sample_code.split('.')[-1]
                delete_pattern = f"%.{actual_suffix}"
            else:
                delete_pattern = f"%.{ex}"  # 兜底

            print(f"    -> 準備刪除舊數據 (後綴: {delete_pattern})...")

            # 入库 (增量更新，使用 replace 或 ignore)
            # 这里为了简单直接用 append，如果主键冲突会报错，所以先处理一下
            # 推荐策略：先读取已有的 ts_code，只插入新的
            # 或者简单粗暴：replace (全量覆盖，量不大，几万条很快)

            # 为了不撑爆内存，我们按交易所分批写入
            # 先删除该交易所的旧数据
            with engine.connect() as conn:
                # 刪除舊數據
                del_sql = text(f"DELETE FROM commodity_option_basic WHERE ts_code LIKE :pattern")
                conn.execute(del_sql, {"pattern": delete_pattern})
                conn.commit()

            # 4. 入庫
            df.to_sql('commodity_option_basic', engine, if_exists='append', index=False, dtype={
                'exercise_price': types.Float()
            })
            print(f" [√] {ex} 更新完成，共 {len(df)} 條")

        except Exception as e:
            print(f" [!] {ex} 失败: {e}")

        time.sleep(1)


if __name__ == "__main__":
    save_option_basic()
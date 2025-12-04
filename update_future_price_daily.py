import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, types, text
from datetime import datetime
import os
from dotenv import load_dotenv
import time
import gc

# --- 1. 初始化配置 ---
load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# 容错：如果没有环境变量，使用默认值
if not DB_USER:
    DB_USER = 'root'
    DB_PASSWORD = 'alva13557941'
    DB_HOST = '39.102.215.198'
    DB_PORT = '3306'
    DB_NAME = 'finance_data'

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url, pool_recycle=3600)


# --- 2. 核心函数：只更新当天 ---
def update_today_price(symbol, name):
    today_str = datetime.now().strftime('%Y%m%d')
    print(f"[*] 检查 {name} ({symbol}) 今日({today_str}) 数据...", end="")

    try:
        # 1. 获取全量历史 (为了计算涨跌幅，必须拿历史数据)
        df = ak.futures_zh_daily_sina(symbol=symbol)

        if df.empty:
            print(" [-] 源数据为空")
            return

        # 2. 清洗与重命名
        df = df.rename(columns={
            'date': 'trade_date',
            'open': 'open_price', 'high': 'high_price', 'low': 'low_price', 'close': 'close_price',
            'volume': 'vol', 'hold': 'oi', 'settle': 'settle_price'
        })
        df['ts_code'] = symbol
        df['name'] = name
        df['trade_date'] = pd.to_datetime(df['trade_date'])

        # 3. 计算涨跌幅 (在过滤日期之前做！)
        df = df.sort_values('trade_date')
        # 优先用结算价算，没有则用收盘价
        base_price = df['settle_price'] if 'settle_price' in df.columns else df['close_price']
        df['pct_chg'] = base_price.pct_change().fillna(0)

        # 4. 【关键】只保留“今天”的数据
        df['trade_date_str'] = df['trade_date'].dt.strftime('%Y%m%d')
        df_today = df[df['trade_date_str'] == today_str].copy()

        # --- 3. 优化：df 已经完成使命（计算完涨跌幅了），立即销毁 ---
        del df
        gc.collect()

        if df_today.empty:
            print(f" [-] 今日数据尚未生成 (最新日期: {df['trade_date_str'].iloc[-1]})")
            return

        # 5. 准备入库字段
        target_cols = ['trade_date', 'ts_code', 'name', 'open_price', 'high_price', 'low_price', 'close_price',
                       'vol', 'oi', 'pct_chg']
        # 补全缺失列
        for c in target_cols:
            if c not in df_today.columns: df_today[c] = 0

        # 格式化日期列为字符串
        df_today['trade_date'] = df_today['trade_date_str']
        df_save = df_today[target_cols]

        # 6. 入库 (先删后写，只动今天的数据)
        with engine.connect() as conn:
            # 只删除【今天】且【该品种】的数据
            del_sql = text(f"DELETE FROM futures_price WHERE trade_date='{today_str}' AND ts_code='{symbol}'")
            conn.execute(del_sql)
            conn.commit()

        df_save.to_sql('futures_price', engine, if_exists='append', index=False, dtype={
            'trade_date': types.VARCHAR(8),
            'ts_code': types.VARCHAR(10),
            'name': types.VARCHAR(50),
            'open_price': types.Float(), 'high_price': types.Float(), 'low_price': types.Float(),
            'close_price': types.Float(),
            'settle_price': types.Float(), 'pct_chg': types.Float(),
            'vol': types.BigInteger(), 'oi': types.BigInteger()
        })
        print(f" [√] 成功更新")

    except Exception as e:
        print(f" [!] 异常: {e}")


# --- 3. 批量执行 ---
if __name__ == "__main__":
    # 检查是不是周末
    if datetime.now().weekday() >= 5:
        print("今天是周末，不执行价格更新。")
    else:
        print(f"=== 开始每日价格更新: {datetime.now().strftime('%Y%m%d')} ===")

        # 全品种列表
        ALL_SYMBOLS = [
            ('lc0', '碳酸锂'), ('si0', '工业硅'),('ps0', '多晶硅'),
            ('rb0', '螺纹钢'), ('hc0', '热卷'), ('au0', '黄金'), ('ag0', '白银'), ('cu0', '沪铜'), ('al0', '沪铝'),
            ('zn0', '沪锌'),('ni0', '镍'),('sp0', '纸浆'),('ru0', '橡胶'),('ao0', '氧化铝'),
            ('m0', '豆粕'), ('i0', '铁矿石'), ('p0', '棕榈油'), ('y0', '豆油'), ('c0', '玉米'),('lh0', '生猪'),
            ('fg0', '玻璃'), ('sa0', '纯碱'), ('ma0', '甲醇'), ('ta0', 'PTA'), ('sr0', '白糖'), ('cf0', '棉花'), ('ap0', '苹果'),
            ('IF0', '沪深300'), ('IM0', '中证1000'), ('IC0', '中证500'), ('IH0', '上证50'), ('T0', '10年国债'), ('TS0', '2年国债'), ('Tl0', '30年国债')
        ]

        for code, name in ALL_SYMBOLS:
            update_today_price(code, name)
            time.sleep(1)  # 稍微停顿

        print("=== 更新结束 ===")
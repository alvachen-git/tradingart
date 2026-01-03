import akshare as ak
import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. 初始化环境
load_dotenv(override=True)

# 2. 数据库配置
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    print("❌ 错误：数据库配置缺失")
    exit()

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def fetch_and_fix_single_stock(symbol, start_date="20200101", end_date="20260101"):
    """
    专门修复某一只港股的历史数据
    symbol: 5位数字代码，例如 '00700'
    """
    # 自动补全代码格式
    symbol = str(symbol).zfill(5)
    ts_code = f"{symbol}.HK"

    print(f"🚀 开始抓取: {ts_code} ({start_date} - {end_date})...")

    try:
        # === 1. 调用 AkShare 接口 ===
        # adjust="qfq" 表示前复权，adjust="" 表示不复权 (通常数据库存不复权数据)
        df = ak.stock_hk_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date=end_date,
            adjust=""
        )

        if df.empty:
            print(f"❌ AkShare 返回数据为空！请检查代码 {symbol} 是否正确，或网络是否通畅。")
            return

        print(f"✅ 成功获取 {len(df)} 条数据，准备清洗...")

        # === 2. 数据清洗 (对齐数据库字段) ===
        # AkShare 返回列名: ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', ...]
        rename_dict = {
            '日期': 'trade_date',
            '开盘': 'open_price',
            '最高': 'high_price',
            '最低': 'low_price',
            '收盘': 'close_price',
            '成交量': 'vol',
            '成交额': 'amount',
            '涨跌幅': 'pct_chg'
        }
        df = df.rename(columns=rename_dict)

        # 格式化日期 YYYY-MM-DD -> YYYYMMDD
        df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')

        # 补充代码和名称
        df['ts_code'] = ts_code

        # 尝试获取名称（这一步不是必须的，但为了完整性）
        try:
            spot_df = ak.stock_hk_spot()
            # 兼容列名变化
            name_col = '中文名称' if '中文名称' in spot_df.columns else '名称'
            code_col = '代码' if '代码' in spot_df.columns else 'symbol'

            # 找到对应的名字
            row = spot_df[spot_df[code_col].astype(str).str.zfill(5) == symbol]
            if not row.empty:
                stock_name = row.iloc[0][name_col]
            else:
                stock_name = ts_code  # 找不到就用代码代替
        except:
            stock_name = ts_code

        df['name'] = stock_name

        # 筛选最终字段
        target_columns = [
            'trade_date', 'ts_code', 'open_price', 'high_price',
            'low_price', 'close_price', 'vol', 'amount',
            'pct_chg', 'name'
        ]

        # 补全缺失列
        for col in target_columns:
            if col not in df.columns:
                df[col] = None

        df_final = df[target_columns]

        # === 3. 数据预览 ===
        print("\n👀 数据预览 (最后5天):")
        print(df_final.tail(5))

        # === 4. 写入数据库 (先删后插，防止重复) ===
        with engine.connect() as conn:
            # 开启事务
            trans = conn.begin()
            try:
                # 删除旧数据
                del_sql = text("DELETE FROM stock_price WHERE ts_code = :code")
                res = conn.execute(del_sql, {"code": ts_code})
                print(f"\n🗑️ 已清理旧数据: {res.rowcount} 条")

                # 写入新数据
                df_final.to_sql('stock_price', conn, if_exists='append', index=False)

                trans.commit()
                print(f"💾 入库成功！共写入 {len(df_final)} 条数据。")

            except Exception as e:
                trans.rollback()
                print(f"❌ 数据库写入失败: {e}")

    except Exception as e:
        print(f"❌ 抓取过程发生错误: {e}")


if __name__ == "__main__":
    # 🛠️在此处修改您要修复的股票
    # 腾讯: 00700, 小米: 01810, 美团: 03690

    TARGET_SYMBOL = "00700"  # 小米
    fetch_and_fix_single_stock(TARGET_SYMBOL)
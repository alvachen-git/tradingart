import akshare as ak
import pandas as pd
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# 1. 初始化环境
load_dotenv(override=True)

# 数据库配置
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 数据库配置缺失，请检查 .env 文件")

# 创建数据库连接
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def update_hk_daily():
    """
    每日收盘后运行：获取全市场港股当天行情，并存入数据库
    """
    print(f"[{datetime.now()}] 🚀 开始执行港股每日更新任务...")

    try:
        # 1. 获取全市场实时行情 (一次性拉取所有股票)
        # 这个接口返回的是当前的快照，收盘后即为当日日线数据
        print("⏳ 正在拉取全市场港股快照 (ak.stock_hk_spot)...")
        df = ak.stock_hk_spot()

        if df.empty:
            print("❌ 未获取到数据，任务终止")
            return

        # 2. 检查列名并建立映射
        # AkShare 返回列: ['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '开盘价', '最高价', '最低价', '昨收价', '换手率', '市盈率-动态', '市净率', '总市值', '流通市值', '日期时间']
        # 注意：不同版本列名可能微调，这里做容错处理

        rename_map = {
            '代码': 'symbol',
            '中文名称': 'name',
            '最新价': 'close_price',
            '今开': 'open_price',
            '最高': 'high_price',
            '最低': 'low_price',
            '成交量': 'vol',
            '成交额': 'amount',
            '涨跌幅': 'pct_chg',
            '日期时间': 'trade_time'
        }

        # 检查关键列是否存在
        if '代码' not in df.columns or '最新价' not in df.columns:
            print(f"❌ 接口列名变更，当前列名: {df.columns.tolist()}")
            return

        df = df.rename(columns=rename_map)

        # 3. 数据清洗与格式化
        print("🧹 正在清洗数据...")

        # A. 提取日期 (YYYYMMDD)
        # 接口返回的 '日期时间' 可能是 "2026/01/02 16:00:00"
        # 我们取第一行的日期作为当天的统一日期
        sample_time = str(df.iloc[0]['trade_time'])
        try:
            # 尝试解析日期
            current_trade_date = pd.to_datetime(sample_time).strftime('%Y%m%d')
        except:
            # 如果解析失败，默认使用今天 (防止接口返回空字符串)
            current_trade_date = datetime.now().strftime('%Y%m%d')

        print(f"📅 识别到数据日期: {current_trade_date}")
        df['trade_date'] = current_trade_date

        # B. 构造 ts_code (5位数字 + .HK)
        df['symbol'] = df['symbol'].astype(str).str.zfill(5)
        df['ts_code'] = df['symbol'] + ".HK"

        # C. 筛选有效列
        target_columns = [
            'trade_date', 'ts_code', 'open_price', 'high_price',
            'low_price', 'close_price', 'vol', 'amount',
            'pct_chg', 'name'
        ]

        # 补齐可能缺失的列 (比如有时候接口没返回 open_price)
        for col in target_columns:
            if col not in df.columns:
                df[col] = None

        df_final = df[target_columns].copy()

        # D. 数据类型转换 (防止入库报错)
        numeric_cols = ['open_price', 'high_price', 'low_price', 'close_price', 'vol', 'amount', 'pct_chg']
        for col in numeric_cols:
            df_final[col] = pd.to_numeric(df_final[col], errors='coerce')

        # 过滤掉没有成交量的停牌股票 (可选)
        # df_final = df_final[df_final['vol'] > 0]

        print(f"📊 准备入库: 共 {len(df_final)} 条数据")

        # 4. 数据库操作 (幂等性设计：先删后插)
        # 防止同一天重复运行脚本导致数据重复
        with engine.connect() as conn:
            # 开启事务
            trans = conn.begin()
            try:
                # A. 删除当天的港股数据 (如果已存在)
                # 注意：只删除 .HK 结尾的数据，不要误删 A股
                delete_sql = text(f"DELETE FROM stock_price WHERE trade_date = :d AND ts_code LIKE '%.HK'")
                result = conn.execute(delete_sql, {"d": current_trade_date})
                print(f"🗑️ 已清理 {current_trade_date} 的旧港股数据 ({result.rowcount} 条)")

                # B. 写入新数据
                df_final.to_sql('stock_price', conn, if_exists='append', index=False)

                trans.commit()
                print("✅ 入库成功！")

            except Exception as db_err:
                trans.rollback()
                print(f"❌ 数据库操作失败，已回滚: {db_err}")
                raise db_err

    except Exception as e:
        print(f"❌ 脚本执行出错: {e}")


if __name__ == "__main__":
    update_hk_daily()
import akshare as ak
import pandas as pd
import os
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from tqdm import tqdm

# 1. 初始化环境
load_dotenv(override=True)

# 数据库配置
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
    raise ValueError("❌ 请检查 .env 文件数据库配置")

# 创建数据库连接
db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def get_hk_codes_map():
    """
    获取港股代码和名称的映射表
    """
    print("⏳ 正在拉取港股实时行情列表以获取代码和名称...")
    try:
        # 获取所有港股的实时行情
        df = ak.stock_hk_spot()

        # --- 🔥【核心修复】适配您测试出来的列名 ---
        # 测试结果显示列名为: ['日期时间', '代码', '中文名称', '英文名称', ...]

        if '代码' in df.columns and '中文名称' in df.columns:
            # 建立映射: {'00001': '长和', ...}
            # 确保代码是5位字符串
            df['代码'] = df['代码'].astype(str).str.zfill(5)
            code_map = df.set_index('代码')['中文名称'].to_dict()

            print(f"✅ 成功获取到 {len(code_map)} 只港股信息")
            return code_map
        else:
            print(f"❌ 错误：列名不匹配。当前列名：{df.columns.tolist()}")
            return {}

    except Exception as e:
        print(f"❌ 获取列表失败: {e}")
        return {}


def fetch_and_save_hk_history(start_date, end_date):
    """
    遍历每只股票，抓取历史数据并存库
    """
    # 1. 获取所有股票代码
    code_map = get_hk_codes_map()
    if not code_map:
        print("❌ 无法获取股票列表，任务终止。")
        return

    # 2. 准备循环
    symbols = list(code_map.keys())

    # 再次过滤，确保是5位数字代码
    symbols = [s for s in symbols if str(s).isdigit() and len(str(s)) == 5]

    print(f"🚀 开始任务：抓取 {len(symbols)} 只港股的历史数据 ({start_date} - {end_date})")

    # 进度条
    pbar = tqdm(symbols)

    for symbol in pbar:
        stock_name = code_map.get(symbol, '')
        pbar.set_description(f"处理: {stock_name}({symbol})")

        try:
            # === 调用 AkShare 接口 (stock_hk_hist) ===
            # 注意：历史接口的参数 symbol 只要5位数字即可
            df = ak.stock_hk_hist(
                symbol=symbol,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust=""
            )

            if df.empty:
                continue

            # === 数据清洗 ===
            # AkShare 历史接口返回列名通常是: ['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', ...]

            # 1. 重命名列 (映射到您的数据库字段)
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

            # 2. 格式化数据
            # 确保日期格式为 YYYYMMDD
            df['trade_date'] = pd.to_datetime(df['trade_date']).dt.strftime('%Y%m%d')

            # 构造 ts_code (例如: 00700.HK)
            ts_code = f"{symbol}.HK"
            df['ts_code'] = ts_code

            # 填充名称
            df['name'] = stock_name

            # 3. 筛选需要的列
            target_columns = [
                'trade_date', 'ts_code', 'open_price', 'high_price',
                'low_price', 'close_price', 'vol', 'amount',
                'pct_chg', 'name'
            ]

            # 补齐缺失列
            for col in target_columns:
                if col not in df.columns:
                    df[col] = None

            df_final = df[target_columns]

            # === 存入数据库 ===
            df_final.to_sql('stock_price', engine, if_exists='append', index=False)

            # 稍微休眠，礼貌爬取
            time.sleep(0.1)

        except Exception as e:
            # 某个股票报错不影响其他股票
            # print(f"❌ {symbol} 出错: {e}")
            pass


if __name__ == "__main__":
    # 配置抓取时间范围
    START = "20250101"
    END = "20260103"

    fetch_and_save_hk_history(START, END)
    print("\n🏁 所有任务结束")
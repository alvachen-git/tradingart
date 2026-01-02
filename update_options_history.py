import pandas as pd
from datetime import datetime, timedelta
import time
import os
from dotenv import load_dotenv

# 引入我们之前写好的抓取函数 (复用代码，保持整洁)
# 确保 save_options_to_db.py 在同一个文件夹下
try:
    from save_options_to_db import fetch_and_save_options
except ImportError:
    print("❌ 错误：找不到 save_options_to_db.py，请确保该文件存在。")
    exit()

# 1. 初始化
load_dotenv(override=True)


def run_options_update_range(start_date, end_date):
    """
    按日期区间循环更新期权数据
    """
    print(f"\n=== 🚀 准备更新期权数据: {start_date} 至 {end_date} ===")

    # 2. 定义要更新的 ETF 列表
    # 这里涵盖了主流的 ETF 期权品种
    TARGET_ETFS = [
        "510050.SH",  # 50ETF
        "510300.SH",  # 300ETF (沪)
        "510500.SH",  # 500ETF
        "588000.SH",  # 科创50
        "159915.SZ"  # 创业板
    ]

    # 3. 生成日期序列
    s_date = datetime.strptime(start_date, '%Y%m%d')
    e_date = datetime.strptime(end_date, '%Y%m%d')

    current_date = s_date
    while current_date <= e_date:
        date_str = current_date.strftime('%Y%m%d')

        # 4. 周末判断 (自动跳过)
        if current_date.weekday() >= 5:
            print(f"[-] {date_str} 是周末，跳过。")
            current_date += timedelta(days=1)
            continue

        print(f"\n>>> 📅 正在处理: {date_str}")

        # 5. 循环抓取每个 ETF
        for etf in TARGET_ETFS:
            try:
                # 调用 fetch_and_save_options (按单日抓取)
                # start_date 和 end_date 都传同一天，确保精细控制
                fetch_and_save_options(etf_code=etf, start_date=date_str, end_date=date_str)
            except Exception as e:
                print(f"   [!] 处理 {etf} 时发生错误: {e}")

            # 避免 API 过于频繁
            time.sleep(0.5)

        current_date += timedelta(days=1)

    print(f"\n=== ✅ 区间期权更新全部结束 ===")


if __name__ == "__main__":
    # ==========================================
    #  🔴 配置区域：指定更新日期范围 (格式 YYYYMMDD)
    # ==========================================

    # 场景 A: 补跑历史数据 (修改这里的日期)
    START_DATE = "20240101"
    END_DATE = "20250205"

    # 场景 B: 只跑今天 (如果想自动跑当天，取消下面两行的注释)
    # today = datetime.now().strftime('%Y%m%d')
    # START_DATE = today; END_DATE = today

    run_options_update_range(START_DATE, END_DATE)
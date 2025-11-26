import pandas as pd
from datetime import datetime
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


def run_daily_options_update():
    # 1. 获取今天日期
    today_str = datetime.now().strftime('%Y%m%d')
    weekday = datetime.now().weekday()

    # 周末跳过
    if weekday >= 5:
        print(f"今天是周末 ({today_str})，期权数据无需更新。")
        return

    print(f"\n=== 开始期权数据每日更新: {today_str} ===")

    # 2. 定义要更新的 ETF 列表
    # 这里涵盖了主流的 ETF 期权品种
    TARGET_ETFS = [
        "510050.SH",  # 50ETF
        "510300.SH",  # 300ETF (沪)
        "510500.SH",  # 500ETF
        "588000.SH",  # 科创50
        "159915.SZ"  # 创业板
    ]

    # 3. 循环抓取
    for etf in TARGET_ETFS:
        try:
            # 调用 fetch_and_save_options 只抓取“今天”
            fetch_and_save_options(etf_code=etf, start_date=today_str, end_date=today_str)
        except Exception as e:
            print(f" [!] 处理 {etf} 时发生错误: {e}")

        time.sleep(1)  # 礼貌延时

    print(f"=== 期权更新结束 ===")


if __name__ == "__main__":
    run_daily_options_update()
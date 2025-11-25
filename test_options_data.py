import tushare as ts
import pandas as pd
import os
from dotenv import load_dotenv

# 1. 初始化
load_dotenv(override=True)
token = os.getenv("TUSHARE_TOKEN")

if not token:
    print("❌ 錯誤：未找到 TUSHARE_TOKEN")
    exit()

print(f"🔑 Token: {token[:5]}******")
ts.set_token(token)
pro = ts.pro_api()


def check_market(exchange, keywords):
    print(f"\n📡 正在掃描 [{exchange}] 交易所的所有期權合約...")
    try:
        # 拉取該交易所所有上市合約
        df = pro.opt_basic(exchange=exchange, list_status='L', fields='ts_code,name')

        if df.empty:
            print(f"   [-] {exchange} 未返回數據，請檢查權限或網絡。")
            return

        print(f"   [√] 獲取到 {len(df)} 個合約。正在搜索關鍵詞: {keywords}")

        # 模糊匹配
        # 使用 | 連接多個關鍵詞，例如 "科創|50ETF"
        mask = df['name'].str.contains('|'.join(keywords))
        found = df[mask]

        if not found.empty:
            print(f"   ✅ 找到 {len(found)} 個相關合約！")
            # 打印出前 5 個不同的名稱，讓我們看看它到底叫什麼
            unique_names = found['name'].apply(lambda x: x.split('购')[0].split('沽')[0]).unique()
            print(f"   👉 合約名稱樣例 (去重後): {unique_names}")
            print(f"   👉 完整名稱示例: {found['name'].iloc[0]}")
            print(f"   👉 代碼示例: {found['ts_code'].iloc[0]}")
        else:
            print(f"   ❌ 未找到包含 {keywords} 的合約。")

    except Exception as e:
        print(f"   [!] 發生異常: {e}")


if __name__ == "__main__":
    # 1. 測試上交所 (SSE) - 找科創50 (588000)
    # 關鍵詞試試 "科創"
    check_market('SSE', ['科创', '588000'])

    # 2. 測試深交所 (SZSE) - 找創業板 (159915)
    # 關鍵詞試試 "创业", "159915"
    check_market('SZSE', ['创业', '159915'])
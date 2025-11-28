import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
from io import StringIO
import os

# 定义一个本地数据文件的路径
CSV_FILE_PATH = "fed_data_cache.csv"


def fetch_online_data():
    """尝试从 Investing.com 联网抓取"""
    url = "https://cn.investing.com/central-banks/fed-rate-monitor"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://cn.investing.com/"
    }

    try:
        response = requests.get(url, headers=headers, timeout=5)  # 设置短一点的超时
        response.encoding = 'utf-8'

        if response.status_code != 200:
            return None

        soup = BeautifulSoup(response.text, 'html.parser')
        clean_data = []
        date_pattern = re.compile(r'^\s*\d{4}年\d{1,2}月\d{1,2}日\s*$')
        date_headers = soup.find_all(text=date_pattern)

        for date_text in date_headers:
            meeting_date = date_text.strip()
            parent_element = date_text.parent
            target_table = parent_element.find_next('table')

            if target_table:
                df_list = pd.read_html(StringIO(str(target_table)))
                if len(df_list) > 0:
                    df = df_list[0]
                    current_prob_col = None
                    for col in df.columns:
                        if "目前" in str(col):
                            current_prob_col = col
                            break

                    if current_prob_col:
                        for index, row in df.iterrows():
                            rate_range = str(row.iloc[0])
                            prob_str = str(row[current_prob_col])
                            try:
                                prob = float(prob_str.replace('%', ''))
                            except:
                                prob = 0

                            if prob > 0:
                                clean_data.append({
                                    "会议日期": meeting_date,
                                    "目标利率": rate_range,
                                    "概率(%)": prob
                                })

        if clean_data:
            df = pd.DataFrame(clean_data)
            # 🟢 关键：只要联网抓取成功，立刻保存一份 CSV 到本地作为备份
            df.to_csv(CSV_FILE_PATH, index=False)
            print("✅ 联网抓取成功，已更新缓存文件。")
            return df

    except Exception as e:
        print(f"⚠️ 联网抓取失败: {e}")
        return None

    return None


def get_fed_probabilities():
    """
    主函数：
    1. 先尝试联网抓取 (本地开发时通常会成功)
    2. 如果联网失败 (阿里云上通常会失败)，则读取本地 CSV 缓存
    """

    # 1. 尝试联网
    df = fetch_online_data()

    if df is not None:
        return df

    # 2. 如果联网失败，尝试读取 CSV
    print("⚠️ 无法联网，正在尝试读取本地缓存文件...")
    if os.path.exists(CSV_FILE_PATH):
        try:
            df_cache = pd.read_csv(CSV_FILE_PATH)
            # 确保列名是字符串，防止读取后格式错乱
            df_cache['会议日期'] = df_cache['会议日期'].astype(str)
            df_cache['目标利率'] = df_cache['目标利率'].astype(str)
            print("✅ 成功读取本地 CSV 缓存。")
            return df_cache
        except Exception as e:
            print(f"❌ 读取 CSV 缓存失败: {e}")
            return None
    else:
        print("❌ 没有找到本地 CSV 缓存文件。")
        return None


# 本地测试
if __name__ == "__main__":
    df = get_fed_probabilities()
    if df is not None:
        print(df)
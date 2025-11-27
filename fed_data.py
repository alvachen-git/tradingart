import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
from io import StringIO


def get_fed_probabilities():
    """
    针对 Investing.com 中文版优化的抓取策略。
    策略：先利用正则找到网页里的 "202x年x月x日" 标题，再抓取紧跟在它后面的表格。
    """
    url = "https://cn.investing.com/central-banks/fed-rate-monitor"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Referer": "https://cn.investing.com/"
    }

    try:
        print(f"🔄 正在连接: {url} ...")
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'  # 强制使用 UTF-8，防止中文乱码

        if response.status_code != 200:
            print("❌ 连接被拒绝")
            return None

        soup = BeautifulSoup(response.text, 'html.parser')

        clean_data = []

        # 1. 寻找所有符合 "xxxx年xx月xx日" 格式的标题
        # Investing 的日期通常放在 h2, div 或者 span 里
        # 我们用正则去匹配文本内容
        date_pattern = re.compile(r'^\s*\d{4}年\d{1,2}月\d{1,2}日\s*$')

        # 找到所有包含日期的元素
        date_headers = soup.find_all(text=date_pattern)

        for date_text in date_headers:
            meeting_date = date_text.strip()

            # 找到这个日期标签的父元素，然后找它接下来的那个 table
            # parent.find_next('table') 是寻找紧邻的表格
            parent_element = date_text.parent
            target_table = parent_element.find_next('table')

            if target_table:
                # 用 Pandas 解析这个特定的 HTML 表格
                # str(target_table) 把 soup 对象转回 html 字符串给 pandas 读
                df_list = pd.read_html(StringIO(str(target_table)))

                if len(df_list) > 0:
                    df = df_list[0]

                    # 2. 清洗表格数据
                    # Investing 中文表的列名通常是：['目标利率', '目前', '上一日'...]
                    # 我们需要第0列(利率) 和 第1列(目前概率)

                    # 确保列名包含 "目前" (Current probability)
                    current_prob_col = None
                    for col in df.columns:
                        if "目前" in str(col):
                            current_prob_col = col
                            break

                    if current_prob_col:
                        # 遍历每一行数据
                        for index, row in df.iterrows():
                            rate_range = str(row.iloc[0])  # 第0列通常是利率 (3.50 - 3.75)
                            prob_str = str(row[current_prob_col])  # 概率 (82.2%)

                            # 去掉百分号转数字
                            try:
                                prob = float(prob_str.replace('%', ''))
                            except:
                                prob = 0

                            if prob > 0:
                                clean_data.append({
                                    "会议日期": meeting_date,  # 这里的日期就是标题上的 "2025年12月11日"
                                    "目标利率": rate_range,
                                    "概率(%)": prob
                                })

        # 生成最终数据
        final_df = pd.DataFrame(clean_data)

        if not final_df.empty:
            print("✅ 成功获取并清洗数据！")
            return final_df
        else:
            print("⚠️ 未找到有效数据，可能是网页结构变了")
            return None

    except Exception as e:
        print(f"❌ 抓取过程出错: {e}")
        return None


if __name__ == "__main__":
    # 本地测试
    df = get_fed_probabilities()
    if df is not None:
        print(df.head())
    else:
        print("Fail")
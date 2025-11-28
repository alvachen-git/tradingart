import os
import pandas as pd
from tiingo import TiingoClient
from gnews import GNews
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import datetime
import time
import requests
import random


load_dotenv(override=True)


# 1. 初始化
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(db_url)


def update_news_google():
    print("📰 开始通过 Google News 抓取新闻 (稳定版)...")

    # --- 1. 配置 Google News ---
    # language='zh-Hans': 简体中文 (如果你想要繁体，改成 'zh-Hant')
    # country='CN': 中国 (或者 'TW' 台湾, 'US' 美国)
    # max_results=5: 每只股票抓 5 条
    google_news = GNews(language='zh-Hans', country='CN', max_results=5)

    tickers = ['苹果公司', '特斯拉', '英伟达', 'AMD', '微软']  # 建议用中文名搜，效果更好
    # 或者依然用代码: tickers = ['AAPL', 'TSLA', 'NVDA', 'AMD', 'MSFT']

    all_news = []

    for keyword in tickers:
        try:
            print(f"   -> 正在搜索: {keyword} ...")

            # 抓取新闻
            news_items = google_news.get_news(keyword)

            for item in news_items:
                # Google 返回的时间格式比较特殊，需要清洗
                # item['published date'] 通常是 "Fri, 28 Nov 2025 07:00:00 GMT"
                try:
                    pub_date = pd.to_datetime(item['published date']).tz_convert(None)
                except:
                    pub_date = datetime.datetime.now()

                all_news.append({
                    'id': item['url'],  # 用链接作为唯一ID
                    'publishedDate': pub_date,
                    'title': item['title'],
                    'url': item['url'],
                    'source': item['publisher']['title'],
                    'description': item.get('description', ''),
                    'tickers': keyword,
                    'thumbnail': ''  # Google News 免费接口很难拿到图，这里留空
                })

        except Exception as e:
            print(f"   ⚠️ 获取 {keyword} 失败: {e}")

    if not all_news:
        print("❌ 未抓取到任何新闻。")
        return

    # 转 DataFrame
    df = pd.DataFrame(all_news)

    # 去重
    df.drop_duplicates(subset=['title'], inplace=True)
    df.sort_values(by='publishedDate', ascending=False, inplace=True)

    # 存入数据库
    try:
        df.to_sql('stock_news', con=engine, if_exists='replace', index=False)
        print(f"✅ 成功更新 {len(df)} 条新闻到数据库！")
    except Exception as e:
        print(f"❌ 数据库存储失败: {e}")


if __name__ == "__main__":
    update_news_google()
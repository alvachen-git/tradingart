# ==========================================
#   趋势监控系统 - 最终版
#   不依赖第三方API，直接调用官方接口
# ==========================================

import time
import random
import re
import json
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv(override=True)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
SERPAPI_KEY = os.getenv("SERPAPI_KEY", "")

def get_db_engine():
    if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_NAME]):
        return None
    db_url = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url, pool_recycle=7200, pool_pre_ping=True)

engine = get_db_engine()
PROXY = 'http://127.0.0.1:7890'
PROXIES = {'http': PROXY, 'https': PROXY}

def make_request(url, headers=None, params=None, timeout=15):
    default_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/json, text/html, */*',
    }
    if headers:
        default_headers.update(headers)
    try:
        return requests.get(url, headers=default_headers, params=params, timeout=timeout)
    except:
        return None

# ==================== Google Trends ====================
def fetch_google_trending_serpapi():
    if not SERPAPI_KEY:
        return []
    try:
        url = "https://serpapi.com/search.json"
        params = {'engine': 'google_trends_trending_now', 'geo': 'TW', 'api_key': SERPAPI_KEY}
        resp = requests.get(url, params=params, timeout=20)
        data = resp.json()
        results = []
        for i, item in enumerate(data.get('trending_searches', [])[:50]):
            query = item.get('query', '')
            if query:
                results.append({'keyword': query, 'source': 'google', 'hot_type': 'trending', 'hot_score': 0, 'ranking': i + 1})
        if results:
            print(f"    SerpAPI: {len(results)} 条")
        return results
    except Exception as e:
        print(f"    SerpAPI失败: {str(e)[:30]}")
        return []

def fetch_google_trending_pytrends():
    try:
        from pytrends.request import TrendReq
        pytrends = TrendReq(hl='zh-TW', tz=480, timeout=(10, 30), proxies=[PROXY])
        results = []
        for region in ['taiwan', 'hong_kong', 'japan']:
            try:
                trending = pytrends.trending_searches(pn=region)
                for i, keyword in enumerate(trending[0].tolist()[:50]):
                    results.append({'keyword': keyword, 'source': 'google', 'hot_type': 'trending', 'hot_score': 0, 'ranking': i + 1})
                if results:
                    print(f"    pytrends从{region}: {len(results)} 条")
                    return results
            except:
                continue
    except Exception as e:
        print(f"    pytrends失败: {str(e)[:30]}")
    return []

def fetch_google_trending():
    results = fetch_google_trending_serpapi()
    if results:
        return results
    results = fetch_google_trending_pytrends()
    if results:
        return results
    print("    Google采集失败(建议配置SERPAPI_KEY)")
    return []

# ==================== 微博 ====================
def fetch_weibo_hot():
    try:
        url = "https://weibo.com/ajax/side/hotSearch"
        headers = {'Referer': 'https://weibo.com/'}
        resp = make_request(url, headers=headers)
        if not resp:
            return []
        data = resp.json()
        hot_list = []
        if data.get('ok') == 1:
            for i, item in enumerate(data.get('data', {}).get('realtime', [])[:50]):
                word = item.get('word', '')
                if word:
                    hot_list.append({'keyword': word, 'source': 'weibo', 'hot_type': 'trending', 'hot_score': item.get('raw_hot', 0), 'ranking': i + 1})
        return hot_list
    except Exception as e:
        print(f"    微博失败: {str(e)[:30]}")
        return []

# ==================== 百度 ====================
def fetch_baidu_hot():
    hot_list = []
    try:
        url = "https://top.baidu.com/api/board?platform=wise&tab=realtime"
        resp = make_request(url)
        if resp and resp.status_code == 200:
            data = resp.json()
            cards = data.get('data', {}).get('cards', [])
            if cards:
                for i, item in enumerate(cards[0].get('content', [])[:50]):
                    word = item.get('word', '') or item.get('query', '')
                    if word:
                        hot_list.append({'keyword': word, 'source': 'baidu', 'hot_type': 'trending', 'hot_score': int(item.get('hotScore', 0) or 0), 'ranking': i + 1})
            if hot_list:
                return hot_list
    except:
        pass
    try:
        url = "https://top.baidu.com/board?tab=realtime"
        resp = make_request(url, headers={'Accept': 'text/html'})
        if resp and resp.status_code == 200:
            match = re.search(r'<!--s-data:(.*?)-->', resp.text)
            if match:
                data = json.loads(match.group(1))
                cards = data.get('data', {}).get('cards', [])
                if cards:
                    for i, item in enumerate(cards[0].get('content', [])[:50]):
                        word = item.get('word', '') or item.get('query', '')
                        if word:
                            hot_list.append({'keyword': word, 'source': 'baidu', 'hot_type': 'trending', 'hot_score': int(item.get('hotScore', 0) or 0), 'ranking': i + 1})
    except:
        pass
    return hot_list

# ==================== 抖音 ====================
def fetch_douyin_hot():
    try:
        url = "https://www.douyin.com/aweme/v1/web/hot/search/list/"
        headers = {'Referer': 'https://www.douyin.com/'}
        resp = make_request(url, headers=headers)
        if resp and resp.status_code == 200:
            data = resp.json()
            hot_list = []
            for i, item in enumerate(data.get('data', {}).get('word_list', [])[:50]):
                word = item.get('word', '')
                if word:
                    hot_list.append({'keyword': word, 'source': 'douyin', 'hot_type': 'trending', 'hot_score': item.get('hot_value', 0), 'ranking': i + 1})
            return hot_list
    except:
        pass
    return []

# ==================== 知乎 ====================
def fetch_zhihu_hot():
    hot_list = []
    try:
        url = "https://www.zhihu.com/api/v3/feed/topstory/hot-lists/total"
        headers = {'Referer': 'https://www.zhihu.com/hot'}
        resp = make_request(url, headers=headers)
        if resp and resp.status_code == 200:
            data = resp.json()
            for i, item in enumerate(data.get('data', [])[:50]):
                title = item.get('target', {}).get('title', '')
                if title:
                    hot_list.append({'keyword': title, 'source': 'zhihu', 'hot_type': 'trending', 'hot_score': 0, 'ranking': i + 1})
        if hot_list:
            return hot_list
    except:
        pass
    try:
        url = "https://www.zhihu.com/hot"
        resp = make_request(url, headers={'Accept': 'text/html'})
        if resp and resp.status_code == 200:
            pattern = r'"title":"([^"]{5,100})"'
            matches = re.findall(pattern, resp.text)
            seen = set()
            for title in matches[:100]:
                if title not in seen and len(title) > 5:
                    seen.add(title)
                    hot_list.append({'keyword': title, 'source': 'zhihu', 'hot_type': 'trending', 'hot_score': 0, 'ranking': len(hot_list) + 1})
                    if len(hot_list) >= 50:
                        break
    except:
        pass
    return hot_list

# ==================== 头条 ====================
def fetch_toutiao_hot():
    try:
        url = "https://www.toutiao.com/hot-event/hot-board/?origin=toutiao_pc"
        resp = make_request(url)
        if resp and resp.status_code == 200:
            data = resp.json()
            hot_list = []
            for i, item in enumerate(data.get('data', [])[:50]):
                title = item.get('Title', '')
                if title:
                    hot_list.append({'keyword': title, 'source': 'toutiao', 'hot_type': 'trending', 'hot_score': item.get('HotValue', 0), 'ranking': i + 1})
            return hot_list
    except:
        pass
    return []

# ==================== B站 ====================
def fetch_bilibili_hot():
    hot_list = []
    try:
        url = "https://api.bilibili.com/x/web-interface/wbi/search/square"
        resp = make_request(url, params={'limit': 50})
        if resp and resp.status_code == 200:
            data = resp.json()
            if data.get('code') == 0:
                trending = data.get('data', {}).get('trending', {}).get('list', [])
                for i, item in enumerate(trending[:50]):
                    keyword = item.get('keyword', '') or item.get('show_name', '')
                    if keyword:
                        hot_list.append({'keyword': keyword, 'source': 'bilibili', 'hot_type': 'trending', 'hot_score': item.get('heat_score', 0), 'ranking': i + 1})
        if hot_list:
            return hot_list
    except:
        pass
    try:
        url = "https://api.bilibili.com/x/web-interface/ranking/v2"
        resp = make_request(url)
        if resp and resp.status_code == 200:
            data = resp.json()
            if data.get('code') == 0:
                for i, item in enumerate(data.get('data', {}).get('list', [])[:50]):
                    title = item.get('title', '')
                    if title:
                        hot_list.append({'keyword': title, 'source': 'bilibili', 'hot_type': 'trending', 'hot_score': item.get('stat', {}).get('view', 0), 'ranking': i + 1})
    except:
        pass
    return hot_list

# ==================== 数据库 ====================
def save_hot_list(hot_list):
    if not hot_list or engine is None:
        return 0
    today = datetime.now().strftime('%Y-%m-%d')
    for item in hot_list:
        item['trend_date'] = today
    sql = text("""INSERT INTO trend_hotlist (keyword, source, hot_type, hot_score, ranking, trend_date)
        VALUES (:keyword, :source, :hot_type, :hot_score, :ranking, :trend_date)
        ON DUPLICATE KEY UPDATE hot_score = VALUES(hot_score), ranking = VALUES(ranking)""")
    try:
        with engine.connect() as conn:
            conn.execute(sql, hot_list)
            conn.commit()
        return len(hot_list)
    except Exception as e:
        print(f"[ERROR] 保存失败: {e}")
        return 0

def detect_rising_keywords():
    if engine is None:
        return []
    alerts = []
    try:
        sql_new = text("""SELECT t1.keyword, t1.source, t1.hot_score, t1.ranking
            FROM trend_hotlist t1 WHERE t1.trend_date = CURDATE()
              AND NOT EXISTS (SELECT 1 FROM trend_hotlist t2 
                  WHERE t2.keyword = t1.keyword AND t2.source = t1.source
                    AND t2.trend_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY))
            ORDER BY t1.ranking ASC LIMIT 50""")
        with engine.connect() as conn:
            df = pd.read_sql(sql_new, conn)
        for _, row in df.iterrows():
            alerts.append({'keyword': row['keyword'], 'source': row['source'], 'alert_type': 'new',
                'description': f"新上榜 (排名#{row['ranking']})", 'hot_score': int(row['hot_score']) if row['hot_score'] else 0})
    except Exception as e:
        print(f"[ERROR] 检测失败: {e}")
    return alerts

def is_finance_related(keyword):
    keywords = ['油', '金', '银', '铜', '铁', '钢', '铝', '锌', '猪', '牛', '肉', '粮', '米', '麦', '豆', '玉米', '糖', '棉',
        '气', '煤', '电', '锂', '美联储', 'Fed', '加息', '降息', '利率', 'CPI', 'GDP', '通胀', '通缩', '衰退', '经济', '央行',
        '货币', '汇率', '美元', '人民币', '俄罗斯', '乌克兰', '伊朗', '中东', '战争', '制裁', '关税', '贸易',
        '股', 'A股', '港股', '美股', '涨停', '跌停', '牛市', '熊市', '基金', '投资', '理财', '券商', '证券', 'IPO',
        '新能源', '光伏', '锂电', '芯片', '半导体', 'AI', '人工智能', '房地产', '楼市', '房价', '汽车','特朗普',
        '茅台', '宁德', '比亚迪', '特斯拉', '苹果', '华为', '腾讯', '阿里', '英伟达', 'AI', '航天', '能源', '地缘', '以色列']
    return any(kw in keyword for kw in keywords)

def save_alerts(alerts):
    if not alerts or engine is None:
        return 0
    for alert in alerts:
        alert['is_finance_related'] = 1 if is_finance_related(alert['keyword']) else 0
    sql = text("""INSERT INTO trend_alert_v2 (keyword, source, alert_type, description, hot_score, is_finance_related)
        VALUES (:keyword, :source, :alert_type, :description, :hot_score, :is_finance_related)""")
    try:
        with engine.connect() as conn:
            conn.execute(sql, alerts)
            conn.commit()
        finance_count = sum(1 for a in alerts if a['is_finance_related'])
        print(f"[INFO] 生成 {len(alerts)} 条警报，其中 {finance_count} 条金融相关")
        return len(alerts)
    except:
        return 0

def main():
    print(f"\n{'='*50}")
    print(f"[{datetime.now()}] 开始采集热榜数据...")
    print(f"{'='*50}\n")
    if engine is None:
        print("[ERROR] 数据库未连接！")
        return
    total = 0
    print("[INFO] 采集 Google Trends...")
    google_items = fetch_google_trending()
    saved = save_hot_list(google_items)
    total += saved
    print(f"  - Google: {saved} 条")
    print("\n[INFO] 采集各平台热榜...")
    platforms = [('weibo', fetch_weibo_hot), ('baidu', fetch_baidu_hot), ('douyin', fetch_douyin_hot),
        ('zhihu', fetch_zhihu_hot), ('toutiao', fetch_toutiao_hot), ('bilibili', fetch_bilibili_hot)]
    for name, func in platforms:
        print(f"  采集 {name}...")
        items = func()
        saved = save_hot_list(items)
        total += saved
        print(f"  - {name}: {saved} 条")
        time.sleep(0.5)
    print(f"\n[INFO] 共保存 {total} 条热榜数据")
    print("\n[INFO] 检测新兴热点...")
    alerts = detect_rising_keywords()
    save_alerts(alerts)
    finance_alerts = [a for a in alerts if is_finance_related(a['keyword'])]
    if finance_alerts:
        print(f"\n[INFO] 金融相关热点:")
        for a in finance_alerts[:15]:
            emoji = {'new': '🆕', 'rising': '📈', 'persistent': '🔄'}.get(a['alert_type'], '📌')
            print(f"  {emoji} {a['keyword']} ({a['source']}) - {a['description']}")
    print(f"\n{'='*50}")
    print(f"[{datetime.now()}] 采集完成!")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
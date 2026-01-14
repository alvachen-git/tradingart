import requests
import json
from langchain.tools import tool
from typing import List

# Polymarket API 端点
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"


def fetch_top_markets(limit=20) -> List[dict]:
    """获取全网成交量最高的 limit 个热门事件"""
    headers = {"User-Agent": "Mozilla/5.0"}
    params = {
        "limit": limit,
        "active": "true",
        "closed": "false",
        "order": "volume24hr",  # 事件层面：依然按热度拉取
        "ascending": "false"
    }
    # 🔥 [新增] 定义代理配置
    proxies = {
        "http": "http://127.0.0.1:7890",
        "https": "http://127.0.0.1:7890",
    }
    try:
        # 🔥 [修改] 在 requests.get 中加入 proxies=proxies
        resp = requests.get(GAMMA_API_URL, params=params, headers=headers, timeout=10, proxies=proxies)

        if resp.status_code == 200:
            return resp.json()
        return []
    except Exception as e:
        print(f"Polymarket API Error: {e}")
        return []


@tool
def tool_get_polymarket_sentiment(keywords: str) -> str:
    """
    Polymarket 预测市场查询工具。
    【使用场景】：当用户询问宏观行情、地缘政治、大选（如美联储主席、总统提名）等不确定性事件时，必须调用此工具。
    【输入】：英文关键词，例如 "Iran", "Oil", "Fed Chair", "Trump"。
    【返回】：当前市场上概率最高的预测结果。
    """
    print(f"\n🔍 AI 正在查阅 Polymarket: {keywords} ...")

    if not keywords:
        return "请提供有效的搜索关键词。"

    keyword_list = [k.strip().lower() for k in keywords.split(",")]

    # 1. 拉取热门事件 (Limit 60 确保能覆盖到大部分热点)
    events = fetch_top_markets(limit=60)

    found_markets = []

    for event in events:
        title = event.get('title', '')
        full_text = title.lower()

        # 2. 匹配关键词
        if any(k in full_text for k in keyword_list):

            # 获取该事件下的所有子市场 (Markets)
            all_markets = event.get('markets', [])
            if not all_markets: continue

            # --- 🔥 核心修改：定义概率提取函数，用于排序 ---
            def get_win_probability(m):
                try:
                    # 优先解析 outcomePrices (['0.39', '0.61'])
                    raw_prices = m.get('outcomePrices')
                    if raw_prices:
                        if isinstance(raw_prices, str):
                            prices = json.loads(raw_prices)
                        else:
                            prices = raw_prices
                        if prices and len(prices) > 0:
                            return float(prices[0]) * 100

                    # 备用 groupPrice
                    gp = m.get('groupPrice')
                    if gp:
                        return float(gp) * 100
                except:
                    pass
                return 0.0

            # --- 🔥 关键排序逻辑变更 ---
            # 从 "按成交量(volume24hr)" 改为 "按获胜概率(get_win_probability)" 倒序
            # 这样 39% 的 Kevin Warsh 一定会排在 1% 的其他人前面
            sorted_markets = sorted(all_markets, key=get_win_probability, reverse=True)

            sub_outcomes = []

            # 只展示前 6 名概率最高的候选人/选项
            for m in sorted_markets[:8]:

                if m.get('closed') is True: continue

                # 获取名称 (Kevin Warsh, Jan 31 等)
                sub_label = m.get('groupItemTitle') or m.get('question')

                # 获取概率 (直接调用刚才写的函数)
                prob = get_win_probability(m)

                # 只有概率 > 0.1% 才显示，过滤掉那些 0% 的垃圾选项
                if prob >= 0.2:
                    sub_outcomes.append(f"   📊 {sub_label}: {prob:.1f}%")

            if sub_outcomes:
                outcomes_str = "\n".join(sub_outcomes)
                found_markets.append(
                    f"🔴 **预测话题**: {title}\n"
                    f"{outcomes_str}\n"
                    f"   💰总成交量: ${event.get('volume24hr', 0):,.0f}"
                )

    if not found_markets:
        return f"在 Polymarket 热门榜单中，暂时未发现关于 '{keywords}' 的活跃预测。"

    # 返回最相关的结果
    result_text = "\n\n".join(found_markets[:3])
    return f"Polymarket 预测市场最新数据：\n\n{result_text}"
import json
from typing import List

from langchain.tools import tool

from risk_index_service import fetch_polymarket_events, normalize_probability


# Polymarket API endpoint
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"


def fetch_top_markets(limit=20) -> List[dict]:
    """Fetch top active Polymarket events by 24h volume."""
    try:
        return fetch_polymarket_events(limit=limit, timeout=10)
    except Exception as e:
        print(f"Polymarket API Error: {e}")
        return []


@tool
def tool_get_polymarket_sentiment(keywords: str) -> str:
    """
    Polymarket prediction market search helper.
    Input English keywords such as "Iran", "Oil", or "Fed Chair".
    Returns the most relevant active prediction markets and their current odds.
    """
    print(f"\n[Polymarket] searching: {keywords}")

    if not keywords:
        return "请提供有效的搜索关键词。"

    keyword_list = [k.strip().lower() for k in keywords.replace(",", " ").split() if k.strip()]
    events = fetch_top_markets(limit=60)
    found_markets = []

    for event in events:
        title = str(event.get("title") or "")
        full_text = title.lower()
        if not any(k in full_text for k in keyword_list):
            continue

        all_markets = event.get("markets") or []
        if not all_markets:
            continue

        def get_win_probability(m):
            raw_prices = m.get("outcomePrices")
            if isinstance(raw_prices, str):
                try:
                    raw_prices = json.loads(raw_prices)
                except Exception:
                    raw_prices = []
            if isinstance(raw_prices, list) and raw_prices:
                return normalize_probability(raw_prices[0]) * 100
            return normalize_probability(m.get("groupPrice") or m.get("probability") or 0) * 100

        sorted_markets = sorted(all_markets, key=get_win_probability, reverse=True)
        sub_outcomes = []

        for m in sorted_markets[:8]:
            if m.get("closed") is True:
                continue
            sub_label = m.get("groupItemTitle") or m.get("question") or m.get("title")
            prob = get_win_probability(m)
            if prob >= 0.2:
                sub_outcomes.append(f"   - {sub_label}: {prob:.1f}%")

        if sub_outcomes:
            outcomes_str = "\n".join(sub_outcomes)
            found_markets.append(
                f"[预测话题] {title}\n"
                f"{outcomes_str}\n"
                f"   24h成交额: ${float(event.get('volume24hr', 0) or 0):,.0f}"
            )

    if not found_markets:
        return f"Polymarket 暂无关于 '{keywords}' 的热门预测。"

    result_text = "\n\n".join(found_markets[:3])
    return f"Polymarket 最新数据：\n\n{result_text}"

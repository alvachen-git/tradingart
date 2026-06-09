import json
from typing import List

try:
    from langchain.tools import tool
except Exception:  # pragma: no cover - used only in minimal test environments
    class _SimpleTool:
        def __init__(self, func):
            self.func = func
            self.__name__ = getattr(func, "__name__", "tool")

        def __call__(self, *args, **kwargs):
            return self.func(*args, **kwargs)

        def invoke(self, args):
            if isinstance(args, dict):
                return self.func(**args)
            return self.func(args)

    def tool(func=None, **_kwargs):
        if func is None:
            return lambda wrapped: _SimpleTool(wrapped)
        return _SimpleTool(func)

try:
    import requests
except Exception:  # pragma: no cover - used only in minimal test environments
    requests = None

from risk_index_service import fetch_polymarket_events, normalize_probability


GAMMA_API_URL = "https://gamma-api.polymarket.com/events"
POLYMARKET_TOOL_TIMEOUT_SECONDS = 5
POLYMARKET_TOOL_MAX_PAGES = 1

_REQUEST_EXCEPTIONS = (Exception,)
if requests is not None:
    _REQUEST_EXCEPTIONS = (
        requests.Timeout,
        requests.ConnectionError,
        requests.HTTPError,
        requests.RequestException,
    )

_POLYMARKET_TASK_FAILED = False
_POLYMARKET_TASK_ERROR = ""


def reset_polymarket_task_guard() -> None:
    """Reset the per-research-task Polymarket circuit breaker."""
    global _POLYMARKET_TASK_FAILED, _POLYMARKET_TASK_ERROR
    _POLYMARKET_TASK_FAILED = False
    _POLYMARKET_TASK_ERROR = ""


def _mark_polymarket_task_failed(exc: Exception) -> None:
    global _POLYMARKET_TASK_FAILED, _POLYMARKET_TASK_ERROR
    _POLYMARKET_TASK_FAILED = True
    _POLYMARKET_TASK_ERROR = f"{type(exc).__name__}: {exc}"


def _polymarket_skip_message() -> str:
    suffix = f"（原因：{_POLYMARKET_TASK_ERROR[:160]}）" if _POLYMARKET_TASK_ERROR else ""
    return f"本次任务 Polymarket 已跳过，避免重复等待外部接口超时{suffix}。"


def fetch_top_markets(limit=20) -> List[dict]:
    """Fetch top active Polymarket events by 24h volume."""
    if _POLYMARKET_TASK_FAILED:
        print(f"[polymarket-tool] status=skipped reason=task_guard error={_POLYMARKET_TASK_ERROR[:160]}")
        return []

    try:
        return fetch_polymarket_events(
            limit=limit,
            timeout=POLYMARKET_TOOL_TIMEOUT_SECONDS,
            max_pages_override=POLYMARKET_TOOL_MAX_PAGES,
        )
    except _REQUEST_EXCEPTIONS as exc:
        _mark_polymarket_task_failed(exc)
        print(f"[polymarket-tool] status=error type={type(exc).__name__} error={str(exc)[:200]}")
        return []
    except Exception as exc:
        _mark_polymarket_task_failed(exc)
        print(f"[polymarket-tool] status=error type={type(exc).__name__} error={str(exc)[:200]}")
        return []


def _get_polymarket_sentiment_text(keywords: str) -> str:
    print(f"\n[Polymarket] searching: {keywords}")

    if not keywords:
        return "请提供有效的搜索关键词。"

    if _POLYMARKET_TASK_FAILED:
        return _polymarket_skip_message()

    keyword_list = [k.strip().lower() for k in keywords.replace(",", " ").split() if k.strip()]
    events = fetch_top_markets(limit=60)
    if not events and _POLYMARKET_TASK_FAILED:
        return _polymarket_skip_message()

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

        for market in sorted_markets[:8]:
            if market.get("closed") is True:
                continue
            sub_label = market.get("groupItemTitle") or market.get("question") or market.get("title")
            prob = get_win_probability(market)
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


@tool
def tool_get_polymarket_sentiment(keywords: str) -> str:
    """
    Polymarket prediction market search helper.
    Input English keywords such as "Iran", "Oil", or "Fed Chair".
    Returns the most relevant active prediction markets and their current odds.
    """
    return _get_polymarket_sentiment_text(keywords)

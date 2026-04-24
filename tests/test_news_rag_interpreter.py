import sys
import types

import news_rag_interpreter as nri


def _install_fake_tool_module(monkeypatch, module_name, attr_name, fn):
    module = types.ModuleType(module_name)
    setattr(module, attr_name, fn)
    monkeypatch.setitem(sys.modules, module_name, module)


def test_interpret_market_news_builds_trader_payload(monkeypatch):
    def fake_ingest(query, analysis_horizon="swing", use_external_news=True):
        return {
            "timeline": [
                {
                    "event_id": "evt1",
                    "source": "新闻工具",
                    "title": "中东冲突升温推高黄金避险需求",
                    "content": "黄金和原油同步走强。",
                    "timestamp": "2026-04-22T10:00:00",
                }
            ],
            "market_context": {},
            "ingest_meta": {"query": query, "market_errors": [], "event_count": 1},
        }

    monkeypatch.setattr(nri, "_load_ingest", lambda query, horizon, use_external_news: fake_ingest(query))

    _install_fake_tool_module(
        monkeypatch,
        "knowledge_tools",
        "search_investment_knowledge",
        lambda query: "黄金通常受地缘避险、实际利率和美元共同影响。",
    )
    _install_fake_tool_module(
        monkeypatch,
        "market_tools",
        "get_market_snapshot",
        lambda query: f"{query} 最新价格上涨。",
    )
    sys.modules["market_tools"].get_recent_price_series = lambda query, days=5: f"{query} 近5日上涨。"
    _install_fake_tool_module(
        monkeypatch,
        "polymarket_tool",
        "tool_get_polymarket_sentiment",
        lambda keywords: "Polymarket 显示中东风险概率上升。",
    )
    fake_de = types.ModuleType("data_engine")
    fake_de.get_latest_geopolitical_risk_snapshot = lambda: {
        "score_raw": 66,
        "band": "elevated",
        "top_markets": [{"display_title": "Middle East escalation"}],
    }
    monkeypatch.setitem(sys.modules, "data_engine", fake_de)

    payload = nri.interpret_market_news("黄金为什么涨", max_events=3)

    assert payload["query"] == "黄金为什么涨"
    assert payload["market_bias"] == "偏多"
    assert payload["confidence"] > 0.5
    assert payload["bullish_points"]
    assert payload["what_to_watch"]
    assert "黄金" in payload["summary"]
    assert "接下来盯" in payload["trader_brief"]
    assert payload["source_coverage"]["news"] is True
    assert payload["source_coverage"]["market"] is True
    assert payload["source_coverage"]["knowledge"] is True
    assert payload["source_coverage"]["polymarket"] is True


def test_interpret_market_news_degrades_without_events(monkeypatch):
    monkeypatch.setattr(
        nri,
        "_load_ingest",
        lambda query, horizon, use_external_news: {
            "timeline": [],
            "market_context": {},
            "ingest_meta": {"query": query, "market_errors": ["series_error"], "event_count": 0},
        },
    )

    payload = nri.interpret_market_news("冷门品种消息怎么看", use_external_news=False)

    assert payload["degraded"] is True
    assert payload["market_bias"] == "中性"
    assert "没有抓到足够清晰的新闻事件" in payload["degrade_reasons"]
    assert "这里只做新闻和行情解读" in "\n".join(payload["risk_notes"])


def test_tool_returns_trader_brief(monkeypatch):
    monkeypatch.setattr(
        nri,
        "interpret_market_news",
        lambda **kwargs: {
            "trader_brief": "黄金偏强，主线是避险。接下来盯美元和美债。",
        },
    )

    result = nri.interpret_market_news_tool.invoke({"query": "黄金为什么涨"})

    assert "黄金偏强" in result
    assert "接下来盯" in result


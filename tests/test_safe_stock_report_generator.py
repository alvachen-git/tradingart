import sys
import types
from unittest.mock import patch

import pandas as pd

knowledge_stub = types.ModuleType("knowledge_tools")
knowledge_stub.search_investment_knowledge = lambda *args, **kwargs: ""
sys.modules.setdefault("knowledge_tools", knowledge_stub)

import safe_stock_report_generator as gen


SECTION_NAMES = [
    "\u5c0f\u7231\u9009\u80a1\u665a\u62a5",
    "\u8d44\u91d1\u56de\u6d41",
    "\u53ef\u4e70\u6807\u7684",
    "\u89c2\u5bdf\u6807\u7684",
    "\u5df2\u4e70\u8ddf\u8e2a",
]

OLD_SECTION_NAMES = [
    "\u5b89\u5168\u9009\u80a1\u6a21\u578b\u62a5\u544a",
    "\u5c0f\u7231\u6284\u5e95\u9009\u80a1\u665a\u62a5",
    "\u8fb9\u9645\u8d44\u91d1\u56de\u6d41\u677f\u5757",
    "\u6a21\u578b\u4e70\u5165\u8ddf\u8e2a",
    "\u89c2\u5bdf\u540d\u5355",
    "\u65e2\u6709\u63a8\u8350\u8ddf\u8e2a",
]


def _sample_data(gate="open", buy_slots=3):
    return {
        "trade_date": "20260511",
        "regime": {"gate": gate, "buy_slots": buy_slots, "summary": "CSI500 gate summary"},
        "sectors": [
            {"rank": 1, "industry": "AI", "sector_type": "concept", "score": 0.92, "improvement": 0.15, "positive_days": 4, "recent_pct_change": 2.5}
        ],
        "buys": [
            {"symbol": "600001.SH", "name": "Sample A", "sector_name": "AI", "sector_rank": 1, "entry_price": 10.0, "stop_price": 9.2, "score": 88.0, "pattern": "bottom breakout", "bottom_turn_score": 88, "stage_note": "stage", "reversal_signal_desc": "signal"}
        ],
        "watches": [
            {"symbol": "600002.SH", "name": "Sample B", "sector_name": "AI", "sector_rank": 1, "entry_price": 12.0, "stop_price": 11.2, "score": 82.0, "pattern": "watch", "bottom_turn_score": 62, "stage_note": "stage", "reversal_signal_desc": "signal"}
        ],
        "tracking": [
            {"symbol": "600003.SH", "name": "Sample C", "status": "bought", "action": "hold", "close": 15.0, "gain": 0.03, "bottom_low": 13.5, "reason": "hold"}
        ],
    }


def _buy_candidate(symbol, name="stock", score=82, sector_rank=1, close=10.0, pattern="signal"):
    return {
        "symbol": symbol,
        "name": name,
        "industry": "AI",
        "sector_rank": sector_rank,
        "score": score,
        "bottom_turn_score": score,
        "bottom_stage_score": 50,
        "reversal_signal_score": 35,
        "anti_chase_flag": 0,
        "anti_chase_reasons": "",
        "right_confirm": 1,
        "amount": 3000,
        "close": close,
        "stop_price": close * 0.9,
        "pattern": pattern,
        "reversal_signal_desc": pattern,
        "ret20": 0.03,
        "ret60": -0.10,
        "drawdown_120d_high": 0.30,
        "position_pct_120d": 0.30,
        "platform_low": close * 0.92,
        "platform_high": close * 1.05,
        "breakout_date": "20260511",
    }


def test_draft_safe_stock_report_contains_required_sections():
    data = _sample_data()
    html = gen.draft_safe_stock_report(data)
    errors = gen.validate_safe_stock_report(html, data)
    assert errors == []
    for section in SECTION_NAMES:
        assert section in html
    for section in OLD_SECTION_NAMES:
        assert section not in html
    for removed_header in ["<th class='nowrap'>风控位</th>", "<th class='nowrap'>底部转折分</th>", "<th class='nowrap'>底部下沿</th>"]:
        assert removed_header not in html
    assert "table-scroll" in html
    assert "信号/说明" in html
    assert "中证500开放买入" not in html
    assert "CSI500 gate summary" not in html
    assert "market-note" in html
    assert "中国资产" in html


def test_validate_blocks_buy_recommendations_when_gate_blocked():
    data = _sample_data(gate="blocked", buy_slots=0)
    html = gen.draft_safe_stock_report(data)
    errors = gen.validate_safe_stock_report(html, data)
    assert any("blocked gate" in err for err in errors)


def test_safe_stock_v2_thresholds_are_exported_by_simulation_service():
    for name in [
        "V2_BOTTOM_BUY_SCORE",
        "V2_BOTTOM_WATCH_SCORE",
        "V2_MIN_REVERSAL_SIGNAL_SCORE",
        "V2_SECTOR_BUY_RANK_LIMIT",
        "V2_SECTOR_WATCH_RANK_LIMIT",
    ]:
        assert hasattr(gen.sim, name)


def test_select_new_recommendations_respects_gate_slots_and_sector_rank():
    candidates = pd.DataFrame([_buy_candidate(f"60000{i}.SH", score=90 - i, sector_rank=1 if i < 4 else 4, close=10 + i) for i in range(6)])
    buys, watches = gen._select_new_recommendations(candidates, {"gate": "open", "buy_slots": 3}, pd.DataFrame())
    assert len(buys) == 3
    assert all(x["sector_rank"] <= 3 for x in buys)
    assert len(watches) <= gen.WATCH_LIMIT
    blocked_buys, blocked_watches = gen._select_new_recommendations(candidates, {"gate": "blocked", "buy_slots": 0}, pd.DataFrame())
    assert blocked_buys == []
    assert len(blocked_watches) > 0


def test_select_new_recommendations_excludes_chase_from_buy_but_keeps_watch():
    chase = _buy_candidate("600001.SH", name="chase", score=88)
    chase.update({"anti_chase_flag": 1, "anti_chase_reasons": "ret20 high", "ret20": 0.25, "ret60": 0.50, "drawdown_120d_high": 0.02, "position_pct_120d": 0.95, "pattern": "new high"})
    bottom = _buy_candidate("600002.SH", name="bottom", score=82)
    left = _buy_candidate("600003.SH", name="left", score=58, sector_rank=2)
    left.update({"reversal_signal_score": 0, "right_confirm": 0, "pattern": ""})
    buys, watches = gen._select_new_recommendations(pd.DataFrame([chase, bottom, left]), {"gate": "open", "buy_slots": 3}, pd.DataFrame())
    assert [x["symbol"] for x in buys] == ["600002.SH"]
    watch_symbols = {x["symbol"] for x in watches}
    assert "600001.SH" in watch_symbols
    assert "600003.SH" in watch_symbols


def test_existing_recommendation_requalifies_as_one_time_add_not_new_buy():
    active = pd.DataFrame([{"symbol": "600001.SH", "name": "held", "status": "bought", "entry_price": 10.0, "stop_price": 9.0, "score": 80, "sector_rank": 1, "take_profit_count": 0, "add_count": 0, "weak_count": 0, "bottom_low": 9.0, "bottom_high": 11.0, "bottom_range_date": "20260510"}])
    candidates = pd.DataFrame([_buy_candidate("600001.SH", name="held", score=86, close=10.5), _buy_candidate("600002.SH", name="new", score=82, close=8.0)])
    with patch("safe_stock_report_generator.sim._fetch_price_snapshot", return_value={"600001.SH": {"close": 10.5}}):
        tracking = gen._tracking_actions(active, candidates, "20260511")
    assert tracking[0]["action"] == "add"
    assert tracking[0]["next_add_count"] == 1
    buys, _ = gen._select_new_recommendations(candidates, {"gate": "open", "buy_slots": 3}, active)
    assert [x["symbol"] for x in buys] == ["600002.SH"]
    active_once_added = active.copy()
    active_once_added["add_count"] = 1
    with patch("safe_stock_report_generator.sim._fetch_price_snapshot", return_value={"600001.SH": {"close": 10.5}}):
        tracking_after_add = gen._tracking_actions(active_once_added, candidates, "20260511")
    assert tracking_after_add[0]["action"] == "hold"


def test_prior_buy_recommendation_is_not_repeated_even_if_status_missing():
    active = pd.DataFrame(
        [
            {
                "symbol": "300315.SZ",
                "name": "held",
                "recommendation_type": "buy",
                "status": "",
                "entry_price": 5.0,
                "stop_price": 4.6,
                "score": 80,
                "sector_rank": 1,
                "take_profit_count": 0,
                "add_count": 0,
                "weak_count": 0,
                "bottom_low": 4.6,
                "bottom_high": 5.4,
                "bottom_range_date": "20260506",
            }
        ]
    )
    candidates = pd.DataFrame([_buy_candidate("300315.SZ", name="held", score=100, close=5.18)])

    with patch("safe_stock_report_generator.sim._fetch_price_snapshot", return_value={"300315.SZ": {"close": 5.18}}):
        tracking = gen._tracking_actions(active, candidates, "20260508")
    buys, _ = gen._select_new_recommendations(candidates, {"gate": "open", "buy_slots": 3}, active)

    assert tracking[0]["action"] == "add"
    assert buys == []


def test_watching_symbol_can_promote_to_new_buy():
    active = pd.DataFrame([{"symbol": "600001.SH", "name": "watch", "status": "watching", "entry_price": 10.0, "stop_price": 9.0, "score": 70, "sector_rank": 1, "take_profit_count": 0, "add_count": 0}])
    candidates = pd.DataFrame([_buy_candidate("600001.SH", name="watch", score=84, close=10.2)])
    tracking = gen._tracking_actions(active, candidates, "20260511")
    buys, _ = gen._select_new_recommendations(candidates, {"gate": "open", "buy_slots": 3}, active)
    assert tracking == []
    assert [x["symbol"] for x in buys] == ["600001.SH"]


def test_noise_bearish_does_not_exit_without_bottom_break():
    active = pd.DataFrame([{"symbol": "601098.SH", "name": "held", "status": "bought", "entry_price": 11.2, "stop_price": 10.6, "score": 80, "sector_rank": 1, "take_profit_count": 0, "add_count": 0, "weak_count": 0, "bottom_low": 10.5, "bottom_high": 12.0, "bottom_range_date": "20260506"}])
    cand = _buy_candidate("601098.SH", name="held", score=50, close=11.44, pattern="\u5047\u7a81\u7834(\u8bf1\u591a)")
    cand.update({"reversal_signal_score": 0, "ma10": 11.0, "ma20": 10.9, "ma60": 11.8})
    with patch("safe_stock_report_generator.sim._fetch_price_snapshot", return_value={"601098.SH": {"close": 11.44}}):
        tracking = gen._tracking_actions(active, pd.DataFrame([cand]), "20260507")
    assert tracking[0]["action"] == "hold"


def test_bottom_range_break_exits_all():
    active = pd.DataFrame([{"symbol": "601098.SH", "name": "held", "status": "bought", "entry_price": 11.2, "stop_price": 10.0, "score": 80, "sector_rank": 1, "take_profit_count": 0, "add_count": 0, "weak_count": 0, "bottom_low": 10.5, "bottom_high": 12.0, "bottom_range_date": "20260506"}])
    with patch("safe_stock_report_generator.sim._fetch_price_snapshot", return_value={"601098.SH": {"close": 10.35}}):
        tracking = gen._tracking_actions(active, pd.DataFrame([{"symbol": "601098.SH", "pattern": "", "ma10": 10.8, "ma20": 10.7}]), "20260507")
    assert tracking[0]["action"] == "exit"
    assert tracking[0]["next_status"] == "exited"


def test_forced_weak_exits_use_incoming_buys_to_keep_tracking_under_limit():
    active = pd.DataFrame([{"symbol": f"60000{i}.SH", "name": f"held{i}", "status": "bought", "entry_price": 10.0, "stop_price": 9.0, "score": 10 if i == 0 else 90, "sector_rank": 5 if i == 0 else 1, "take_profit_count": 0, "add_count": 0, "weak_count": 0} for i in range(gen.MAX_BUY_TRACKING)])
    prices = {f"60000{i}.SH": {"close": 10.0} for i in range(gen.MAX_BUY_TRACKING)}
    with patch("safe_stock_report_generator.sim._fetch_price_snapshot", return_value=prices):
        exits = gen._forced_weak_exits(active, "20260511", incoming_buy_count=1, excluded_symbols=set())
    assert len(exits) == 1
    assert exits[0]["symbol"] == "600000.SH"
    assert exits[0]["action"] == "exit"


@patch("safe_stock_report_generator.persist_report_state")
@patch("safe_stock_report_generator.ensure_safe_stock_tables")
@patch("safe_stock_report_generator.sub_svc.publish_content")
def test_publish_safe_stock_report_uses_safe_stock_channel(mock_publish, mock_ensure, mock_persist):
    mock_publish.return_value = (True, 123)
    data = _sample_data()
    html = gen.draft_safe_stock_report(data)
    success, result = gen.publish_safe_stock_report(html, data)
    assert success is True
    assert result == 123
    assert mock_publish.call_args.kwargs["channel_code"] == gen.CHANNEL_CODE
    mock_persist.assert_called_once()

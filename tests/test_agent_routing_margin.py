import agent_core
import unittest


def test_margin_query_prioritizes_monitor_only():
    plan = ["analyst", "strategist"]
    out = agent_core._enforce_margin_monitor_routing("螺纹钢一手保证金是多少？", plan)
    assert out[0] == "monitor"
    assert "strategist" not in out
    assert "analyst" in out


def test_margin_and_strategy_query_forces_monitor_then_strategist():
    plan = ["analyst"]
    out = agent_core._enforce_margin_monitor_routing("白银保证金多少，顺便给我策略建议", plan)
    assert out[:2] == ["monitor", "strategist"]
    assert "analyst" in out


def test_non_margin_query_keeps_plan_unchanged():
    plan = ["researcher", "macro_analyst"]
    out = agent_core._enforce_margin_monitor_routing("美联储什么时候降息", plan)
    assert out == plan


def test_pure_option_data_query_forces_monitor_only():
    plan = ["researcher", "macro_analyst", "analyst", "strategist"]
    out = agent_core._enforce_option_data_monitor_routing("300ETF期权波动率高吗", plan)
    assert out == ["monitor"]


def test_option_data_plus_strategy_query_does_not_force_monitor_only():
    plan = ["researcher", "macro_analyst", "analyst", "strategist"]
    out = agent_core._enforce_option_data_monitor_routing("300ETF期权波动率高吗，适合卖方吗", plan)
    assert out == plan


def test_fundamental_and_technical_query_forces_analyst_and_researcher():
    out = agent_core._enforce_research_analyst_routing(
        "中天科技的基本面和技术面分析下",
        ["analyst"],
    )
    assert out[:2] == ["analyst", "researcher"]


def test_named_stock_analysis_removes_accidental_screener():
    plan = agent_core._enforce_research_analyst_routing(
        "澜起科技的基本面和技术面分析下",
        ["analyst", "researcher", "screener"],
    )
    out = agent_core._enforce_named_stock_analysis_screener_isolation(
        "澜起科技的基本面和技术面分析下",
        plan,
    )
    assert out == ["analyst", "researcher"]


def test_stock_selection_intent_keeps_screener():
    out = agent_core._enforce_named_stock_analysis_screener_isolation(
        "帮我选几只AI概念股",
        ["screener"],
    )
    assert out == ["screener"]


def test_stock_selection_query_forces_screener():
    out = agent_core._enforce_stock_selection_routing(
        "帮我找放量突破的股票",
        ["analyst", "researcher"],
    )
    assert out == ["screener"]


def test_stock_selection_post_processing_keeps_screener_after_isolation():
    query = "帮我找放量突破的股票"
    plan = []
    plan = agent_core._enforce_research_analyst_routing(query, plan)
    plan = agent_core._enforce_stock_selection_routing(query, plan)
    plan = agent_core._enforce_named_stock_analysis_screener_isolation(query, plan)
    assert plan == ["screener"]


def test_analysis_task_policy_stock_selection_overrides_empty_plan():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "帮我找放量突破的股票",
        [],
        "****",
    )
    assert plan == ["screener"]
    assert symbol == ""


def test_analysis_task_policy_single_stock_removes_screener_only():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "澜起科技的基本面和技术面分析下",
        ["analyst", "researcher", "screener"],
        "688008",
    )
    assert plan == ["analyst", "researcher"]
    assert symbol == "688008"


def test_analysis_task_policy_unspecified_option_strategy_clarifies():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "趋势突破有效，期权到期还比较长，我能不能买深虚期权？",
        ["analyst", "strategist"],
        "510050",
    )
    assert plan == ["chatter"]
    assert symbol == ""


def test_analysis_task_policy_option_with_subject_fills_empty_plan():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "500ETF趋势突破有效，到期还比较长，我能不能买深虚期权？",
        [],
        "510500",
    )
    assert plan == ["analyst", "strategist"]
    assert symbol == "510500"


def test_recent_company_news_routes_to_researcher_only():
    out = agent_core._enforce_research_analyst_routing(
        "中天科技最近有什么消息",
        ["analyst"],
    )
    assert out == ["researcher"]


def test_pure_technical_query_routes_to_analyst_only():
    out = agent_core._enforce_research_analyst_routing(
        "中天科技技术面分析下",
        ["researcher"],
    )
    assert out == ["analyst"]


def test_sanitize_unauthorized_technical_indicators_by_default():
    out = agent_core._sanitize_unauthorized_technical_indicators(
        "K线处于反弹。\nRSI进入强势区。\n均线开始走平。",
        query="中天科技技术面分析下",
    )
    assert "RSI" not in out
    assert "K线处于反弹" in out
    assert "均线开始走平" in out


def test_sanitize_declines_indicator_when_user_explicitly_asks():
    out = agent_core._sanitize_unauthorized_technical_indicators(
        "RSI进入强势区。",
        query="帮我看RSI",
    )
    assert "RSI" in out
    assert "暂不展开" in out
    assert "进入强势区" not in out


class UnspecifiedOptionStrategyRoutingTest(unittest.TestCase):
    def test_unspecified_option_strategy_routes_to_chatter_without_symbol(self):
        plan, symbol = agent_core._enforce_unspecified_option_strategy_routing(
            "趋势突破有效，期权到期还比较长，我能不能买深虚期权？",
            ["analyst", "strategist"],
            "510050",
        )
        self.assertEqual(plan, ["chatter"])
        self.assertEqual(symbol, "")

    def test_explicit_option_underlying_keeps_strategy_route(self):
        plan, symbol = agent_core._enforce_unspecified_option_strategy_routing(
            "500ETF趋势突破有效，到期还比较长，我能不能买深虚期权？",
            ["analyst", "strategist"],
            "510500",
        )
        self.assertEqual(plan, ["analyst", "strategist"])
        self.assertEqual(symbol, "510500")

    def test_generic_option_concept_routes_to_chatter_not_default_underlying(self):
        plan, symbol = agent_core._enforce_unspecified_option_strategy_routing(
            "不涉及标的，单纯想象题：突破行情里买方策略一般怎么考虑？",
            ["analyst", "strategist"],
            "510050",
        )
        self.assertEqual(plan, ["chatter"])
        self.assertEqual(symbol, "")

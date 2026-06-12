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


def test_macro_policy_impact_query_drops_researcher_for_static_transmission():
    out = agent_core._enforce_macro_policy_impact_routing(
        "美联储加息对黄金白银有什么影响?",
        ["researcher", "macro_analyst"],
    )
    assert out == ["macro_analyst"]


def test_latest_macro_policy_impact_query_keeps_researcher():
    out = agent_core._enforce_macro_policy_impact_routing(
        "最新美联储加息消息对黄金白银有什么影响?",
        ["researcher", "macro_analyst"],
    )
    assert out == ["researcher", "macro_analyst"]


def test_latest_macro_policy_asset_impact_query_is_fast_path_family():
    assert agent_core._is_macro_policy_asset_impact_query("最新美联储加息消息对黄金白银有什么影响?")


def test_macro_policy_impact_fast_path_skips_react_agent(monkeypatch):
    class FakeTool:
        def __init__(self, response):
            self.response = response
            self.calls = []

        def invoke(self, params):
            self.calls.append(params)
            return self.response

    health = FakeTool("宏观健康快照\n| US10Y | 4.56% | fresh |\n| DXY | 120.08 | fresh |")
    curve = FakeTool("收益率曲线分析\n- 10Y-2Y利差: +0.41% ✅ 正常")
    anchors = FakeTool("US10Y 趋势: 上行\nDXY 趋势: 上行\nDFII10 最新值: 2.19%")

    def fail_create_agent(*_args, **_kwargs):
        raise AssertionError("macro fast path should not create ReAct agent")

    monkeypatch.setattr(agent_core, "get_macro_health_snapshot", health)
    monkeypatch.setattr(agent_core, "analyze_yield_curve", curve)
    monkeypatch.setattr(agent_core, "get_macro_indicator", anchors)
    monkeypatch.setattr(agent_core, "create_react_agent", fail_create_agent)

    out = agent_core.macro_analyst_node(
        {
            "user_query": "美联储加息对黄金白银有什么影响?",
            "symbol": "",
            "symbol_name": "",
            "news_summary": "暂无最新宏观新闻",
        },
        llm=object(),
    )

    content = out["messages"][0].content
    assert "宏观快答" in content
    assert "紧缩交易" in content
    assert "黄金" in content
    assert "白银" in content
    assert out["macro_chart"] == ""
    assert health.calls == [{"indicator_code": "FEDFUNDS,SOFR,US10Y,US2Y,DXY,DFII10"}]
    assert anchors.calls == [{"indicator_code": "US10Y,US2Y,DXY,DFII10", "days": 30}]


def test_pure_option_data_query_forces_monitor_only():
    plan = ["researcher", "macro_analyst", "analyst", "strategist"]
    out = agent_core._enforce_option_data_monitor_routing("300ETF期权波动率高吗", plan)
    assert out == ["monitor"]


def test_option_data_plus_strategy_query_does_not_force_monitor_only():
    plan = ["researcher", "macro_analyst", "analyst", "strategist"]
    out = agent_core._enforce_option_data_monitor_routing("300ETF期权波动率高吗，适合卖方吗", plan)
    assert out == plan


def test_option_scenario_projection_is_not_forced_to_monitor_only():
    plan = ["monitor"]
    out = agent_core._enforce_option_data_monitor_routing(
        "如果创业板ETF周一-10%开盘，IV会到多少，平值认沽涨多少",
        plan,
    )
    assert out == plan


def test_option_scenario_projection_recovers_strategy_route_from_monitor_only():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "如果创业板ETF周一-10%开盘，IV会到多少，平值认沽涨多少",
        ["monitor"],
        "",
    )
    assert plan == ["analyst", "strategist"]
    assert symbol == ""


def test_volatility_direction_view_forces_monitor_only():
    out = agent_core._enforce_volatility_market_view_routing(
        "中证500现在上涨是会升波还是降波呢",
        ["chatter", "analyst"],
    )
    assert out == ["monitor"]


def test_volatility_direction_policy_recovers_from_chatter_plan():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "中证500现在上涨是会升波还是降波呢",
        ["chatter", "analyst"],
        "510500.SH",
    )
    assert plan == ["monitor"]
    assert symbol == "510500.SH"


def test_volatility_direction_strategy_question_adds_strategist_without_analyst():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "中证500现在上涨是会升波还是降波，期权策略怎么做",
        ["chatter", "analyst"],
        "510500.SH",
    )
    assert plan == ["monitor", "strategist"]
    assert symbol == "510500.SH"


def test_volatility_direction_monitor_only_can_bypass_finalizer():
    assert agent_core._can_bypass_finalizer(
        {
            "plan": ["monitor"],
            "execution_batches": [["monitor"]],
            "agent_reports": {"monitor": "【数据监控】\n结论：当前更偏降波。"},
        }
    )


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


def test_analysis_task_policy_us_stock_selection_forces_screener():
    for original_plan in (["chatter"], ["generalist"]):
        plan, symbol = agent_core._apply_analysis_task_policy(
            "推荐一些美股，最好是从底部起来刚突破的",
            original_plan,
            "NVDA.US",
        )
        assert plan == ["screener"]
        assert symbol == ""


def test_screener_node_us_stock_selection_uses_us_tool_then_llm_reasoning(monkeypatch):
    seen = {}

    class FakeLLM:
        def invoke(self, prompt, *_args, **_kwargs):
            seen["prompt"] = prompt

            class Response:
                content = (
                    "【精选股票】\n"
                    "结论：AAA.US 更贴近底部刚突破，EXT.US 技术面强但已不早。\n"
                    "- 数据日期：2026-05-22，美股日线 EOD。"
                )

            return Response()

    class FakeUsScreener:
        @staticmethod
        def invoke(params):
            seen["params"] = params
            return (
                "结论：美股技术候选如下，需按分层解读\n"
                "- 数据日期：2026-05-22\n\n"
                "| 分层 | 代码 | 当前状态 |\n"
                "|:---|:---|:---|\n"
                "| 底部刚突破优先观察 | AAA.US | 突破前20日高点 |\n"
                "| 强势延续但不算底部刚启动 | EXT.US | 突破前20日高点 |"
            )

    monkeypatch.setattr(agent_core, "search_us_stocks_by_technical_setup", FakeUsScreener)

    out = agent_core.screener_node(
        {"user_query": "推荐一些美股，最好是从底部起来刚突破的", "symbol": ""},
        FakeLLM(),
    )
    content = out["messages"][0].content
    assert "【精选股票】" in content
    assert "AAA.US" in content
    assert "EXT.US 技术面强但已不早" in content
    assert seen["params"] == {"setup": "bottom_breakout", "limit": 10}
    assert "只能使用" in seen["prompt"]
    assert "底部刚突破优先观察" in seen["prompt"]


def test_screener_node_us_stock_short_selection_uses_bearish_setup(monkeypatch):
    seen = {}

    class FakeLLM:
        def invoke(self, prompt, *_args, **_kwargs):
            seen["prompt"] = prompt

            class Response:
                content = (
                    "【精选股票】\n"
                    "结论：BRK.US 和 WEA.US 更贴近做空观察候选。\n"
                    "- 数据日期：2026-05-22，美股日线 EOD。"
                )

            return Response()

    class FakeUsScreener:
        @staticmethod
        def invoke(params):
            seen["params"] = params
            return (
                "结论：美股看跌/做空观察候选如下，需按分层解读\n"
                "- 数据日期：2026-05-22\n\n"
                "| 分层 | 代码 | 当前状态 |\n"
                "|:---|:---|:---|\n"
                "| 破位做空优先观察 | BRK.US | 跌破前20日低点 |\n"
                "| 弱势延续观察 | WEA.US | 低于60日线 |"
            )

    monkeypatch.setattr(agent_core, "search_us_stocks_by_technical_setup", FakeUsScreener)

    out = agent_core.screener_node(
        {"user_query": "帮我找适合做空的美股，给我3只名称", "symbol": ""},
        FakeLLM(),
    )
    content = out["messages"][0].content
    assert "【精选股票】" in content
    assert "BRK.US" in content
    assert seen["params"] == {"setup": "bearish_breakdown", "limit": 3}
    assert "看跌/做空观察候选" in seen["prompt"]
    assert "底部刚突破优先观察" not in seen["prompt"]


def test_screener_node_us_stock_selection_falls_back_when_llm_fails(monkeypatch):
    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise RuntimeError("boom")

    class FakeUsScreener:
        @staticmethod
        def invoke(_params):
            return (
                "结论：美股技术候选如下，需按分层解读\n"
                "- 数据日期：2026-05-22\n\n"
                "| 分层 | 代码 | 当前状态 |\n"
                "|:---|:---|:---|\n"
                "| 底部刚突破优先观察 | AAA.US | 突破前20日高点 |"
            )

    monkeypatch.setattr(agent_core, "search_us_stocks_by_technical_setup", FakeUsScreener)

    out = agent_core.screener_node(
        {"user_query": "推荐一些美股，最好是从底部起来刚突破的", "symbol": ""},
        FailingLLM(),
    )
    content = out["messages"][0].content
    assert "【精选股票】" in content
    assert "AAA.US" in content
    assert "结论：美股技术候选如下" in content


def test_analysis_task_policy_single_stock_removes_screener_only():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "澜起科技的基本面和技术面分析下",
        ["analyst", "researcher", "screener"],
        "688008",
    )
    assert plan == ["analyst", "researcher"]
    assert symbol == "688008"


def test_analysis_task_policy_generic_single_stock_analysis_defaults_to_analyst():
    plan, symbol = agent_core._apply_analysis_task_policy(
        "\u5206\u6790\u4e00\u4e0b688223",
        [],
        "688223",
    )
    assert plan == ["analyst"]
    assert symbol == "688223"


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


def test_analysis_task_policy_futures_broker_signal_forces_monitor():
    for query, original_plan in [
        ("螺纹钢现在从期货商正反指标看偏多还是偏空？", ["chatter"]),
        ("螺纹刚现在从期货商正反指标看偏多还是偏空？", ["generalist"]),
        ("中信建投的持仓如果持续加多是不是利多？", ["portfolio_analyst"]),
        ("反指标最近在哪些商品上做多", ["generalist"]),
    ]:
        plan, symbol = agent_core._apply_analysis_task_policy(query, original_plan, "601066")
        assert plan == ["monitor"]
        assert symbol == ""


def test_monitor_node_futures_broker_single_broker_uses_profile_without_llm(monkeypatch):
    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise AssertionError("monitor should not invoke llm for broker profile")

    monkeypatch.setattr(agent_core, "get_latest_data_date", lambda: "20260520")
    monkeypatch.setattr(
        agent_core,
        "_build_futures_broker_indicator_profile",
        lambda broker: f"结论：{broker}属于反指标期货商。\n- 做多/加多解读：按反指标口径是一种利空。",
    )

    out = agent_core.monitor_node(
        {"user_query": "中信建投的持仓如果持续加多是不是利多？", "symbol": ""},
        FailingLLM(),
    )
    content = out["messages"][0].content
    assert "【数据监控】" in content
    assert "中信建投属于反指标期货商" in content
    assert "利空" in content


def test_monitor_node_futures_broker_product_uses_position_signal_without_llm(monkeypatch):
    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise AssertionError("monitor should not invoke llm for broker signal")

    monkeypatch.setattr(agent_core, "get_latest_data_date", lambda: "20260520")
    monkeypatch.setattr(
        agent_core,
        "_build_futures_broker_position_signal",
        lambda product, **_kwargs: f"结论：偏空\n- 品种：{product}\n- 最近5日趋势：0513->0520 偏空。",
    )

    out = agent_core.monitor_node(
        {"user_query": "鸡蛋现在从期货商正反指标看偏多还是偏空？", "symbol": ""},
        FailingLLM(),
    )
    content = out["messages"][0].content
    assert "【数据监控】" in content
    assert "品种：鸡蛋" in content
    assert "最近5日趋势" in content


def test_monitor_node_futures_broker_group_uses_group_tool_without_llm(monkeypatch):
    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise AssertionError("monitor should not invoke llm for broker group moves")

    monkeypatch.setattr(agent_core, "get_latest_data_date", lambda: "20260522")
    monkeypatch.setattr(
        agent_core,
        "_build_futures_broker_group_position_moves",
        lambda **kwargs: (
            "结论：反指标最近主要在以下品种多单增加\n"
            "- 指标组：反指标（中信建投, 东方财富, 方正中期）\n"
            "- 方向解读：反指标做多按当前口径是反向利空观察，不能直接当利多。\n"
            "| 品种 | 多单变化 |\n| 纸浆 | +1,000 |"
        ),
    )

    out = agent_core.monitor_node(
        {"user_query": "反指标最近在哪些商品上做多", "symbol": ""},
        FailingLLM(),
    )
    content = out["messages"][0].content
    assert "【数据监控】" in content
    assert "反指标最近主要" in content
    assert "中信建投" in content
    assert "国泰君安" not in content
    assert "反向利空" in content


def test_finalizer_keeps_futures_broker_monitor_output_without_audit_rewrite():
    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise AssertionError("finalizer should not audit rewrite broker signal")

    source = "【数据监控】\n结论：中信建投属于反指标期货商。\n- 做多/加多解读：按反指标口径是一种利空。"
    out = agent_core.finalizer_node(
        {
            "messages": [],
            "agent_reports": {"monitor": source},
            "user_query": "中信建投的持仓如果持续加多是不是利多？",
        },
        FailingLLM(),
    )

    content = out["messages"][0].content
    assert "【数据监控】" in content
    assert "反指标期货商" in content
    assert "利空" in content
    assert "风控修正" not in content


def test_finalizer_returns_single_researcher_report_without_audit_rewrite():
    class FailingLLM:
        def invoke(self, *_args, **_kwargs):
            raise AssertionError("finalizer should not audit rewrite single researcher report")

    source = "【情报与舆情】\n中兴通讯近期消息：算力集采为媒体报道，专利诉讼金额待公告确认。"
    out = agent_core.finalizer_node(
        {
            "messages": [],
            "agent_reports": {"researcher": source},
            "user_query": "中兴通讯最近有什么利好吗",
        },
        FailingLLM(),
    )

    content = out["messages"][0].content
    assert "【情报与舆情】" in content
    assert "中兴通讯近期消息" in content
    assert "风控修正" not in content


def test_recent_company_news_researcher_uses_fast_path(monkeypatch):
    class FakeLLM:
        def invoke(self, prompt):
            assert "中兴通讯" in prompt

            class Response:
                content = "一句话：有待核验的潜在利好，但不算完全确认。"

            return Response()

    def fail_create_agent(*_args, **_kwargs):
        raise AssertionError("recent company news should not enter ReAct researcher")

    monkeypatch.setattr(agent_core, "create_react_agent", fail_create_agent)
    monkeypatch.setattr(
        agent_core,
        "_invoke_search_web_for_researcher",
        lambda query: "中兴通讯公告与媒体报道：算力服务器集采进展、专利诉讼进展待确认。",
    )

    out = agent_core.researcher_node(
        {"user_query": "中兴通讯最近有什么利好吗", "symbol": "中兴通讯", "symbol_name": ""},
        FakeLLM(),
    )

    content = out["messages"][0].content
    assert "【情报与舆情】" in content
    assert "潜在利好" in content


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


def test_market_data_query_forces_monitor_only():
    plan = ["researcher", "analyst", "strategist"]
    out = agent_core._enforce_option_data_monitor_routing("查看甲醇2609的iv波动率", plan)
    assert out == ["monitor"]


def test_market_price_query_forces_monitor_only():
    plan = ["chatter", "generalist"]
    out = agent_core._enforce_option_data_monitor_routing("甲醇2609价格多少", plan)
    assert out == ["monitor"]


def test_market_data_query_with_strategy_keeps_original_plan():
    plan = ["researcher", "macro_analyst", "analyst", "strategist"]
    out = agent_core._enforce_option_data_monitor_routing("甲醇2609波动率高吗，适合卖方吗", plan)
    assert out == plan

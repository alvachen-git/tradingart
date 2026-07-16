from agent_prompt_policy import TASK_TYPE_STOCK_SELECTION, classify_analysis_task_type, is_option_strategy_question
from chat_routing import is_market_data_query, is_stock_selection_query, is_us_option_market_profile_query


QUERY = "美股里，帮我筛选，前期跌幅大、最近技术面出现止跌转折、且期权波动率还偏高的"
US_POOL_QUERY = (
    "从美股池里找过去60个交易日最大回撤至少25%，RSI14不高于40，"
    "今天成交量达到此前30日均量2倍，同时假跌破或多头吞噬命中一个即可，只看前5只"
)
US_POOL_NUMERIC_QUERY = (
    "从美股池里找过去60个交易日最大回撤至少25%，RSI14不高于40，"
    "今天成交量达到此前30日均量2倍，只看前5只"
)


def test_multifactor_us_stock_query_routes_to_screener_not_monitor():
    policy = classify_analysis_task_type(QUERY)

    assert policy.task_type == TASK_TYPE_STOCK_SELECTION
    assert policy.recommended_plan == ("screener",)
    assert policy.clear_symbol
    assert not is_option_strategy_question(QUERY)
    assert not is_market_data_query(QUERY)
    assert not is_us_option_market_profile_query(QUERY)


def test_single_symbol_iv_query_remains_market_data():
    assert is_market_data_query("SPY期权IV Rank现在多少")
    assert is_us_option_market_profile_query("SPY期权IV Rank现在多少")


def test_single_symbol_option_strategy_remains_strategy():
    policy = classify_analysis_task_type("SPY现在适合卖put吗")
    assert policy.task_type != TASK_TYPE_STOCK_SELECTION
    assert "strategist" in policy.recommended_plan


def test_supervisor_policy_clears_sentence_symbol_and_forces_screener():
    import agent_core

    plan, symbol = agent_core._apply_analysis_task_policy(QUERY, ["monitor"], QUERY)
    assert plan == ["screener"]
    assert symbol == ""


def test_us_pool_natural_wording_is_recognized_as_us_multifactor_screen():
    import agent_core
    from us_stock_multifactor_screener import is_us_multifactor_screen_query

    assert agent_core._is_us_stock_selection_query(US_POOL_QUERY)
    assert is_us_multifactor_screen_query(US_POOL_QUERY)
    policy = classify_analysis_task_type(US_POOL_QUERY)
    assert policy.task_type == TASK_TYPE_STOCK_SELECTION
    assert policy.recommended_plan == ("screener",)


def test_numeric_us_pool_query_is_forced_to_screener_before_market_data_routing():
    import agent_core
    from us_stock_multifactor_screener import is_us_multifactor_screen_query

    policy = classify_analysis_task_type(US_POOL_NUMERIC_QUERY)

    assert policy.task_type == TASK_TYPE_STOCK_SELECTION
    assert policy.recommended_plan == ("screener",)
    assert policy.clear_symbol
    assert is_stock_selection_query(US_POOL_NUMERIC_QUERY)
    assert is_us_multifactor_screen_query(US_POOL_NUMERIC_QUERY)
    assert not is_market_data_query(US_POOL_NUMERIC_QUERY)
    assert agent_core._apply_analysis_task_policy(
        US_POOL_NUMERIC_QUERY,
        ["monitor"],
        US_POOL_NUMERIC_QUERY,
    ) == (["screener"], "")


def test_screener_node_never_sends_explicit_us_pool_query_to_a_share_tool(monkeypatch):
    import agent_core

    seen = {}

    class FakeUsMultifactorTool:
        @staticmethod
        def invoke(payload):
            seen["payload"] = payload
            return "【美股多维筛选】\n1. **TEST.US** · 现价 10.00"

    class ForbiddenAShareTool:
        @staticmethod
        def invoke(_payload):
            raise AssertionError("明确的美股池请求不允许调用A股选股工具")

    class NoopLLM:
        def invoke(self, *_args, **_kwargs):
            raise AssertionError("规则模式下不应调用LLM")

    monkeypatch.setenv("US_STOCK_SCREEN_LLM_MODE", "off")
    monkeypatch.setattr(agent_core, "screen_us_stocks", FakeUsMultifactorTool)
    monkeypatch.setattr(agent_core, "search_top_stocks", ForbiddenAShareTool)

    result = agent_core.screener_node({"user_query": US_POOL_QUERY, "symbol": "整句误识别"}, NoopLLM())

    assert "【美股多维筛选】" in result["messages"][0].content
    assert "TEST.US" in result["messages"][0].content
    assert result["symbol"] == ""
    assert seen["payload"] == {"query": US_POOL_QUERY, "limit": 5}


def test_screener_node_uses_injected_compiler_and_rule_fallback_on_no_tool_call(monkeypatch):
    import agent_core
    from us_stock_multifactor_screener import ScreenPlanCompileResult

    seen = {}
    compiler_llm = object()

    def fake_compile(query, llm, *, limit):
        seen["compile"] = {"query": query, "llm": llm, "limit": limit}
        return ScreenPlanCompileResult(
            status="no_tool_call",
            model="fake-screen-compiler",
            error="模型未返回ScreenPlan工具调用",
            has_tool_call=False,
        )

    class FakeUsMultifactorTool:
        @staticmethod
        def invoke(payload):
            seen["payload"] = payload
            return "【美股多维筛选】\n规则降级成功"

    monkeypatch.setenv("US_STOCK_SCREEN_LLM_MODE", "on")
    monkeypatch.setattr(agent_core, "compile_screen_plan_with_llm", fake_compile)
    monkeypatch.setattr(agent_core, "screen_us_stocks", FakeUsMultifactorTool)

    result = agent_core.screener_node(
        {"user_query": US_POOL_NUMERIC_QUERY, "symbol": ""},
        object(),
        compiler_llm=compiler_llm,
    )

    assert seen["compile"] == {
        "query": US_POOL_NUMERIC_QUERY,
        "llm": compiler_llm,
        "limit": 5,
    }
    assert seen["payload"] == {"query": US_POOL_NUMERIC_QUERY, "limit": 5}
    assert "规则降级成功" in result["messages"][0].content

import agent_core
from option_scenario_policy import detect_option_hypothetical_scenario


def _strategy_state(user_query: str) -> dict:
    return {
        "symbol": "159915",
        "user_query": user_query,
        "risk_preference": "稳健型",
        "fund_data": "无",
        "trend_signal": "看跌",
        "memory_context": "",
        "profile_context": "",
        "technical_summary": "当前价格仍在短中期均线下方，空头结构尚未解除",
        "key_levels": "压力3.89",
        "portfolio_top_corr_index": "",
        "portfolio_top_corr_value": "",
        "portfolio_summary": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }


def _capture_strategist_prompt(monkeypatch, user_query: str) -> str:
    captured = {}

    class _DummyAgent:
        def invoke(self, *_args, **_kwargs):
            class _Msg:
                content = "策略正文"

            return {"messages": [_Msg()]}

    def _fake_create_react_agent(*_args, **kwargs):
        captured["prompt"] = kwargs["prompt"]
        return _DummyAgent()

    monkeypatch.setattr(agent_core, "create_react_agent", _fake_create_react_agent)
    agent_core.strategist_node(_strategy_state(user_query), llm=object())
    return captured["prompt"]


def _capture_finalizer_prompt(monkeypatch, user_query: str) -> str:
    captured = {}

    class _DummyKnowledge:
        @staticmethod
        def invoke(_query):
            return "知识库上下文"

    class _DummyResp:
        content = """### 🎯 核心结论
- 当前基线与条件结论分开。

### 📈 市场深度解析
- 技术面事实。

### ⚖️ 交易策略部署
- 条件策略。"""

    class _DummyLLM:
        def invoke(self, prompt):
            captured["prompt"] = prompt
            return _DummyResp()

    monkeypatch.setattr(agent_core, "search_investment_knowledge", _DummyKnowledge())
    state = {
        "messages": [
            agent_core.HumanMessage(content="【技术分析】\n当前技术面看跌，空头结构尚未解除。"),
            agent_core.HumanMessage(content="【期权策略】\n条件成立后给出风险有限策略。"),
        ],
        "user_query": user_query,
        "symbol": "159915.SZ",
        "symbol_name": "创业板ETF",
        "risk_preference": "稳健型",
        "macro_view": "无宏观分析",
        "trend_signal": "看跌",
        "key_levels": "压力3.89",
        "memory_context": "",
        "profile_context": "",
        "vision_position_domain": "",
        "vision_position_payload": {},
    }
    agent_core.finalizer_node(state, llm=_DummyLLM())
    return captured["prompt"]


def test_bullish_technical_scenario_is_detected():
    scenario = detect_option_hypothetical_scenario(
        "如果创业板ETF有效站回3.89，并确认破坏空头结构，期权应该怎么操作？"
    )

    assert scenario.active is True
    assert scenario.kind == "technical"
    assert scenario.assumed_market_bias == "bullish"
    assert scenario.assumed_iv_move == "unknown"
    assert scenario.condition_text == "如果创业板ETF有效站回3.89，并确认破坏空头结构"


def test_bearish_technical_scenario_is_detected():
    scenario = detect_option_hypothetical_scenario(
        "如果创业板ETF跌破3.70，并确认破坏多头结构，期权应该怎么操作？"
    )

    assert scenario.active is True
    assert scenario.kind == "technical"
    assert scenario.assumed_market_bias == "bearish"


def test_numeric_breakout_is_detected_as_bullish_scenario():
    scenario = detect_option_hypothetical_scenario(
        "如果创业板ETF有效突破3.89，期权应该怎么操作？"
    )

    assert scenario.kind == "technical"
    assert scenario.assumed_market_bias == "bullish"


def test_iv_up_and_down_scenarios_do_not_invent_price_direction():
    up = detect_option_hypothetical_scenario(
        "如果创业板ETF的IV升到60%，期权应该怎么操作？"
    )
    down = detect_option_hypothetical_scenario(
        "如果创业板ETF的隐含波动率明显回落，期权应该怎么操作？"
    )

    assert up.kind == "volatility"
    assert up.assumed_market_bias == "unknown"
    assert up.assumed_iv_move == "up"
    assert down.kind == "volatility"
    assert down.assumed_market_bias == "unknown"
    assert down.assumed_iv_move == "down"


def test_combined_scenario_preserves_both_assumptions():
    scenario = detect_option_hypothetical_scenario(
        "如果创业板ETF站回3.89，同时IV明显回落，期权应该怎么操作？"
    )

    assert scenario.kind == "combined"
    assert scenario.assumed_market_bias == "bullish"
    assert scenario.assumed_iv_move == "down"


def test_conflicting_technical_triggers_do_not_force_direction():
    scenario = detect_option_hypothetical_scenario(
        "如果创业板ETF既有效站回3.89又跌破3.70，期权应该怎么操作？"
    )

    assert scenario.kind == "technical"
    assert scenario.assumed_market_bias == "unknown"


def test_ordinary_and_non_option_questions_are_inactive():
    queries = [
        "创业板ETF期权，现在应该如何操作？",
        "如果创业板ETF有效站回3.89，股票应该怎么操作？",
        "创业板ETF 7月3.8认购多少钱？",
    ]

    for query in queries:
        assert detect_option_hypothetical_scenario(query).active is False


def test_pure_projection_is_not_treated_as_strategy_scenario():
    scenario = detect_option_hypothetical_scenario(
        "如果创业板ETF周一-10%开盘，IV会到多少，平值认沽涨多少？"
    )

    assert scenario.active is False


def test_negated_trigger_does_not_activate_directional_scenario():
    queries = [
        "如果创业板ETF并未有效站回3.89，期权应该怎么操作？",
        "如果创业板ETF未跌破3.70，期权应该怎么操作？",
    ]

    for query in queries:
        assert detect_option_hypothetical_scenario(query).active is False


def test_strategist_adds_scenario_context_only_for_hypothetical_query(monkeypatch):
    prompt = _capture_strategist_prompt(
        monkeypatch,
        "如果创业板ETF有效站回3.89，并确认破坏空头结构，期权应该怎么操作？",
    )

    assert "【条件场景推演（仅本题生效" in prompt
    assert "假设成立后的价格方向：看涨" in prompt
    assert "若上述条件成立" in prompt
    assert "不得把假设写成已经发生的事实" in prompt


def test_strategist_keeps_original_prompt_for_ordinary_query(monkeypatch):
    prompt = _capture_strategist_prompt(
        monkeypatch,
        "创业板ETF期权，现在应该如何操作？",
    )

    assert "【条件场景推演" not in prompt
    assert "如果技术面参考是做多或看涨，就不要给做空策略" in prompt
    assert "如果技术面参考是做空或看跌，就不要给做多策略" in prompt


def test_finalizer_adds_scenario_rules_without_replacing_cio_layout(monkeypatch):
    prompt = _capture_finalizer_prompt(
        monkeypatch,
        "如果创业板ETF有效站回3.89，并确认破坏空头结构，期权应该怎么操作？",
    )

    assert "【条件场景整合（仅本题生效）】" in prompt
    assert "假设成立后的价格方向：看涨" in prompt
    assert "不得声称条件已经触发" in prompt
    assert "### 🎯 核心结论 (Executive Summary)" in prompt
    assert "### 📈 市场深度解析" in prompt
    assert "### ⚖️ 交易策略部署" in prompt
    assert "### 🛡️ 风控与对冲" in prompt


def test_finalizer_keeps_original_cio_prompt_for_ordinary_query(monkeypatch):
    prompt = _capture_finalizer_prompt(
        monkeypatch,
        "创业板ETF期权，现在应该如何操作？",
    )

    assert "【条件场景整合" not in prompt
    assert "### 🎯 核心结论 (Executive Summary)" in prompt
    assert "### 📈 市场深度解析" in prompt
    assert "### ⚖️ 交易策略部署" in prompt

import agent_core


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

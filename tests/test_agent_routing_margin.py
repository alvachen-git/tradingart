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

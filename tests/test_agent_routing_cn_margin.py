import agent_core


def _tool_names(tools):
    return {str(getattr(item, "name", "") or getattr(item, "__name__", "")) for item in tools}


def test_explicit_cn_margin_queries_are_recognized():
    assert agent_core._is_cn_margin_explicit_query("今天沪深融资余额多少")
    assert agent_core._is_cn_margin_explicit_query("两融资金连续下降了吗")
    assert agent_core._is_cn_margin_explicit_query("融资融券最近怎么样")
    assert agent_core._is_cn_margin_explicit_query("两融怎么看")
    assert agent_core._is_cn_margin_explicit_query("融资创新高是不是过热")


def test_company_financing_and_futures_margin_are_not_cn_margin_queries():
    assert not agent_core._is_cn_margin_explicit_query("这家公司再融资计划怎么样")
    assert not agent_core._is_cn_margin_explicit_query("A轮融资估值多少")
    assert not agent_core._is_cn_margin_explicit_query("螺纹钢一手保证金多少")


def test_market_context_auto_call_scope_excludes_ordinary_stock_analysis():
    assert agent_core._is_cn_margin_auto_context_query("现在A股大盘风险偏好怎么看")
    assert agent_core._is_cn_margin_auto_context_query("300ETF期权适合卖方策略吗")
    assert agent_core._is_cn_margin_auto_context_query("中证1000走势和资金面分析")
    assert not agent_core._is_cn_margin_auto_context_query("贵州茅台走势分析")
    assert not agent_core._is_cn_margin_auto_context_query("宁德时代基本面怎么样")


def test_pure_cn_margin_data_query_routes_monitor_only():
    result = agent_core._enforce_cn_margin_monitor_routing(
        "2026年7月17日融资余额多少", ["analyst", "researcher"]
    )
    assert result == ["monitor"]


def test_cn_margin_analysis_adds_monitor_without_dropping_existing_analysis():
    result = agent_core._enforce_cn_margin_monitor_routing(
        "融资连续下降对A股行情有什么影响", ["analyst", "researcher"]
    )
    assert result == ["monitor", "analyst", "researcher"]


def test_etf_option_strategy_adds_monitor_and_strategist():
    result = agent_core._enforce_cn_margin_monitor_routing(
        "300ETF期权现在适合卖方策略吗", ["analyst"]
    )
    assert result == ["monitor", "strategist", "analyst"]


def test_non_cn_margin_query_keeps_route_unchanged():
    plan = ["analyst"]
    assert agent_core._enforce_cn_margin_monitor_routing("贵州茅台技术分析", plan) == plan
    assert agent_core._enforce_cn_margin_monitor_routing("某公司定增影响", plan) == plan


def test_direct_data_path_calls_cn_margin_tool_with_extracted_date(monkeypatch):
    class FakeTool:
        def __init__(self):
            self.calls = []

        def invoke(self, payload):
            self.calls.append(payload)
            return "融资确定性报告"

    tool = FakeTool()
    monkeypatch.setattr(agent_core, "get_cn_margin_market_signal", tool)
    result = agent_core._try_monitor_direct_data_query("2026年7月17日沪深融资余额多少")
    assert result == "融资确定性报告"
    assert tool.calls == [{"as_of_date": "20260717"}]


def test_analytical_cn_margin_query_stays_on_agent_path(monkeypatch):
    class FailTool:
        def invoke(self, _payload):
            raise AssertionError("analytical question must not use the direct-data shortcut")

    monkeypatch.setattr(agent_core, "get_cn_margin_market_signal", FailTool())
    assert agent_core._try_monitor_direct_data_query("融资连续下降对大盘有什么影响") is None


def test_cn_margin_tool_mounting_matches_scope():
    assert "get_cn_margin_market_signal" in _tool_names(agent_core.build_generalist_tools())
    assert "get_cn_margin_market_signal" in _tool_names(agent_core.build_monitor_tools())
    assert "get_cn_margin_market_signal" not in _tool_names(agent_core.build_strategist_tools())
    assert "get_cn_margin_market_signal" not in _tool_names(agent_core.build_chatter_tools())


def test_cn_margin_date_extraction_supports_common_formats():
    assert agent_core._extract_cn_margin_as_of_date("查2026-07-17融资余额") == "20260717"
    assert agent_core._extract_cn_margin_as_of_date("查20260717融资余额") == "20260717"
    assert agent_core._extract_cn_margin_as_of_date("查7月17日融资余额").endswith("0717")
    assert agent_core._extract_cn_margin_as_of_date("查2026年13月40日融资余额") == ""
    assert agent_core._extract_cn_margin_as_of_date("查20261340融资余额") == ""

import unittest
from unittest.mock import patch

from langchain_core.messages import HumanMessage

import agent_core


def _tool_names(tools):
    return {
        str(getattr(item, "name", "") or getattr(item, "__name__", ""))
        for item in tools
    }


class ValuationAgentRoutingTest(unittest.TestCase):
    def test_simple_index_data_routes_monitor(self):
        self.assertEqual(
            agent_core._enforce_valuation_routing(
                "标普500当前PE分位多少", ["researcher", "analyst"]
            ),
            ["monitor"],
        )
        self.assertEqual(
            agent_core._enforce_valuation_routing(
                "全球哪个指数估值分位最低", ["generalist"]
            ),
            ["monitor"],
        )

    def test_named_stock_valuation_routes_generalist(self):
        self.assertEqual(
            agent_core._enforce_valuation_routing("茅台现在估值贵不贵", ["analyst"]),
            ["generalist"],
        )
        self.assertEqual(
            agent_core._enforce_valuation_routing(
                "茅台和宁德时代谁更便宜", ["screener"]
            ),
            ["generalist"],
        )

    def test_value_investing_uses_research_then_generalist(self):
        for query in (
            "茅台适合长期价值投资吗",
            "宁德时代的盈利质量和现金流能否支撑长期持有",
            "比亚迪的安全边际和护城河怎么样",
        ):
            with self.subTest(query=query):
                self.assertEqual(
                    agent_core._enforce_valuation_routing(query, ["analyst"]),
                    ["researcher", "generalist"],
                )

    def test_value_investing_researcher_uses_official_filing_snapshot(self):
        state = {
            "user_query": "贵州茅台适合长期价值投资吗",
            "symbol": "600519.SH",
            "symbol_name": "贵州茅台",
        }
        with patch.object(
            agent_core,
            "build_latest_a_share_filing_snapshot",
            return_value={"report": "【最新官方财报】\n- 2026年第一季度归母净利润：25亿元"},
        ) as filing:
            result = agent_core._researcher_node_impl(state, llm=None)

        filing.assert_called_once_with("贵州茅台适合长期价值投资吗")
        self.assertIn("【情报与舆情】", result["messages"][0].content)
        self.assertIn("2026年第一季度", result["messages"][0].content)

    def test_value_investing_generalist_uses_fixed_five_section_answer(self):
        class FakeTool:
            def __init__(self, result):
                self.result = result
                self.calls = []

            def invoke(self, payload):
                self.calls.append(payload)
                return self.result

        class FakeResponse:
            content = (
                "### 研究结论\n- **适合长期研究**：估值较低，但仍需确认基本面。\n\n"
                "### 公司质量\n- 最新官方报告显示盈利和现金流为正。\n\n"
                "### 估值与安全边际\n- PE与PB处于历史低位。\n\n"
                "### 市场环境\n- 宽基指数处于偏高区间。\n\n"
                "### 证伪条件\n- 收入、扣非利润或经营现金流持续转弱。"
            )

        class FakeLLM:
            def __init__(self):
                self.prompts = []

            def invoke(self, prompt):
                self.prompts.append(prompt)
                return FakeResponse()

        stock_tool = FakeTool("PE 19.53，分位4/100；PB 5.96，分位5/100；股息率4.00%。")
        index_tool = FakeTool("沪深300分位79/100，中证500分位80/100，中证1000分位69/100。")
        llm = FakeLLM()
        state = {
            "user_query": "贵州茅台适合长期价值投资吗",
            "symbol": "600519.SH",
            "symbol_name": "贵州茅台",
            "is_followup": False,
            "messages": [HumanMessage(content="贵州茅台适合长期价值投资吗")],
            "agent_reports": {
                "researcher": "2026年第一季度营业收入480亿元，归母净利润250亿元，经营现金流180亿元。"
            },
        }
        with patch.object(agent_core, "get_stock_valuation", stock_tool), \
             patch.object(agent_core, "get_global_index_valuation", index_tool):
            result = agent_core.generalist_node(state, llm)

        content = result["messages"][0].content
        self.assertTrue(agent_core._is_valid_value_investing_response(content))
        self.assertEqual(
            stock_tool.calls,
            [{"symbol": "贵州茅台", "as_of_date": ""}],
        )
        self.assertEqual(
            index_tool.calls,
            [{"query": "沪深300 中证500 中证1000", "as_of_date": ""}],
        )
        self.assertIn("禁止加入期权", llm.prompts[0])

    def test_value_investing_subject_falls_back_to_official_report_company(self):
        report = (
            "【情报与舆情】\n【最新官方财报】\n"
            "结论：已核对 贵州茅台 的2026年第一季度。\n"
            "- 报告：贵州茅台2026年第一季度报告；披露日期 2026-04-25。"
        )
        state = {
            "user_query": "贵州茅台适合长期价值投资吗",
            "symbol": "",
            "symbol_name": "",
        }
        self.assertEqual(
            agent_core._resolve_value_investing_subject(state, report),
            "贵州茅台",
        )
        self.assertEqual(
            agent_core._value_investing_benchmark_query("贵州茅台"),
            "沪深300 中证500 中证1000",
        )

    def test_value_investing_invalid_llm_answer_falls_back_without_strategy(self):
        class InvalidLLM:
            def invoke(self, _prompt):
                return type("Response", (), {"content": "建议卖出认沽并给出仓位建议"})()

        class FakeTool:
            def __init__(self, result):
                self.result = result

            def invoke(self, _payload):
                return self.result

        state = {
            "user_query": "贵州茅台适合长期价值投资吗",
            "symbol": "600519.SH",
            "symbol_name": "贵州茅台",
            "is_followup": False,
            "messages": [HumanMessage(content="贵州茅台适合长期价值投资吗")],
            "agent_reports": {
                "researcher": "营业收入480亿元；归母净利润250亿元；经营现金流量净额180亿元。"
            },
        }
        with patch.object(agent_core, "get_stock_valuation", FakeTool("PE、PB均为历史低位，股息率4%。")), \
             patch.object(agent_core, "get_global_index_valuation", FakeTool("沪深300分位79/100。")):
            result = agent_core.generalist_node(state, InvalidLLM())

        content = result["messages"][0].content
        self.assertTrue(agent_core._is_valid_value_investing_response(content))
        self.assertNotIn("卖出认沽", content)
        self.assertNotIn("仓位建议", content)

    def test_value_investing_chain_bypasses_generic_finalizer(self):
        state = {
            "user_query": "贵州茅台适合长期价值投资吗",
            "plan": ["researcher", "generalist"],
            "execution_batches": [["researcher"], ["generalist"]],
            "agent_reports": {
                "researcher": "官方财报证据",
                "generalist": "五段式研究结论",
            },
        }
        self.assertTrue(agent_core._can_bypass_finalizer(state))

    def test_open_value_stock_selection_does_not_use_screener(self):
        self.assertTrue(agent_core._is_open_value_stock_selection_query("帮我选几只价值股"))
        self.assertEqual(
            agent_core._enforce_valuation_routing("帮我选几只价值股", ["screener"]),
            ["generalist"],
        )

    def test_non_securities_valuation_meanings_are_excluded(self):
        unchanged = ["chatter"]
        for query in (
            "A轮融资估值是多少",
            "某公司再融资计划对估值有什么影响",
            "房产估值怎么做",
            "DCF概念教学",
            "期权定价怎么计算",
            "OpenAI最近有什么消息",
        ):
            with self.subTest(query=query):
                self.assertEqual(
                    agent_core._enforce_valuation_routing(query, unchanged), unchanged
                )

    def test_direct_index_query_calls_local_tool_with_date(self):
        class FakeTool:
            def __init__(self):
                self.calls = []

            def invoke(self, payload):
                self.calls.append(payload)
                return "指数估值确定性报告"

        fake_tool = FakeTool()
        with patch.object(agent_core, "get_global_index_valuation", fake_tool):
            result = agent_core._try_monitor_direct_data_query(
                "查2026年7月21日标普500 PE分位"
            )

        self.assertEqual(result, "指数估值确定性报告")
        self.assertEqual(
            fake_tool.calls,
            [{"query": "查2026年7月21日标普500 PE分位", "as_of_date": "20260721"}],
        )

    def test_analytical_index_query_stays_off_direct_shortcut(self):
        self.assertIsNone(
            agent_core._try_monitor_direct_data_query("标普500估值偏高，对长期投资有什么风险")
        )

    def test_tool_mounting_matches_agent_scope(self):
        self.assertIn("get_stock_valuation", _tool_names(agent_core.build_generalist_tools()))
        self.assertIn(
            "get_global_index_valuation", _tool_names(agent_core.build_generalist_tools())
        )
        self.assertIn(
            "get_global_index_valuation", _tool_names(agent_core.build_monitor_tools())
        )
        self.assertNotIn(
            "get_global_index_valuation", _tool_names(agent_core.build_strategist_tools())
        )
        self.assertNotIn(
            "get_global_index_valuation", _tool_names(agent_core.build_chatter_tools())
        )

    def test_generalist_returns_value_selection_boundary_without_llm(self):
        state = {
            "user_query": "帮我选几只价值股",
            "symbol": "",
            "is_followup": False,
            "agent_reports": {},
            "context_layers": [],
            "context_layer_summary": [],
        }

        result = agent_core.generalist_node(state, llm=None)

        self.assertIsInstance(result["messages"][0], HumanMessage)
        content = result["messages"][0].content
        self.assertIn("没有完整的价值股基本面筛选器", content)
        self.assertIn("指定市场或给出候选名单", content)


if __name__ == "__main__":
    unittest.main()

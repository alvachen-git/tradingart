import unittest
from unittest import mock

import agent_core


class FakeTool:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def invoke(self, params):
        self.calls.append(params)
        return self.response


class MacroFastPathTest(unittest.TestCase):
    def test_latest_macro_policy_asset_impact_is_fast_path_family(self):
        self.assertTrue(
            agent_core._is_macro_policy_asset_impact_query("最新美联储加息消息对黄金白银有什么影响?")
        )

    def test_static_macro_policy_impact_forces_macro_only(self):
        self.assertEqual(
            agent_core._enforce_macro_policy_impact_routing(
                "美联储降息对黄金白银有什么影响?",
                ["macro_analyst", "analyst", "strategist"],
            ),
            ["macro_analyst"],
        )

    def test_broad_us_stock_macro_impact_stays_macro(self):
        self.assertEqual(
            agent_core._enforce_macro_policy_impact_routing(
                "如果美联储维持高利率和美元走强，对美股有什么影响？",
                ["screener"],
            ),
            ["macro_analyst"],
        )

    def test_us_stock_selection_is_not_hijacked_by_macro_guard(self):
        query = "美联储维持高利率下，推荐几只受影响小的美股"

        self.assertFalse(agent_core._is_macro_policy_impact_query(query))
        plan, symbol = agent_core._apply_analysis_task_policy(query, ["macro_analyst"], "")

        self.assertEqual(plan, ["screener"])
        self.assertEqual(symbol, "")

    def test_macro_policy_impact_fast_path_skips_react_agent(self):
        health = FakeTool("宏观健康快照\n| US10Y | 4.56% | fresh |\n| DXY | 120.08 | fresh |")
        curve = FakeTool("收益率曲线分析\n- 10Y-2Y利差: +0.41% 正常")
        anchors = FakeTool("US10Y 趋势: 上行\nDXY 趋势: 上行\nDFII10 最新值: 2.19%")

        with mock.patch.object(agent_core, "get_macro_health_snapshot", health), mock.patch.object(
            agent_core, "analyze_yield_curve", curve
        ), mock.patch.object(agent_core, "get_macro_indicator", anchors), mock.patch.object(
            agent_core,
            "create_react_agent",
            side_effect=AssertionError("macro fast path should not create ReAct agent"),
        ):
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
        self.assertIn("宏观快答", content)
        self.assertIn("紧缩交易", content)
        self.assertIn("黄金", content)
        self.assertIn("白银", content)
        self.assertEqual(out["macro_chart"], "")
        self.assertEqual(
            health.calls,
            [{"indicator_code": "FEDFUNDS,SOFR,US10Y,US2Y,DXY,DFII10"}],
        )
        self.assertEqual(
            anchors.calls,
            [{"indicator_code": "US10Y,US2Y,DXY,DFII10", "days": 30}],
        )


if __name__ == "__main__":
    unittest.main()

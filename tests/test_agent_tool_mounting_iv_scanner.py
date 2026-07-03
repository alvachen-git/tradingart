import unittest

import agent_core


def _tool_name_set(tools):
    names = set()
    for tool in tools:
        names.add(getattr(tool, "name", getattr(tool, "__name__", str(tool))))
    return names


class IVScannerToolMountingTest(unittest.TestCase):
    def test_generalist_tools_include_iv_scanner(self):
        self.assertIn("scan_iv_change_ranking", _tool_name_set(agent_core.build_generalist_tools()))
        self.assertIn("scan_volatility_divergence", _tool_name_set(agent_core.build_generalist_tools()))
        self.assertIn("get_us_option_market_profile", _tool_name_set(agent_core.build_generalist_tools()))
        self.assertIn("get_us_option_strategy_candidates", _tool_name_set(agent_core.build_generalist_tools()))

    def test_monitor_tools_include_iv_scanner(self):
        self.assertIn("scan_iv_change_ranking", _tool_name_set(agent_core.build_monitor_tools()))
        self.assertIn("scan_volatility_divergence", _tool_name_set(agent_core.build_monitor_tools()))
        self.assertIn("get_us_option_market_profile", _tool_name_set(agent_core.build_monitor_tools()))

    def test_strategist_tools_include_iv_scanner(self):
        self.assertIn("scan_iv_change_ranking", _tool_name_set(agent_core.build_strategist_tools()))
        self.assertIn("scan_volatility_divergence", _tool_name_set(agent_core.build_strategist_tools()))
        self.assertIn("get_us_option_market_profile", _tool_name_set(agent_core.build_strategist_tools()))
        self.assertIn("get_us_option_strategy_candidates", _tool_name_set(agent_core.build_strategist_tools()))

    def test_chatter_tools_do_not_include_us_option_profile(self):
        self.assertNotIn("get_us_option_market_profile", _tool_name_set(agent_core.build_chatter_tools()))
        self.assertNotIn("get_us_option_strategy_candidates", _tool_name_set(agent_core.build_chatter_tools()))

    def test_volatility_divergence_routes_to_monitor(self):
        self.assertEqual(
            agent_core._enforce_volatility_divergence_routing(
                "有没有什么品种的波动率背离",
                ["generalist", "analyst"],
            ),
            ["monitor"],
        )

    def test_volatility_divergence_strategy_adds_strategist(self):
        self.assertEqual(
            agent_core._enforce_volatility_divergence_routing(
                "波动率背离后期权策略怎么做",
                ["generalist", "analyst"],
            ),
            ["monitor", "strategist"],
        )

    def test_volatility_divergence_strategy_survives_task_policy(self):
        plan, symbol = agent_core._apply_analysis_task_policy(
            "波动率背离后期权策略怎么做",
            ["monitor", "strategist"],
            "",
        )
        self.assertEqual(plan, ["monitor", "strategist"])
        self.assertEqual(symbol, "")

    def test_volatility_divergence_reason_adds_researcher(self):
        self.assertEqual(
            agent_core._enforce_volatility_divergence_routing(
                "白银波动率背离背后有什么消息原因",
                ["generalist", "analyst"],
            ),
            ["monitor", "researcher"],
        )


if __name__ == "__main__":
    unittest.main()

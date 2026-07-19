import unittest

import agent_core
from agent_prompt_policy import (
    TASK_TYPE_MARKET_DATA,
    TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT,
    TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT,
    classify_analysis_task_type,
)
from chat_routing import CHAT_MODE_ANALYSIS, classify_chat_mode, is_market_data_query


SCREENSHOT_QUERY = (
    "我现在看好A股的上证指数到底，会有一次大反弹，创业板和科创板也会有反弹，"
    "但是反弹的力度不如主板大，请你结合当前情况，期权波动率，时间损耗，方向判断，"
    "我应该买什么etf期权比较好"
)


class RoutingIntentPriorityTest(unittest.TestCase):
    def test_actionable_option_decisions_never_degrade_to_monitor_only(self):
        queries = [
            "300ETF期权能买吗",
            "创业板ETF期权结合当前IV可以卖吗",
            "白银期权波动率偏高，现在买方和卖方哪个更合适",
            "黄金期权结合价格和IV应该买什么好",
            "AAPL现在适合卖put吗",
            "SPY期权IV偏低，可以买call吗",
            SCREENSHOT_QUERY,
        ]
        for query in queries:
            with self.subTest(query=query):
                policy = classify_analysis_task_type(query)
                self.assertEqual(policy.task_type, TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT)
                self.assertEqual(classify_chat_mode(query), CHAT_MODE_ANALYSIS)
                self.assertFalse(is_market_data_query(query))

                plan, _symbol = agent_core._apply_analysis_task_policy(query, ["monitor"], "")
                self.assertEqual(plan, ["analyst", "monitor", "strategist"])
                self.assertEqual(agent_core._enforce_option_data_monitor_routing(query, plan), plan)
                self.assertTrue(agent_core._has_monitor_direct_data_blocker(query))

    def test_pure_option_data_queries_remain_monitor_only(self):
        queries = [
            "创业板ETF期权IV现在多少",
            "黄金期权波动率高吗",
            "AAPL期权IV Rank多少",
            "创业板ETF 7月3.8认购多少钱",
        ]
        for query in queries:
            with self.subTest(query=query):
                self.assertEqual(classify_analysis_task_type(query).task_type, TASK_TYPE_MARKET_DATA)
                self.assertTrue(is_market_data_query(query))
                plan = agent_core._enforce_option_data_monitor_routing(
                    query,
                    ["analyst", "monitor", "strategist"],
                )
                self.assertEqual(plan, ["monitor"])
                self.assertFalse(agent_core._has_monitor_direct_data_blocker(query))

    def test_option_decision_without_underlying_requires_clarification(self):
        query = "期权现在可以买了吗"
        policy = classify_analysis_task_type(query)
        self.assertEqual(policy.task_type, TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT)
        self.assertFalse(is_market_data_query(query))

        plan, symbol = agent_core._apply_analysis_task_policy(
            query,
            ["monitor"],
            "510050",
        )
        self.assertEqual(plan, ["chatter"])
        self.assertEqual(symbol, "")

    def test_option_strategy_composes_data_and_preserves_context_experts(self):
        query = "结合新闻、当前IV和方向判断，黄金期权现在能买吗"
        plan, _symbol = agent_core._apply_analysis_task_policy(
            query,
            ["researcher", "macro_analyst", "monitor"],
            "",
        )
        self.assertEqual(
            plan,
            ["analyst", "monitor", "researcher", "macro_analyst", "strategist"],
        )
        self.assertEqual(
            agent_core._build_execution_batches(plan),
            [["analyst", "monitor", "researcher", "macro_analyst"], ["strategist"]],
        )

    def test_volatility_strategy_with_subject_keeps_full_decision_pipeline(self):
        query = "中证500现在上涨会升波还是降波，期权策略怎么做"
        plan, _symbol = agent_core._apply_analysis_task_policy(
            query,
            ["monitor"],
            "510500.SH",
        )
        self.assertEqual(plan, ["analyst", "monitor", "strategist"])
        self.assertEqual(
            agent_core._build_execution_batches(plan),
            [["analyst", "monitor"], ["strategist"]],
        )


if __name__ == "__main__":
    unittest.main()

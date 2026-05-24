import unittest

from agent_prompt_policy import (
    TASK_TYPE_FUTURES_BROKER_SIGNAL,
    TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT,
    TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT,
    TASK_TYPE_SINGLE_STOCK_ANALYSIS,
    TASK_TYPE_STOCK_SELECTION,
    TASK_TYPE_TECHNICAL_CONCEPT,
    build_data_policy_context,
    build_profile_policy,
    build_subject_policy,
    classify_analysis_task_type,
    enforce_unspecified_option_strategy_routing,
)


class SubjectPolicyTest(unittest.TestCase):
    def test_unspecified_actionable_option_strategy_needs_clarification(self):
        policy = build_subject_policy("趋势突破有效，期权到期还比较长，我能不能买深虚期权？")
        self.assertTrue(policy.is_option_strategy)
        self.assertFalse(policy.has_explicit_subject)
        self.assertTrue(policy.needs_clarification)
        self.assertFalse(policy.is_concept_question)

    def test_generic_option_concept_does_not_need_market_subject(self):
        policy = build_subject_policy("不涉及标的，单纯想象题：突破行情里买方策略怎么考虑？")
        self.assertTrue(policy.is_option_strategy)
        self.assertTrue(policy.is_concept_question)
        self.assertFalse(policy.needs_clarification)

    def test_explicit_underlying_allows_strategy_route(self):
        policy = build_subject_policy("500ETF趋势突破有效，到期还长，能不能买深虚期权？")
        self.assertTrue(policy.has_explicit_subject)
        self.assertFalse(policy.needs_clarification)

    def test_common_option_underlying_alias_is_explicit_subject(self):
        policy = build_subject_policy("创业板现在适合用什么期权策略操作？")
        self.assertTrue(policy.is_option_strategy)
        self.assertTrue(policy.has_explicit_subject)
        self.assertFalse(policy.needs_clarification)

    def test_commodity_underlying_aliases_are_explicit_subjects(self):
        for query in [
            "黄金现在适合用什么期权策略操作？",
            "豆粕现在适合用什么期权策略操作？",
            "沪铜突破后适合买认购期权吗？",
            "纯碱震荡行情适合做双卖吗？",
        ]:
            with self.subTest(query=query):
                policy = build_subject_policy(query)
                self.assertTrue(policy.is_option_strategy)
                self.assertTrue(policy.has_explicit_subject)
                self.assertFalse(policy.needs_clarification)

    def test_futures_option_codes_with_option_context_are_explicit_subjects(self):
        for query in [
            "AU期权现在适合怎么做？",
            "M期权震荡时能不能双卖？",
            "IO2606到期还远能不能买虚值认购？",
        ]:
            with self.subTest(query=query):
                policy = build_subject_policy(query)
                self.assertTrue(policy.has_explicit_subject)

    def test_generic_option_strategy_without_underlying_still_needs_clarification(self):
        policy = build_subject_policy("现在适合用什么期权策略操作？")
        self.assertTrue(policy.is_option_strategy)
        self.assertFalse(policy.has_explicit_subject)
        self.assertTrue(policy.needs_clarification)

    def test_routing_ignores_planner_default_symbol_when_query_has_no_subject(self):
        plan, symbol = enforce_unspecified_option_strategy_routing(
            "趋势突破有效，期权到期还比较长，我能不能买深虚期权？",
            ["analyst", "strategist"],
            "510050",
        )
        self.assertEqual(plan, ["chatter"])
        self.assertEqual(symbol, "")


class AnalysisTaskPolicyTest(unittest.TestCase):
    def test_stock_selection_task_type(self):
        policy = classify_analysis_task_type("帮我找放量突破的股票")
        self.assertEqual(policy.task_type, TASK_TYPE_STOCK_SELECTION)
        self.assertEqual(policy.recommended_plan, ("screener",))
        self.assertTrue(policy.clear_symbol)
        self.assertTrue(policy.hard_override)

    def test_stock_selection_followup_stays_new_task(self):
        policy = classify_analysis_task_type(
            "帮我找放量突破的股票",
            is_followup=True,
            recent_context="用户: 澜起科技的基本面和技术面分析下\nAI: 澜起科技报告。",
        )
        self.assertEqual(policy.task_type, TASK_TYPE_STOCK_SELECTION)
        self.assertEqual(policy.recommended_plan, ("screener",))

    def test_single_stock_analysis_task_type(self):
        policy = classify_analysis_task_type("澜起科技的基本面和技术面分析下")
        self.assertEqual(policy.task_type, TASK_TYPE_SINGLE_STOCK_ANALYSIS)
        self.assertEqual(policy.recommended_plan, ("analyst", "researcher"))

    def test_technical_concept_task_type(self):
        for query in ["什么是放量突破", "如何判断放量突破真假"]:
            with self.subTest(query=query):
                policy = classify_analysis_task_type(query)
                self.assertEqual(policy.task_type, TASK_TYPE_TECHNICAL_CONCEPT)

    def test_option_strategy_without_subject_task_type(self):
        policy = classify_analysis_task_type("趋势突破有效，到期还长，能不能买深虚期权？")
        self.assertEqual(policy.task_type, TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT)
        self.assertEqual(policy.recommended_plan, ("chatter",))
        self.assertTrue(policy.clear_symbol)

    def test_option_strategy_with_subject_task_type(self):
        policy = classify_analysis_task_type("500ETF趋势突破有效，到期还长，能不能买深虚期权？")
        self.assertEqual(policy.task_type, TASK_TYPE_OPTION_STRATEGY_WITH_SUBJECT)
        self.assertEqual(policy.recommended_plan, ("analyst", "strategist"))

    def test_futures_broker_signal_task_type(self):
        for query in [
            "螺纹钢现在从期货商正反指标看偏多还是偏空？",
            "螺纹刚现在从期货商正反指标看偏多还是偏空？",
            "中信建投的持仓如果持续加多是不是利多？",
            "反指标最近在哪些商品上做多",
        ]:
            with self.subTest(query=query):
                policy = classify_analysis_task_type(query)
                self.assertEqual(policy.task_type, TASK_TYPE_FUTURES_BROKER_SIGNAL)
                self.assertEqual(policy.recommended_plan, ("monitor",))
                self.assertTrue(policy.clear_symbol)
                self.assertTrue(policy.hard_override)


    def test_option_terms_take_priority_over_stock_selection(self):
        query = "如果用卖出认购的方式来做空，应该遵循什么原则，卖虚值，平值，还是实值，希腊字母有什么要求，还是根据对应标的的K线形态操作"
        policy = classify_analysis_task_type(query)
        self.assertEqual(policy.task_type, TASK_TYPE_OPTION_STRATEGY_NEEDS_SUBJECT)
        self.assertEqual(policy.recommended_plan, ("chatter",))
        self.assertNotEqual(policy.task_type, TASK_TYPE_STOCK_SELECTION)

    def test_generic_what_and_target_words_do_not_trigger_screener(self):
        for query in ["这个策略有什么标的要求", "有什么标的需要注意", "有什么原则"]:
            with self.subTest(query=query):
                policy = classify_analysis_task_type(query)
                self.assertNotEqual(policy.task_type, TASK_TYPE_STOCK_SELECTION)

    def test_explicit_stock_questions_still_use_screener(self):
        for query in ["有哪些高股息股票", "有什么防御性板块个股", "帮我筛选放量突破的股票", "给我几个候选股"]:
            with self.subTest(query=query):
                policy = classify_analysis_task_type(query)
                self.assertEqual(policy.task_type, TASK_TYPE_STOCK_SELECTION)
                self.assertEqual(policy.recommended_plan, ("screener",))


class ProfilePolicyTest(unittest.TestCase):
    def test_structured_profile_overrides_old_risk_field(self):
        policy = build_profile_policy(
            risk_preference="偏保守",
            profile_context="【交易画像】\n- 风险偏好：偏积极",
            user_query="500ETF期权怎么做？",
        )
        self.assertEqual(policy.risk_key, "aggressive")
        self.assertEqual(policy.source, "结构化画像")

    def test_current_query_overrides_structured_profile_temporarily(self):
        policy = build_profile_policy(
            risk_preference="偏积极",
            profile_context="【交易画像】\n- 风险偏好：偏积极",
            user_query="这次我保守一点，500ETF期权怎么做？",
        )
        self.assertEqual(policy.risk_key, "conservative")
        self.assertEqual(policy.source, "当前问题明确表达")


class DataPolicyTest(unittest.TestCase):
    def test_data_policy_blocks_unverified_fundamentals_and_indicators(self):
        context = build_data_policy_context(symbol="中天科技(600522.SH)", mode="finalizer")
        self.assertIn("财报数字", context)
        self.assertIn("机构目标价", context)
        self.assertIn("RSI", context)
        self.assertIn("K 线和均线", context)
        self.assertIn("研究员工具链", context)


    def test_stock_selection_execution_followup_uses_screener(self):
        policy = classify_analysis_task_type(
            "帮我筛选",
            is_followup=True,
            recent_context="AI: 需要我帮你筛选一下目前综合评分较高或高股息/防御性板块的股票名单吗？",
        )
        self.assertEqual(policy.task_type, TASK_TYPE_STOCK_SELECTION)
        self.assertEqual(policy.recommended_plan, ("screener",))
        self.assertTrue(policy.hard_override)


if __name__ == "__main__":
    unittest.main()

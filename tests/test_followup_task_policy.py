import unittest

from followup_task_policy import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    CHAT_MODE_SIMPLE,
    INTENT_ANALYZE_REASON_OR_IMPACT,
    INTENT_CORRECTION_OR_CHALLENGE,
    INTENT_EXECUTE_SUGGESTION,
    INTENT_FETCH_DETAIL_OR_DATA,
    INTENT_MODIFY_CONSTRAINT,
    INTENT_CONTINUE_EXPLANATION,
    classify_followup_task_policy,
)


class FollowupTaskPolicyTest(unittest.TestCase):
    def test_continue_explanation_for_knowledge_context(self):
        policy = classify_followup_task_policy(
            "详细说说",
            is_followup=True,
            followup_goal="explain_more",
            recent_context="用户: 什么是牛市价差\nAI: 牛市价差是一种期权概念，用于解释温和看涨。",
            focus_topic="概念解释",
        )
        self.assertEqual(policy.followup_intent, INTENT_CONTINUE_EXPLANATION)
        self.assertEqual(policy.recommended_chat_mode, CHAT_MODE_KNOWLEDGE)
        self.assertEqual(policy.recommended_plan, ("chatter",))

    def test_fetch_numeric_followup_prefers_monitor(self):
        policy = classify_followup_task_policy(
            "那具体数值呢",
            is_followup=True,
            followup_goal="fetch_numeric",
            recent_context="用户: 500ETF期权IV高吗\nAI: 当前IV和IV Rank需要看具体数据。",
        )
        self.assertEqual(policy.followup_intent, INTENT_FETCH_DETAIL_OR_DATA)
        self.assertEqual(policy.recommended_chat_mode, CHAT_MODE_ANALYSIS)
        self.assertEqual(policy.recommended_plan, ("monitor",))

    def test_analyze_reason_followup_prefers_analysis(self):
        policy = classify_followup_task_policy(
            "那为什么会这样",
            is_followup=True,
            followup_goal="analyze_reason",
            recent_context="用户: 黄金怎么看\nAI: 黄金走势偏强，受美元和利率预期影响。",
        )
        self.assertEqual(policy.followup_intent, INTENT_ANALYZE_REASON_OR_IMPACT)
        self.assertEqual(policy.recommended_chat_mode, CHAT_MODE_ANALYSIS)
        self.assertEqual(policy.recommended_plan, ("analyst",))

    def test_execute_option_strategy_suggestion_prefers_strategy_chain(self):
        policy = classify_followup_task_policy(
            "按你说的做",
            is_followup=True,
            followup_goal="execute_suggested_action",
            recent_context="AI: 如果500ETF趋势确认，可以先做技术分析，再给出认购期权策略。",
        )
        self.assertEqual(policy.followup_intent, INTENT_EXECUTE_SUGGESTION)
        self.assertEqual(policy.recommended_plan, ("analyst", "strategist"))

    def test_execute_stock_screen_suggestion_prefers_screener(self):
        policy = classify_followup_task_policy(
            "帮我筛选",
            is_followup=True,
            followup_goal="execute_suggested_action",
            recent_context="AI: 需要我帮你筛选综合评分较高、高股息或防御性板块的股票名单吗？",
        )
        self.assertEqual(policy.followup_intent, INTENT_EXECUTE_SUGGESTION)
        self.assertEqual(policy.recommended_plan, ("screener",))

    def test_modify_constraint_reuses_context_plan(self):
        policy = classify_followup_task_policy(
            "换成稳健一点",
            is_followup=True,
            recent_context="用户: 500ETF期权怎么做\nAI: 可以考虑认购期权或牛市价差策略。",
        )
        self.assertEqual(policy.followup_intent, INTENT_MODIFY_CONSTRAINT)
        self.assertEqual(policy.recommended_plan, ("analyst", "strategist"))

    def test_correction_of_entity_routes_to_knowledge(self):
        policy = classify_followup_task_policy(
            "你说错了，不是这个公司",
            is_followup=True,
            correction_intent=True,
            recent_context="用户: 中微半导体是做什么的\nAI: 这里可能指中微公司。",
        )
        self.assertEqual(policy.followup_intent, INTENT_CORRECTION_OR_CHALLENGE)
        self.assertEqual(policy.recommended_chat_mode, CHAT_MODE_KNOWLEDGE)
        self.assertEqual(policy.recommended_plan, ("chatter",))


    def test_execute_suggestion_without_context_does_not_force_analysis(self):
        policy = classify_followup_task_policy(
            "帮我筛选一些餐厅",
            is_followup=False,
            followup_goal="execute_suggested_action",
            recent_context="",
        )
        self.assertEqual(policy.followup_intent, INTENT_EXECUTE_SUGGESTION)
        self.assertEqual(policy.recommended_chat_mode, CHAT_MODE_SIMPLE)
        self.assertEqual(policy.override_level, "context_only")


if __name__ == "__main__":
    unittest.main()

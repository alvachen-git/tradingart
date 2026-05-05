import unittest

import chat_context_utils as ctx


class TestChatContextUtils(unittest.TestCase):
    def test_infer_followup_intent_for_lookup_phrase(self):
        self.assertTrue(ctx.infer_followup_intent("那你帮我查一下具体是因为什么？"))
        self.assertTrue(ctx.infer_lookup_followup_intent("那你帮我查一下具体是因为什么？"))

    def test_infer_followup_goal_for_numeric_and_fact_requests(self):
        self.assertEqual(ctx.infer_followup_goal("我要详细数值"), "fetch_numeric")
        self.assertEqual(ctx.infer_followup_goal("给我具体来源"), "fetch_facts")
        self.assertEqual(ctx.infer_followup_goal("那这意味着什么"), "analyze_reason")
        self.assertEqual(ctx.infer_followup_goal("再举个例子"), "explain_more")

    def test_short_numeric_followup_is_recognized_as_followup(self):
        self.assertTrue(ctx.infer_followup_intent("我要详细数值"))

    def test_preserve_recent_context_for_short_pronoun_followup(self):
        self.assertTrue(
            ctx.should_preserve_recent_context(
                "那具体原因呢",
                is_followup=False,
                semantic_related=False,
                is_same_domain=True,
                recent_turns=[{"role": "user", "content": "为什么今晚英特尔涨这么多？"}],
                recent_focus_entity="英特尔",
                recent_focus_topic="异动原因",
            )
        )

    def test_extract_focus_entity_supports_common_market_entities(self):
        self.assertEqual(ctx.extract_focus_entity("为什么今晚英特尔涨这么多？"), "英特尔")
        self.assertEqual(ctx.extract_focus_entity("特斯拉为什么大跌"), "特斯拉")

    def test_infer_focus_topic_supports_price_move_reason(self):
        self.assertEqual(ctx.infer_focus_topic("为什么今晚英特尔涨这么多？"), ("异动原因", "price_move_reason"))


if __name__ == "__main__":
    unittest.main()

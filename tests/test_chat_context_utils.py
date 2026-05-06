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

    def test_select_target_anchor_prefers_latest_completed_topic_for_short_numeric_followup(self):
        messages = [
            {"role": "user", "content": "澜起科技跟科创50的相关度有多少"},
            {"role": "assistant", "content": "澜起科技和科创50存在一定相关性。"},
            {"role": "user", "content": "黄金跟白银的相关性高吗"},
            {"role": "assistant", "content": "黄金和白银通常呈现较高相关性。"},
        ]
        anchors = ctx.build_topic_anchors(messages, max_anchors=3)
        out = ctx.select_target_anchor(
            "我要详细数值",
            anchors,
            followup_goal="fetch_numeric",
            is_followup=True,
        )
        target = out.get("target_anchor") or {}
        self.assertEqual(target.get("user_query"), "黄金跟白银的相关性高吗")
        self.assertFalse(out.get("followup_anchor_ambiguous"))

    def test_select_target_anchor_asks_to_clarify_for_low_info_multi_topic_followup(self):
        messages = [
            {"role": "user", "content": "法国大革命是什么"},
            {"role": "assistant", "content": "法国大革命是18世纪末法国发生的政治社会革命。"},
            {"role": "user", "content": "第一次世界大战是什么"},
            {"role": "assistant", "content": "第一次世界大战是1914年至1918年间的全球战争。"},
        ]
        anchors = ctx.build_topic_anchors(messages, max_anchors=3)
        out = ctx.select_target_anchor(
            "详细一点",
            anchors,
            followup_goal="explain_more",
            is_followup=True,
        )
        self.assertTrue(out.get("followup_anchor_ambiguous"))
        self.assertIn("法国大革命", out.get("followup_anchor_clarify", ""))
        self.assertIn("第一次世界大战", out.get("followup_anchor_clarify", ""))


if __name__ == "__main__":
    unittest.main()

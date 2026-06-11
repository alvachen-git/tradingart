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

    def test_infer_correction_intent_for_fact_and_challenge_phrases(self):
        self.assertTrue(ctx.infer_correction_intent("不是中微公司，就叫中微半导"))
        self.assertTrue(ctx.infer_correction_intent("有这家公司，你仔细思考下"))
        self.assertFalse(ctx.infer_correction_intent("再举个例子"))

    def test_short_numeric_followup_is_recognized_as_followup(self):
        self.assertTrue(ctx.infer_followup_intent("我要详细数值"))

    def test_explicit_technical_subject_is_not_marked_as_followup(self):
        self.assertFalse(ctx.infer_followup_intent("汇川技术技术面怎么看"))
        self.assertEqual(ctx.extract_focus_entity("汇川技术技术面怎么看"), "汇川技术")

    def test_explicit_new_subject_does_not_inherit_recent_context(self):
        self.assertFalse(
            ctx.should_preserve_recent_context(
                "汇川技术技术面怎么看",
                is_followup=False,
                semantic_related=False,
                is_same_domain=True,
                recent_turns=[{"role": "user", "content": "科创50为什么大涨"}],
                recent_focus_entity="科创50",
                recent_focus_topic="异动原因",
            )
        )

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
        self.assertEqual(ctx.extract_focus_entity("创业板为什么大跌"), "创业板")
        self.assertEqual(ctx.extract_focus_entity("科创50为什么大涨"), "科创50")

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


    def test_execute_suggested_action_goal_for_stock_screening_followup(self):
        self.assertEqual(ctx.infer_followup_goal("帮我筛选"), "execute_suggested_action")
        self.assertTrue(ctx.infer_followup_intent("好，帮我筛选"))

    def test_preserve_recent_context_for_execute_suggested_action(self):
        self.assertTrue(
            ctx.should_preserve_recent_context(
                "好，帮我筛选",
                is_followup=False,
                semantic_related=False,
                is_same_domain=True,
                recent_turns=[
                    {
                        "role": "assistant",
                        "content": "需要我帮你筛选一下目前综合评分较高或高股息/防御性板块的股票名单吗？",
                    }
                ],
                recent_focus_topic="精选股票",
            )
        )

    def test_build_topic_anchors_extracts_suggested_actions(self):
        messages = [
            {"role": "user", "content": "股票要做对冲避险的话有什么好方法吗"},
            {
                "role": "assistant",
                "content": "给您的实操建议：需要我帮你筛选一下目前综合评分较高或高股息/防御性板块的股票名单吗？",
            },
        ]
        anchors = ctx.build_topic_anchors(messages, max_anchors=3)
        self.assertTrue(anchors[0].get("suggested_actions"))
        self.assertIn("股票名单", anchors[0].get("suggested_actions")[0])

    def test_select_target_anchor_prefers_latest_suggested_action_anchor(self):
        messages = [
            {"role": "user", "content": "什么是牛市价差"},
            {"role": "assistant", "content": "牛市价差是一种偏温和看涨的期权策略。"},
            {"role": "user", "content": "股票要做对冲避险的话有什么好方法吗"},
            {
                "role": "assistant",
                "content": "给您的实操建议：需要我帮你筛选一下目前综合评分较高或高股息/防御性板块的股票名单吗？",
            },
        ]
        anchors = ctx.build_topic_anchors(messages, max_anchors=3)
        out = ctx.select_target_anchor(
            "好，帮我筛选",
            anchors,
            followup_goal="execute_suggested_action",
            is_followup=True,
        )
        target = out.get("target_anchor") or {}
        self.assertEqual(target.get("user_query"), "股票要做对冲避险的话有什么好方法吗")
        self.assertFalse(out.get("followup_anchor_ambiguous"))


    def test_non_finance_full_screening_request_is_not_execute_followup(self):
        self.assertNotEqual(
            ctx.infer_followup_goal("中午不知道吃什么，帮我筛选一些餐厅"),
            "execute_suggested_action",
        )


if __name__ == "__main__":
    unittest.main()

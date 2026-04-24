import unittest

from chat_routing import (
    CHAT_MODE_ANALYSIS,
    CHAT_MODE_KNOWLEDGE,
    CHAT_MODE_SIMPLE,
    classify_chat_mode,
)


class TestChatRouting(unittest.TestCase):
    def test_classify_simple_chat(self):
        self.assertEqual(classify_chat_mode("你好"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("谢谢你"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("法国大革命是什么"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("怎么缓解焦虑"), CHAT_MODE_SIMPLE)
        self.assertEqual(classify_chat_mode("帮我写一段生日祝福"), CHAT_MODE_SIMPLE)

    def test_classify_knowledge_chat(self):
        self.assertEqual(classify_chat_mode("什么是牛市价差"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("牛市价差策略是什么"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("解释一下IV"), CHAT_MODE_KNOWLEDGE)
        self.assertEqual(classify_chat_mode("delta和gamma有什么区别"), CHAT_MODE_KNOWLEDGE)

    def test_classify_analysis_chat(self):
        self.assertEqual(classify_chat_mode("黄金怎么看"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("创业板期权做什么策略"), CHAT_MODE_ANALYSIS)
        self.assertEqual(classify_chat_mode("这条新闻对铜价影响大吗"), CHAT_MODE_ANALYSIS)

    def test_non_finance_followup_can_stay_simple_chat(self):
        self.assertEqual(
            classify_chat_mode("继续说说", is_followup=True, recent_context="用户: 法国大革命是什么\nAI: ..."),
            CHAT_MODE_SIMPLE,
        )

    def test_finance_followup_does_not_fall_into_simple_chat(self):
        self.assertEqual(
            classify_chat_mode("那为什么", is_followup=True, recent_context="用户: 黄金怎么看\nAI: ..."),
            CHAT_MODE_ANALYSIS,
        )


if __name__ == "__main__":
    unittest.main()

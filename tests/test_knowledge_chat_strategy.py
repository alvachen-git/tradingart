import unittest

import agent_core


class TestKnowledgeChatStrategy(unittest.TestCase):
    def test_company_news_strategy_can_be_selected_from_focus_hint(self):
        state = {
            "user_query": "他的机器人业务最近怎么样",
            "focus_mode_hint": "company_news",
            "focus_topic": "公司近期动态",
            "recent_context": "用户: 汇川技术最近有什么好消息吗\nAI: 最近有机器人业务相关动态。",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_company_news_strategy_can_be_selected_from_recent_context(self):
        state = {
            "user_query": "你详细展开",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "用户: 汇川技术最近有什么好消息吗\nAI: 最近有机器人业务相关动态。",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_default_strategy_remains_concept_explain(self):
        state = {
            "user_query": "什么是牛市价差",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "concept_explain")

    def test_build_chatter_tools_uses_web_search_not_financial_news(self):
        tool_names = [tool.name for tool in agent_core.build_chatter_tools()]
        self.assertIn("search_web", tool_names)
        self.assertNotIn("get_financial_news", tool_names)


if __name__ == "__main__":
    unittest.main()

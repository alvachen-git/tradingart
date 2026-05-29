import unittest
from unittest.mock import Mock, patch

try:
    import agent_core
except ModuleNotFoundError as exc:
    agent_core = None
    _AGENT_CORE_IMPORT_ERROR = exc
else:
    _AGENT_CORE_IMPORT_ERROR = None


@unittest.skipIf(agent_core is None, f"agent_core dependencies unavailable: {_AGENT_CORE_IMPORT_ERROR}")
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

    def test_recent_filing_query_selects_company_news_strategy(self):
        state = {
            "user_query": "汇川技术最近财报怎么样",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_recent_filing_query_with_particle_selects_company_news_strategy(self):
        state = {
            "user_query": "寒武纪最近的财报好吗",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_current_first_quarter_profit_query_selects_company_news_strategy(self):
        state = {
            "user_query": "汇川技术今年第一季财报是赚钱吗",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_recent_filing_query_for_suffixless_company_selects_company_news_strategy(self):
        state = {
            "user_query": "中芯国际最近财报如何",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_current_month_company_news_selects_company_news_strategy(self):
        state = {
            "user_query": "宁德时代这个月有什么公告",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_build_chatter_tools_uses_web_search_not_financial_news(self):
        tool_names = [tool.name for tool in agent_core.build_chatter_tools()]
        self.assertIn("search_web", tool_names)
        self.assertNotIn("get_financial_news", tool_names)

    def test_latest_company_fact_uses_direct_web_fast_path(self):
        state = {
            "user_query": "汇川技术最近财报怎么样",
            "focus_mode_hint": "company_news",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
        }
        fresh_answer = "最新披露包括2025年年度报告和2026年一季度报告，披露日期明确。"

        with patch.object(agent_core.search_web, "invoke", return_value=fresh_answer) as search_invoke, \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=True), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn(fresh_answer, result["messages"][0].content)
        search_invoke.assert_called_once_with({"query": "汇川技术最近财报怎么样"})
        create_agent.assert_not_called()

    def test_current_first_quarter_profit_uses_direct_web_fast_path(self):
        state = {
            "user_query": "汇川技术今年第一季财报是赚钱吗",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
        }
        fresh_answer = "根据官方报告，2026年第一季度是盈利的，归母净利润为正。"

        with patch.object(agent_core.search_web, "invoke", return_value=fresh_answer) as search_invoke, \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=True), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn(fresh_answer, result["messages"][0].content)
        search_invoke.assert_called_once_with({"query": "汇川技术今年第一季财报是赚钱吗"})
        create_agent.assert_not_called()

    def test_latest_company_fact_invalid_direct_web_stops_without_llm_fallback(self):
        state = {
            "user_query": "汇川技术最近财报怎么样",
            "focus_mode_hint": "company_news",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
        }
        llm = Mock()

        with patch.object(agent_core.search_web, "invoke", return_value="截至我知识更新时间（2024年中）"), \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=False), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=llm)

        self.assertIn("没有检索到足够新的公开资料", result["messages"][0].content)
        create_agent.assert_not_called()
        llm.invoke.assert_not_called()

    def test_concept_question_still_uses_react_path(self):
        state = {
            "user_query": "什么是牛市价差",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
        }
        fake_agent = Mock()
        fake_agent.invoke.return_value = {"messages": [Mock(content="牛市价差是一种期权价差策略。")]}

        with patch.object(agent_core, "create_react_agent", return_value=fake_agent) as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        create_agent.assert_called_once()
        self.assertIn("牛市价差", result["messages"][0].content)

    def test_latest_company_fact_react_exception_does_not_call_llm_fallback(self):
        state = {
            "user_query": "汇川技术最近财报怎么样",
            "focus_mode_hint": "company_news",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
        }
        fake_agent = Mock()
        fake_agent.invoke.side_effect = RuntimeError("Recursion limit of 15 reached")
        llm = Mock()

        with patch.object(agent_core, "_try_direct_company_fact_search", return_value=None), \
             patch.object(agent_core, "create_react_agent", return_value=fake_agent):
            result = agent_core.knowledge_chatter_node(state, llm=llm)

        self.assertIn("没有检索到足够新的公开资料", result["messages"][0].content)
        llm.invoke.assert_not_called()


if __name__ == "__main__":
    unittest.main()

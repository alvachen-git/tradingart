import unittest
from unittest.mock import Mock, patch

import pandas as pd

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

        with patch.object(agent_core, "_invoke_search_web_direct", return_value=fresh_answer) as search_invoke, \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=True), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn(fresh_answer, result["messages"][0].content)
        search_invoke.assert_called_once_with("汇川技术最近财报怎么样")
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

        with patch.object(agent_core, "_invoke_search_web_direct", return_value=fresh_answer) as search_invoke, \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=True), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn(fresh_answer, result["messages"][0].content)
        search_invoke.assert_called_once_with("汇川技术今年第一季财报是赚钱吗")
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

        with patch.object(agent_core, "_invoke_search_web_direct", return_value="截至我知识更新时间（2024年中）"), \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=False), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=llm)

        self.assertIn("没有检索到足够新的公开资料", result["messages"][0].content)
        create_agent.assert_not_called()
        llm.invoke.assert_not_called()

    def test_freshness_listing_query_selects_company_news_strategy(self):
        state = {
            "user_query": "今天spacex是不是要上市",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "freshness_required": True,
            "quick_answer_scenario": "freshness",
        }
        self.assertEqual(agent_core._select_knowledge_chat_strategy(state), "company_news")

    def test_freshness_listing_query_uses_quote_lookup_before_glm_search(self):
        state = {
            "user_query": "spacex是不是上市了",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
            "freshness_required": True,
            "freshness_query_target": "SpaceX",
            "quick_answer_scenario": "freshness",
        }
        quote_answer = "【实时核验】\nSpaceX 已能通过公开行情源查到美股股票代码 SPCX 的近期交易数据。"

        with patch.object(agent_core, "_try_listing_quote_status_answer", return_value=quote_answer), \
             patch.object(agent_core, "_invoke_search_web_direct") as search_direct, \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn("SPCX", result["messages"][0].content)
        search_direct.assert_not_called()
        create_agent.assert_not_called()

    def test_listing_quote_candidate_filter_accepts_public_equity(self):
        raw_candidates = [
            {
                "symbol": "CRWV",
                "quoteType": "EQUITY",
                "exchange": "NMS",
                "shortname": "CoreWeave, Inc.",
                "longname": "CoreWeave, Inc.",
                "exchDisp": "NASDAQ",
                "score": 20027,
            },
            {
                "symbol": "CRWV.MX",
                "quoteType": "EQUITY",
                "exchange": "MEX",
                "shortname": "COREWEAVE INC",
                "score": 20001,
            },
        ]

        candidates = agent_core._filter_listing_quote_candidates(raw_candidates, "CoreWeave")

        self.assertEqual(candidates[0]["ticker"], "CRWV")
        self.assertEqual(candidates[0]["market"], "NASDAQ")

    def test_listing_quote_candidate_filter_rejects_fund_and_tokenized_results(self):
        raw_candidates = [
            {
                "symbol": "OPENAI-USD",
                "quoteType": "CRYPTOCURRENCY",
                "shortname": "OpenAI tokenized stock (PreStocks) USD",
                "longname": "OpenAI tokenized stock (PreStocks) USD",
                "exchange": "CCC",
                "score": 20001,
            },
            {
                "symbol": "OPEAZZX",
                "quoteType": "MUTUALFUND",
                "longname": "OpenAI - Company Level",
                "exchange": "NAS",
                "score": 20001,
            },
        ]

        candidates = agent_core._filter_listing_quote_candidates(raw_candidates, "OpenAI")

        self.assertEqual(candidates, [])

    def test_listing_quote_candidate_filter_rejects_leveraged_space_etfs(self):
        raw_candidates = [
            {
                "symbol": "SPCF",
                "quoteType": "EQUITY",
                "exchange": "PCX",
                "shortname": "ProShares Ultra SpaceX",
                "longname": "Proshares Ultra Spacex",
                "exchDisp": "NYSEArca",
                "score": 20004,
            },
            {
                "symbol": "SPCX",
                "quoteType": "EQUITY",
                "exchange": "NMS",
                "shortname": "Space Exploration Technologies ",
                "longname": "Space Exploration Technologies Corp.",
                "exchDisp": "NASDAQ",
                "score": 20697,
            },
        ]

        candidates = agent_core._filter_listing_quote_candidates(raw_candidates, "SpaceX")

        self.assertEqual([candidate["ticker"] for candidate in candidates], ["SPCX"])

    def test_listing_status_quote_lookup_prefers_alias_before_search_candidates(self):
        frame = pd.DataFrame({"Close": [153.23]}, index=pd.to_datetime(["2026-06-26"]))

        with patch.object(
            agent_core,
            "_search_listing_quote_candidates",
            return_value=[{"ticker": "SPCF", "name": "ProShares Ultra SpaceX", "market": "NYSEArca"}],
        ), patch.object(agent_core, "_download_listing_quote_frame", return_value=frame) as download_frame:
            answer = agent_core._try_listing_quote_status_answer("spacex已经上市了吗", {})

        self.assertIn("SPCX", answer)
        self.assertNotIn("SPCF", answer)
        self.assertEqual(download_frame.call_args.args[0], "SPCX")

    def test_yfinance_session_does_not_inherit_system_proxy(self):
        session = agent_core._make_yfinance_session()

        if session is not None:
            self.assertFalse(session.trust_env)

    def test_listing_status_quote_lookup_is_generic_not_spacex_only(self):
        frame = pd.DataFrame({"Close": [97.5]}, index=pd.to_datetime(["2026-06-26"]))

        with patch.object(
            agent_core,
            "_search_listing_quote_candidates",
            return_value=[{"ticker": "CRWV", "name": "CoreWeave, Inc.", "market": "NASDAQ"}],
        ) as search_candidates, \
             patch.object(agent_core, "_download_listing_quote_frame", return_value=frame) as download_frame:
            answer = agent_core._try_listing_quote_status_answer("CoreWeave是不是上市了", {})

        self.assertIn("CRWV", answer)
        self.assertIn("已上市/已公开交易", answer)
        search_candidates.assert_called_once()
        download_frame.assert_called_once()

    def test_freshness_listing_query_uses_direct_glm_search_gate_when_quote_unavailable(self):
        state = {
            "user_query": "今天spacex是不是要上市",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
            "freshness_required": True,
            "freshness_query_target": "SpaceX",
            "quick_answer_scenario": "freshness",
        }
        fresh_answer = "SpaceX 已上市交易，股票代码 SPCX，在 Nasdaq 交易，公开报道给出了 IPO 价格和交易日期。"

        with patch.object(agent_core, "_try_listing_quote_status_answer", return_value=""), \
             patch.object(agent_core, "_invoke_search_web_direct", return_value=fresh_answer) as search_direct, \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=True), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        content = result["messages"][0].content
        self.assertIn("【实时核验】", content)
        self.assertIn("SPCX", content)
        search_query = search_direct.call_args.args[0]
        self.assertIn("SpaceX", search_query)
        self.assertIn("IPO", search_query)
        self.assertIn("ticker", search_query)
        self.assertNotIn("公告", search_query)
        create_agent.assert_not_called()

    def test_freshness_listing_query_rejects_stale_answer_without_llm_fallback(self):
        state = {
            "user_query": "今天spacex是不是要上市",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
            "freshness_required": True,
            "freshness_query_target": "SpaceX",
            "quick_answer_scenario": "freshness",
        }
        llm = Mock()

        with patch.object(agent_core, "_try_listing_quote_status_answer", return_value=""), \
             patch.object(agent_core, "_invoke_search_web_direct", return_value="截至我知识更新时间，SpaceX 仍是私营公司。"), \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=False), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=llm)

        content = result["messages"][0].content
        self.assertIn("不能凭模型记忆判断是否已上市", content)
        create_agent.assert_not_called()
        llm.invoke.assert_not_called()

    def test_freshness_listing_query_rejects_old_as_of_search_answer(self):
        state = {
            "user_query": "spacex是不是上市了",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
            "freshness_required": True,
            "freshness_query_target": "SpaceX",
            "quick_answer_scenario": "freshness",
        }
        stale_answer = "一句话结论：截至2023年10月，SpaceX尚未确定IPO时间表，也未在Nasdaq或NYSE上市。"

        with patch.object(agent_core, "_try_listing_quote_status_answer", return_value=""), \
             patch.object(agent_core, "_invoke_search_web_direct", return_value=stale_answer), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn("不能凭模型记忆判断是否已上市", result["messages"][0].content)
        create_agent.assert_not_called()

    def test_listing_query_without_freshness_state_still_uses_evidence_gate(self):
        state = {
            "user_query": "今天spacex是不是要上市",
            "focus_mode_hint": "",
            "focus_topic": "",
            "recent_context": "",
            "symbol": "",
        }
        fresh_answer = "SpaceX 已上市，股票代码 SPCX，Nasdaq 报道显示其股票已经开始交易。"

        with patch.object(agent_core, "_try_listing_quote_status_answer", return_value=""), \
             patch.object(agent_core, "_invoke_search_web_direct", return_value=fresh_answer) as search_direct, \
             patch.object(agent_core, "is_search_answer_acceptable", return_value=True), \
             patch.object(agent_core, "create_react_agent") as create_agent:
            result = agent_core.knowledge_chatter_node(state, llm=Mock())

        self.assertIn("SPCX", result["messages"][0].content)
        self.assertIn("IPO", search_direct.call_args.args[0])
        create_agent.assert_not_called()

    def test_state_context_payload_preserves_freshness_fields(self):
        payload = agent_core._state_context_payload({
            "user_query": "今天spacex是不是要上市",
            "freshness_required": True,
            "freshness_quick_status": "timeout",
            "freshness_query_target": "SpaceX",
            "quick_answer_scenario": "freshness",
        })

        self.assertTrue(payload.get("freshness_required"))
        self.assertEqual(payload.get("freshness_quick_status"), "timeout")
        self.assertEqual(payload.get("freshness_query_target"), "SpaceX")
        self.assertEqual(payload.get("quick_answer_scenario"), "freshness")

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

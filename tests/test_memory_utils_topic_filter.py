import unittest
from unittest.mock import patch

from langchain_core.documents import Document

import memory_utils


class _FakeSearchStore:
    def __init__(self, results):
        self._results = results

    def similarity_search_with_score(self, query, k=3, filter=None):
        return list(self._results)[:k]


class _FakeWriteStore:
    def __init__(self):
        self.docs = []

    def add_documents(self, docs):
        self.docs.extend(docs)


class TestMemoryUtilsTopicFilter(unittest.TestCase):
    def test_retrieve_strict_option_filters_stock_topic(self):
        option_doc = Document(
            page_content="[2026-04-12 09:00] 用户问: 创业板期权持仓怎么调\nAI回答: 建议先看认购认沽结构",
            metadata={"user_id": "u1", "topic": "option"},
        )
        stock_doc = Document(
            page_content="[2026-04-12 08:00] 用户问: 自动持仓体检\nAI回答: 当前股票持仓较分散",
            metadata={"user_id": "u1", "topic": "stock_portfolio"},
        )
        fake_store = _FakeSearchStore([(stock_doc, 0.11), (option_doc, 0.19)])
        with patch.object(memory_utils, "get_vector_store", return_value=fake_store):
            out = memory_utils.retrieve_relevant_memory(
                user_id="u1",
                query="期权持仓怎么调",
                k=2,
                query_topic="option",
                strict_topic=True,
            )
        self.assertIn("期权持仓", out)
        self.assertNotIn("自动持仓体检", out)

    def test_retrieve_strict_option_compatible_with_legacy_records_without_topic(self):
        option_old = Document(
            page_content="[2026-04-11 10:00] 用户问: 认购期权怎么做\nAI回答: 先看IV与到期日",
            metadata={"user_id": "u1"},
        )
        stock_old = Document(
            page_content="[2026-04-10 10:00] 用户问: 自动持仓体检\nAI回答: 股票组合行业分布偏半导体",
            metadata={"user_id": "u1"},
        )
        fake_store = _FakeSearchStore([(stock_old, 0.08), (option_old, 0.12)])
        with patch.object(memory_utils, "get_vector_store", return_value=fake_store):
            out = memory_utils.retrieve_relevant_memory(
                user_id="u1",
                query="期权策略建议",
                k=2,
                query_topic="option",
                strict_topic=True,
            )
        self.assertIn("认购期权", out)
        self.assertNotIn("自动持仓体检", out)

    def test_save_interaction_backwards_compatible_and_infers_topic(self):
        fake_store = _FakeWriteStore()
        with patch.object(memory_utils, "get_vector_store", return_value=fake_store):
            memory_utils.save_interaction(
                user_id="u1",
                user_input="自动持仓体检",
                ai_response="股票组合风险中等",
            )
            memory_utils.save_interaction(
                user_id="u1",
                user_input="期权持仓怎么调",
                ai_response="建议先处理近月腿",
                topic="option",
                source="mobile",
            )

        self.assertEqual(len(fake_store.docs), 2)
        first_meta = fake_store.docs[0].metadata
        second_meta = fake_store.docs[1].metadata
        self.assertEqual(first_meta.get("topic"), "stock_portfolio")
        self.assertEqual(second_meta.get("topic"), "option")
        self.assertEqual(second_meta.get("source"), "mobile")


if __name__ == "__main__":
    unittest.main()

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


class _FakeGetStore:
    def __init__(self, docs):
        self._docs = list(docs)
        self.last_where = None

    @staticmethod
    def _matches(metadata, where):
        if not where:
            return True
        if "$and" in where:
            return all(_FakeGetStore._matches(metadata, clause) for clause in where.get("$and") or [])
        for key, expected in where.items():
            value = metadata.get(key)
            if isinstance(expected, dict):
                if "$gte" in expected and not (str(value or "") >= str(expected["$gte"])):
                    return False
                if "$lte" in expected and not (str(value or "") <= str(expected["$lte"])):
                    return False
            elif value != expected:
                return False
        return True

    def get(self, limit=20, where=None):
        self.last_where = where
        matched = [doc for doc in self._docs if self._matches(doc.metadata, where)]
        return {
            "documents": [doc.page_content for doc in matched[:limit]],
            "metadatas": [doc.metadata for doc in matched[:limit]],
        }


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

    def test_retrieve_recent_conversation_memory_filters_by_user_and_time(self):
        docs = [
            Document(
                page_content="[2026-05-08 21:00:00] 用户问: 昨天我们聊了网球类比\nAI回答: 用网球解释牛市价差。",
                metadata={"user_id": "u1", "timestamp": "2026-05-08 21:00:00", "topic": "general"},
            ),
            Document(
                page_content="[2026-05-09 09:00:00] 用户问: 今天聊ETF期权\nAI回答: 先看波动率。",
                metadata={"user_id": "u1", "timestamp": "2026-05-09 09:00:00", "topic": "option"},
            ),
            Document(
                page_content="[2026-05-08 20:00:00] 用户问: 其他用户的问题\nAI回答: 不应出现。",
                metadata={"user_id": "u2", "timestamp": "2026-05-08 20:00:00", "topic": "general"},
            ),
        ]
        fake_store = _FakeGetStore(docs)

        with patch.object(memory_utils, "get_vector_store", return_value=fake_store):
            out = memory_utils.retrieve_recent_conversation_memory(
                user_id="u1",
                since="2026-05-08 00:00:00",
                until="2026-05-08 23:59:59",
            )

        self.assertIn("【对话历史记忆】", out)
        self.assertIn("网球类比", out)
        self.assertNotIn("今天聊ETF期权", out)
        self.assertNotIn("其他用户", out)
        self.assertEqual(fake_store.last_where, {"user_id": "u1"})

    def test_retrieve_recent_conversation_memory_returns_empty_when_missing(self):
        fake_store = _FakeGetStore([])
        with patch.object(memory_utils, "get_vector_store", return_value=fake_store):
            out = memory_utils.retrieve_recent_conversation_memory(user_id="u1")

        self.assertEqual(out, "")


if __name__ == "__main__":
    unittest.main()

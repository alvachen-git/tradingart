import unittest
from datetime import datetime
from unittest.mock import patch

import view_memory


class _FakeVectorStore:
    def __init__(self, payload):
        self.payload = payload

    def get(self, limit=20000):
        return self.payload


class TestViewMemoryCli(unittest.TestCase):
    def test_parse_qa_fields(self):
        q, a = view_memory._parse_qa_fields(
            "[2026-03-31 10:00] 用户问: 期权怎么做对冲\nAI回答: 先明确风险预算。"
        )
        self.assertIn("期权怎么做对冲", q)
        self.assertIn("先明确风险预算", a)

    def test_filter_rows_by_user_keyword_time_and_limit(self):
        rows = [
            {
                "user_id": "u1",
                "time": "2026-03-31 10:00",
                "ts": datetime(2026, 3, 31, 10, 0, 0),
                "question": "期权策略",
                "answer": "做多波动率",
                "raw": "",
            },
            {
                "user_id": "u1",
                "time": "2026-02-28 10:00",
                "ts": datetime(2026, 2, 28, 10, 0, 0),
                "question": "股票",
                "answer": "持有",
                "raw": "",
            },
            {
                "user_id": "u2",
                "time": "2026-03-30 10:00",
                "ts": datetime(2026, 3, 30, 10, 0, 0),
                "question": "期权",
                "answer": "谨慎",
                "raw": "",
            },
        ]

        out = view_memory._filter_rows(
            rows,
            user="u1",
            contains="期权",
            since="2026-03-01",
            until="2026-03-31",
            limit=5,
        )
        self.assertEqual(len(out), 1)
        self.assertEqual(out[0]["user_id"], "u1")
        self.assertIn("期权", out[0]["question"])

    def test_view_all_memories_with_filters(self):
        payload = {
            "ids": ["a1", "a2"],
            "documents": [
                "[2026-03-31 09:00] 用户问: 期权如何控仓\nAI回答: 建议分批。",
                "[2026-03-30 09:00] 用户问: 黄金怎么看\nAI回答: 关注波动率。",
            ],
            "metadatas": [
                {"user_id": "u1", "timestamp": "2026-03-31 09:00"},
                {"user_id": "u2", "timestamp": "2026-03-30 09:00"},
            ],
        }
        with patch.object(view_memory, "_load_vector_store", return_value=_FakeVectorStore(payload)):
            count = view_memory.view_all_memories(user="u1", limit=10, contains="期权")
        self.assertEqual(count, 1)


if __name__ == "__main__":
    unittest.main()

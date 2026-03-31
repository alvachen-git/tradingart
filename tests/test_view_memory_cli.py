import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

import view_memory


class _FakeVectorStore:
    def __init__(self, payload):
        self.payload = payload

    @staticmethod
    def _matches_where(meta, where):
        if not where:
            return True
        if "$and" in where:
            return all(_FakeVectorStore._matches_where(meta, cond) for cond in where["$and"])
        for key, rule in where.items():
            value = str(meta.get(key, ""))
            if isinstance(rule, dict):
                gte = rule.get("$gte")
                lte = rule.get("$lte")
                if gte is not None and value < str(gte):
                    return False
                if lte is not None and value > str(lte):
                    return False
            else:
                if value != str(rule):
                    return False
        return True

    def get(self, limit=20000, offset=0, where=None):
        ids = self.payload.get("ids", [])
        docs = self.payload.get("documents", [])
        metas = self.payload.get("metadatas", [])
        packed = []
        for i, _id in enumerate(ids):
            doc = docs[i] if i < len(docs) else ""
            meta = metas[i] if i < len(metas) and isinstance(metas[i], dict) else {}
            if self._matches_where(meta, where):
                packed.append((_id, doc, meta))
        chunk = packed[offset: offset + limit]
        return {
            "ids": [x[0] for x in chunk],
            "documents": [x[1] for x in chunk],
            "metadatas": [x[2] for x in chunk],
        }


class TestViewMemoryCli(unittest.TestCase):
    def test_build_time_where(self):
        where = view_memory._build_time_where("2026-03-01", "2026-03-31")
        self.assertEqual(
            where,
            {
                "$and": [
                    {"timestamp": {"$gte": "2026-03-01 00:00:00"}},
                    {"timestamp": {"$lte": "2026-03-31 23:59:59"}},
                ]
            },
        )

    def test_fetch_data_paginated_breaks_20k_cap(self):
        total = 25050
        payload = {
            "ids": [f"id{i}" for i in range(total)],
            "documents": [f"doc{i}" for i in range(total)],
            "metadatas": [{"user_id": "u1", "timestamp": "2026-03-31 10:00:00"} for _ in range(total)],
        }
        store = _FakeVectorStore(payload)
        data = view_memory._fetch_data_paginated(store, page_size=5000)
        self.assertEqual(len(data.get("ids", [])), total)

    def test_fetch_data_paginated_with_time_window(self):
        payload = {
            "ids": ["a", "b", "c"],
            "documents": ["doc-a", "doc-b", "doc-c"],
            "metadatas": [
                {"user_id": "u1", "timestamp": "2026-02-11 10:00:00"},
                {"user_id": "u1", "timestamp": "2026-03-31 10:00:00"},
                {"user_id": "u1", "timestamp": "2026-04-01 10:00:00"},
            ],
        }
        store = _FakeVectorStore(payload)
        data = view_memory._fetch_data_paginated(
            store,
            page_size=2,
            scan_since="2026-03-01",
            scan_until="2026-03-31",
        )
        self.assertEqual(data.get("ids"), ["b"])

    def test_parse_qa_fields(self):
        q, a = view_memory._parse_qa_fields(
            "[2026-03-31 10:00] 用户问: 期权怎么做对冲\nAI回答: 先明确风险预算。"
        )
        self.assertIn("期权怎么做对冲", q)
        self.assertIn("先明确风险预算", a)

    def test_parse_qa_fields_prefers_answer_snippet(self):
        q, a = view_memory._parse_qa_fields(
            "用户问: 牛市价差策略是什么\nAI回答: 【结构化摘要】这是摘要【回答片段】这是回答片段"
        )
        self.assertEqual(q, "牛市价差策略是什么")
        self.assertEqual(a, "这是回答片段")

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

    def test_filter_rows_default_order_is_asc(self):
        rows = [
            {
                "user_id": "u1",
                "time": "2026-03-31 10:00",
                "ts": datetime(2026, 3, 31, 10, 0, 0),
                "question": "new",
                "answer": "new",
                "raw": "",
            },
            {
                "user_id": "u1",
                "time": "2026-03-30 10:00",
                "ts": datetime(2026, 3, 30, 10, 0, 0),
                "question": "old",
                "answer": "old",
                "raw": "",
            },
        ]
        out = view_memory._filter_rows(rows, user="u1", limit=10)
        self.assertEqual([x["question"] for x in out], ["old", "new"])

        out_desc = view_memory._filter_rows(rows, user="u1", limit=10, order="desc")
        self.assertEqual([x["question"] for x in out_desc], ["new", "old"])

    def test_filter_rows_asc_limit_keeps_newest_window(self):
        base = datetime(2026, 3, 1, 0, 0, 0)
        rows = []
        for i in range(1, 301):
            ts = base + timedelta(seconds=i)
            rows.append(
                {
                    "user_id": "u1",
                    "time": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "ts": ts,
                    "question": f"q{i}",
                    "answer": f"a{i}",
                    "raw": "",
                }
            )
        out = view_memory._filter_rows(rows, user="u1", limit=200, order="asc")
        self.assertEqual(len(out), 200)
        self.assertEqual(out[0]["question"], "q101")
        self.assertEqual(out[-1]["question"], "q300")

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

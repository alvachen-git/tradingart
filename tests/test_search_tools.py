import unittest
from unittest.mock import patch

import search_tools


class TestSearchTools(unittest.TestCase):
    def test_precise_finance_query_skips_keyword_optimization(self):
        with patch.object(search_tools, "_optimize_search_query", side_effect=AssertionError("should not optimize")):
            queries = search_tools._build_search_queries("汇川技术最近有什么好消息吗")
        self.assertGreaterEqual(len(queries), 1)
        self.assertIn("汇川技术", queries[0])

    def test_precise_finance_query_builds_multiple_template_queries(self):
        queries = search_tools._build_search_queries("汇川技术的机器人业务，最近有没有好消息")
        self.assertLessEqual(len(queries), 3)
        self.assertTrue(any("机器人业务" in query for query in queries))
        self.assertTrue(any(("最近动态" in query) or ("公告" in query) or ("财报" in query) for query in queries))
        self.assertTrue(queries[0].startswith("site:cninfo.com.cn "))
        self.assertTrue(any(query.startswith("site:eastmoney.com ") for query in queries))

    def test_precise_finance_filing_query_prefers_a_share_domains(self):
        queries = search_tools._build_search_queries("汇川技术最近财报怎么样")
        self.assertEqual(len(queries), 3)
        self.assertTrue(queries[0].startswith("site:cninfo.com.cn "))
        self.assertIn("财报", queries[0])
        self.assertTrue(queries[1].startswith("site:eastmoney.com "))

    def test_generic_query_still_uses_optimizer_and_keeps_fallback_raw_query(self):
        with patch.object(search_tools, "_optimize_search_query", return_value="铝价 走势 原因 2026年4月"):
            queries = search_tools._build_search_queries("最近的铝价格走势原因")
        self.assertEqual(queries[0], "铝价 走势 原因 2026年4月")
        self.assertIn("最近的铝价格走势原因", queries)

    def test_search_web_impl_tries_multiple_queries_until_hit(self):
        with patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2", "q3"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=["📭 未搜索到相关内容。", "命中结果", "不该走到第三次"]):
            answer = search_tools._search_web_impl("汇川技术最近有什么好消息吗")
        self.assertEqual(answer, "命中结果")

    def test_search_web_impl_returns_first_non_empty_fallback_when_all_miss(self):
        with patch.object(search_tools, "ZHIPU_API_KEY", "fake-key"), \
             patch.object(search_tools, "_build_search_queries", return_value=["q1", "q2"]), \
             patch.object(search_tools, "ZhipuAI", return_value=object()), \
             patch.object(search_tools, "_invoke_search_once", side_effect=["暂未查到清晰利好", "📭 未搜索到相关内容。"]):
            answer = search_tools._search_web_impl("汇川技术最近有什么好消息吗")
        self.assertEqual(answer, "暂未查到清晰利好")


if __name__ == "__main__":
    unittest.main()

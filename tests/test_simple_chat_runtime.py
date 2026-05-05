import unittest
from datetime import datetime

from simple_chat_runtime import (
    build_simple_runtime_context,
    format_simple_runtime_context,
    maybe_answer_simple_runtime_question,
)


class TestSimpleChatRuntime(unittest.TestCase):
    def setUp(self):
        self.fixed_now = datetime(2026, 5, 5, 13, 28)

    def test_build_runtime_context_includes_identity_and_time_fields(self):
        out = build_simple_runtime_context(current_user_label="mike0919", now=self.fixed_now)
        self.assertEqual(out["assistant_name"], "爱波塔AI")
        self.assertEqual(out["product_identity"], "你是爱波塔AI，由交易艺术汇团队开发")
        self.assertEqual(out["site_specialty"], "本站更擅长期权、K线、交易知识和市场分析")
        self.assertEqual(out["current_date"], "2026年5月5日")
        self.assertEqual(out["current_time"], "13:28")
        self.assertEqual(out["current_weekday"], "星期二")
        self.assertEqual(out["timezone_label"], "北京时间（Asia/Shanghai）")
        self.assertEqual(out["current_user_label"], "mike0919")

    def test_maybe_answer_runtime_time_question(self):
        runtime_context = build_simple_runtime_context(now=self.fixed_now)
        out = maybe_answer_simple_runtime_question("现在几点", runtime_context)
        self.assertEqual(out, "现在是北京时间（Asia/Shanghai）13:28。")

    def test_maybe_answer_runtime_date_question(self):
        runtime_context = build_simple_runtime_context(now=self.fixed_now)
        out = maybe_answer_simple_runtime_question("今天几号", runtime_context)
        self.assertEqual(out, "今天是北京时间（Asia/Shanghai）2026年5月5日。")

    def test_maybe_answer_runtime_weekday_question(self):
        runtime_context = build_simple_runtime_context(now=self.fixed_now)
        out = maybe_answer_simple_runtime_question("今天星期几", runtime_context)
        self.assertEqual(out, "今天是星期二。")

    def test_format_runtime_context_keeps_prompt_short(self):
        runtime_context = build_simple_runtime_context(current_user_label="访客", now=self.fixed_now)
        out = format_simple_runtime_context(runtime_context)
        self.assertIn("身份：你是爱波塔AI，由交易艺术汇团队开发", out)
        self.assertIn("站点特色：本站更擅长期权、K线、交易知识和市场分析", out)
        self.assertIn("当前时间：2026年5月5日 星期二 13:28", out)


if __name__ == "__main__":
    unittest.main()

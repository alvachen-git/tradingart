import unittest
from datetime import datetime

import agent_core
from simple_chat_runtime import build_simple_runtime_context


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content


class _FakeLLM:
    def __init__(self):
        self.last_prompt = ""
        self.invoke_count = 0

    def invoke(self, prompt: str):
        self.last_prompt = prompt
        self.invoke_count += 1
        return _FakeResponse("好的，我继续展开说明。")


class TestSimpleChatContext(unittest.TestCase):
    def test_simple_chatter_reply_includes_recent_context_for_followup(self):
        fake_llm = _FakeLLM()
        out = agent_core.simple_chatter_reply(
            "详细说明下",
            fake_llm,
            recent_context="用户: 棉花期货交易是不是有季节性\nAI: 有，核心在种植和收获周期。",
            memory_context="",
            is_followup=True,
        )

        self.assertEqual(out, "好的，我继续展开说明。")
        self.assertIn("【近期对话历史】", fake_llm.last_prompt)
        self.assertIn("棉花期货交易是不是有季节性", fake_llm.last_prompt)
        self.assertIn("详细说明下", fake_llm.last_prompt)
        self.assertIn("必须先参考【近期对话历史】", fake_llm.last_prompt)

    def test_simple_chatter_reply_supports_memory_context(self):
        fake_llm = _FakeLLM()
        agent_core.simple_chatter_reply(
            "再展开说说",
            fake_llm,
            recent_context="",
            memory_context="【结构化摘要】用户之前关注过法国大革命的背景和后果",
            is_followup=True,
        )

        self.assertIn("【相关长期记忆】", fake_llm.last_prompt)
        self.assertIn("法国大革命", fake_llm.last_prompt)

    def test_simple_chatter_reply_conversation_memory_query_has_strict_rule(self):
        fake_llm = _FakeLLM()
        agent_core.simple_chatter_reply(
            "记得我们昨天聊了什么吗",
            fake_llm,
            memory_context="【未检索到历史对话记录】没有查到昨天可用的历史对话记录。",
            profile_context="- 常看品种：ETF期权",
            conversation_memory_query=True,
            conversation_memory_label="昨天",
        )

        self.assertIn("【历史查询范围】", fake_llm.last_prompt)
        self.assertIn("昨天", fake_llm.last_prompt)
        self.assertIn("只能基于【近期对话历史】和【相关长期记忆】自然总结", fake_llm.last_prompt)
        self.assertIn("不要用【用户专属画像】替代聊天记录", fake_llm.last_prompt)
        self.assertIn("【未检索到历史对话记录】", fake_llm.last_prompt)

    def test_simple_chatter_reply_supports_profile_context(self):
        fake_llm = _FakeLLM()
        agent_core.simple_chatter_reply(
            "创业板期权怎么看",
            fake_llm,
            profile_context="- 风险偏好：偏保守\n- 常看品种：ETF期权",
        )

        self.assertIn("【用户专属画像】", fake_llm.last_prompt)
        self.assertIn("偏保守", fake_llm.last_prompt)
        self.assertIn("克制自然的个性化", fake_llm.last_prompt)
        self.assertIn("相关时使用，不相关时不要硬提年龄、性别、爱好", fake_llm.last_prompt)
        self.assertIn("当前问题里的明确要求与画像冲突，必须以当前问题为准", fake_llm.last_prompt)

    def test_simple_chatter_reply_includes_focus_slots(self):
        fake_llm = _FakeLLM()
        agent_core.simple_chatter_reply(
            "那你帮我查一下具体是因为什么？",
            fake_llm,
            recent_context="用户: 为什么今晚英特尔涨这么多？\nAI: 可能和业绩预期、行业消息或市场情绪有关。",
            memory_context="",
            is_followup=True,
            focus_entity="英特尔",
            focus_topic="异动原因",
            focus_aspect="",
        )

        self.assertIn("【当前核心实体】", fake_llm.last_prompt)
        self.assertIn("英特尔", fake_llm.last_prompt)
        self.assertIn("异动原因", fake_llm.last_prompt)

    def test_simple_chatter_reply_includes_runtime_context(self):
        fake_llm = _FakeLLM()
        runtime_context = build_simple_runtime_context(current_user_label="访客")
        agent_core.simple_chatter_reply(
            "你是谁",
            fake_llm,
            recent_context="",
            memory_context="",
            is_followup=False,
            runtime_context=runtime_context,
        )

        self.assertIn("【运行时上下文】", fake_llm.last_prompt)
        self.assertIn("你是爱波塔AI，由交易艺术汇团队开发", fake_llm.last_prompt)
        self.assertIn("本站更擅长期权、K线、交易知识和市场分析", fake_llm.last_prompt)

    def test_simple_chatter_reply_answers_time_without_llm_guessing(self):
        fake_llm = _FakeLLM()
        runtime_context = build_simple_runtime_context(now=datetime(2026, 5, 5, 13, 28))
        out = agent_core.simple_chatter_reply(
            "今天几号",
            fake_llm,
            runtime_context=runtime_context,
        )

        self.assertEqual(out, "今天是北京时间（Asia/Shanghai）2026年5月5日。")
        self.assertEqual(fake_llm.invoke_count, 0)


if __name__ == "__main__":
    unittest.main()

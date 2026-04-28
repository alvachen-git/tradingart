import unittest

import agent_core


class _FakeResponse:
    def __init__(self, content: str):
        self.content = content


class _FakeLLM:
    def __init__(self):
        self.last_prompt = ""

    def invoke(self, prompt: str):
        self.last_prompt = prompt
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


if __name__ == "__main__":
    unittest.main()

import json
import unittest

import agent_core
from langchain_core.messages import HumanMessage
from langchain_core.runnables import RunnableLambda

from chat_context_layers import (
    append_chat_trace_event,
    attach_context_layers,
    render_agent_context,
    summarize_context_layers,
)


class _FakeRedis:
    def __init__(self):
        self.values = {}
        self.ttls = {}

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, ttl, value):
        self.values[key] = value
        self.ttls[key] = ttl


class _FakeSupervisorLLM:
    def with_structured_output(self, _schema):
        return RunnableLambda(lambda _prompt_value: agent_core.PlanningOutput(plan=["chatter"], symbol=""))


class TestChatContextLayers(unittest.TestCase):
    def test_render_prefers_context_layers_over_legacy_fields(self):
        payload = {
            "recent_context": "旧近期上下文不应出现",
            "memory_context": "旧长期记忆不应出现",
            "profile_context": "旧画像不应出现",
            "context_layers": [
                {
                    "layer": "recent_turns",
                    "content": "新近期上下文",
                    "source": "session_history",
                    "include_reason": "followup_context",
                    "char_count": 6,
                }
            ],
        }

        rendered = render_agent_context(payload, target="supervisor")

        self.assertIn("新近期上下文", rendered)
        self.assertNotIn("旧近期上下文不应出现", rendered)
        self.assertNotIn("旧长期记忆不应出现", rendered)
        self.assertNotIn("旧画像不应出现", rendered)

    def test_render_falls_back_to_legacy_fields_without_layers(self):
        rendered = render_agent_context(
            {
                "recent_context": "用户: 刚才聊了500ETF",
                "memory_context": "用户长期关注ETF期权",
                "profile_context": "偏保守",
            },
            target="knowledge",
        )

        self.assertIn("【近期对话历史】", rendered)
        self.assertIn("刚才聊了500ETF", rendered)
        self.assertIn("【相关长期记忆】", rendered)
        self.assertIn("ETF期权", rendered)
        self.assertIn("【用户专属画像】", rendered)
        self.assertIn("偏保守", rendered)

    def test_render_empty_context_layers_does_not_fall_back_to_legacy_fields(self):
        rendered = render_agent_context(
            {
                "context_layers": [],
                "recent_context": "旧近期上下文不应回退",
                "memory_context": "旧长期记忆不应回退",
            },
            target="supervisor",
        )

        self.assertEqual(rendered, "无")
        self.assertNotIn("旧近期上下文不应回退", rendered)
        self.assertNotIn("旧长期记忆不应回退", rendered)

    def test_attach_context_layers_marks_profile_as_preference_not_fact(self):
        payload = attach_context_layers(
            {
                "intent_domain": "option",
                "profile_context": "- 风险偏好：偏保守",
            },
            prompt_text="按我的风格讲简单点",
            channel="web",
        )

        summary = summarize_context_layers(payload["context_layers"])
        self.assertEqual(summary[0]["layer"], "profile")
        self.assertEqual(summary[0]["trust"], "preference_not_fact")
        rendered = render_agent_context(payload)
        self.assertIn("不作为行情事实来源", rendered)

    def test_attach_context_layers_includes_link_article_first(self):
        payload = attach_context_layers(
            {
                "intent_domain": "general",
                "link_context": {
                    "ok": True,
                    "url": "https://wallstreetcn.com/articles/3774521",
                    "title": "六氟化钨涨价",
                    "snippet": "六氟化钨涨价，利好含氟电子特气及高纯钨制品企业。",
                    "snippet_len": 28,
                    "source": "url_preprocess",
                },
                "recent_context": "旧上下文",
            },
            prompt_text="根据这篇文章，利好哪些A股",
            channel="mobile",
        )

        self.assertEqual(payload["context_layer_summary"][0]["layer"], "link_article")
        rendered = render_agent_context(payload)
        self.assertIn("【链接文章正文】", rendered)
        self.assertIn("六氟化钨涨价", rendered)
        self.assertLess(rendered.index("【链接文章正文】"), rendered.index("【近期对话历史】"))

    def test_trace_event_writes_summary_only(self):
        fake_redis = _FakeRedis()

        ok = append_chat_trace_event(
            fake_redis,
            "task-1",
            "context_built",
            {
                "layers": [
                    {
                        "layer": "profile",
                        "char_count": 12,
                        "include_reason": "personalization",
                        "content": "完整画像内容不应写入trace",
                    }
                ],
                "profile_context": "这段完整画像不应该由调用方传入trace",
                "nested": {"memory_context": "完整长期记忆也不应写入trace"},
            },
            ttl_seconds=60,
        )

        self.assertTrue(ok)
        raw = fake_redis.values["chat_trace:task-1"]
        events = json.loads(raw)
        self.assertEqual(events[0]["event"], "context_built")
        self.assertEqual(events[0]["layers"][0]["layer"], "profile")
        self.assertNotIn("完整画像", raw)
        self.assertNotIn("完整长期记忆", raw)
        self.assertNotIn("profile_context", raw)
        self.assertNotIn("memory_context", raw)
        self.assertNotIn("content", raw)
        self.assertEqual(fake_redis.ttls["chat_trace:task-1"], 60)

    def test_supervisor_accepts_context_layer_json_braces(self):
        result = agent_core.supervisor_node(
            {
                "user_query": "刚才那个策略按我的风格讲简单点",
                "messages": [HumanMessage(content="刚才那个策略按我的风格讲简单点")],
                "is_followup": True,
                "recent_context": "用户: 500ETF牛市价差策略",
                "memory_context": "",
                "profile_context": "",
                "followup_goal": "explain_previous_strategy",
                "followup_action_context": "",
                "followup_task_policy": {"recommended_plan": ["chatter"]},
                "followup_route_context": "",
                "has_portfolio": False,
                "context_layers": [
                    {
                        "layer": "route_policy",
                        "content": '{"context_note": "contains braces that must not become template variables"}',
                        "source": "routing_policy",
                        "include_reason": "route_and_followup_control",
                    }
                ],
            },
            _FakeSupervisorLLM(),
        )

        self.assertEqual(result["plan"], ["chatter"])


if __name__ == "__main__":
    unittest.main()

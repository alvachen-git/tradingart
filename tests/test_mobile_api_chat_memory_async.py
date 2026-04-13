import unittest
import json
import types
from datetime import datetime, timedelta
from unittest.mock import patch, Mock


_IMPORT_ERROR = None
try:
    import mobile_api
except Exception as exc:  # pragma: no cover
    mobile_api = None
    _IMPORT_ERROR = exc


class _FakeRedis:
    def __init__(self):
        self.data = {}

    def setex(self, key, ttl, value):
        self.data[key] = value
        return True

    def get(self, key):
        return self.data.get(key)

    def delete(self, key):
        existed = key in self.data
        self.data.pop(key, None)
        return 1 if existed else 0

    def set(self, key, value, nx=False, ex=None):
        if nx and key in self.data:
            return False
        self.data[key] = value
        return True


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiChatMemoryAsync(unittest.TestCase):
    def test_mobile_prompt_keeps_raw_input(self):
        raw = "请做螺纹钢技术分析并给期权策略"
        self.assertEqual(mobile_api._build_mobile_chat_prompt(raw), raw)

    def test_queue_mobile_chat_memory_is_idempotent(self):
        fake_redis = _FakeRedis()
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api, "_dispatch_mobile_chat_memory_task"
        ) as mocked_dispatch:
            s1 = mobile_api._queue_mobile_chat_memory_persist(
                task_id="task-1",
                username="u1",
                user_prompt="请分析期权策略",
                ai_response="建议先控制仓位。",
            )
            s2 = mobile_api._queue_mobile_chat_memory_persist(
                task_id="task-1",
                username="u1",
                user_prompt="请分析期权策略",
                ai_response="建议先控制仓位。",
            )

        self.assertEqual(s1, "queued")
        self.assertEqual(s2, "already_queued")
        self.assertEqual(mocked_dispatch.call_count, 1)

    def test_chat_submit_caches_raw_prompt(self):
        fake_redis = _FakeRedis()
        body = mobile_api.ChatSubmitRequest(prompt="原始问题", history=[])
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.de, "get_user_profile", return_value={}
        ), patch.object(
            mobile_api, "_build_mobile_context_payload", return_value={"is_followup": False}
        ) as mocked_ctx, patch.object(
            mobile_api, "_detect_mobile_has_portfolio", return_value=False
        ) as mocked_has_portfolio, patch.object(
            mobile_api.TaskManager, "create_task", return_value="task-2"
        ) as mocked_create:
            out = mobile_api.chat_submit(body=body, username="u1")

        self.assertEqual(out["task_id"], "task-2")
        self.assertEqual(fake_redis.get(mobile_api._mobile_chat_prompt_key("task-2")), "原始问题")
        mocked_ctx.assert_called_once()
        mocked_has_portfolio.assert_called_once_with("u1")
        self.assertEqual(mocked_create.call_args.kwargs.get("prompt"), "原始问题")
        self.assertIn("context_payload", mocked_create.call_args.kwargs)
        self.assertIn("has_portfolio", mocked_create.call_args.kwargs)

    def test_chat_submit_builds_followup_context_from_history(self):
        fake_redis = _FakeRedis()
        history = [
            {"role": "user", "content": "先看黄金技术面"},
            {"role": "assistant", "content": "前面偏震荡。"},
        ]
        body = mobile_api.ChatSubmitRequest(prompt="继续讲讲入场点", history=history)
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.de, "get_user_profile", return_value={}
        ), patch.object(
            mobile_api, "_detect_mobile_has_portfolio", return_value=False
        ), patch.object(
            mobile_api.TaskManager, "create_task", return_value="task-2"
        ) as mocked_create:
            out = mobile_api.chat_submit(body=body, username="u1")

        self.assertEqual(out["task_id"], "task-2")
        ctx = mocked_create.call_args.kwargs.get("context_payload") or {}
        self.assertTrue(ctx.get("is_followup"))
        self.assertIn("用户: 先看黄金技术面", ctx.get("recent_context", ""))

    def test_mobile_context_cross_domain_does_not_inject_recent_or_memory(self):
        history = [
            {"role": "user", "content": "我的股票持仓要不要调仓"},
            {"role": "assistant", "content": "你当前股票持仓较分散。"},
        ]
        out = mobile_api._build_mobile_context_payload(
            prompt_text="创业板期权持仓怎么调？",
            current_user="u1",
            history=history,
        )
        self.assertEqual(out.get("intent_domain"), "option")
        self.assertEqual(out.get("recent_domain"), "stock_portfolio")
        self.assertEqual(out.get("recent_context"), "")
        self.assertEqual(out.get("memory_context"), "")

    def test_mobile_context_option_uses_strict_topic_memory_retrieval(self):
        history = [
            {"role": "user", "content": "创业板期权怎么看"},
            {"role": "assistant", "content": "可先看波动率和行权价分布。"},
        ]
        fake_mem = types.SimpleNamespace()
        fake_mem.retrieve_relevant_memory = Mock(return_value="- [2026-04-12 09:00] 用户问: 期权策略\nAI回答: ...")

        with patch.dict("sys.modules", {"memory_utils": fake_mem}):
            out = mobile_api._build_mobile_context_payload(
                prompt_text="创业板期权持仓怎么调？",
                current_user="u1",
                history=history,
            )

        fake_mem.retrieve_relevant_memory.assert_called_once()
        kwargs = fake_mem.retrieve_relevant_memory.call_args.kwargs
        self.assertEqual(kwargs.get("query_topic"), "option")
        self.assertTrue(kwargs.get("strict_topic"))
        self.assertIn("期权", out.get("memory_context", ""))

    def test_mobile_context_extracts_account_total_capital_and_upserts_profile(self):
        with patch.object(mobile_api.de, "parse_account_total_capital", return_value=1200000.0), patch.object(
            mobile_api.de, "upsert_user_account_total_capital", return_value=True
        ) as mocked_upsert:
            out = mobile_api._build_mobile_context_payload(
                prompt_text="我账户总资金120万，创业板期权持仓怎么调？",
                current_user="u1",
                history=[],
                profile={},
            )
        self.assertEqual(out.get("account_total_capital"), 1200000.0)
        mocked_upsert.assert_called_once()

    def test_mobile_context_falls_back_to_profile_capital(self):
        with patch.object(mobile_api.de, "parse_account_total_capital", return_value=None):
            out = mobile_api._build_mobile_context_payload(
                prompt_text="创业板期权持仓怎么调？",
                current_user="u1",
                history=[],
                profile={"account_total_capital": 3000000},
            )
        self.assertEqual(out.get("account_total_capital"), 3000000.0)

    def test_chat_submit_only_keeps_recent_four_history(self):
        fake_redis = _FakeRedis()
        history = [
            {"role": "user", "content": "u1"},
            {"role": "assistant", "content": "a1"},
            {"role": "user", "content": "u2"},
            {"role": "assistant", "content": "a2"},
            {"role": "user", "content": "u3"},
            {"role": "assistant", "content": "a3"},
        ]
        body = mobile_api.ChatSubmitRequest(prompt="分析一下", history=history)
        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.de, "get_user_profile", return_value={}
        ), patch.object(
            mobile_api, "_detect_mobile_has_portfolio", return_value=False
        ), patch.object(
            mobile_api.TaskManager, "create_task", return_value="task-5"
        ) as mocked_create:
            mobile_api.chat_submit(body=body, username="u1")

        sent_history = mocked_create.call_args.kwargs.get("history_messages") or []
        self.assertEqual(len(sent_history), 4)
        self.assertEqual([x["content"] for x in sent_history], ["u2", "a2", "u3", "a3"])

    def test_chat_status_success_dispatches_once_and_cleans_prompt(self):
        fake_redis = _FakeRedis()
        prompt_key = mobile_api._mobile_chat_prompt_key("task-3")
        fake_redis.setex(prompt_key, 7200, "原始问题")
        success_status = {"status": "success", "result": {"response": "AI 回答"}}

        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value=success_status
        ), patch.object(
            mobile_api.TaskManager, "clear_user_pending_task"
        ) as mocked_clear, patch.object(
            mobile_api, "_dispatch_mobile_chat_memory_task"
        ) as mocked_dispatch:
            mobile_api.chat_status(task_id="task-3", username="u1")
            mobile_api.chat_status(task_id="task-3", username="u1")

        self.assertEqual(mocked_dispatch.call_count, 1)
        self.assertIsNone(fake_redis.get(prompt_key))
        self.assertEqual(mocked_clear.call_count, 2)

    def test_chat_status_dispatch_failure_does_not_break_response(self):
        fake_redis = _FakeRedis()
        prompt_key = mobile_api._mobile_chat_prompt_key("task-4")
        fake_redis.setex(prompt_key, 7200, "原始问题")
        success_status = {"status": "success", "result": {"response": "AI 回答"}}

        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value=success_status
        ), patch.object(
            mobile_api.TaskManager, "clear_user_pending_task"
        ) as mocked_clear, patch.object(
            mobile_api, "_dispatch_mobile_chat_memory_task", side_effect=RuntimeError("queue down")
        ):
            out = mobile_api.chat_status(task_id="task-4", username="u1")

        self.assertEqual(out["status"], "success")
        self.assertEqual(fake_redis.get(prompt_key), "原始问题")
        mocked_clear.assert_called_once_with("u1")

    def test_chat_status_uses_cached_success_even_if_celery_pending(self):
        fake_redis = _FakeRedis()
        task_id = "task-cached-success"
        fake_redis.setex(
            mobile_api._mobile_chat_state_key(task_id),
            86400,
            json.dumps(
                {
                    "task_id": task_id,
                    "user_id": "u1",
                    "status": "success",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
            ),
        )
        fake_redis.setex(
            mobile_api._mobile_chat_result_key(task_id),
            86400,
            json.dumps(
                {
                    "task_id": task_id,
                    "user_id": "u1",
                    "result": {"response": "缓存回答"},
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
            ),
        )

        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.TaskManager, "get_task_status", return_value={"status": "pending"}
        ), patch.object(
            mobile_api.TaskManager, "clear_user_pending_task"
        ) as mocked_clear, patch.object(
            mobile_api, "_dispatch_mobile_chat_memory_task"
        ) as mocked_dispatch:
            out = mobile_api.chat_status(task_id=task_id, username="u1")

        self.assertEqual(out["status"], "success")
        self.assertEqual(out["result"]["response"], "缓存回答")
        self.assertEqual(mocked_dispatch.call_count, 0)  # 无 prompt 时不入记忆队列
        mocked_clear.assert_called_once_with("u1")

    def test_chat_status_turns_timeout_when_pending_too_long(self):
        fake_redis = _FakeRedis()
        task_id = "task-timeout"
        old_created = (datetime.now() - timedelta(seconds=10)).isoformat()
        fake_redis.setex(
            mobile_api._mobile_chat_state_key(task_id),
            86400,
            json.dumps(
                {
                    "task_id": task_id,
                    "user_id": "u1",
                    "status": "pending",
                    "created_at": old_created,
                    "updated_at": old_created,
                },
                ensure_ascii=False,
            ),
        )

        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api, "_MOBILE_CHAT_MAX_PENDING_SECONDS", 1
        ), patch.object(
            mobile_api.TaskManager, "clear_user_pending_task"
        ) as mocked_clear:
            out = mobile_api.chat_status(task_id=task_id, username="u1")

        self.assertEqual(out["status"], "error")
        self.assertEqual(out.get("code"), "task_timeout")
        state = json.loads(fake_redis.get(mobile_api._mobile_chat_state_key(task_id)))
        self.assertEqual(state.get("status"), "timeout")
        mocked_clear.assert_called_with("u1")

    def test_chat_pending_returns_terminal_once(self):
        fake_redis = _FakeRedis()
        task_id = "task-pending-terminal"
        fake_redis.setex(
            mobile_api._mobile_chat_last_task_key("u1"),
            86400,
            task_id,
        )
        fake_redis.setex(
            mobile_api._mobile_chat_state_key(task_id),
            86400,
            json.dumps(
                {
                    "task_id": task_id,
                    "user_id": "u1",
                    "status": "success",
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
            ),
        )
        fake_redis.setex(
            mobile_api._mobile_chat_result_key(task_id),
            86400,
            json.dumps(
                {
                    "task_id": task_id,
                    "user_id": "u1",
                    "result": {"response": "ok"},
                    "updated_at": datetime.now().isoformat(),
                },
                ensure_ascii=False,
            ),
        )

        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.TaskManager, "get_user_pending_task", return_value={}
        ):
            first = mobile_api.chat_pending(username="u1")
            second = mobile_api.chat_pending(username="u1")

        self.assertTrue(first.get("has_task"))
        self.assertEqual(first.get("status"), "success")
        self.assertEqual(first.get("result", {}).get("response"), "ok")
        self.assertFalse(second.get("has_task"))

    def test_chat_cancel_marks_canceled_and_clears_last_task(self):
        fake_redis = _FakeRedis()
        task_id = "task-cancel"
        fake_redis.setex(mobile_api._mobile_chat_last_task_key("u1"), 86400, task_id)

        with patch.object(mobile_api, "_redis", fake_redis), patch.object(
            mobile_api.TaskManager, "clear_user_pending_task"
        ) as mocked_clear, patch("celery.result.AsyncResult") as mocked_async_result:
            mocked_async_result.return_value.revoke.return_value = None
            out = mobile_api.chat_cancel(
                body=mobile_api.ChatCancelRequest(task_id=task_id, reason="clear"),
                username="u1",
            )

        self.assertEqual(out["status"], "ok")
        state = json.loads(fake_redis.get(mobile_api._mobile_chat_state_key(task_id)))
        self.assertEqual(state.get("status"), "canceled")
        self.assertIsNone(fake_redis.get(mobile_api._mobile_chat_last_task_key("u1")))
        mocked_clear.assert_called_once_with("u1")


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch


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
            mobile_api.TaskManager, "create_task", return_value="task-2"
        ):
            out = mobile_api.chat_submit(body=body, username="u1")

        self.assertEqual(out["task_id"], "task-2")
        self.assertEqual(fake_redis.get(mobile_api._mobile_chat_prompt_key("task-2")), "原始问题")

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


if __name__ == "__main__":
    unittest.main()

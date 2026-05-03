import types
import unittest
from unittest.mock import patch

import task_manager


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


class _FakeCeleryTask:
    def __init__(self):
        self.calls = []

    def apply_async(self, kwargs=None, task_id=None):
        self.calls.append({"kwargs": kwargs or {}, "task_id": task_id})
        return types.SimpleNamespace(id=task_id)


class TestTaskManagerQueue(unittest.TestCase):
    def test_second_task_is_queued_and_promoted_after_first_finishes(self):
        fake_redis = _FakeRedis()
        fake_analysis = _FakeCeleryTask()
        fake_tasks_module = types.SimpleNamespace(
            process_ai_query=fake_analysis,
            process_knowledge_chat=_FakeCeleryTask(),
        )

        with patch.object(task_manager, "redis_client", fake_redis), patch.dict(
            "sys.modules", {"tasks": fake_tasks_module}
        ):
            task_id_1 = task_manager.TaskManager.create_task(
                user_id="u1",
                prompt="为什么今晚英特尔涨这么多？",
                context_payload={"chat_mode": task_manager.CHAT_MODE_ANALYSIS},
            )
            task_id_2 = task_manager.TaskManager.create_task(
                user_id="u1",
                prompt="为什么最近google涨那么多",
                context_payload={"chat_mode": task_manager.CHAT_MODE_ANALYSIS},
            )

            self.assertEqual(len(fake_analysis.calls), 1)
            self.assertEqual(fake_analysis.calls[0]["task_id"], task_id_1)

            queued_status = task_manager.TaskManager.get_task_status(task_id_2)
            self.assertEqual(queued_status["status"], "queued")
            self.assertEqual(queued_status["queue_ahead"], 1)

            snapshot = task_manager.TaskManager.get_user_task_queue("u1")
            self.assertEqual([item["task_id"] for item in snapshot], [task_id_1, task_id_2])
            self.assertEqual(snapshot[0]["queue_state"], "active")
            self.assertEqual(snapshot[1]["queue_state"], "queued")

            promoted = task_manager.TaskManager.complete_user_task("u1", task_id_1)

            self.assertIsNotNone(promoted)
            self.assertEqual(promoted["task_id"], task_id_2)
            self.assertEqual(len(fake_analysis.calls), 2)
            self.assertEqual(fake_analysis.calls[1]["task_id"], task_id_2)

    def test_queue_limit_rejects_fourth_task(self):
        fake_redis = _FakeRedis()
        fake_analysis = _FakeCeleryTask()
        fake_tasks_module = types.SimpleNamespace(
            process_ai_query=fake_analysis,
            process_knowledge_chat=_FakeCeleryTask(),
        )

        with patch.object(task_manager, "redis_client", fake_redis), patch.dict(
            "sys.modules", {"tasks": fake_tasks_module}
        ):
            task_manager.TaskManager.create_task(
                user_id="u1",
                prompt="q1",
                context_payload={"chat_mode": task_manager.CHAT_MODE_ANALYSIS},
            )
            task_manager.TaskManager.create_task(
                user_id="u1",
                prompt="q2",
                context_payload={"chat_mode": task_manager.CHAT_MODE_ANALYSIS},
            )
            task_manager.TaskManager.create_task(
                user_id="u1",
                prompt="q3",
                context_payload={"chat_mode": task_manager.CHAT_MODE_ANALYSIS},
            )

            with self.assertRaises(task_manager.UserTaskQueueFullError):
                task_manager.TaskManager.create_task(
                    user_id="u1",
                    prompt="q4",
                    context_payload={"chat_mode": task_manager.CHAT_MODE_ANALYSIS},
                )


if __name__ == "__main__":
    unittest.main()

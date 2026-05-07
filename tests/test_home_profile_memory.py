import unittest
from unittest.mock import patch


_IMPORT_ERROR = None
try:
    import Home
except Exception as exc:  # pragma: no cover
    Home = None
    _IMPORT_ERROR = exc


@unittest.skipIf(Home is None, f"Home import failed: {_IMPORT_ERROR}")
class TestHomeProfileMemory(unittest.TestCase):
    def test_build_context_payload_keeps_profile_query_short_circuit(self):
        payload = {
            "profile_context": "- 风险偏好：偏保守",
            "memory_action": "query",
            "confirmation": "你当前记录的风险偏好是：偏保守。",
            "should_short_circuit": True,
            "temporary_overrides": {},
        }
        with patch.object(Home.st, "session_state", {"messages": [], "conversation_id": "test-conv"}), patch.object(
            Home, "build_profile_memory_context", return_value=payload
        ) as mocked_memory, patch.object(
            Home.de, "parse_account_total_capital", return_value=None
        ), patch.object(
            Home.de, "get_user_profile", return_value={}
        ):
            out = Home.build_context_payload("我的风险偏好是什么", "u1")

        self.assertTrue(out.get("profile_memory_should_short_circuit"))
        self.assertEqual(out.get("profile_memory_action"), "query")
        self.assertIn("偏保守", out.get("profile_memory_confirmation", ""))
        self.assertEqual(
            getattr(mocked_memory.call_args.kwargs.get("portfolio_snapshot_loader"), "__name__", ""),
            "get_user_portfolio_snapshot",
        )

    def test_build_context_payload_keeps_portfolio_status_short_circuit(self):
        payload = {
            "profile_context": "",
            "memory_action": "portfolio_status_query",
            "confirmation": "我记得你有一份结构化持仓记录，最近一次识别到 3 只。",
            "should_short_circuit": True,
            "temporary_overrides": {},
        }
        with patch.object(Home.st, "session_state", {"messages": [], "conversation_id": "test-conv"}), patch.object(
            Home, "build_profile_memory_context", return_value=payload
        ), patch.object(
            Home.de, "parse_account_total_capital", return_value=None
        ), patch.object(
            Home.de, "get_user_profile", return_value={}
        ):
            out = Home.build_context_payload("你记得我持仓吗", "u1")

        self.assertTrue(out.get("profile_memory_should_short_circuit"))
        self.assertEqual(out.get("profile_memory_action"), "portfolio_status_query")
        self.assertIn("结构化持仓记录", out.get("profile_memory_confirmation", ""))


if __name__ == "__main__":
    unittest.main()

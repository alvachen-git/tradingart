import sys
import types
import unittest


if "streamlit" not in sys.modules:
    fake_streamlit = types.ModuleType("streamlit")
    sys.modules["streamlit"] = fake_streamlit

import invite_landing


class TestInviteLanding(unittest.TestCase):
    def test_build_invite_landing_copy_for_valid_code(self):
        copy = invite_landing.build_invite_landing_copy(
            {
                "invite_code": "AIBX123456",
                "is_valid": True,
                "inviter_user_id": "mike0919",
                "reward_points": 300,
            }
        )
        self.assertEqual(copy["status_title"], "邀请码已锁定")
        self.assertEqual(copy["headline"], "懂期权实战的AI")
        self.assertEqual(
            copy["subheadline"],
            "爱波塔不是一般大模型，而是受过专业交易训练，能根据行情给出合适的股票、期货、期权交易策略，不是空泛的分析。",
        )
        self.assertEqual(copy["status_note"], "来自 mike0919 的专属邀请。")
        self.assertEqual(copy["invite_code"], "AIBX123456")

    def test_build_invite_landing_copy_for_invalid_code(self):
        copy = invite_landing.build_invite_landing_copy(
            {
                "invite_code": "BADCODE",
                "is_valid": False,
                "inviter_user_id": "",
                "reward_points": 300,
            }
        )
        self.assertEqual(copy["status_title"], "邀请码待确认")
        self.assertIn("请确认链接是否完整", copy["status_note"])


if __name__ == "__main__":
    unittest.main()

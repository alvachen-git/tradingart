import unittest

from followup_task_policy import apply_followup_supervisor_policy


class FollowupSupervisorPolicyTest(unittest.TestCase):
    def test_force_policy_replaces_generalist_plan(self):
        out = apply_followup_supervisor_policy(
            ["generalist"],
            is_followup=True,
            has_context=True,
            followup_task_policy={
                "recommended_plan": ["screener"],
                "override_level": "force",
            },
        )
        self.assertEqual(out, ["screener"])

    def test_suggest_policy_replaces_weak_chatter_plan(self):
        out = apply_followup_supervisor_policy(
            ["chatter"],
            is_followup=True,
            has_context=True,
            followup_task_policy={
                "recommended_plan": ["monitor"],
                "override_level": "suggest",
            },
        )
        self.assertEqual(out, ["monitor"])

    def test_specific_plan_is_not_prepended_with_generalist(self):
        out = apply_followup_supervisor_policy(
            ["analyst", "strategist"],
            is_followup=True,
            has_context=True,
            followup_task_policy={},
        )
        self.assertEqual(out, ["analyst", "strategist"])

    def test_missing_context_asks_chatter_to_clarify(self):
        out = apply_followup_supervisor_policy(
            ["analyst"],
            is_followup=True,
            has_context=False,
            followup_task_policy={},
        )
        self.assertEqual(out, ["chatter"])


if __name__ == "__main__":
    unittest.main()

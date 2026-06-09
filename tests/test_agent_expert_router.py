import unittest

from agent_expert_router import build_route_decision


class ExpertRouterTest(unittest.TestCase):
    def test_empty_plan_falls_back_to_low_confidence_chatter(self):
        decision = build_route_decision(query="help me look at this", plan=[])

        self.assertEqual(decision.plan, ["chatter"])
        self.assertEqual(decision.route_mode, "clarify")
        self.assertLess(decision.confidence, 0.5)
        self.assertEqual(decision.expert_scores[0].name, "chatter")

    def test_market_data_single_expert_route_gets_high_confidence(self):
        decision = build_route_decision(
            query="what is the IV rank",
            plan=["monitor"],
            route_tags=["market_data"],
        )

        self.assertEqual(decision.plan, ["monitor"])
        self.assertEqual(decision.route_mode, "single")
        self.assertGreaterEqual(decision.confidence, 0.9)
        self.assertEqual(decision.expert_scores[0].name, "monitor")
        self.assertIn("data_query_guardrail", decision.expert_scores[0].evidence)

    def test_parallel_experts_are_marked_as_top_k(self):
        decision = build_route_decision(
            query="macro and news context",
            plan=["researcher", "macro_analyst"],
        )

        self.assertEqual(decision.plan, ["researcher", "macro_analyst"])
        self.assertEqual(decision.route_mode, "top_k")
        self.assertEqual(decision.selected_expert_count, 2)
        self.assertGreaterEqual(decision.confidence, 0.7)

    def test_strategy_dependency_route_is_serial_pipeline(self):
        decision = build_route_decision(
            query="gold trend and option strategy",
            plan=["analyst", "researcher", "macro_analyst", "strategist"],
        )

        self.assertEqual(decision.route_mode, "serial_pipeline")
        self.assertEqual(decision.plan[-1], "strategist")
        strategist_score = next(score for score in decision.expert_scores if score.name == "strategist")
        self.assertIn("depends_on_prior_experts", strategist_score.evidence)

    def test_planner_scores_are_preserved_and_normalized(self):
        decision = build_route_decision(
            query="compare assets",
            plan=["generalist"],
            planner_expert_scores={"generalist": 1.7, "monitor": "bad", "unknown": 0.8},
            planner_confidence=0.6,
        )

        self.assertEqual(decision.plan, ["generalist"])
        generalist_score = next(score for score in decision.expert_scores if score.name == "generalist")
        self.assertEqual(generalist_score.score, 1.0)
        self.assertIn("planner_score", generalist_score.evidence)


if __name__ == "__main__":
    unittest.main()

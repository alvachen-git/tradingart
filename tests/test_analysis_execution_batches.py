import unittest

from agent_core import _build_execution_batches


class AnalysisExecutionBatchesTest(unittest.TestCase):
    def test_parallel_batch_for_researcher_and_macro(self):
        self.assertEqual(
            _build_execution_batches(["researcher", "macro_analyst"]),
            [["researcher", "macro_analyst"]],
        )

    def test_parallel_then_strategist(self):
        self.assertEqual(
            _build_execution_batches(["analyst", "monitor", "researcher", "macro_analyst", "strategist"]),
            [["analyst", "monitor", "researcher", "macro_analyst"], ["strategist"]],
        )

    def test_monitor_then_strategist_stays_serial(self):
        self.assertEqual(
            _build_execution_batches(["monitor", "strategist"]),
            [["monitor"], ["strategist"]],
        )

    def test_generalist_stays_single_batch(self):
        self.assertEqual(
            _build_execution_batches(["generalist"]),
            [["generalist"]],
        )

    def test_portfolio_analyst_is_not_parallelized(self):
        self.assertEqual(
            _build_execution_batches(["portfolio_analyst", "researcher"]),
            [["portfolio_analyst"], ["researcher"]],
        )


if __name__ == "__main__":
    unittest.main()

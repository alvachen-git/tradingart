import subprocess
import sys
import unittest
from pathlib import Path

from astock_target_supplements import (
    CSI2000_MISSING_20260430,
    MANDATORY_A_SHARE_SUPPLEMENTS,
    a_share_supplement_targets,
    merge_a_share_targets,
)


ROOT = Path(__file__).resolve().parents[1]


class AStockTargetSupplementTests(unittest.TestCase):
    def test_supplement_targets_are_unique_and_include_zhongtian(self):
        targets = a_share_supplement_targets()

        self.assertEqual(len(targets), len(set(targets)))
        self.assertEqual(len(CSI2000_MISSING_20260430), 1699)
        self.assertIn("600522.SH", MANDATORY_A_SHARE_SUPPLEMENTS)
        self.assertIn("600522.SH", targets)

    def test_merge_keeps_existing_order_then_appends_supplements(self):
        merged = merge_a_share_targets(["000001.SZ", "600522.SH", "000001.SZ"])

        self.assertEqual(merged[0], "000001.SZ")
        self.assertEqual(merged.count("000001.SZ"), 1)
        self.assertEqual(merged.count("600522.SH"), 1)
        self.assertIn(CSI2000_MISSING_20260430[0], merged)

    def test_daily_script_uses_supplement_merge_for_stock_targets(self):
        source = (ROOT / "update_astock_daily.py").read_text()

        self.assertIn("from astock_target_supplements import merge_a_share_targets", source)
        self.assertIn("STOCK_TARGETS = build_stock_targets(STOCK_TARGETS)", source)

    def test_history_supplement_dry_run_does_not_write(self):
        result = subprocess.run(
            [
                sys.executable,
                "update_astock_history.py",
                "--target-set",
                "supplement",
                "--days",
                "1000",
                "--dry-run",
            ],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("[DRY-RUN] target_set=supplement", result.stdout)
        self.assertIn("[DRY-RUN] ETF targets=0", result.stdout)
        self.assertIn("[DRY-RUN] stock targets=1700", result.stdout)
        self.assertIn("[DRY-RUN] includes_600522=True", result.stdout)


if __name__ == "__main__":
    unittest.main()

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]


class RunDaily3MarketClimateTests(unittest.TestCase):
    def test_market_climate_update_is_the_last_isolated_step(self):
        source = (ROOT / "run_daily3.sh").read_text(encoding="utf-8")
        steps = re.findall(
            r'^run_step\s+(\d+)\s+(\d+)\s+"([^"]+)"\s+"([^"]+)"',
            source,
            flags=re.MULTILINE,
        )

        self.assertEqual(len(steps), 11)
        self.assertTrue(all(total == "11" for _, total, _, _ in steps))
        self.assertEqual(
            steps[-1],
            ("11", "11", "更新ETF期权市场环境指标", "update_cn_market_climate_daily.py"),
        )
        self.assertLess(
            source.index('"update_astock_daily.py"'),
            source.index('"update_cn_market_climate_daily.py"'),
        )


if __name__ == "__main__":
    unittest.main()

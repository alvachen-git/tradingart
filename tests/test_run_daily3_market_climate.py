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

    def test_run_daily3_uses_project_venv_and_propagates_failures(self):
        source = (ROOT / "run_daily3.sh").read_text(encoding="utf-8")
        self.assertIn('${APP_DIR}/venv/bin/python', source)
        self.assertIn('${APP_DIR}/.venv311/bin/python', source)
        self.assertNotIn('/usr/bin/python3 "$script"', source)
        self.assertRegex(source, r'if \[ "\$\{FAILED_STEPS\}" -gt 0 \]; then')
        self.assertIn("exit 1", source)

    def test_late_climate_job_has_lock_timeout_and_three_strict_attempts(self):
        source = (ROOT / "run_cn_market_climate_daily.sh").read_text(encoding="utf-8")
        self.assertIn('ATTEMPTS="${CLIMATE_UPDATE_ATTEMPTS:-3}"', source)
        self.assertIn('RETRY_SLEEP_SECONDS="${CLIMATE_RETRY_SLEEP_SECONDS:-600}"', source)
        self.assertIn('TIMEOUT_SECONDS="${CLIMATE_UPDATE_TIMEOUT_SECONDS:-900}"', source)
        self.assertIn("flock -n 9", source)
        self.assertIn("timeout --signal=TERM --kill-after=30", source)
        self.assertIn("update_cn_market_climate_daily.py --require-core-date", source)
        self.assertNotIn('/usr/bin/python3', source)
        self.assertIn('exit "${last_rc}"', source)


if __name__ == "__main__":
    unittest.main()

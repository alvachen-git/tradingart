import unittest
from datetime import datetime

from sqlalchemy import create_engine, text

import kline_game as kg
import kline_trade_analyzer as analyzer


class TestKlineReviewPermissions(unittest.TestCase):
    def setUp(self):
        self.orig_engine = kg.engine
        self.orig_coach = set(kg.REVIEW_COACH_USER_IDS)

        self.engine = create_engine("sqlite:///:memory:", future=True)
        kg.engine = self.engine
        kg.REVIEW_COACH_USER_IDS = {"coach1"}

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE kline_game_records (
                        id INTEGER PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        symbol TEXT,
                        symbol_name TEXT,
                        symbol_type TEXT,
                        leverage INTEGER,
                        profit REAL,
                        profit_rate REAL,
                        trade_count INTEGER,
                        max_drawdown REAL,
                        status TEXT,
                        end_reason TEXT,
                        game_end_time DATETIME
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_records
                    (id, user_id, symbol, symbol_name, symbol_type, leverage, profit, profit_rate,
                     trade_count, max_drawdown, status, end_reason, game_end_time)
                    VALUES
                    (1, 'u1', 'AAA', 'AAA', 'stock', 1, 100, 0.01, 2, 0.03, 'finished', 'completed', :t1),
                    (2, 'u2', 'BBB', 'BBB', 'stock', 1, -50, -0.01, 3, 0.05, 'finished', 'completed', :t2)
                    """
                ),
                {"t1": datetime.now(), "t2": datetime.now()},
            )

        analyzer.ensure_review_tables(self.engine)
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_analysis_reports
                    (game_id, user_id, analysis_version, overall_score, direction_score, risk_score,
                     execution_score, metrics_json, mistakes_json, strengths_json,
                     ai_report_json, ai_status, ai_model, created_at, updated_at)
                    VALUES
                    (2, 'u2', 'v1', 60, 55, 70, 58, '{}', '[]', '[]', '{}', 'rule_only', '', :now, :now)
                    """
                ),
                {"now": datetime.now()},
            )

    def tearDown(self):
        kg.engine = self.orig_engine
        kg.REVIEW_COACH_USER_IDS = self.orig_coach

    def test_non_coach_cannot_query_other_user(self):
        out = kg.list_review_games(viewer_id="u1", target_user="u2", limit=20)
        self.assertFalse(out.get("ok"))
        self.assertIn("permission", str(out.get("message", "")).lower())

    def test_coach_can_query_other_user(self):
        out = kg.list_review_games(viewer_id="coach1", target_user="u2", limit=20)
        self.assertTrue(out.get("ok"))
        self.assertEqual(out.get("target_user"), "u2")
        self.assertGreaterEqual(len(out.get("items") or []), 1)

    def test_non_coach_cannot_generate_other_user_global_ai(self):
        out = kg.generate_user_global_review_ai(viewer_id="u1", target_user="u2", max_trades=2000, force=False)
        self.assertFalse(out.get("ok"))
        self.assertIn("permission", str(out.get("message", "")).lower())


if __name__ == "__main__":
    unittest.main()

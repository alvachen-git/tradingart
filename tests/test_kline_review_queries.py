import unittest
from datetime import date, datetime, timedelta

from sqlalchemy import create_engine, text

import kline_game as kg


class TestKlineReviewQueries(unittest.TestCase):
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
                    CREATE TABLE kline_game_trades (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        game_id INTEGER,
                        user_id TEXT,
                        trade_seq INTEGER,
                        action TEXT,
                        bar_index INTEGER,
                        trade_time DATETIME,
                        bar_date DATE,
                        symbol TEXT,
                        symbol_name TEXT,
                        symbol_type TEXT,
                        price REAL,
                        lots INTEGER,
                        leverage INTEGER,
                        position_before TEXT,
                        position_after TEXT,
                        realized_pnl_after REAL,
                        floating_pnl_after REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE stock_price (
                        ts_code TEXT,
                        trade_date DATE,
                        open_price REAL,
                        high_price REAL,
                        low_price REAL,
                        close_price REAL
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
                    (1, 'u1', 'AAA', 'AAA', 'stock', 10, 800, 0.008, 3, 0.06, 'finished', 'completed', :t)
                    """
                ),
                {"t": datetime.now()},
            )

            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_trades
                    (game_id, user_id, trade_seq, action, bar_index, trade_time, bar_date, symbol, symbol_type,
                     price, lots, leverage, position_before, position_after, realized_pnl_after, floating_pnl_after)
                    VALUES
                    (1, 'u1', 1, 'open_long', 60, :t1, :d1, 'AAA', 'stock', 108, 1, 10,
                     :pb1, :pa1, 0, 0),
                    (1, 'u1', 2, 'add_long', 70, :t2, :d2, 'AAA', 'stock', 110, 1, 10,
                     :pb2, :pa2, 0, -10),
                    (1, 'u1', 3, 'close_long_all', 80, :t3, :d3, 'AAA', 'stock', 114, 2, 10,
                     :pb3, :pa3, 800, 0)
                    """
                ),
                {
                    "t1": datetime.now(),
                    "t2": datetime.now(),
                    "t3": datetime.now(),
                    "d1": date(2024, 2, 20),
                    "d2": date(2024, 3, 1),
                    "d3": date(2024, 3, 12),
                    "pb1": '{"direction":null,"lots":0}',
                    "pa1": '{"direction":"long","lots":1}',
                    "pb2": '{"direction":"long","lots":1}',
                    "pa2": '{"direction":"long","lots":2}',
                    "pb3": '{"direction":"long","lots":2}',
                    "pa3": '{"direction":null,"lots":0}',
                },
            )

            start = date(2023, 10, 1)
            for i in range(220):
                d = start + timedelta(days=i)
                o = 95 + i * 0.15
                c = o + 0.35
                conn.execute(
                    text(
                        """
                        INSERT INTO stock_price
                        (ts_code, trade_date, open_price, high_price, low_price, close_price)
                        VALUES (:code, :d, :o, :h, :l, :c)
                        """
                    ),
                    {"code": "AAA", "d": d, "o": o, "h": c + 0.2, "l": o - 0.2, "c": c},
                )

        out = kg.analyze_game_trades(game_id=1, user_id="u1", force=True)
        self.assertTrue(out.get("ok"))

    def tearDown(self):
        kg.engine = self.orig_engine
        kg.REVIEW_COACH_USER_IDS = self.orig_coach

    def test_list_review_games_has_scores(self):
        out = kg.list_review_games(viewer_id="u1", target_user="u1", limit=20)
        self.assertTrue(out.get("ok"))
        self.assertGreaterEqual(len(out.get("items") or []), 1)
        self.assertGreaterEqual(float((out.get("items") or [])[0].get("overall_score") or 0), 0)

    def test_get_review_detail_contains_chart_and_ai(self):
        out = kg.get_review_detail(viewer_id="u1", game_id=1, target_user="u1")
        self.assertTrue(out.get("ok"))
        self.assertGreaterEqual(len(out.get("trades") or []), 3)
        self.assertGreaterEqual(len(out.get("evaluations") or []), 3)
        chart = out.get("chart") or {}
        self.assertGreaterEqual(len(chart.get("bars") or []), 1)
        self.assertGreaterEqual(len(chart.get("trade_markers") or []), 3)
        report = out.get("report") or {}
        self.assertIn("ai_report", report)
        habit = out.get("habit_profile") or {}
        self.assertTrue(habit.get("ok"))


if __name__ == "__main__":
    unittest.main()

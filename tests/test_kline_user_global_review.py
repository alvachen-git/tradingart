import unittest
from datetime import date, datetime, timedelta
from unittest.mock import patch

from sqlalchemy import create_engine, text

import kline_game as kg


class TestKlineUserGlobalReview(unittest.TestCase):
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
                    (1, 'u1', 'AAA', 'AAA', 'stock', 10, 1200, 0.012, 15, 0.05, 'finished', 'completed', :t1),
                    (2, 'u2', 'BBB', 'BBB', 'stock', 10, -300, -0.003, 8, 0.08, 'finished', 'completed', :t2)
                    """
                ),
                {"t1": datetime.now(), "t2": datetime.now()},
            )

            # u1: 15 笔
            for i in range(15):
                seq = i + 1
                d = date(2024, 3, 1) + timedelta(days=i)
                action = "open_long" if i % 3 == 0 else ("add_long" if i % 3 == 1 else "close_long_all")
                pb = '{"direction":"long","lots":1}' if i > 0 else '{"direction":null,"lots":0}'
                pa = '{"direction":"long","lots":1}' if action != "close_long_all" else '{"direction":null,"lots":0}'
                conn.execute(
                    text(
                        """
                        INSERT INTO kline_game_trades
                        (game_id, user_id, trade_seq, action, bar_index, trade_time, bar_date, symbol, symbol_type,
                         price, lots, leverage, position_before, position_after, realized_pnl_after, floating_pnl_after)
                        VALUES
                        (1, 'u1', :seq, :action, :bar_index, :tt, :bd, 'AAA', 'stock',
                         :price, 1, 10, :pb, :pa, :rpnl, :fpnl)
                        """
                    ),
                    {
                        "seq": seq,
                        "action": action,
                        "bar_index": 50 + seq,
                        "tt": datetime.now(),
                        "bd": d,
                        "price": 100 + seq,
                        "pb": pb,
                        "pa": pa,
                        "rpnl": 80 if action == "close_long_all" else 0,
                        "fpnl": -5 if action == "add_long" else 0,
                    },
                )

            # u2: 8 笔
            for i in range(8):
                seq = i + 1
                d = date(2024, 3, 1) + timedelta(days=i)
                action = "open_short" if i % 2 == 0 else "close_short_all"
                pb = '{"direction":"short","lots":1}' if i > 0 else '{"direction":null,"lots":0}'
                pa = '{"direction":"short","lots":1}' if action == "open_short" else '{"direction":null,"lots":0}'
                conn.execute(
                    text(
                        """
                        INSERT INTO kline_game_trades
                        (game_id, user_id, trade_seq, action, bar_index, trade_time, bar_date, symbol, symbol_type,
                         price, lots, leverage, position_before, position_after, realized_pnl_after, floating_pnl_after)
                        VALUES
                        (2, 'u2', :seq, :action, :bar_index, :tt, :bd, 'BBB', 'stock',
                         :price, 1, 10, :pb, :pa, :rpnl, :fpnl)
                        """
                    ),
                    {
                        "seq": seq,
                        "action": action,
                        "bar_index": 70 + seq,
                        "tt": datetime.now(),
                        "bd": d,
                        "price": 120 + seq,
                        "pb": pb,
                        "pa": pa,
                        "rpnl": -40 if action == "close_short_all" else 0,
                        "fpnl": -3 if action == "open_short" else 0,
                    },
                )

            start = date(2023, 9, 1)
            for i in range(260):
                d = start + timedelta(days=i)
                o1 = 90 + i * 0.1
                c1 = o1 + 0.3
                o2 = 110 + i * 0.08
                c2 = o2 - 0.25
                conn.execute(
                    text(
                        """
                        INSERT INTO stock_price
                        (ts_code, trade_date, open_price, high_price, low_price, close_price)
                        VALUES (:code, :d, :o, :h, :l, :c)
                        """
                    ),
                    {"code": "AAA", "d": d, "o": o1, "h": c1 + 0.2, "l": o1 - 0.2, "c": c1},
                )
                conn.execute(
                    text(
                        """
                        INSERT INTO stock_price
                        (ts_code, trade_date, open_price, high_price, low_price, close_price)
                        VALUES (:code, :d, :o, :h, :l, :c)
                        """
                    ),
                    {"code": "BBB", "d": d, "o": o2, "h": o2 + 0.25, "l": c2 - 0.2, "c": c2},
                )

        self.assertTrue(kg.analyze_game_trades(game_id=1, user_id="u1", force=True).get("ok"))
        self.assertTrue(kg.analyze_game_trades(game_id=2, user_id="u2", force=True).get("ok"))

    def tearDown(self):
        kg.engine = self.orig_engine
        kg.REVIEW_COACH_USER_IDS = self.orig_coach

    def test_get_user_global_review_and_radar_range(self):
        out = kg.get_user_global_review(viewer_id="u1", target_user="u1", max_trades=10)
        self.assertTrue(out.get("ok"))
        report = out.get("report") or {}
        self.assertLessEqual(int(report.get("source_trade_count") or 0), 10)
        radar = report.get("radar") or {}
        values = radar.get("values") or []
        self.assertEqual(len(values), 5)
        for v in values:
            self.assertGreaterEqual(float(v), 0.0)
            self.assertLessEqual(float(v), 100.0)

    def test_generate_user_global_review_ai_cached(self):
        fake_ai = {
            "profile_summary": "test",
            "core_habits": [],
            "dimension_diagnosis": [],
            "improvement_plan_7d": [],
            "improvement_plan_30d": [],
            "watchlist_risks": [],
            "representative_cases": [],
        }
        with patch(
            "kline_trade_analyzer._try_generate_user_global_ai_report",
            return_value=(fake_ai, "ai", "mock-model"),
        ) as mocked_ai:
            out = kg.generate_user_global_review_ai(
                viewer_id="u1",
                target_user="u1",
                max_trades=10,
                force=False,
            )
        self.assertTrue(out.get("ok"))
        self.assertFalse(bool(out.get("cached")))
        mocked_ai.assert_called_once()

        with patch("kline_trade_analyzer._try_generate_user_global_ai_report") as mocked_ai2:
            cached = kg.generate_user_global_review_ai(
                viewer_id="u1",
                target_user="u1",
                max_trades=10,
                force=False,
            )
        self.assertTrue(cached.get("ok"))
        self.assertTrue(bool(cached.get("cached")))
        mocked_ai2.assert_not_called()

    def test_non_coach_cannot_generate_other_user_global_ai(self):
        out = kg.generate_user_global_review_ai(
            viewer_id="u1",
            target_user="u2",
            max_trades=20,
            force=False,
        )
        self.assertFalse(out.get("ok"))
        self.assertIn("permission", str(out.get("message") or "").lower())

    def test_coach_can_generate_other_user_global_ai(self):
        fake_ai = {
            "profile_summary": "coach-run",
            "core_habits": [],
            "dimension_diagnosis": [],
            "improvement_plan_7d": [],
            "improvement_plan_30d": [],
            "watchlist_risks": [],
            "representative_cases": [],
        }
        with patch(
            "kline_trade_analyzer._try_generate_user_global_ai_report",
            return_value=(fake_ai, "ai", "mock-model"),
        ):
            out = kg.generate_user_global_review_ai(
                viewer_id="coach1",
                target_user="u2",
                max_trades=20,
                force=True,
            )
        self.assertTrue(out.get("ok"))
        report = out.get("report") or {}
        self.assertEqual(str(report.get("ai_status") or ""), "ai")


if __name__ == "__main__":
    unittest.main()

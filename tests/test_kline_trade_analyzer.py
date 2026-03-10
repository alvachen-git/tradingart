import unittest
from datetime import date, timedelta, datetime
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

import kline_trade_analyzer as analyzer


class TestKlineTradeAnalyzer(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:", future=True)
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
                    (1, 'u1', 'AAA', 'AAA', 'stock', 10, 1000, 0.01, 2, 0.05, 'finished', 'completed', :t)
                    """
                ),
                {"t": datetime.now()},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_records
                    (id, user_id, symbol, symbol_name, symbol_type, leverage, profit, profit_rate,
                     trade_count, max_drawdown, status, end_reason, game_end_time)
                    VALUES
                    (2, 'u1', 'AAA', 'AAA', 'stock', 10, 0, 0, 2, 0.05, 'finished', 'completed', :t)
                    """
                ),
                {"t": datetime.now()},
            )
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_records
                    (id, user_id, symbol, symbol_name, symbol_type, leverage, profit, profit_rate,
                     trade_count, max_drawdown, status, end_reason, game_end_time)
                    VALUES
                    (3, 'u1', 'AAA', 'AAA', 'stock', 10, 0, 0, 2, 0.05, 'finished', 'completed', :t)
                    """
                ),
                {"t": datetime.now()},
            )

            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_trades
                    (game_id, user_id, trade_seq, action, bar_date, symbol, symbol_type, price, lots,
                     leverage, position_before, position_after, realized_pnl_after, floating_pnl_after)
                    VALUES
                    (1, 'u1', 1, 'open_long', :d1, 'AAA', 'stock', 110, 1, 10,
                     :pb1, :pa1, 0, 0),
                    (1, 'u1', 2, 'close_long_all', :d2, 'AAA', 'stock', 112, 1, 10,
                     :pb2, :pa2, 120, 0)
                    """
                ),
                {
                    "d1": date(2024, 3, 1),
                    "d2": date(2024, 3, 4),
                    "pb1": '{"direction":null,"lots":0}',
                    "pa1": '{"direction":"long","lots":1}',
                    "pb2": '{"direction":"long","lots":1}',
                    "pa2": '{"direction":null,"lots":0}',
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_trades
                    (game_id, user_id, trade_seq, action, bar_date, symbol, symbol_type, price, lots,
                     leverage, position_before, position_after, realized_pnl_after, floating_pnl_after)
                    VALUES
                    (2, 'u1', 1, 'open_long', :d1, 'AAA', 'stock', 110, 1, 10,
                     :pb1, :pa1, 0, 0),
                    (2, 'u1', 2, 'close_long_all', :d2, 'AAA', 'stock', 108, 1, 10,
                     :pb2, :pa2, -20, 0)
                    """
                ),
                {
                    "d1": date(2024, 3, 1),
                    "d2": date(2024, 3, 4),
                    "pb1": '{"direction":null,"lots":0}',
                    "pa1": '{"direction":"long","lots":1}',
                    "pb2": '{"direction":"long","lots":1}',
                    "pa2": '{"direction":null,"lots":0}',
                },
            )
            conn.execute(
                text(
                    """
                    INSERT INTO kline_game_trades
                    (game_id, user_id, trade_seq, action, bar_date, symbol, symbol_type, price, lots,
                     leverage, position_before, position_after, realized_pnl_after, floating_pnl_after)
                    VALUES
                    (3, 'u1', 1, 'open_long', :d1, 'AAA', 'stock', 110, 1, 10,
                     :pb1, :pa1, 0, 0),
                    (3, 'u1', 2, 'add_long', :d2, 'AAA', 'stock', 108, 1, 10,
                     :pb2, :pa2, 0, -20)
                    """
                ),
                {
                    "d1": date(2024, 3, 1),
                    "d2": date(2024, 3, 4),
                    "pb1": '{"direction":null,"lots":0}',
                    "pa1": '{"direction":"long","lots":1}',
                    "pb2": '{"direction":"long","lots":1}',
                    "pa2": '{"direction":"long","lots":2}',
                },
            )

            start = date(2023, 11, 1)
            for i in range(160):
                d = start + timedelta(days=i)
                o = 100 + i * 0.2
                c = o + 0.4
                conn.execute(
                    text(
                        """
                        INSERT INTO stock_price
                        (ts_code, trade_date, open_price, high_price, low_price, close_price)
                        VALUES (:c, :d, :o, :h, :l, :cl)
                        """
                    ),
                    {"c": "AAA", "d": d, "o": o, "h": c + 0.1, "l": o - 0.1, "cl": c},
                )

    def test_analyze_game_and_idempotent(self):
        res1 = analyzer.analyze_game(self.engine, game_id=1, user_id="u1", force=False)
        self.assertTrue(res1.get("ok"))

        res2 = analyzer.analyze_game(self.engine, game_id=1, user_id="u1", force=False)
        self.assertTrue(res2.get("ok"))
        self.assertTrue(res2.get("already"))

        report = analyzer.fetch_report(self.engine, game_id=1)
        self.assertIsNotNone(report)
        self.assertIn("metrics", report)

        evals = analyzer.fetch_evaluations(self.engine, game_id=1)
        self.assertEqual(len(evals), 2)
        self.assertIn("alignment", evals[0])
        self.assertIn("direction_points", evals[0])
        self.assertIn("direction_reasons", evals[0])

    def test_direction_score_rewards_reversal_response(self):
        mocked = [
            {"score": 80, "patterns": [], "trends": []},
            {"score": 20, "patterns": [], "trends": []},
        ]
        with patch("kline_trade_analyzer.calculate_kline_signals", side_effect=mocked):
            res = analyzer.analyze_game(self.engine, game_id=2, user_id="u1", force=True)
        self.assertTrue(res.get("ok"))
        report = analyzer.fetch_report(self.engine, game_id=2)
        self.assertIsNotNone(report)
        self.assertGreater(float(report.get("direction_score") or 0), 50.0)
        metrics = report.get("metrics") or {}
        comps = metrics.get("direction_components") or {}
        self.assertEqual(int(comps.get("reversal_events") or 0), 1)
        self.assertEqual(int(comps.get("reversal_responded") or 0), 1)

    def test_direction_score_penalizes_missed_reversal(self):
        mocked = [
            {"score": 80, "patterns": [], "trends": []},
            {"score": 20, "patterns": [], "trends": []},
        ]
        with patch("kline_trade_analyzer.calculate_kline_signals", side_effect=mocked):
            res = analyzer.analyze_game(self.engine, game_id=3, user_id="u1", force=True)
        self.assertTrue(res.get("ok"))
        report = analyzer.fetch_report(self.engine, game_id=3)
        self.assertIsNotNone(report)
        self.assertLess(float(report.get("direction_score") or 0), 50.0)
        metrics = report.get("metrics") or {}
        comps = metrics.get("direction_components") or {}
        self.assertEqual(int(comps.get("reversal_events") or 0), 1)
        self.assertEqual(int(comps.get("reversal_missed") or 0), 1)

    def test_analyze_game_default_rule_only_without_ai_call(self):
        with patch("kline_trade_analyzer._try_generate_ai_review") as mocked_ai:
            res = analyzer.analyze_game(
                self.engine,
                game_id=1,
                user_id="u1",
                force=True,
                generate_ai=False,
            )
        self.assertTrue(res.get("ok"))
        mocked_ai.assert_not_called()
        report = analyzer.fetch_report(self.engine, game_id=1)
        self.assertIsNotNone(report)
        self.assertEqual(str(report.get("ai_status") or ""), "rule_only")
        ai_report = report.get("ai_report") or {}
        self.assertIn("overall_judgement", ai_report)

    def test_generate_game_ai_review_on_demand(self):
        base = analyzer.analyze_game(
            self.engine,
            game_id=1,
            user_id="u1",
            force=True,
            generate_ai=False,
        )
        self.assertTrue(base.get("ok"))
        with patch(
            "kline_trade_analyzer._try_generate_ai_review",
            return_value=(
                {
                    "overall_judgement": "AI测试",
                    "what_was_right": ["a"],
                    "mistakes_to_fix": [],
                    "next_game_checklist": [],
                    "key_examples": [],
                },
                "ai",
                "mock-model",
            ),
        ) as mocked_ai:
            out = analyzer.generate_game_ai_review(self.engine, game_id=1, user_id="u1", force=False)
        self.assertTrue(out.get("ok"))
        self.assertFalse(bool(out.get("cached")))
        mocked_ai.assert_called_once()
        report = analyzer.fetch_report(self.engine, game_id=1)
        self.assertEqual(str(report.get("ai_status") or ""), "ai")
        self.assertEqual(str(report.get("ai_model") or ""), "mock-model")

        with patch("kline_trade_analyzer._try_generate_ai_review") as mocked_ai2:
            cached = analyzer.generate_game_ai_review(self.engine, game_id=1, user_id="u1", force=False)
        self.assertTrue(cached.get("ok"))
        self.assertTrue(bool(cached.get("cached")))
        mocked_ai2.assert_not_called()

    def _mock_window_df(self):
        return pd.DataFrame({"x": list(range(30))})

    def _mock_breakout_window_df(self):
        start = date(2025, 1, 1)
        rows = []
        for i in range(40):
            d = start + timedelta(days=i)
            o = 100.0 + (i % 3) * 0.2
            h = o + 1.0
            l = o - 1.0
            c = o + 0.1
            rows.append(
                {
                    "trade_date": d,
                    "open_price": o,
                    "high_price": h,
                    "low_price": l,
                    "close_price": c,
                }
            )
        # 最后一根收盘不突破，但交易价可突破前高，模拟盘中突破入场
        rows[-1]["open_price"] = 100.0
        rows[-1]["high_price"] = 102.0
        rows[-1]["low_price"] = 99.0
        rows[-1]["close_price"] = 99.5
        return pd.DataFrame(rows)

    def _mock_down_breakout_window_df(self):
        start = date(2025, 2, 1)
        rows = []
        for i in range(40):
            d = start + timedelta(days=i)
            o = 100.0 + (i % 4) * 0.1
            h = o + 0.8
            l = o - 0.8
            c = o + 0.05
            rows.append(
                {
                    "trade_date": d,
                    "open_price": o,
                    "high_price": h,
                    "low_price": l,
                    "close_price": c,
                }
            )
        rows[-1]["open_price"] = 99.8
        rows[-1]["high_price"] = 100.2
        rows[-1]["low_price"] = 98.9
        rows[-1]["close_price"] = 99.2
        return pd.DataFrame(rows)

    def test_up_breakout_long_plus_short_minus(self):
        sig = {"score": 35, "patterns": ["20日平台突破"], "trends": []}
        with patch("kline_trade_analyzer.calculate_kline_signals", return_value=sig):
            ev_long = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 1,
                    "action": "open_long",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "long", "lots": 1},
                },
                self._mock_window_df(),
            )
            ev_short = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 2,
                    "action": "open_short",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "short", "lots": 1},
                },
                self._mock_window_df(),
            )
        self.assertGreater(float(ev_long.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_long.get("alignment") or ""), "aligned")
        self.assertNotIn("counter_trend_entry", list(ev_long.get("violation_tags") or []))
        self.assertLess(float(ev_short.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_short.get("alignment") or ""), "counter")
        self.assertIn("counter_trend_entry", list(ev_short.get("violation_tags") or []))

    def test_down_breakout_short_plus_long_minus(self):
        sig = {"score": 65, "patterns": ["20日平台跌破"], "trends": []}
        with patch("kline_trade_analyzer.calculate_kline_signals", return_value=sig):
            ev_short = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 1,
                    "action": "open_short",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "short", "lots": 1},
                },
                self._mock_window_df(),
            )
            ev_long = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 2,
                    "action": "open_long",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "long", "lots": 1},
                },
                self._mock_window_df(),
            )
        self.assertGreater(float(ev_short.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_short.get("alignment") or ""), "aligned")
        self.assertLess(float(ev_long.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_long.get("alignment") or ""), "counter")

    def test_bull_trend_bearish_engulf_rule(self):
        sig = {"score": 75, "patterns": ["空头吞噬"], "trends": ["均线多头排列"]}
        with patch("kline_trade_analyzer.calculate_kline_signals", return_value=sig):
            ev_open_long = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 1,
                    "action": "open_long",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "long", "lots": 1},
                },
                self._mock_window_df(),
            )
            ev_close_long = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 2,
                    "action": "close_long_all",
                    "position_before": {"direction": "long", "lots": 1},
                    "position_after": {"direction": None, "lots": 0},
                    "realized_pnl_after": 10,
                },
                self._mock_window_df(),
            )
        self.assertLess(float(ev_open_long.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_open_long.get("alignment") or ""), "counter")
        self.assertGreater(float(ev_close_long.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_close_long.get("alignment") or ""), "risk_control_good")

    def test_bear_trend_bullish_engulf_rule(self):
        sig = {"score": 25, "patterns": ["多头吞噬"], "trends": ["均线空头排列"]}
        with patch("kline_trade_analyzer.calculate_kline_signals", return_value=sig):
            ev_open_short = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 1,
                    "action": "open_short",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "short", "lots": 1},
                },
                self._mock_window_df(),
            )
            ev_close_short = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 2,
                    "action": "close_short_all",
                    "position_before": {"direction": "short", "lots": 1},
                    "position_after": {"direction": None, "lots": 0},
                    "realized_pnl_after": 10,
                },
                self._mock_window_df(),
            )
            ev_open_long = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 3,
                    "action": "open_long",
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "long", "lots": 1},
                },
                self._mock_window_df(),
            )
        self.assertLess(float(ev_open_short.get("direction_points") or 0), 0.0)
        self.assertGreater(float(ev_close_short.get("direction_points") or 0), 0.0)
        self.assertLess(float(ev_open_long.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev_open_short.get("alignment") or ""), "counter")
        self.assertEqual(str(ev_close_short.get("alignment") or ""), "risk_control_good")
        self.assertNotIn("premature_take_profit", list(ev_close_short.get("violation_tags") or []))

    def test_trade_price_breakout_can_override_counter_tag(self):
        sig = {"score": 25, "patterns": [], "trends": ["跌破20日线(中空)", "均线空头排列"]}
        with patch("kline_trade_analyzer.calculate_kline_signals", return_value=sig):
            ev = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 9,
                    "action": "open_long",
                    "price": 102.5,
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "long", "lots": 1},
                },
                self._mock_breakout_window_df(),
            )
        self.assertGreater(float(ev.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev.get("alignment") or ""), "aligned")
        self.assertNotIn("counter_trend_entry", list(ev.get("violation_tags") or []))
        pats = list(ev.get("evidence_patterns") or [])
        self.assertTrue(any("交易价上破" in str(x) for x in pats))

    def test_trade_price_down_breakout_not_penalize_open_long_by_itself(self):
        sig = {"score": 75, "patterns": [], "trends": ["站稳20日线且向上(中多)"]}
        with patch("kline_trade_analyzer.calculate_kline_signals", return_value=sig):
            ev = analyzer._evaluate_one_trade(
                {
                    "trade_seq": 10,
                    "action": "open_long",
                    "price": 98.8,
                    "position_before": {"direction": None, "lots": 0},
                    "position_after": {"direction": "long", "lots": 1},
                },
                self._mock_down_breakout_window_df(),
            )
        # 交易价下破兜底不再单独用于反向扣分（避免震荡误杀）
        self.assertEqual(float(ev.get("direction_points") or 0), 0.0)
        self.assertEqual(str(ev.get("alignment") or ""), "aligned")

    def test_fetch_kline_window_handles_mixed_trade_date_formats(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO stock_price
                    (ts_code, trade_date, open_price, high_price, low_price, close_price)
                    VALUES
                    ('MIX', '20231229', 10, 11, 9, 10.5),
                    ('MIX', '2024-09-12', 20, 21, 19, 20.5),
                    ('MIX', '20240913', 21, 22, 20, 21.5)
                    """
                )
            )
        with self.engine.connect() as conn:
            df = analyzer._fetch_kline_window(
                conn,
                symbol="MIX",
                symbol_type="stock",
                bar_date=date(2024, 9, 13),
                window=10,
            )
        self.assertGreaterEqual(len(df), 2)
        self.assertEqual(df["trade_date"].max().date(), date(2024, 9, 13))


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import MagicMock, patch

import ai_simulation_service as svc
import pandas as pd


class TestAISimulationService(unittest.TestCase):
    def test_normalize_symbol_appends_market_suffix(self):
        self.assertEqual(svc._normalize_symbol("600519"), "600519.SH")
        self.assertEqual(svc._normalize_symbol("000001"), "000001.SZ")
        self.assertEqual(svc._normalize_symbol("830001"), "830001.BJ")
        self.assertEqual(svc._normalize_symbol("000300.SH"), "000300.SH")

    def test_compute_sharpe_ratio_from_nav(self):
        nav_df = pd.DataFrame({"daily_return": [0.01, -0.005, 0.015, 0.0]})
        out = svc.compute_sharpe_ratio_from_nav(nav_df)
        expected = nav_df["daily_return"].mean() / nav_df["daily_return"].std() * (252.0 ** 0.5)
        self.assertAlmostEqual(out, expected)

    @patch("ai_simulation_service.get_latest_data_date")
    @patch("ai_simulation_service._latest_stock_price_date")
    def test_normalize_trade_date_prefers_stock_price_date(self, mock_stock_date, mock_latest_data_date):
        mock_stock_date.return_value = "20260528"
        mock_latest_data_date.return_value = "20260529"

        self.assertEqual(svc._normalize_trade_date(None), "20260528")
        mock_latest_data_date.assert_not_called()

    @patch("ai_simulation_service.pd.read_sql")
    def test_fetch_price_snapshot_keeps_source_trade_date(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "trade_date": "20260528",
                    "name": "Kweichow Moutai",
                    "close_price": 1500.0,
                    "amount": 2.5e9,
                    "vol": 100000.0,
                }
            ]
        )
        fake_engine = MagicMock()
        fake_engine.connect.return_value.__enter__.return_value = MagicMock()
        fake_engine.connect.return_value.__exit__.return_value = False

        with patch("ai_simulation_service.engine", fake_engine):
            out = svc._fetch_price_snapshot(["600519.SH"], "20260529")

        self.assertEqual(out["600519.SH"]["trade_date"], "20260528")
        self.assertEqual(out["600519.SH"]["close"], 1500.0)

    def test_stale_price_symbols_flags_missing_or_old_prices(self):
        price_map = {
            "600519.SH": {"trade_date": "20260529", "close": 1500.0},
            "000001.SZ": {"trade_date": "20260528", "close": 12.0},
            "510300.SH": {"trade_date": "20260529", "close": 0.0},
        }

        out = svc._stale_price_symbols(
            ["600519.SH", "000001.SZ", "510300.SH", "159915.SZ"],
            price_map,
            "20260529",
        )

        self.assertEqual(out, ["000001.SZ", "510300.SH", "159915.SZ"])

    def test_build_closed_trade_extremes_returns_top_three_each_side(self):
        trades_df = pd.DataFrame(
            [
                {"trade_date": "20260321", "symbol": "A", "side": "sell", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": 120},
                {"trade_date": "20260322", "symbol": "B", "side": "sell", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": -80},
                {"trade_date": "20260323", "symbol": "C", "side": "sell", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": 300},
                {"trade_date": "20260324", "symbol": "D", "side": "sell", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": -240},
                {"trade_date": "20260325", "symbol": "E", "side": "sell", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": 90},
                {"trade_date": "20260326", "symbol": "F", "side": "sell", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": -30},
                {"trade_date": "20260327", "symbol": "G", "side": "buy", "quantity": 100, "price": 10, "amount": 1000, "realized_pnl": 999},
            ]
        )

        out = svc.build_closed_trade_extremes(trades_df, limit=2)

        self.assertEqual([x["symbol"] for x in out["top_gains"]], ["C", "A"])
        self.assertEqual([x["symbol"] for x in out["top_losses"]], ["D", "B"])

    def test_apply_risk_gates_enforces_caps_and_universe(self):
        raw_actions = [
            {
                "symbol": "600519.SH",
                "action": "buy",
                "target_weight": 0.45,
                "reason": "high score",
                "confidence": 0.9,
            },
            {
                "symbol": "688001.SH",
                "action": "buy",
                "target_weight": 0.15,
                "reason": "unknown",
                "confidence": 0.8,
            },
            {
                "symbol": "510300.SH",
                "action": "sell",
                "target_weight": 0.0,
                "reason": "risk off",
                "confidence": 0.6,
            },
        ]
        current_weights = {"510300.SH": 0.25}
        candidate_symbols = {"600519.SH", "510300.SH"}
        config = {
            "max_single_weight_hard": 0.30,
            "max_single_weight_soft": 0.20,
            "max_positions": 5,
        }
        csi500_regime = {"regime": "neutral", "score": 0}
        style_map = {"600519.SH": "steady", "510300.SH": "steady", "688001.SH": "aggressive"}
        candidate_score_map = {"600519.SH": 90.0, "510300.SH": 80.0}

        audited, targets, notes = svc._apply_risk_gates(
            raw_actions,
            current_weights,
            candidate_symbols,
            config,
            csi500_regime,
            style_map,
            candidate_score_map,
        )

        status_map = {x["symbol"]: x["gate_status"] for x in audited}
        self.assertEqual(status_map["600519.SH"], "adjusted")
        self.assertEqual(status_map["688001.SH"], "rejected")
        self.assertLessEqual(targets["600519.SH"], 0.20 + 1e-9)
        self.assertTrue("510300.SH" not in targets or targets["510300.SH"] == 0.0)
        self.assertIsInstance(notes, list)

    def test_plan_trades_limits_count_and_turnover(self):
        target_weights = {
            "600519.SH": 0.25,
            "000001.SZ": 0.25,
            "510300.SH": 0.25,
            "159915.SZ": 0.25,
        }
        current_positions = {
            "510300.SH": {"quantity": 5000, "avg_cost": 3.8, "name": "300ETF"},
            "159915.SZ": {"quantity": 3000, "avg_cost": 2.0, "name": "创业板ETF"},
        }
        price_map = {
            "600519.SH": {"close": 1500.0},
            "000001.SZ": {"close": 12.0},
            "510300.SH": {"close": 4.0},
            "159915.SZ": {"close": 2.5},
        }

        trades = svc._plan_trades(
            target_weights=target_weights,
            current_positions=current_positions,
            price_map=price_map,
            nav_prev=1_000_000.0,
            max_daily_trades=2,
            max_turnover_hard=0.10,
            has_prev_day=True,
        )

        self.assertLessEqual(len(trades), 2)
        turnover = sum(float(x["amount"]) for x in trades) / (2 * 1_000_000.0)
        self.assertLessEqual(turnover, 0.10 + 1e-9)
        for t in trades:
            self.assertEqual(t["quantity"] % 100, 0)

    def test_build_review_payload_diary_style_fallback_non_trigger_has_no_persona(self):
        candidates_df = pd.DataFrame(
            [
                {"symbol": "600519.SH", "name": "贵州茅台", "industry": "白酒", "score": 92, "pct_chg": 1.2, "amount": 5.5e9},
                {"symbol": "300750.SZ", "name": "宁德时代", "industry": "新能源", "score": 88, "pct_chg": 0.4, "amount": 4.8e9},
                {"symbol": "601012.SH", "name": "隆基绿能", "industry": "新能源", "score": 85, "pct_chg": -0.3, "amount": 3.1e9},
            ]
        )
        executed_trades = [
            {"symbol": "600519.SH", "side": "buy", "quantity": 100.0, "price": 1678.0, "amount": 167800.0},
            {"symbol": "300750.SZ", "side": "sell", "quantity": 200.0, "price": 188.5, "amount": 37700.0},
        ]
        orders_audited = [
            {"symbol": "600519.SH", "action": "buy", "reason": "资金回流消费龙头，技术形态转强", "gate_status": "passed"},
            {"symbol": "300750.SZ", "action": "sell", "reason": "短线资金分歧扩大，先落袋并降波动", "gate_status": "adjusted"},
        ]
        out = svc._build_review_payload(
            trade_date="20260312",
            nav_prev=1_000_000.0,
            nav_now=1_005_000.0,
            executed_trades=executed_trades,
            orders_audited=orders_audited,
            risk_notes=["单票权重触发软上限，已降权"],
            ai_payload={"summary": "大盘震荡，资金偏好高景气+高现金流龙头", "risk_notes": "追高风险仍在"},
            candidates_df=candidates_df,
            final_positions={"600519.SH": {"quantity": 100.0}},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )

        self.assertIn("复盘日记", out["summary_md"])
        for kw in ["宏观", "政策", "资金流", "板块", "技术面"]:
            self.assertIn(kw, out["summary_md"])
        for kw in ["单身", "运动", "游艇"]:
            self.assertNotIn(kw, out["summary_md"])
        self.assertIn("今天为什么买", out["buys_md"])
        self.assertIn("600519.SH", out["buys_md"])
        self.assertIn("今天为什么卖", out["sells_md"])
        self.assertIn("300750.SZ", out["sells_md"])
        self.assertIn("明天继续盯什么", out["risk_md"])
        self.assertIsInstance(out.get("next_watchlist"), list)

    def test_build_review_payload_persona_trigger_mentions_once(self):
        candidates_df = pd.DataFrame(
            [
                {"symbol": "600519.SH", "name": "贵州茅台", "industry": "白酒", "score": 92, "pct_chg": 1.2, "amount": 5.5e9},
            ]
        )
        out = svc._build_review_payload(
            trade_date="20260313",
            nav_prev=1_000_000.0,
            nav_now=980_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场波动较大", "risk_notes": "短线噪音偏多"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        persona_hits = sum(out["summary_md"].count(k) for k in ["跑", "游艇", "运动", "单身"])
        self.assertGreaterEqual(persona_hits, 1)
        self.assertLessEqual(persona_hits, 4)
        self.assertNotIn("我还是那个单身交易员，喜欢运动，早上跑步、晚上复盘，心里一直惦记着那天能开着自己的游艇去环游世界", out["summary_md"])
        self.assertTrue(any(k in out["summary_md"] for k in ["交了点学费", "并不顺", "吃了点亏"]))

    def test_should_inject_persona_rules(self):
        flag1, reason1 = svc._should_inject_persona(
            trade_date="20260312",
            pnl_pct=0.013,
            executed_trades=[],
            risk_notes=[],
        )
        self.assertTrue(flag1)
        self.assertIn("盈亏", reason1)

        flag2, reason2 = svc._should_inject_persona(
            trade_date="20260312",
            pnl_pct=0.001,
            executed_trades=[],
            risk_notes=["触发止损，先收缩风险"],
        )
        self.assertTrue(flag2)
        self.assertIn("情绪事件", reason2)

        flag3, _ = svc._should_inject_persona(
            trade_date="20260310",
            pnl_pct=0.001,
            executed_trades=[],
            risk_notes=[],
        )
        self.assertFalse(flag3)

    def test_fallback_diary_varies_across_days(self):
        candidates_df = pd.DataFrame(
            [
                {"symbol": "600519.SH", "name": "贵州茅台", "industry": "白酒", "score": 92, "pct_chg": 1.2, "amount": 5.5e9},
                {"symbol": "300750.SZ", "name": "宁德时代", "industry": "新能源", "score": 88, "pct_chg": 0.4, "amount": 4.8e9},
            ]
        )
        out1 = svc._build_review_payload(
            trade_date="20260310",
            nav_prev=1_000_000.0,
            nav_now=999_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场分化", "risk_notes": "控制回撤"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        out2 = svc._build_review_payload(
            trade_date="20260311",
            nav_prev=1_000_000.0,
            nav_now=999_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场分化", "risk_notes": "控制回撤"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        out3 = svc._build_review_payload(
            trade_date="20260312",
            nav_prev=1_000_000.0,
            nav_now=999_000.0,
            executed_trades=[],
            orders_audited=[],
            risk_notes=[],
            ai_payload={"summary": "市场分化", "risk_notes": "控制回撤"},
            candidates_df=candidates_df,
            final_positions={},
            config={"review_use_llm": 0, "model_name": "qwen3.5-plus"},
            tool_calls=[],
        )
        s1, s2, s3 = out1["summary_md"], out2["summary_md"], out3["summary_md"]
        self.assertNotEqual(s1, s2)
        self.assertNotEqual(s2, s3)
        self.assertNotEqual(s1, s3)

    def test_format_recent_trade_memory_uses_latest_five_days(self):
        rows = []
        base_days = ["20260312", "20260311", "20260310", "20260307", "20260306", "20260305"]
        for i, td in enumerate(base_days):
            rows.append(
                {
                    "trade_date": td,
                    "symbol": f"6000{i}0.SH",
                    "side": "buy" if i % 2 == 0 else "sell",
                    "quantity": 1000.0,
                    "price": 10.0 + i,
                    "amount": 100000.0 + i * 1000.0,
                    "reason_short": "测试原因",
                }
            )
        df = pd.DataFrame(rows)
        summary = svc._format_recent_trade_memory(df, max_days=5)
        self.assertIn("2026-03-12", summary)
        self.assertIn("2026-03-06", summary)
        self.assertNotIn("2026-03-05", summary)
        self.assertIn("主要动作", summary)

    @patch("ai_simulation_service._get_previous_nav_row")
    @patch("ai_simulation_service.pd.read_sql")
    def test_load_previous_positions_uses_prev_nav_day_without_stale_fallback(self, mock_read_sql, mock_prev_nav):
        mock_prev_nav.return_value = {"trade_date": "20260327"}
        mock_read_sql.return_value = pd.DataFrame([])

        mock_conn = MagicMock()
        conn_cm = MagicMock()
        conn_cm.__enter__.return_value = mock_conn
        conn_cm.__exit__.return_value = False
        fake_engine = MagicMock()
        fake_engine.connect.return_value = conn_cm

        with patch("ai_simulation_service.engine", fake_engine):
            out = svc._load_previous_positions("official_cn_a_etf_v1", "20260330")

        self.assertEqual(out, {})
        self.assertEqual(mock_read_sql.call_args.kwargs["params"]["td"], "20260327")
        self.assertEqual(mock_read_sql.call_count, 1)

    @patch("ai_simulation_service.pd.read_sql")
    def test_load_previous_positions_reads_exact_prev_trade_date(self, mock_read_sql):
        mock_read_sql.return_value = pd.DataFrame(
            [
                {
                    "symbol": "600519.SH",
                    "name": "贵州茅台",
                    "quantity": 100.0,
                    "avg_cost": 1650.0,
                }
            ]
        )

        mock_conn = MagicMock()
        conn_cm = MagicMock()
        conn_cm.__enter__.return_value = mock_conn
        conn_cm.__exit__.return_value = False
        fake_engine = MagicMock()
        fake_engine.connect.return_value = conn_cm

        with patch("ai_simulation_service.engine", fake_engine):
            out = svc._load_previous_positions(
                "official_cn_a_etf_v1", "20260330", prev_trade_date="20260327"
            )

        self.assertIn("600519.SH", out)
        self.assertEqual(out["600519.SH"]["quantity"], 100.0)
        self.assertEqual(mock_read_sql.call_args.kwargs["params"]["td"], "20260327")

    def _index_box_df(self, last_open=102.0, last_high=103.0, last_low=97.0, last_close=98.0):
        rows = []
        for i in range(39):
            rows.append(
                {
                    "trade_date": f"202603{i + 1:02d}",
                    "open_price": 101.0,
                    "high_price": 104.0,
                    "low_price": 99.0,
                    "close_price": 101.0,
                }
            )
        rows.append(
            {
                "trade_date": "20260410",
                "open_price": last_open,
                "high_price": last_high,
                "low_price": last_low,
                "close_price": last_close,
            }
        )
        return pd.DataFrame(rows)

    def test_detect_compression_breakdown_triggers_on_close_below_box(self):
        out = svc._detect_compression_breakdown(self._index_box_df())
        self.assertTrue(out["triggered"])
        self.assertEqual(out["period"], 5)

    def test_detect_compression_breakdown_ignores_intraday_break_without_close(self):
        out = svc._detect_compression_breakdown(self._index_box_df(last_open=101.0, last_low=97.0, last_close=100.0))
        self.assertFalse(out["triggered"])

    def test_detect_compression_breakdown_requires_large_body(self):
        out = svc._detect_compression_breakdown(self._index_box_df(last_open=98.4, last_high=103.0, last_low=97.0, last_close=98.0))
        self.assertFalse(out["triggered"])

    @patch("ai_simulation_service.pd.read_sql")
    def test_get_csi500_regime_blocks_on_bear_ma_stack(self, mock_read_sql):
        rows = []
        for i in range(80):
            close = 180.0 - i
            rows.append(
                {
                    "trade_date": f"202603{i + 1:02d}",
                    "open_price": close + 0.2,
                    "high_price": close + 1.0,
                    "low_price": close - 1.0,
                    "close_price": close,
                }
            )
        mock_read_sql.return_value = pd.DataFrame(rows)
        conn_cm = MagicMock()
        conn_cm.__enter__.return_value = MagicMock()
        conn_cm.__exit__.return_value = False
        fake_engine = MagicMock()
        fake_engine.connect.return_value = conn_cm

        with patch("ai_simulation_service.engine", fake_engine):
            out = svc._get_csi500_regime("20260430")

        self.assertEqual(out["regime"], "bear")
        self.assertEqual(out["gate"], "blocked")
        self.assertEqual(out["buy_slots"], 0)
        self.assertTrue(out["bear_ma_stack"])

    def test_v2_rule_targets_caps_new_buys_by_regime_slots(self):
        candidates = pd.DataFrame(
            [
                {
                    "symbol": f"60000{i}.SH",
                    "signal_active": 1,
                    "pullback_ready": 1,
                    "chase_ok": 1,
                    "sector_rank": 1,
                    "score": 90 - i,
                    "amount": 10_000_000_000 - i,
                    "stop_price": 9.0,
                }
                for i in range(4)
            ]
        )

        targets, _, _, _, eligible = svc._v2_build_rule_targets(
            current_positions={},
            current_weights={},
            candidates_df=candidates,
            price_map={},
            csi500_regime={"regime": "bull", "buy_slots": 3},
            max_positions=10,
        )
        self.assertEqual(len([v for v in targets.values() if v > 0]), 3)
        self.assertEqual(len(eligible), 3)

        targets2, _, notes2, _, eligible2 = svc._v2_build_rule_targets(
            current_positions={},
            current_weights={},
            candidates_df=candidates,
            price_map={},
            csi500_regime={"regime": "bear", "buy_slots": 0},
            max_positions=10,
        )
        self.assertEqual(targets2, {})
        self.assertEqual(eligible2, set())
        self.assertTrue(any("禁买" in x for x in notes2))


    def test_marginal_flow_sectors_no_longer_require_low_prior_rank(self):
        dates = [f"202604{i + 1:02d}" for i in range(25)]
        rows = []
        for d in dates[:20]:
            rows.extend(
                [
                    {
                        "trade_date": d,
                        "industry": "强关注板块",
                        "sector_type": "行业",
                        "main_net_inflow": 100.0,
                        "medium_net_inflow": 0.0,
                        "total_turnover": 1000.0,
                        "pct_change": 0.1,
                    },
                    {
                        "trade_date": d,
                        "industry": "弱板块",
                        "sector_type": "行业",
                        "main_net_inflow": -20.0,
                        "medium_net_inflow": 0.0,
                        "total_turnover": 1000.0,
                        "pct_change": -0.1,
                    },
                ]
            )
        for d in dates[20:]:
            rows.extend(
                [
                    {
                        "trade_date": d,
                        "industry": "强关注板块",
                        "sector_type": "行业",
                        "main_net_inflow": 150.0,
                        "medium_net_inflow": 0.0,
                        "total_turnover": 1000.0,
                        "pct_change": 0.2,
                    },
                    {
                        "trade_date": d,
                        "industry": "弱板块",
                        "sector_type": "行业",
                        "main_net_inflow": -30.0,
                        "medium_net_inflow": 0.0,
                        "total_turnover": 1000.0,
                        "pct_change": -0.1,
                    },
                ]
            )

        out = svc._rank_marginal_flow_sectors(pd.DataFrame(rows), limit=5)

        self.assertTrue(any(x["industry"] == "强关注板块" for x in out))

    def test_match_v2_sector_rank_uses_concept_keyword_text(self):
        matchers = svc._build_v2_sector_matchers(
            [{"industry": "环氧丙烷", "sector_type": "概念", "rank": 2}]
        )

        rank = svc._match_v2_sector_rank(
            "化工原料",
            matchers,
            ["公司主营环氧丙烷及相关化工材料"],
        )

        self.assertEqual(rank, 2)

    def test_bottom_turn_score_flags_chasing_even_with_breakout(self):
        out = svc._score_v2_bottom_turn(
            "创新高,平台突破",
            "均线多头",
            {
                "ret20": 0.25,
                "ret60": 0.48,
                "drawdown_120d_high": 0.02,
                "position_pct_120d": 0.96,
                "right_confirm": 1,
                "ma10_slope": 0.05,
                "lows_stabilized": 1,
            },
        )

        self.assertEqual(out["anti_chase_flag"], 1)
        self.assertIn("创新高", out["anti_chase_reasons"])
        self.assertLess(out["reversal_signal_score"], svc.V2_MIN_REVERSAL_SIGNAL_SCORE)

    def test_bottom_turn_score_accepts_right_side_bottom_reversal(self):
        out = svc._score_v2_bottom_turn(
            "破底翻,多头吞噬",
            "均线修复",
            {
                "ret20": 0.03,
                "ret60": -0.12,
                "drawdown_120d_high": 0.32,
                "position_pct_120d": 0.28,
                "right_confirm": 1,
                "ma10_slope": 0.02,
                "lows_stabilized": 1,
            },
        )

        self.assertEqual(out["anti_chase_flag"], 0)
        self.assertGreaterEqual(out["bottom_turn_score"], svc.V2_BOTTOM_BUY_SCORE)
        self.assertGreaterEqual(out["reversal_signal_score"], svc.V2_MIN_REVERSAL_SIGNAL_SCORE)


if __name__ == "__main__":
    unittest.main()

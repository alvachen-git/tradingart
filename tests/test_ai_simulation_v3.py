import unittest
import os
from unittest.mock import patch

import pandas as pd
from sqlalchemy import create_engine, text

os.environ.setdefault("DB_HOST", "127.0.0.1")
os.environ.setdefault("DB_PORT", "3306")
os.environ.setdefault("DB_USER", "test")
os.environ.setdefault("DB_PASSWORD", "test")
os.environ.setdefault("DB_NAME", "test")

import ai_simulation_service as svc


def make_ohlc(rows: int = 90) -> pd.DataFrame:
    data = []
    for i in range(rows):
        close = 100 + i * 0.1
        data.append(
            {
                "trade_date": f"2026{i // 30 + 1:02d}{i % 30 + 1:02d}",
                "open_price": close - 0.2,
                "high_price": close + 0.5,
                "low_price": close - 0.5,
                "close_price": close,
            }
        )
    return pd.DataFrame(data)


def make_index_history(trend: str) -> pd.DataFrame:
    rows = []
    for code in svc.V3_MARKET_INDEX_CODES:
        for i in range(80):
            if trend == "strong":
                close = 100 + i * 0.8
            elif trend == "bear":
                close = 180 - i * 0.8
            else:
                close = 100.0
            rows.append({"trade_date": f"2026{i // 30 + 1:02d}{i % 30 + 1:02d}", "ts_code": code, "close_price": close})
    return pd.DataFrame(rows)


class TestAISimulationV3(unittest.TestCase):
    def test_v3_portfolio_identity_and_default_config(self):
        self.assertTrue(svc._is_v3_portfolio(svc.OFFICIAL_PORTFOLIO_3_ID))
        self.assertTrue(svc._is_v3_portfolio("backtest_v3_base_1y"))
        self.assertFalse(svc._is_official_v3_portfolio("backtest_v3_base_1y"))
        self.assertEqual(svc.DEFAULT_CONFIG_V3["portfolio_id"], svc.OFFICIAL_PORTFOLIO_3_ID)
        self.assertEqual(svc.DEFAULT_CONFIG_V3["max_positions"], 10)

    def test_load_config_backtest_v3_inherits_v3_but_keeps_run_id(self):
        class DummyResult:
            def mappings(self):
                return self

            def fetchone(self):
                return None

        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return DummyResult()

        with patch.object(svc.engine, "connect", return_value=DummyConn()):
            config = svc._load_config("backtest_v3_test_run")

        self.assertEqual(config["portfolio_id"], "backtest_v3_test_run")
        self.assertEqual(config["max_positions"], svc.DEFAULT_CONFIG_V3["max_positions"])

    def test_get_trade_dates_between_reads_stock_price_dates(self):
        class DummyResult:
            def fetchall(self):
                return [("20260506",), ("20260507",), ("20260508",)]

        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, _sql, params):
                self.params = params
                return DummyResult()

        conn = DummyConn()
        with patch.object(svc.engine, "connect", return_value=conn):
            dates = svc._get_trade_dates_between("2026-05-08", "2026-05-06")

        self.assertEqual(dates, ["20260506", "20260507", "20260508"])
        self.assertEqual(conn.params["start_date"], "20260506")
        self.assertEqual(conn.params["end_date"], "20260508")

    def test_rank_v3_flow_sectors_only_industry_and_mixed_strength(self):
        df = pd.DataFrame(
            [
                {"trade_date": "20260318", "industry": "巨额低强度", "sector_type": "行业", "main_net_inflow": 900, "medium_net_inflow": 0, "total_turnover": 90000, "net_rate": 1},
                {"trade_date": "20260318", "industry": "强度改善", "sector_type": "行业", "main_net_inflow": 150, "medium_net_inflow": 0, "total_turnover": 1500, "net_rate": 10},
                {"trade_date": "20260318", "industry": "概念噪音", "sector_type": "概念", "main_net_inflow": 9999, "medium_net_inflow": 0, "total_turnover": 100, "net_rate": 99},
                {"trade_date": "20260319", "industry": "巨额低强度", "sector_type": "行业", "main_net_inflow": 1000, "medium_net_inflow": 0, "total_turnover": 100000, "net_rate": 1},
                {"trade_date": "20260319", "industry": "强度改善", "sector_type": "行业", "main_net_inflow": 300, "medium_net_inflow": 0, "total_turnover": 1000, "net_rate": 30},
                {"trade_date": "20260319", "industry": "概念噪音", "sector_type": "概念", "main_net_inflow": 9999, "medium_net_inflow": 0, "total_turnover": 100, "net_rate": 99},
            ]
        )

        out = svc._rank_v3_flow_sectors(df, limit=2)

        self.assertEqual(out[0]["industry"], "强度改善")
        self.assertTrue(all(x["sector_type"] == "行业" for x in out))
        self.assertNotIn("概念噪音", [x["industry"] for x in out])

    @patch("ai_simulation_service.kline_algo.calculate_kline_signals")
    def test_v3_sector_breakout_requires_positive_breakout(self, mock_signals):
        mock_signals.return_value = {
            "patterns": ["20日平台突破"],
            "trends": ["站稳20日线且向上(中多)"],
            "score": 82,
        }

        out = svc._is_v3_sector_breakout(make_ohlc(95))

        self.assertTrue(out["is_breakout"])
        self.assertEqual(out["reason"], "breakout_confirmed")

    @patch("ai_simulation_service.kline_algo.calculate_kline_signals")
    def test_v3_sector_breakout_rejects_bearish_tokens(self, mock_signals):
        mock_signals.return_value = {
            "patterns": ["假突破(诱多)", "20日平台突破"],
            "trends": ["站上5日线(短强)"],
            "score": 90,
        }

        out = svc._is_v3_sector_breakout(make_ohlc(95))

        self.assertFalse(out["is_breakout"])
        self.assertEqual(out["reason"], "no_valid_breakout")

    def test_v3_sector_breakout_rejects_missing_or_short_ohlc(self):
        missing = svc._is_v3_sector_breakout(pd.DataFrame())
        short = svc._is_v3_sector_breakout(make_ohlc(20))

        self.assertFalse(missing["is_breakout"])
        self.assertEqual(missing["reason"], "missing_sector_ohlc")
        self.assertFalse(short["is_breakout"])
        self.assertEqual(short["reason"], "insufficient_sector_ohlc")

    def test_fetch_v3_sector_ohlc_history_rejects_stale_latest_date(self):
        test_engine = create_engine("sqlite:///:memory:")
        with test_engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE sector_index_price (
                      trade_date TEXT NOT NULL,
                      ths_code TEXT NOT NULL,
                      sector_name TEXT NOT NULL,
                      sector_type TEXT NOT NULL,
                      open_price REAL,
                      high_price REAL,
                      low_price REAL,
                      close_price REAL,
                      pct_chg REAL,
                      vol REAL,
                      amount REAL
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO sector_index_price (
                      trade_date, ths_code, sector_name, sector_type,
                      open_price, high_price, low_price, close_price, pct_chg, vol, amount
                    ) VALUES
                    ('20260317', '881001.TI', '半导体', '行业', 10, 11, 9, 10.5, 1, 100, 1000),
                    ('20260318', '881001.TI', '半导体', '行业', 10.5, 12, 10, 11.5, 2, 120, 1200)
                    """
                )
            )

        with patch("ai_simulation_service.engine", test_engine):
            current = svc._fetch_v3_sector_ohlc_history("半导体", "20260318", limit=2)
            stale = svc._fetch_v3_sector_ohlc_history("半导体", "20260319", limit=2)

        self.assertEqual(len(current), 2)
        self.assertTrue(stale.empty)

    def test_v3_market_gate_budget_by_regime(self):
        strong = svc._classify_v3_market_gate(make_index_history("strong"))
        range_state = svc._classify_v3_market_gate(make_index_history("range"))
        bear = svc._classify_v3_market_gate(make_index_history("bear"))

        self.assertEqual(strong["state"], "strong")
        self.assertEqual(strong["buy_budget_pct"], 0.30)
        self.assertEqual(range_state["state"], "range")
        self.assertEqual(range_state["buy_budget_pct"], 0.20)
        self.assertEqual(bear["state"], "bear")
        self.assertEqual(bear["buy_budget_pct"], 0.10)

    def test_v3_new_buy_budget_scales_only_incremental_buys(self):
        target = {"000001.SZ": 0.40, "000002.SZ": 0.20, "000003.SZ": 0.05}
        current_positions = {
            "000001.SZ": {"quantity": 2000},
            "000003.SZ": {"quantity": 1000},
        }
        price_map = {
            "000001.SZ": {"close": 100},
            "000002.SZ": {"close": 100},
            "000003.SZ": {"close": 100},
        }
        adjusted, notes = svc._apply_v3_new_buy_budget(
            target_weights=target,
            current_positions=current_positions,
            price_map=price_map,
            nav_prev=1_000_000,
            market_gate={"buy_budget_pct": 0.10, "summary": "偏空"},
        )

        current_buy_after = (adjusted["000001.SZ"] - 0.20) + adjusted["000002.SZ"]
        self.assertAlmostEqual(current_buy_after, 0.10, places=6)
        self.assertEqual(adjusted["000003.SZ"], 0.05)
        self.assertTrue(notes)

    def test_v3_risk_gate_does_not_apply_csi500_total_cap(self):
        actions = [
            {"symbol": "000001.SZ", "action": "buy", "target_weight": 0.80, "reason": "", "confidence": 0.8}
        ]
        _audited, target, notes = svc._apply_risk_gates(
            raw_actions=actions,
            current_weights={},
            candidate_symbols={"000001.SZ"},
            config={**svc.DEFAULT_CONFIG_V3, "max_single_weight_hard": 1.0, "max_single_weight_soft": 1.0},
            csi500_regime={"regime": "bear", "summary": "中证500偏空"},
            style_map={},
            candidate_score_map={"000001.SZ": 80},
        )

        self.assertAlmostEqual(target["000001.SZ"], 0.80, places=6)
        self.assertFalse(any("中证500" in x for x in notes))

    def test_v3_ai_prompt_uses_hs300_star50_gate_not_csi500(self):
        system_prompt, user_prompt = svc._build_ai_prompt(
            trade_date="20260529",
            nav_prev=1_000_000,
            cash=1_000_000,
            positions={},
            candidates_df=pd.DataFrame(columns=["symbol", "name", "score", "amount", "close"]),
            config=svc.DEFAULT_CONFIG_V3,
            csi500_regime={"summary": "3号大盘状态=强", "buy_budget_pct": 0.30},
            style_map={},
        )

        self.assertIn("沪深300+科创50", system_prompt)
        self.assertIn("3号大盘闸门", user_prompt)
        self.assertNotIn("中证500技术面", user_prompt)

    def _run_minimal_v3_day(
        self,
        *,
        decision_mode: str = "rule",
        generate_review: bool = False,
        config_overrides=None,
        llm_side_effect=None,
    ):
        class DummyResult:
            def scalar(self):
                return None

            def fetchone(self):
                return None

        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, *_args, **_kwargs):
                return DummyResult()

        class RecordingBegin:
            def __init__(self):
                self.statements = []
                self.params = []

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, sql, params=None):
                self.statements.append(str(sql))
                self.params.append(params or {})
                return DummyResult()

        candidate = pd.DataFrame(
            [
                {
                    "symbol": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "score": 90,
                    "amount": 100_000_000,
                    "close": 10,
                    "pct_chg": 0,
                    "from_holdings_fallback": 0,
                }
            ]
        )
        recorder = RecordingBegin()
        patches = [
            patch("ai_simulation_service.ensure_ai_sim_tables"),
            patch("ai_simulation_service._stock_price_row_count", return_value=1),
            patch("ai_simulation_service._load_config", return_value={**svc.DEFAULT_CONFIG_V3, "portfolio_id": "backtest_v3_unit"}),
            patch("ai_simulation_service._get_previous_nav_row", return_value=None),
            patch("ai_simulation_service._load_previous_positions", return_value={}),
            patch("ai_simulation_service._get_csi500_regime", return_value={"regime": "neutral", "summary": "中性"}),
            patch("ai_simulation_service._get_v3_market_gate", return_value={"state": "strong", "state_cn": "强", "buy_budget_pct": 0.30, "summary": "强"}),
            patch("ai_simulation_service._load_recent_trade_memory", return_value=""),
            patch("ai_simulation_service._build_candidate_pool_v3", return_value=(candidate, ["1.银行"])),
            patch("ai_simulation_service._fetch_price_snapshot", return_value={"000001.SZ": {"name": "平安银行", "close": 10, "amount": 100_000_000, "vol": 1000}}),
            patch("ai_simulation_service._stale_price_symbols", return_value=[]),
            patch("ai_simulation_service._apply_v3_position_exit_gates", side_effect=lambda target_weights, audited_actions, **_kwargs: (target_weights, audited_actions, [])),
            patch("ai_simulation_service._save_v2_watchlist"),
            patch("ai_simulation_service._compute_max_drawdown", return_value=0.0),
            patch("ai_simulation_service._compute_benchmark_values", return_value=(1.0, 1.0)),
            patch.object(svc.engine, "connect", return_value=DummyConn()),
            patch.object(svc.engine, "begin", return_value=recorder),
        ]
        if llm_side_effect is None:
            patches.append(patch("ai_simulation_service._generate_ai_actions_with_tools"))
        else:
            patches.append(patch("ai_simulation_service._generate_ai_actions_with_tools", side_effect=llm_side_effect))
        patches.append(patch("ai_simulation_service._build_review_payload"))

        entered = [p.start() for p in patches]
        self.addCleanup(lambda: [p.stop() for p in reversed(patches)])
        result = svc.run_daily_simulation(
            trade_date="20260507",
            portfolio_id="backtest_v3_unit",
            force=False,
            generate_review=generate_review,
            save_watchlist=False,
            decision_mode=decision_mode,
            config_overrides=config_overrides or {},
        )
        return result, recorder, entered[-2], entered[-1]

    def test_run_daily_v3_rule_no_review_skips_llm_and_review_insert(self):
        result, recorder, mock_llm, mock_review = self._run_minimal_v3_day(
            decision_mode="rule",
            generate_review=False,
            config_overrides={"v3_strong_budget": 0.05},
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["decision_mode"], "rule")
        self.assertFalse(result["review_generated"])
        self.assertAlmostEqual(result["cash"], 950000.0, places=2)
        mock_llm.assert_not_called()
        mock_review.assert_not_called()
        self.assertFalse(any("INSERT INTO ai_sim_review_daily" in s for s in recorder.statements))

    def test_run_daily_v3_llm_fallback_uses_rule_when_llm_raises(self):
        result, _recorder, mock_llm, _mock_review = self._run_minimal_v3_day(
            decision_mode="llm_fallback",
            generate_review=False,
            llm_side_effect=RuntimeError("boom"),
        )

        self.assertEqual(result["status"], "success")
        self.assertEqual(result["decision_mode"], "llm_fallback")
        self.assertIn("llm_fallback", result["ai_warning"])
        mock_llm.assert_called_once()

    @patch("ai_simulation_service._fetch_v3_sector_ohlc_history", return_value=pd.DataFrame())
    @patch("ai_simulation_service._get_v3_top_flow_sectors")
    def test_get_v3_breakout_sectors_skips_without_real_ohlc(self, mock_top, _mock_fetch):
        mock_top.return_value = [
            {"industry": "半导体", "sector_type": "行业", "rank": 1, "score": 0.9}
        ]

        out = svc._get_v3_breakout_sectors("20260319")

        self.assertEqual(out, [])

    @patch("ai_simulation_service._is_valid_universe_symbol", return_value=True)
    @patch("ai_simulation_service._fetch_price_snapshot")
    @patch("ai_simulation_service._fetch_profile_match_text", return_value={})
    @patch("ai_simulation_service.pd.read_sql")
    @patch("ai_simulation_service._latest_screener_date", return_value="20260319")
    @patch("ai_simulation_service._get_v3_breakout_sectors")
    def test_build_v3_candidate_pool_takes_top10_per_breakout_sector(
        self,
        mock_breakout,
        _mock_screener_date,
        mock_read_sql,
        _mock_profile,
        mock_price,
        _mock_valid,
    ):
        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        mock_breakout.return_value = [
            {"industry": "半导体", "rank": 1, "score": 0.9, "breakout_score": 82, "breakout_reason": "breakout_confirmed"},
            {"industry": "机器人", "rank": 2, "score": 0.8, "breakout_score": 78, "breakout_reason": "breakout_confirmed"},
        ]

        def make_rows(prefix: str, industry: str, count: int, offset: int = 0) -> pd.DataFrame:
            return pd.DataFrame(
                [
                    {
                        "ts_code": f"{offset + i:06d}.SZ",
                        "name": f"{prefix}{i}",
                        "industry": industry,
                        "score": 70 + i % 5,
                        "close": 10 + i,
                        "pct_chg": 1.0,
                        "pattern": "平台突破",
                        "ma_trend": "多头",
                        "main_net_amount": 1000 - i,
                    }
                    for i in range(1, count + 1)
                ]
            )

        mock_read_sql.return_value = pd.concat(
            [
                make_rows("芯片", "半导体", 12),
                make_rows("机器人", "机器人", 3, offset=100),
            ],
            ignore_index=True,
        )
        mock_price.return_value = {
            f"{i:06d}.SZ": {"name": f"个股{i}", "close": 10 + i, "amount": 1_000_000, "vol": 1000}
            for i in list(range(1, 13)) + list(range(101, 104))
        }

        with patch.object(svc.engine, "connect", return_value=DummyConn()):
            df, notes = svc._build_candidate_pool_v3("20260319", {})

        self.assertEqual(len(df[df["industry"] == "半导体"]), 10)
        self.assertEqual(len(df[df["industry"] == "机器人"]), 3)
        self.assertTrue(notes)
        self.assertTrue((df["from_holdings_fallback"] == 0).all())

    @patch("ai_simulation_service._is_valid_universe_symbol", return_value=True)
    @patch("ai_simulation_service._fetch_price_snapshot")
    @patch("ai_simulation_service._fetch_profile_match_text")
    @patch("ai_simulation_service.pd.read_sql")
    @patch("ai_simulation_service._latest_screener_date", return_value="20260319")
    @patch("ai_simulation_service._get_v3_breakout_sectors")
    def test_build_v3_candidate_pool_matches_sector_by_profile_text(
        self,
        mock_breakout,
        _mock_screener_date,
        mock_read_sql,
        mock_profile,
        mock_price,
        _mock_valid,
    ):
        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        mock_breakout.return_value = [
            {"industry": "通信线缆及配套", "rank": 1, "score": 0.9, "breakout_score": 95}
        ]
        mock_read_sql.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "300502.SZ",
                    "name": "新易盛",
                    "industry": "元器件",
                    "score": 90,
                    "close": 100,
                    "pct_chg": 2.0,
                    "pattern": "",
                    "ma_trend": "",
                    "main_net_amount": 5000,
                }
            ]
        )
        mock_profile.return_value = {"300502.SZ": "主营光通信模块和通信网络设备，属于通信线缆及配套产业链。"}
        mock_price.return_value = {"300502.SZ": {"name": "新易盛", "close": 100, "amount": 1_000_000, "vol": 1000}}

        with patch.object(svc.engine, "connect", return_value=DummyConn()):
            df, _notes = svc._build_candidate_pool_v3("20260319", {})

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["industry"], "通信线缆及配套")

    def test_v3_sector_keywords_drop_generic_tail_words(self):
        self.assertIn("热力", svc._sector_keyword_candidates_v3("热力服务"))
        self.assertNotIn("服务", svc._sector_keyword_candidates_v3("热力服务"))
        self.assertIn("通信", svc._sector_keyword_candidates_v3("通信线缆及配套"))
        self.assertNotIn("配套", svc._sector_keyword_candidates_v3("通信线缆及配套"))
        self.assertIn("钨", svc._sector_keyword_candidates_v3("钨"))

    @patch("ai_simulation_service._is_valid_universe_symbol", return_value=True)
    @patch("ai_simulation_service._fetch_price_snapshot")
    @patch("ai_simulation_service._fetch_profile_match_text", return_value={})
    @patch("ai_simulation_service.pd.read_sql")
    @patch("ai_simulation_service._latest_screener_date", return_value="20260603")
    @patch("ai_simulation_service._get_v3_breakout_sectors")
    def test_build_v3_candidate_pool_matches_single_char_resource_sector(
        self,
        mock_breakout,
        _mock_screener_date,
        mock_read_sql,
        _mock_profile,
        mock_price,
        _mock_valid,
    ):
        class DummyConn:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        mock_breakout.return_value = [
            {"industry": "钨", "rank": 1, "score": 0.9, "breakout_score": 135}
        ]
        mock_read_sql.return_value = pd.DataFrame(
            [
                {
                    "ts_code": "000657.SZ",
                    "name": "中钨高新",
                    "industry": "小金属",
                    "score": 90,
                    "close": 70,
                    "pct_chg": 2.0,
                    "pattern": "5日平台突破",
                    "ma_trend": "均线多头排列",
                    "main_net_amount": 88000,
                }
            ]
        )
        mock_price.return_value = {"000657.SZ": {"name": "中钨高新", "close": 70, "amount": 1_000_000, "vol": 1000}}

        with patch.object(svc.engine, "connect", return_value=DummyConn()):
            df, _notes = svc._build_candidate_pool_v3("20260603", {})

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["industry"], "钨")
        self.assertEqual(df.iloc[0]["symbol"], "000657.SZ")

    @patch("ai_simulation_service.kline_algo.calculate_kline_signals")
    @patch("ai_simulation_service._fetch_v3_sector_ohlc_history")
    @patch("ai_simulation_service._load_v3_position_sector_map")
    def test_v3_position_exit_gate_sells_losing_stock_when_sector_fails(
        self,
        mock_sector_map,
        mock_ohlc,
        mock_signals,
    ):
        mock_sector_map.return_value = {"000001.SZ": "机器人"}
        mock_ohlc.return_value = make_ohlc(95)
        mock_signals.return_value = {
            "patterns": ["假突破", "空头吞噬"],
            "trends": ["跌破20日线"],
            "score": 20,
        }

        adjusted, actions, notes = svc._apply_v3_position_exit_gates(
            target_weights={"000001.SZ": 0.08},
            audited_actions=[],
            current_positions={"000001.SZ": {"quantity": 1000, "avg_cost": 12}},
            price_map={"000001.SZ": {"close": 10}},
            nav_prev=100_000,
            market_gate={"state": "range"},
            portfolio_id=svc.OFFICIAL_PORTFOLIO_3_ID,
            trade_date="20260520",
        )

        self.assertEqual(adjusted["000001.SZ"], 0.0)
        self.assertEqual(actions[0]["action"], "sell")
        self.assertIn("机器人", actions[0]["reason"])
        self.assertTrue(any("假突破" in x for x in notes))

    @patch("ai_simulation_service._load_v3_position_sector_map", return_value={})
    def test_v3_position_exit_gate_sells_losing_stock_in_bear_market(self, _mock_sector_map):
        adjusted, actions, notes = svc._apply_v3_position_exit_gates(
            target_weights={"000002.SZ": 0.095},
            audited_actions=[],
            current_positions={"000002.SZ": {"quantity": 100, "avg_cost": 100}},
            price_map={"000002.SZ": {"close": 95}},
            nav_prev=100_000,
            market_gate={"state": "bear"},
            portfolio_id=svc.OFFICIAL_PORTFOLIO_3_ID,
            trade_date="20260520",
        )

        self.assertEqual(adjusted["000002.SZ"], 0.0)
        self.assertEqual(actions[0]["action"], "sell")
        self.assertIn("偏空", actions[0]["reason"])
        self.assertTrue(notes)


if __name__ == "__main__":
    unittest.main()

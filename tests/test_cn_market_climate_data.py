import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

from cn_market_climate_data import (
    CARD_ORDER,
    CHINEXT_CSI1000_RS,
    CN10Y_RATE,
    IM_BASIS,
    MARKET_AMOUNT,
    MARGIN_LEVERAGE,
    MARGIN_MOMENTUM_5D,
    MarginDailyRecord,
    SZ50_CSI1000_RS,
    OfficialMarginClient,
    ClimateMetricRow,
    build_cn10y_metric_row,
    build_im_basis_metric_row,
    build_margin_metric_rows,
    build_market_amount_metric_row,
    build_relative_strength_metric_rows,
    empirical_percentile,
    ensure_cn_market_climate_tables,
    load_cn_market_climate_strip,
    mark_stale_metrics,
    store_climate_rows,
    store_margin_records,
)
from update_cn_market_climate_daily import (
    fetch_margin_records,
    fetch_sse_margin_mirror,
    fetch_szse_margin_mirror,
    normalize_market_amount_frames,
    validate_szse_mirror_against_official,
)
import pandas as pd


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, payloads):
        self.payloads = payloads
        self.trust_env = True
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append((url, params, headers, timeout))
        day = str(params.get("beginDate") or params.get("txtDate") or "").replace("-", "")
        exchange = "SSE" if "sse.com.cn" in url else "SZSE"
        return _Response(self.payloads.get((exchange, day), {} if exchange == "SSE" else []))


def _sse_payload(day, balance=1_200_000_000_000, buy=50_000_000_000):
    return {"result": [{"opDate": day, "rzye": balance, "rzmre": buy}]}


def _szse_payload(balance_yi=10_000.0, buy_yi=600.0):
    return [
        {
            "metadata": {"tabkey": "tab1"},
            "data": [{"jrrzye": f"{balance_yi:,.2f}", "jrrzmr": f"{buy_yi:,.2f}"}],
        },
        {"metadata": {"tabkey": "tab2"}, "data": []},
    ]


class OfficialMarginClientTests(unittest.TestCase):
    def test_parses_exchange_units_and_disables_environment_proxy(self):
        session = _Session(
            {
                ("SSE", "20260717"): _sse_payload("20260717"),
                ("SZSE", "20260717"): _szse_payload(),
            }
        )
        client = OfficialMarginClient(session=session, attempts=1)

        sse = client.fetch_sse("2026-07-17")
        szse = client.fetch_szse("20260717")

        self.assertFalse(session.trust_env)
        self.assertEqual(sse.financing_balance_yuan, 1_200_000_000_000)
        self.assertEqual(szse.financing_balance_yuan, 1_000_000_000_000)
        self.assertEqual(szse.financing_buy_yuan, 60_000_000_000)
        self.assertTrue(all(call[3] == 10.0 for call in session.calls))

    def test_latest_common_day_does_not_mix_exchange_dates(self):
        session = _Session(
            {
                ("SSE", "20260717"): _sse_payload("20260717"),
                ("SZSE", "20260717"): _szse_payload(balance_yi=0),
                ("SSE", "20260716"): _sse_payload("20260716"),
                ("SZSE", "20260716"): _szse_payload(),
            }
        )
        # Simulate the exchange returning no published rows on 7/17.
        session.payloads[("SZSE", "20260717")] = [
            {"metadata": {"tabkey": "tab1"}, "data": []}
        ]
        client = OfficialMarginClient(session=session, attempts=1)

        result = client.fetch_common_day("20260717", lookback_calendar_days=2)

        self.assertEqual(result[0], "20260716")
        self.assertEqual({row.exchange_id for row in result[1]}, {"SSE", "SZSE"})
        self.assertTrue(all(row.trade_date == "20260716" for row in result[1]))

    def test_sse_range_uses_one_batch_response(self):
        session = _Session(
            {
                (
                    "SSE",
                    "20260716",
                ): {
                    "result": [
                        {"opDate": "20260716", "rzye": 100.0, "rzmre": 10.0},
                        {"opDate": "20260717", "rzye": 110.0, "rzmre": 11.0},
                    ]
                }
            }
        )
        records = OfficialMarginClient(session=session, attempts=1).fetch_sse_range(
            "20260716", "20260717"
        )
        self.assertEqual([record.trade_date for record in records], ["20260716", "20260717"])
        self.assertEqual(len(session.calls), 1)


class ClimateStorageTests(unittest.TestCase):
    def test_tables_and_margin_upsert_are_idempotent(self):
        engine = create_engine("sqlite:///:memory:")
        ensure_cn_market_climate_tables(engine)
        client = OfficialMarginClient(
            session=_Session({("SSE", "20260716"): _sse_payload("20260716")}),
            attempts=1,
        )
        record = client.fetch_sse("20260716")

        self.assertEqual(store_margin_records(engine, [record]), 1)
        self.assertEqual(store_margin_records(engine, [record]), 1)
        with engine.connect() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM cn_margin_daily")).scalar()
        self.assertEqual(count, 1)

    def test_empirical_percentile_includes_current_and_reaches_boundaries(self):
        percentile, count = empirical_percentile([1, 2, 2, 4], 2, min_samples=4)
        self.assertEqual(count, 4)
        self.assertAlmostEqual(percentile, 75.0)
        self.assertEqual(empirical_percentile([1, 2, 3, 4], 4)[0], 100.0)
        self.assertEqual(empirical_percentile([1, 2, 3, 4], 1)[0], 25.0)
        missing, count = empirical_percentile([1, 2], 2, min_samples=3)
        self.assertIsNone(missing)
        self.assertEqual(count, 2)

    def test_builds_all_eight_metrics_and_loads_fixed_card_order(self):
        self.assertEqual(
            list(CARD_ORDER[:3]),
            [MARKET_AMOUNT, MARGIN_MOMENTUM_5D, MARGIN_LEVERAGE],
        )
        dates = pd.bdate_range("2025-01-02", periods=270).strftime("%Y%m%d")
        margin_rows = []
        for index, day in enumerate(dates):
            margin_rows.extend(
                [
                    {"trade_date": day, "exchange_id": "SSE", "financing_balance_yuan": 1.0e12 + index * 1e9},
                    {"trade_date": day, "exchange_id": "SZSE", "financing_balance_yuan": 0.9e12 + index * 0.8e9},
                ]
            )
        float_mv = pd.DataFrame(
            {"trade_date": dates, "float_mv_yuan": [90e12 + i * 10e9 for i in range(len(dates))]}
        )
        margin_metrics = build_margin_metric_rows(pd.DataFrame(margin_rows), float_mv)
        self.assertEqual([row.metric_code for row in margin_metrics], [MARGIN_LEVERAGE, MARGIN_MOMENTUM_5D])

        amount_metric = build_market_amount_metric_row(
            pd.DataFrame({"trade_date": dates, "amount_yuan": [1e12 + i * 1e9 for i in range(len(dates))]})
        )
        self.assertEqual(amount_metric.metric_code, MARKET_AMOUNT)

        index_rows = []
        codes = ["000016.SH", "000852.SH", "000688.SH", "399006.SZ"]
        for i, day in enumerate(dates[-126:]):
            for j, code in enumerate(codes):
                index_rows.append({"trade_date": day, "ts_code": code, "close_price": 1000 + j * 200 + i * (j + 1)})
        relative_metrics = build_relative_strength_metric_rows(pd.DataFrame(index_rows))
        self.assertEqual(len(relative_metrics), 3)
        self.assertIn(CHINEXT_CSI1000_RS, {row.metric_code for row in relative_metrics})

        rate_metric = build_cn10y_metric_row(
            pd.DataFrame({"trade_date": dates, "close_value": [2.0 + i / 1000 for i in range(len(dates))]})
        )
        self.assertEqual(rate_metric.metric_code, CN10Y_RATE)

        basis_dates = dates[-130:]
        basis_metric = build_im_basis_metric_row(
            pd.DataFrame(
                {
                    "trade_date": basis_dates,
                    "contract": ["IM2608"] * len(basis_dates),
                    "futures_close": [7000 + i for i in range(len(basis_dates))],
                    "spot_close": [7050 + i for i in range(len(basis_dates))],
                }
            )
        )
        self.assertEqual(basis_metric.metric_code, IM_BASIS)

        engine = create_engine("sqlite:///:memory:")
        ensure_cn_market_climate_tables(engine)
        all_metrics = margin_metrics + [amount_metric] + relative_metrics + [rate_metric, basis_metric]
        self.assertEqual(store_climate_rows(engine, all_metrics), 8)
        cards = load_cn_market_climate_strip(engine)
        self.assertEqual([card["metric_code"] for card in cards], list(CARD_ORDER))
        self.assertTrue(all(card["as_of"] for card in cards))
        self.assertTrue(all(card["hint"] for card in cards))
        self.assertTrue(all("/" in card["detail"] for card in cards))
        self.assertTrue(all("样本" not in card["hint"] for card in cards))
        self.assertTrue(all("样本" not in card["detail"] for card in cards))
        by_code = {card["metric_code"]: card for card in cards}
        self.assertIn("一起上涨或下跌", by_code[SZ50_CSI1000_RS]["hint"])
        self.assertIn("更强", by_code[SZ50_CSI1000_RS]["detail"])
        self.assertEqual(by_code[IM_BASIS]["value"], f"{basis_metric.percentile:.0f}/100")
        self.assertIn("贴水", by_code[IM_BASIS]["detail"])
        self.assertIn("全部交易日", by_code[IM_BASIS]["hint"])
        self.assertIn("贴水比更多历史交易日更深", by_code[IM_BASIS]["hint"])

    def test_relative_strength_uses_only_common_dates(self):
        rows = [
            {"trade_date": "20260101", "ts_code": "000016.SH", "close_price": 100},
            {"trade_date": "20260102", "ts_code": "000016.SH", "close_price": 102},
            {"trade_date": "20260102", "ts_code": "000852.SH", "close_price": 200},
        ]
        metrics = build_relative_strength_metric_rows(
            pd.DataFrame(rows), lookback_sessions=126, min_samples=1
        )
        sz50 = next(row for row in metrics if row.metric_code == "SZ50_CSI1000_RS")
        self.assertEqual(sz50.trade_date, "20260102")
        self.assertEqual(sz50.sample_count, 1)

    def test_im_basis_requires_minimum_history(self):
        metric = build_im_basis_metric_row(
            pd.DataFrame(
                {
                    "trade_date": ["20260716", "20260717"],
                    "contract": ["IM2607", "IM2608"],
                    "futures_close": [7100, 7120],
                    "spot_close": [7150, 7168],
                }
            ),
            min_samples=120,
        )
        self.assertEqual(metric.quality_status, "insufficient")
        self.assertIsNone(metric.percentile)

    def test_im_basis_percentile_measures_pressure_across_all_data_points(self):
        metric = build_im_basis_metric_row(
            pd.DataFrame(
                {
                    "trade_date": ["20260714", "20260715", "20260716", "20260717"],
                    "contract": ["IM2608"] * 4,
                    "futures_close": [98.0, 100.0, 101.0, 99.0],
                    "spot_close": [100.0] * 4,
                }
            ),
            min_samples=4,
        )

        self.assertAlmostEqual(metric.metric_value, -1.0)
        self.assertAlmostEqual(metric.percentile, 75.0)
        self.assertAlmostEqual(metric.secondary_value, 50.0)
        self.assertEqual(metric.sample_count, 4)
        self.assertEqual(metric.payload["pressure_definition"], "negative_basis_ecdf_all_days")

    def test_market_amount_requires_same_date_and_converts_yi_to_yuan(self):
        result = normalize_market_amount_frames(
            pd.DataFrame(
                {
                    "trade_date": ["20260716", "20260717"],
                    "amount": [6_000.0, 6_100.0],
                }
            ),
            pd.DataFrame(
                {
                    "trade_date": ["20260716"],
                    "amount": [8_000.0],
                }
            ),
        )
        self.assertEqual(result["trade_date"].tolist(), ["20260716"])
        self.assertEqual(result.iloc[0]["amount_yuan"], 1_400_000_000_000)

    def test_marks_data_older_than_two_trading_sessions_stale(self):
        row = ClimateMetricRow(
            trade_date="20260713",
            metric_code=CN10Y_RATE,
            metric_value=1.7,
            percentile=50.0,
            secondary_value=0.0,
            sample_count=300,
            payload={},
            source_dates={"CN10Y": "20260713"},
        )
        marked = mark_stale_metrics(
            [row],
            as_of_date="20260717",
            trading_dates=["20260713", "20260714", "20260715", "20260716", "20260717"],
        )
        self.assertEqual(marked[0].quality_status, "stale")
        self.assertEqual(marked[0].payload["stale_trading_days"], 4)

    def test_szse_history_mirror_keeps_yuan_and_requires_official_overlap(self):
        session = _Session({})
        session.get = lambda *args, **kwargs: _Response(
            {
                "values": {
                    "2026-07-16": [10_000_000_000, 728_856_400_000],
                    "2026-07-17": [11_000_000_000, 730_000_000_000],
                }
            }
        )
        records = fetch_szse_margin_mirror("20260716", "20260717", session=session)
        self.assertFalse(session.trust_env)
        self.assertEqual(records[0].financing_balance_yuan, 728_856_400_000)
        self.assertEqual(records[0].source_name, "jin10_szse_history_validated")

        official = pd.DataFrame(
            {
                "trade_date": ["20260716"] * 20,
                "exchange_id": ["SZSE"] * 20,
                "financing_balance_yuan": [728_856_000_000] * 20,
                "source_name": ["szse_official"] * 20,
            }
        )
        validate_szse_mirror_against_official(records, official, min_overlap=1)
        with self.assertRaisesRegex(RuntimeError, "重合样本不足"):
            validate_szse_mirror_against_official(records, official, min_overlap=21)

    def test_daily_official_failure_uses_same_day_validated_mirrors(self):
        dates = pd.bdate_range("2026-06-15", periods=21).strftime("%Y%m%d").tolist()
        existing_rows = []
        sse_mirror = []
        szse_mirror = []
        for index, day in enumerate(dates):
            sse_balance = 800_000_000_000 + index * 1_000_000
            szse_balance = 700_000_000_000 + index * 1_000_000
            existing_rows.append(
                {
                    "trade_date": day,
                    "exchange_id": "SSE",
                    "financing_balance_yuan": sse_balance,
                    "source_name": "sse_official",
                }
            )
            if index < 20:
                existing_rows.append(
                    {
                        "trade_date": day,
                        "exchange_id": "SZSE",
                        "financing_balance_yuan": szse_balance,
                        "source_name": "szse_official",
                    }
                )
                sse_mirror.append(
                    MarginDailyRecord(day, "SSE", sse_balance, None, "jin10_sse_history_validated")
                )
            szse_mirror.append(
                MarginDailyRecord(day, "SZSE", szse_balance, None, "jin10_szse_history_validated")
            )

        class _FailingClient:
            def fetch_common_day(self, *args, **kwargs):
                raise RuntimeError("official unavailable")

        with patch(
            "update_cn_market_climate_daily.fetch_sse_margin_mirror",
            return_value=sse_mirror,
        ), patch(
            "update_cn_market_climate_daily.fetch_szse_margin_mirror",
            return_value=szse_mirror,
        ):
            records, warnings = fetch_margin_records(
                _FailingClient(),
                trading_dates=[dates[-1]],
                end_date=dates[-1],
                backfill=False,
                existing_margin=pd.DataFrame(existing_rows),
            )

        self.assertEqual({record.exchange_id for record in records}, {"SSE", "SZSE"})
        self.assertTrue(all(record.trade_date == dates[-1] for record in records))
        self.assertEqual(
            {record.quality_status for record in records},
            {"ok", "fallback_validated"},
        )
        self.assertEqual(
            {record.source_name for record in records},
            {"sse_official", "jin10_szse_daily_fallback_validated"},
        )
        self.assertIn("已回退到校验镜像", warnings[0])

    def test_backfill_supplements_sse_dates_missing_from_official_range(self):
        dates = pd.bdate_range("2026-05-01", periods=21).strftime("%Y%m%d").tolist()
        official_dates = dates[1:]
        sse_official = [
            MarginDailyRecord(day, "SSE", 800_000_000_000 + index, None, "sse_official")
            for index, day in enumerate(official_dates)
        ]
        existing_rows = [
            {
                "trade_date": day,
                "exchange_id": "SZSE",
                "financing_balance_yuan": 700_000_000_000 + index,
                "source_name": "szse_official",
            }
            for index, day in enumerate(official_dates)
        ]
        sse_mirror = [
            MarginDailyRecord(
                day,
                "SSE",
                800_000_000_000 + max(index - 1, 0),
                None,
                "jin10_sse_history_validated",
                "mirror_validated",
            )
            for index, day in enumerate(dates)
        ]
        szse_mirror = [
            MarginDailyRecord(
                day,
                "SZSE",
                700_000_000_000 + index,
                None,
                "jin10_szse_history_validated",
                "mirror_validated",
            )
            for index, day in enumerate(official_dates)
        ]

        class _Client:
            def fetch_sse_range(self, *args, **kwargs):
                return sse_official

        with patch(
            "update_cn_market_climate_daily.fetch_sse_margin_mirror",
            return_value=sse_mirror,
        ), patch(
            "update_cn_market_climate_daily.fetch_szse_margin_mirror",
            return_value=szse_mirror,
        ):
            records, warnings = fetch_margin_records(
                _Client(),
                trading_dates=dates,
                end_date=dates[-1],
                backfill=True,
                existing_margin=pd.DataFrame(existing_rows),
                history_start=dates[0],
            )

        early = [record for record in records if record.exchange_id == "SSE" and record.trade_date == dates[0]]
        self.assertEqual(len(early), 1)
        self.assertEqual(early[0].quality_status, "mirror_validated")
        self.assertTrue(any("历史镜像补充1日" in warning for warning in warnings))

    def test_sse_history_mirror_marks_source(self):
        session = _Session({})
        session.get = lambda *args, **kwargs: _Response(
            {"values": {"2026-07-16": [12_000_000_000, 1_430_000_000_000]}}
        )
        records = fetch_sse_margin_mirror("20260716", "20260716", session=session)
        self.assertEqual(records[0].exchange_id, "SSE")
        self.assertEqual(records[0].source_name, "jin10_sse_history_validated")


if __name__ == "__main__":
    unittest.main()

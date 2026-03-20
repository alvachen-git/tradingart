import pandas as pd
import pytest

from update_stock_moneyflow_daily import transform_moneyflow_df, validate_trade_date


def test_validate_trade_date_accepts_compact_and_dash():
    assert validate_trade_date("20260319") == "20260319"
    assert validate_trade_date("2026-03-19") == "20260319"


def test_validate_trade_date_rejects_invalid():
    with pytest.raises(ValueError):
        validate_trade_date("2026/03/19")


def test_transform_moneyflow_df_filters_a_shares_and_computes_fields():
    raw = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "net_mf_amount": 100.0,
                "buy_lg_amount": 50.0,
                "buy_elg_amount": 20.0,
                "sell_lg_amount": 10.0,
                "sell_elg_amount": 5.0,
                "buy_sm_amount": 9.0,
                "buy_md_amount": 6.0,
                "sell_sm_amount": 4.0,
                "sell_md_amount": 3.0,
            },
            {
                "ts_code": "00700.HK",
                "net_mf_amount": 88.0,
                "buy_lg_amount": 5.0,
                "buy_elg_amount": 1.0,
                "sell_lg_amount": 2.0,
                "sell_elg_amount": 1.0,
                "buy_sm_amount": 2.0,
                "buy_md_amount": 2.0,
                "sell_sm_amount": 1.0,
                "sell_md_amount": 1.0,
            },
        ]
    )

    out = transform_moneyflow_df(raw, trade_date="20260319")

    assert len(out) == 1
    row = out.iloc[0].to_dict()
    assert row["trade_date"] == "20260319"
    assert row["ts_code"] == "000001.SZ"
    assert row["main_net_amount"] == pytest.approx(55.0)
    assert row["small_mid_net_amount"] == pytest.approx(8.0)

import hashlib
import json
import os
import time
import warnings

import numpy as np
import pandas as pd
import pytest

import backtest_engine as be


RESULT_FRAME_KEYS = (
    "trades",
    "equity",
    "open_positions",
    "calendar_diag",
    "pick_diag",
    "missing_detail",
)


STRATEGY_CASES = {
    "hold_underlying": {"strategy": "hold_underlying"},
    "single_call_atm": {"strategy": "single_call", "strike_mode": "ATM"},
    "single_put_otm5": {"strategy": "single_put", "strike_mode": "OTM5"},
    "single_sell_call": {"strategy": "single_sell_call", "strike_mode": "ATM"},
    "single_sell_put": {"strategy": "single_sell_put", "strike_mode": "OTM5"},
    "double_buy": {"strategy": "double_buy", "strike_mode": "ATM"},
    "double_sell": {"strategy": "double_sell", "strike_mode": "OTM5"},
    "bull_spread": {"strategy": "bull_spread", "strike_mode": "ATM"},
    "bear_spread": {"strategy": "bear_spread", "strike_mode": "ATM"},
    "calendar_spread": {
        "strategy": "calendar_spread",
        "strike_mode": "ATM",
        "calendar_type": "卖近买远(认购)",
    },
    "manual_call": {
        "strategy": "single_call",
        "strike_mode": "MANUAL",
        "manual_params": {"single_strike": 100.0},
    },
}


EXPECTED_DIGESTS = {
    "hold_underlying": "8c736be1446bcc3f414f2679727e5fac1c606ad4aac54cf56372280f6bba9591",
    "single_call_atm": "3180c6738b7d369fa46f1f5e4eda907c69e0fce68478dcbe0b6cdeb11f58c71b",
    "single_put_otm5": "3b544c314a51f6ae4ba3f3f89df696098edc8f9b6b2e51e008f5cc1df0059bf0",
    "single_sell_call": "4d55c166b9fede69dc9730a738bd638eaaf45de782dd33555b688aea751e06ce",
    "single_sell_put": "a791c45253bd321f62128d8c62506bffebfac8805a65757abde4f7622d9d8501",
    "double_buy": "6bd657ceead28817d7dbeb7b313dad90aa31df6fbdbb4d4b5338a3c1186589db",
    "double_sell": "51712abd38b828c930ac18c65094ce13bb22ef396f8e3c765c0ab31010ccf75a",
    "bull_spread": "b39d1c04a31571b9c75fece9f72abd9a32d3af9db6c3058279a1e9dc74656ada",
    "bear_spread": "cb1aa2a378b6407ad02a4b11639b7e1f83734424b3f6df43c280782a958a4434",
    "calendar_spread": "eef4cf79ca4e14af4bd6726b758c1ee62b0a6194a90228e1918421fe1d75d539",
    "manual_call": "ed15433dad8f55c0c76ae43a6816ecd301f988e4150904b21022e79e149d8527",
}


def _build_option_fixture(num_dates: int = 10, strike_count: int = 5):
    dates = pd.bdate_range("2026-01-02", periods=num_dates)
    date_text = [d.strftime("%Y%m%d") for d in dates]
    expiry_step = 4 if num_dates <= 20 else 20
    expiry_dates = [dates[i] for i in range(expiry_step - 1, len(dates), expiry_step)]
    expiry_dates.extend(
        [
            dates[-1] + pd.Timedelta(days=30),
            dates[-1] + pd.Timedelta(days=60),
            dates[-1] + pd.Timedelta(days=90),
        ]
    )
    expiry_dates = sorted({d.strftime("%Y%m%d") for d in expiry_dates})

    if strike_count <= 5:
        strikes = [90.0, 95.0, 100.0, 105.0, 110.0]
    else:
        half = strike_count // 2
        strikes = [float(100 + (i - half) * 2) for i in range(strike_count)]

    rows = []
    for day_index, trade_date in enumerate(date_text):
        active_expiries = [expiry for expiry in expiry_dates if expiry >= trade_date][:3]
        underlying_price = 100.0 + day_index * 0.35
        for expiry_index, expiry in enumerate(active_expiries):
            for call_put in ("C", "P"):
                for strike in strikes:
                    intrinsic = (
                        max(underlying_price - strike, 0.0)
                        if call_put == "C"
                        else max(strike - underlying_price, 0.0)
                    )
                    time_value = 4.0 + expiry_index * 0.8 + abs(strike - underlying_price) * 0.03
                    direction_move = day_index * (0.06 if call_put == "C" else -0.04)
                    close = max(0.1, intrinsic + time_value + direction_move)
                    strike_label = str(int(strike * 1000))
                    rows.append(
                        {
                            "trade_date": trade_date,
                            "ts_code": f"510300{call_put}{expiry}{strike_label}",
                            "close": round(close, 6),
                            "oi": int(100000 - abs(strike - underlying_price) * 100 + expiry_index * 10),
                            "call_put": call_put,
                            "underlying": "510300.SH",
                            "exercise_price": strike,
                            "delist_date": expiry,
                        }
                    )

    underlying_prices = {
        trade_date: 100.0 + day_index * 0.35
        for day_index, trade_date in enumerate(date_text)
    }
    return pd.DataFrame(rows), underlying_prices, date_text[0], date_text[-1]


def _normalize_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (np.floating, float)):
        normalized = round(float(value), 10)
        return 0.0 if normalized == 0.0 else normalized
    if isinstance(value, (np.integer, int)):
        return int(value)
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    return value


def _result_payload(result: dict) -> dict:
    payload = {
        "summary": {
            str(key): _normalize_value(value)
            for key, value in sorted((result.get("summary") or {}).items())
        },
        "missing_dates": [_normalize_value(value) for value in result.get("missing_dates", [])],
        "no_contract_dates": [_normalize_value(value) for value in result.get("no_contract_dates", [])],
    }
    for key in RESULT_FRAME_KEYS:
        frame = result.get(key)
        if not isinstance(frame, pd.DataFrame):
            frame = pd.DataFrame()
        payload[key] = {
            "columns": [str(column) for column in frame.columns],
            "records": [
                {str(column): _normalize_value(value) for column, value in row.items()}
                for row in frame.to_dict(orient="records")
            ],
        }
    return payload


def _result_digest(result: dict) -> str:
    encoded = json.dumps(
        _result_payload(result),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _run_case(monkeypatch, option_df, underlying_prices, start_date, end_date, config):
    monkeypatch.setattr(be, "engine", object())
    monkeypatch.setattr(be, "_fetch_option_data", lambda *args, **kwargs: option_df.copy())
    monkeypatch.setattr(be, "_fetch_underlying_prices", lambda *args, **kwargs: dict(underlying_prices))
    return be.run_etf_roll_backtest(
        underlying="510300.SH",
        start_date=start_date,
        end_date=end_date,
        fee_per_lot=2.0,
        margin_rate=0.15,
        lots=1,
        **config,
    )


@pytest.mark.parametrize("case_name", tuple(STRATEGY_CASES))
def test_strategy_results_match_preoptimization_golden(monkeypatch, case_name):
    option_df, underlying_prices, start_date, end_date = _build_option_fixture()
    result = _run_case(
        monkeypatch,
        option_df,
        underlying_prices,
        start_date,
        end_date,
        STRATEGY_CASES[case_name],
    )
    assert "error" not in result
    digest = _result_digest(result)
    if os.getenv("GENERATE_BACKTEST_GOLDEN") == "1":
        print(f'    "{case_name}": "{digest}",')
        return
    assert digest == EXPECTED_DIGESTS[case_name]


def test_roll_backtest_has_no_setting_with_copy_warning(monkeypatch):
    option_df, underlying_prices, start_date, end_date = _build_option_fixture()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = _run_case(
            monkeypatch,
            option_df,
            underlying_prices,
            start_date,
            end_date,
            STRATEGY_CASES["double_sell"],
        )
    assert "error" not in result
    setting_warnings = [
        warning
        for warning in caught
        if issubclass(warning.category, pd.errors.SettingWithCopyWarning)
    ]
    assert setting_warnings == []


def test_production_scale_backtest_benchmark(monkeypatch):
    option_df, underlying_prices, start_date, end_date = _build_option_fixture(
        num_dates=180,
        strike_count=21,
    )
    started = time.perf_counter()
    result = _run_case(
        monkeypatch,
        option_df,
        underlying_prices,
        start_date,
        end_date,
        STRATEGY_CASES["double_sell"],
    )
    elapsed_ms = (time.perf_counter() - started) * 1000
    assert "error" not in result
    assert len(result["equity"]) == 180
    print(f"BACKTEST_BENCHMARK rows={len(option_df)} elapsed_ms={elapsed_ms:.3f}")

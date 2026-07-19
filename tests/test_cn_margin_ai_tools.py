import json

import pandas as pd
from sqlalchemy import create_engine

import cn_margin_ai_tools
from cn_margin_ai_tools import (
    CSI1000,
    CSI300,
    MARGIN_LEVERAGE,
    MARGIN_MOMENTUM_5D,
    MARKET_AMOUNT,
    _build_market_confirmation,
    _is_new_high,
    build_cn_margin_market_signal,
    build_cn_margin_market_signal_from_frames,
    classify_cn_margin_signal,
    get_cn_margin_market_signal,
)


def _signal(**overrides):
    params = {
        "leverage_percentile": 50,
        "momentum_5d_pct": 0.5,
        "momentum_percentile": 60,
        "up_streak": 1,
        "down_streak": 0,
    }
    params.update(overrides)
    return classify_cn_margin_signal(**params)


def test_high_leverage_rising_new_high_is_overheated():
    result = _signal(
        leverage_percentile=95,
        momentum_5d_pct=2.5,
        momentum_percentile=90,
        up_streak=4,
        new_high_252d=True,
    )
    assert result["signal_code"] == "OVERHEATED_RISING"
    assert result["risk_level"] == "high"
    assert "过热" in result["summary"]


def test_high_leverage_falling_is_deleveraging_not_generic_retreat():
    result = _signal(
        leverage_percentile=96,
        momentum_5d_pct=-3.0,
        momentum_percentile=5,
        up_streak=0,
        down_streak=5,
    )
    assert result["signal_code"] == "HIGH_LEVERAGE_DELEVERAGING"
    assert "波动" in result["summary"]


def test_cooling_below_high_leverage_is_speculative_retreat():
    result = _signal(
        leverage_percentile=55,
        momentum_5d_pct=-1.5,
        momentum_percentile=12,
        up_streak=0,
        down_streak=3,
    )
    assert result["signal_code"] == "SPECULATIVE_RETREAT"


def test_low_leverage_warming_is_risk_appetite_recovery():
    result = _signal(
        leverage_percentile=18,
        momentum_5d_pct=1.8,
        momentum_percentile=88,
        up_streak=3,
    )
    assert result["signal_code"] == "RISK_APPETITE_RECOVERY"
    assert result["market_bias"] == "supportive"


def test_direction_or_percentile_alone_does_not_trigger_warning():
    short_streak = _signal(
        leverage_percentile=95,
        momentum_5d_pct=2.0,
        momentum_percentile=90,
        up_streak=2,
    )
    non_extreme = _signal(
        leverage_percentile=95,
        momentum_5d_pct=2.0,
        momentum_percentile=70,
        up_streak=6,
    )
    assert short_streak["signal_code"] == "NEUTRAL_WATCH"
    assert non_extreme["signal_code"] == "NEUTRAL_WATCH"


def test_signed_momentum_is_not_ranked_by_absolute_value():
    result = _signal(
        leverage_percentile=95,
        momentum_5d_pct=-4.0,
        momentum_percentile=95,
        up_streak=0,
        down_streak=6,
    )
    assert result["signal_code"] == "NEUTRAL_WATCH"


def test_stale_and_insufficient_history_disable_active_signal():
    stale = _signal(stale=True)
    insufficient = _signal(sufficient=False)
    assert stale["signal_code"] == "DATA_STALE"
    assert stale["risk_level"] == "unknown"
    assert insufficient["signal_code"] == "INSUFFICIENT_HISTORY"


def test_new_high_requires_full_prior_window_and_strict_breakout():
    values = pd.Series(range(253), dtype=float)
    assert _is_new_high(values, 252) is True
    assert _is_new_high(values.iloc[:252], 252) is None
    flat = pd.Series([100.0] * 253)
    assert _is_new_high(flat, 252) is False


def test_market_confirmation_covers_four_direction_combinations():
    rising = _build_market_confirmation(
        momentum_5d_pct=1, csi300_5d_pct=2, csi1000_5d_pct=3, turnover_ma20_ratio=1.1
    )
    dip_buying = _build_market_confirmation(
        momentum_5d_pct=1, csi300_5d_pct=-2, csi1000_5d_pct=-3, turnover_ma20_ratio=1.0
    )
    retreat = _build_market_confirmation(
        momentum_5d_pct=-1, csi300_5d_pct=-2, csi1000_5d_pct=-3, turnover_ma20_ratio=0.8
    )
    unleveraged = _build_market_confirmation(
        momentum_5d_pct=-1, csi300_5d_pct=2, csi1000_5d_pct=3, turnover_ma20_ratio=1.0
    )
    assert rising["confirmation_code"] == "RISK_APPETITE_CONFIRMED"
    assert dip_buying["confirmation_code"] == "LEVERAGED_DIP_BUYING"
    assert retreat["confirmation_code"] == "RETREAT_CONFIRMED"
    assert unleveraged["confirmation_code"] == "UNLEVERAGED_RALLY"


def _frames(periods=40):
    dates = pd.bdate_range("2026-01-05", periods=periods).strftime("%Y%m%d").tolist()
    margin_rows = []
    climate_rows = []
    index_rows = []
    for index, day in enumerate(dates):
        total_balance = 2_000_000_000_000 + index * 1_000_000_000
        total_buy = 150_000_000_000 + index * 10_000_000
        for exchange_id, share in (("SSE", 0.55), ("SZSE", 0.45)):
            margin_rows.append(
                {
                    "trade_date": day,
                    "exchange_id": exchange_id,
                    "financing_balance_yuan": total_balance * share,
                    "financing_buy_yuan": total_buy * share,
                    "source_name": f"{exchange_id.lower()}_official",
                    "quality_status": "ok",
                }
            )
        for code, value, percentile, secondary, payload in (
            (MARGIN_LEVERAGE, 2.5, 60, None, {}),
            (MARGIN_MOMENTUM_5D, 0.5, 60, None, {}),
            (
                MARKET_AMOUNT,
                1_000_000_000_000 + index * 1_000_000_000,
                55,
                1.0,
                {"ma20_ratio": 1.0},
            ),
        ):
            climate_rows.append(
                {
                    "trade_date": day,
                    "metric_code": code,
                    "metric_value": value,
                    "percentile": percentile,
                    "secondary_value": secondary,
                    "sample_count": 300,
                    "payload_json": json.dumps(payload),
                    "source_dates_json": json.dumps({"source": day}),
                    "quality_status": "ok",
                }
            )
        index_rows.extend(
            [
                {"trade_date": day, "ts_code": CSI300, "close_price": 4000 + index * 2},
                {"trade_date": day, "ts_code": CSI1000, "close_price": 6000 + index * 3},
            ]
        )
    return pd.DataFrame(margin_rows), pd.DataFrame(climate_rows), pd.DataFrame(index_rows), dates


def test_builder_falls_back_to_latest_fully_aligned_day():
    margin, climate, index_prices, dates = _frames()
    climate = climate[
        ~((climate["trade_date"] == dates[-1]) & (climate["metric_code"] == MARKET_AMOUNT))
    ]
    result = build_cn_margin_market_signal_from_frames(
        margin, climate, index_prices, as_of_date=dates[-1]
    )
    assert result["data_date"] == dates[-2]
    assert set(result["source_dates"].values()) == {dates[-2]}
    assert "已回退" in result["date_note"]


def test_builder_rejects_climate_row_with_mismatched_internal_source_date():
    margin, climate, index_prices, dates = _frames()
    target = (climate["trade_date"] == dates[-1]) & (
        climate["metric_code"] == MARGIN_LEVERAGE
    )
    climate.loc[target, "source_dates_json"] = json.dumps({"margin": dates[-2]})
    result = build_cn_margin_market_signal_from_frames(
        margin, climate, index_prices, as_of_date=dates[-1]
    )
    assert result["data_date"] == dates[-2]
    assert set(result["source_dates"].values()) == {dates[-2]}


def test_builder_marks_more_than_two_market_sessions_stale():
    margin, climate, index_prices, dates = _frames()
    cutoff = dates[-4]
    margin = margin[margin["trade_date"] <= cutoff]
    climate = climate[climate["trade_date"] <= cutoff]
    result = build_cn_margin_market_signal_from_frames(
        margin, climate, index_prices, as_of_date=dates[-1]
    )
    assert result["status"] == "stale"
    assert result["signal_code"] == "DATA_STALE"
    assert result["stale_trading_days"] == 3


def test_builder_rejects_invalid_date_and_missing_same_day_data():
    margin, climate, index_prices, _ = _frames()
    invalid = build_cn_margin_market_signal_from_frames(
        margin, climate, index_prices, as_of_date="2026-99-99"
    )
    missing = build_cn_margin_market_signal_from_frames(
        margin.iloc[0:0], climate, index_prices
    )
    assert invalid["status"] == "invalid_request"
    assert missing["status"] == "no_data"


def test_activity_percentile_uses_financing_buy_share_of_turnover():
    margin, climate, index_prices, _ = _frames(periods=300)
    result = build_cn_margin_market_signal_from_frames(margin, climate, index_prices)
    activity = result["metrics"]["activity"]
    assert activity["sample_count"] == 300
    assert activity["percentile"] is not None
    assert activity["buy_turnover_ratio_pct"] > 0


def test_report_is_plain_language_and_does_not_expose_sample_counts():
    margin, climate, index_prices, _ = _frames(periods=300)
    result = build_cn_margin_market_signal_from_frames(margin, climate, index_prices)
    report = result["report"]
    assert "融资余额" in report
    assert "辅助信号" in report
    assert "样本数" not in report


def test_missing_financing_buy_is_reported_as_gap_not_zero():
    margin, climate, index_prices, _ = _frames(periods=300)
    margin = margin.drop(columns=["financing_buy_yuan"])
    result = build_cn_margin_market_signal_from_frames(margin, climate, index_prices)
    assert result["metrics"]["activity"]["buy_yuan"] is None
    assert "当日融资买入 --亿元" in result["report"]
    assert any("历史分位" in gap for gap in result["gaps"])


def test_validated_fallback_source_is_exposed_in_status():
    margin, climate, index_prices, _ = _frames(periods=300)
    margin.loc[margin["exchange_id"] == "SZSE", "source_name"] = "jin10_szse_history_validated"
    margin.loc[margin["exchange_id"] == "SZSE", "quality_status"] = "mirror_validated"
    result = build_cn_margin_market_signal_from_frames(margin, climate, index_prices)
    assert result["status"] == "fallback_validated"
    assert result["sources"]["SZSE"] == "jin10_szse_history_validated"
    assert "深交所：jin10_szse_history_validated" in result["report"]


def test_database_read_failure_returns_safe_report():
    engine = create_engine("sqlite:///:memory:")
    result = build_cn_margin_market_signal(engine=engine)
    assert result["status"] == "error"
    assert "数据不足" in result["report"]
    assert "读取失败" in result["report"]


def test_public_tool_returns_deterministic_builder_report(monkeypatch):
    monkeypatch.setattr(
        cn_margin_ai_tools,
        "build_cn_margin_market_signal",
        lambda as_of_date="", **_kwargs: {"report": f"融资工具结果:{as_of_date}"},
    )
    out = get_cn_margin_market_signal.invoke({"as_of_date": "20260717"})
    assert out == "融资工具结果:20260717"

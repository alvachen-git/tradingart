import pandas as pd

from tools.kronos_poc.engine import FallbackIntervalEngine


def test_fallback_interval_engine_returns_required_quantiles():
    df = pd.DataFrame(
        {
            "trade_date": [f"202401{i:02d}" for i in range(1, 61)],
            "close_price": [100 + i * 0.2 for i in range(60)],
            "open_price": [99 + i * 0.2 for i in range(60)],
            "high_price": [101 + i * 0.2 for i in range(60)],
            "low_price": [98 + i * 0.2 for i in range(60)],
            "vol": [1000 + i for i in range(60)],
        }
    )
    preds, warnings, debug = FallbackIntervalEngine().predict(df, horizon=3, quantiles=[0.1, 0.5, 0.9])

    assert len(preds) == 3
    for idx, row in enumerate(preds, start=1):
        assert row["step"] == idx
        assert row["p10_close"] <= row["p50_close"] <= row["p90_close"]
    assert warnings
    assert debug["engine_mode"] == "fallback-lognormal"

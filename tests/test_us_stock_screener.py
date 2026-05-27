import pandas as pd

import screener_tool


def _rows(symbol, closes, highs=None, lows=None, volumes=None):
    dates = pd.bdate_range("2026-01-01", periods=len(closes))
    highs = highs or [x * 1.01 for x in closes]
    lows = lows or [x * 0.99 for x in closes]
    volumes = volumes or [1000] * len(closes)
    return [
        {
            "date": dates[i],
            "symbol": symbol,
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i],
        }
        for i in range(len(closes))
    ]


def test_us_stock_bottom_breakout_candidate_is_selected():
    closes = [96.0] * 40 + [90.0] * 20 + [98.0] * 19 + [110.0]
    highs = [98.0] * 79 + [112.0]
    lows = [95.0] * 40 + [89.0] * 20 + [96.0] * 20
    volumes = [1000] * 79 + [1800]
    df = pd.DataFrame(_rows("AAA", closes, highs=highs, lows=lows, volumes=volumes))

    out, warning = screener_tool._build_us_stock_technical_candidates(
        df,
        df["date"].max(),
        limit=5,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_technical_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "AAA.US" in text
    assert "底部刚突破优先观察" in text
    assert "当前状态" in text
    assert "日线 EOD 数据" in text
    assert "突破前20日高点" in text


def test_us_stock_extended_rebound_is_not_ranked_as_bottom_start():
    closes = [100.0] * 40 + [90.0] * 20 + [120.0] * 19 + [170.0]
    highs = [102.0] * 60 + [125.0] * 19 + [172.0]
    lows = [98.0] * 40 + [89.0] * 20 + [118.0] * 20
    volumes = [1000] * 79 + [1800]
    df = pd.DataFrame(_rows("EXT", closes, highs=highs, lows=lows, volumes=volumes))

    out, warning = screener_tool._build_us_stock_technical_candidates(
        df,
        df["date"].max(),
        limit=5,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_technical_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "EXT.US" in text
    assert "强势延续但不算底部刚启动" in text
    assert "底部刚突破优先观察 | EXT.US" not in text


def test_us_stock_low_volume_breakout_is_bucketed_as_volume_weak():
    closes = [96.0] * 40 + [90.0] * 20 + [98.0] * 19 + [110.0]
    highs = [98.0] * 79 + [112.0]
    lows = [95.0] * 40 + [89.0] * 20 + [96.0] * 20
    volumes = [1000] * 79 + [600]
    df = pd.DataFrame(_rows("LOWV", closes, highs=highs, lows=lows, volumes=volumes))

    out, warning = screener_tool._build_us_stock_technical_candidates(
        df,
        df["date"].max(),
        limit=5,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_technical_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "LOWV.US" in text
    assert "突破但量能不足" in text
    assert "量能未放大" in text


def test_us_stock_screener_reports_data_insufficient_without_fabrication():
    out, warning = screener_tool._build_us_stock_technical_candidates(
        pd.DataFrame(),
        "2026-05-22",
    )
    text = screener_tool._format_us_stock_technical_candidates(out, "2026-05-22", warning)

    assert "结论：数据不足" in text
    assert "日线 EOD 数据" in text
    assert ".US" not in text


def test_us_stock_screener_reports_no_candidates():
    closes = [100.0] * 80
    df = pd.DataFrame(_rows("BBB", closes))

    out, warning = screener_tool._build_us_stock_technical_candidates(
        df,
        df["date"].max(),
        limit=5,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_technical_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "结论：暂无符合条件候选" in text
    assert "可放宽条件" in text
    assert "BBB.US" not in text

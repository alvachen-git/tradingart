import pandas as pd

import screener_tool


def _rows(symbol, closes, highs=None, lows=None, volumes=None, opens=None):
    dates = pd.bdate_range("2026-01-01", periods=len(closes))
    opens = opens or closes
    highs = highs or [x * 1.01 for x in closes]
    lows = lows or [x * 0.99 for x in closes]
    volumes = volumes or [1000] * len(closes)
    return [
        {
            "date": dates[i],
            "symbol": symbol,
            "open": opens[i],
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


def test_us_stock_bearish_breakdown_candidate_is_selected():
    closes = [120.0] * 40 + [130.0] * 20 + [124.0] * 18 + [124.5] + [122.5]
    opens = [120.0] * 78 + [123.0, 125.0]
    highs = [122.0] * 40 + [132.0] * 20 + [126.0] * 20
    lows = [118.0] * 40 + [128.0] * 20 + [123.0] * 19 + [122.0]
    volumes = [1000] * 79 + [1800]
    df = pd.DataFrame(_rows("BRK", closes, highs=highs, lows=lows, volumes=volumes, opens=opens))

    out, warning = screener_tool._build_us_stock_bearish_candidates(
        df,
        df["date"].max(),
        limit=3,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_bearish_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "BRK.US" in text
    assert "破位做空优先观察" in text
    assert "跌破前20日低点" in text
    assert "空头吞噬" in text
    assert "美股看跌/做空观察候选" in text
    assert "券源" in text
    assert "底部刚突破" not in text


def test_us_stock_bearish_breakdown_detects_falling_three_methods():
    closes = [108.0] * 40 + [112.0] * 20 + [111.0] * 15 + [110.0, 108.0, 108.8, 109.2, 108.9, 107.5]
    opens = [108.0] * 75 + [110.0, 112.0, 108.2, 108.8, 109.3, 108.8]
    highs = [109.0] * 40 + [114.0] * 20 + [112.0] * 15 + [111.0, 113.0, 109.2, 109.6, 109.5, 109.0]
    lows = [107.5] * 40 + [111.0] * 20 + [110.0] * 15 + [109.0, 107.8, 108.0, 108.5, 108.4, 107.2]
    volumes = [1000] * (len(closes) - 1) + [1500]
    df = pd.DataFrame(_rows("THR", closes, highs=highs, lows=lows, volumes=volumes, opens=opens))

    out, warning = screener_tool._build_us_stock_bearish_candidates(
        df,
        df["date"].max(),
        limit=3,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_bearish_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "THR.US" in text
    assert "下降三法" in text
    assert "破位做空优先观察" in text


def test_us_stock_bearish_breakdown_downgrades_late_chase_risk():
    closes = [120.0] * 40 + [130.0] * 20 + [124.0] * 18 + [124.5] + [108.0]
    opens = [120.0] * 78 + [123.0, 126.0]
    highs = [122.0] * 40 + [132.0] * 20 + [126.0] * 20
    lows = [118.0] * 40 + [128.0] * 20 + [123.0] * 19 + [107.0]
    volumes = [1000] * 79 + [2200]
    df = pd.DataFrame(_rows("LATE", closes, highs=highs, lows=lows, volumes=volumes, opens=opens))

    out, warning = screener_tool._build_us_stock_bearish_candidates(
        df,
        df["date"].max(),
        limit=3,
        min_bars=80,
    )
    text = screener_tool._format_us_stock_bearish_candidates(out, df["date"].max(), warning)

    assert warning == ""
    assert "LATE.US" in text
    assert "急跌需等反抽确认" in text
    assert "追空偏晚" in text
    assert "破位做空优先观察 | LATE.US" not in text


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

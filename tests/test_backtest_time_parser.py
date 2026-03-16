from backtest_time_parser import parse_time_expr, resolve_window


def test_parse_quarter_expr():
    window = parse_time_expr("2024Q3", anchor_date="20260210")
    assert window is not None
    assert window.start_date == "20240701"
    assert window.end_date == "20240930"


def test_parse_yearly_expr():
    window = parse_time_expr("去年全年", anchor_date="20260210")
    assert window is not None
    assert window.start_date == "20250101"
    assert window.end_date == "20251231"


def test_parse_month_expr():
    window = parse_time_expr("2024年2月", anchor_date="20260210")
    assert window is not None
    assert window.start_date == "20240201"
    assert window.end_date == "20240229"


def test_priority_explicit_over_time_expr():
    window = resolve_window(
        start_date="20250101",
        end_date="20251231",
        time_expr="近6个月",
        lookback_days=90,
        anchor_date="20260210",
    )
    assert window.start_date == "20250101"
    assert window.end_date == "20251231"


def test_priority_time_expr_over_lookback():
    window = resolve_window(
        time_expr="2024Q3",
        lookback_days=90,
        anchor_date="20260210",
    )
    assert window.start_date == "20240701"
    assert window.end_date == "20240930"


def test_unparsable_time_expr_should_fail():
    try:
        resolve_window(time_expr="最近几段时间", anchor_date="20260210")
    except ValueError as exc:
        assert "无法解析" in str(exc)
    else:
        raise AssertionError("应抛出 ValueError")

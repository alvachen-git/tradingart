from symbol_match import sql_prefix_condition, strict_futures_prefix_pattern


def test_sql_prefix_condition_escapes_like_percent_for_multi_letter_code():
    cond = sql_prefix_condition("AU")
    assert cond == "UPPER(ts_code) LIKE 'AU%%'"


def test_sql_prefix_condition_single_letter_uses_regexp():
    cond = sql_prefix_condition("I")
    assert cond == "UPPER(ts_code) REGEXP '^I(0|[-]?[0-9])'"


def test_strict_futures_prefix_pattern_normalizes_input():
    assert strict_futures_prefix_pattern("au-") == "^AU(0|[-]?[0-9])"

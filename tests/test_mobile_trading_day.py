from mobile_trading_day import _is_night_session_product


def test_br_is_night_session_product():
    assert _is_night_session_product("BR2505")
    assert _is_night_session_product("br2511")


def test_user_reported_non_night_products_are_excluded():
    for code in ["LC2505", "LH2505", "PS2505", "SI2505", "CJ2505", "SF2505", "SM2505"]:
        assert _is_night_session_product(code) is False


def test_known_night_products_still_enabled():
    for code in ["RB2505", "AU2506", "I2509", "MA2509", "SC2507"]:
        assert _is_night_session_product(code)

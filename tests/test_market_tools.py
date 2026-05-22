import pandas as pd

import market_tools


def _invoke_snapshot(query: str) -> str:
    return market_tools.get_market_snapshot.invoke(query)


def test_futures_product_snapshot_uses_dominant_month_not_far_month(monkeypatch):
    monkeypatch.setattr(market_tools.symbol_map, "resolve_symbol", lambda query: ("jd", "future"))

    def fake_read_sql(sql, _engine, params=None):
        sql_text = str(sql)
        assert "REGEXP :pattern" in sql_text
        assert "MAX(trade_date)" in sql_text
        assert "COALESCE(oi, 0) DESC" in sql_text
        assert params == {"pattern": r"^JD(0|[-]?[0-9])"}
        return pd.DataFrame(
            [
                {
                    "ts_code": "JD2607",
                    "trade_date": "20260522",
                    "close_price": 3992.0,
                    "pct_chg": 0.94,
                    "oi": 341679,
                }
            ]
        )

    monkeypatch.setattr(market_tools.pd, "read_sql", fake_read_sql)

    out = _invoke_snapshot("鸡蛋")

    assert "JD2607" in out
    assert "3992.0" in out
    assert "JD2704" not in out
    assert "JDL" not in out


def test_single_letter_futures_product_uses_strict_prefix(monkeypatch):
    monkeypatch.setattr(market_tools.symbol_map, "resolve_symbol", lambda query: ("m", "future"))

    def fake_read_sql(sql, _engine, params=None):
        sql_text = str(sql)
        assert "REGEXP :pattern" in sql_text
        assert params == {"pattern": r"^M(0|[-]?[0-9])"}
        return pd.DataFrame(
            [
                {
                    "ts_code": "M2609",
                    "trade_date": "20260522",
                    "close_price": 2990.0,
                    "pct_chg": 0.2,
                    "oi": 2461400,
                }
            ]
        )

    monkeypatch.setattr(market_tools.pd, "read_sql", fake_read_sql)

    out = _invoke_snapshot("豆粕")

    assert "M2609" in out
    assert "2990.0" in out
    assert "MA" not in out


def test_specific_futures_contract_snapshot_keeps_user_contract(monkeypatch):
    monkeypatch.setattr(market_tools.symbol_map, "resolve_symbol", lambda query: ("M2609", "future"))

    def fake_read_sql(sql, _engine, params=None):
        sql_text = str(sql)
        assert "LIKE :contract_like" in sql_text
        assert "REGEXP :pattern" not in sql_text
        assert params == {"contract_like": "M2609%"}
        return pd.DataFrame(
            [
                {
                    "ts_code": "M2609",
                    "trade_date": "20260522",
                    "close_price": 2990.0,
                    "pct_chg": 0.2,
                    "oi": 2461400,
                }
            ]
        )

    monkeypatch.setattr(market_tools.pd, "read_sql", fake_read_sql)

    out = _invoke_snapshot("M2609")

    assert "M2609" in out
    assert "2990.0" in out


def test_multi_letter_futures_product_uses_same_dominant_month_rule(monkeypatch):
    monkeypatch.setattr(market_tools.symbol_map, "resolve_symbol", lambda query: ("ag", "future"))

    def fake_read_sql(sql, _engine, params=None):
        sql_text = str(sql)
        assert "REGEXP :pattern" in sql_text
        assert "COALESCE(oi, 0) DESC" in sql_text
        assert params == {"pattern": r"^AG(0|[-]?[0-9])"}
        return pd.DataFrame(
            [
                {
                    "ts_code": "AG2606",
                    "trade_date": "20260522",
                    "close_price": 18681.0,
                    "pct_chg": 0.5,
                    "oi": 233074,
                }
            ]
        )

    monkeypatch.setattr(market_tools.pd, "read_sql", fake_read_sql)

    out = _invoke_snapshot("白银")

    assert "AG2606" in out
    assert "18681.0" in out
    assert "AGL" not in out

import pandas as pd

import futures_structure_tools as fst


def _reset_cache():
    fst._CACHE.clear()
    fst._SNAPSHOT.clear()


def test_basis_profile_primary_source(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_spot_price",
        lambda date, vars_list: pd.DataFrame(
            [{"品种": "RB", "现货价格": 3500, "期货价格": 3470, "基差": 30}]
        ),
    )
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda symbol, indicator: pd.DataFrame())

    out = fst.get_futures_basis_profile.invoke({"query": "螺纹钢", "date": "2026-04-01"})
    assert "数据来源: futures_spot_price" in out
    assert "RB" in out
    assert "基差" in out


def test_basis_profile_fallback_to_previous(monkeypatch):
    _reset_cache()

    def _raise(*args, **kwargs):
        raise RuntimeError("network")

    monkeypatch.setattr(fst.ak, "futures_spot_price", _raise)
    monkeypatch.setattr(
        fst.ak,
        "futures_spot_price_previous",
        lambda date: pd.DataFrame(
            [{"品种": "AU", "现货价格": 500, "期货价格": 498, "基差": 2}]
        ),
    )
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda symbol, indicator: pd.DataFrame())

    out = fst.get_futures_basis_profile.invoke({"query": "黄金", "date": "2026-04-05"})
    assert "数据来源: futures_spot_price_previous" in out
    assert "未取到来源: futures_spot_price" in out


def test_basis_profile_fallback_to_default_without_date(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(fst.ak, "futures_spot_price", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(
        fst.ak,
        "futures_spot_price_previous",
        lambda *args, **kwargs: pd.DataFrame([{"品种": "M", "现货价格": 3000, "期货价格": 2980, "基差": 20}]),
    )
    monkeypatch.setattr(fst.ak, "futures_spot_price_daily", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda *args, **kwargs: pd.DataFrame())

    out = fst.get_futures_basis_profile.invoke({"query": "豆粕"})
    assert "futures_spot_price_previous(default)" in out or "futures_spot_price_previous" in out
    assert "命中日期:" in out


def test_basis_profile_rolls_back_candidate_trade_date(monkeypatch):
    _reset_cache()

    def _spot_price(date, vars_list):
        if date == "20260405":
            raise RuntimeError("20260405非交易日")
        return pd.DataFrame([{"品种": "M", "日期": date, "现货价格": 3200, "期货价格": 3190, "基差": 10}])

    monkeypatch.setattr(fst.ak, "futures_spot_price", _spot_price)
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_daily", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda *args, **kwargs: pd.DataFrame())

    out = fst.get_futures_basis_profile.invoke({"query": "豆粕", "date": "2026-04-05"})
    assert "数据来源: futures_spot_price" in out
    assert "命中日期: 20260404" in out
    assert "未取到来源: futures_spot_price(20260405): 非交易日" in out


def test_basis_profile_falls_back_to_local_futures_price(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(fst.ak, "futures_spot_price", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_daily", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(
        fst,
        "_get_latest_price_for_margin_estimation",
        lambda code, month=None: (3366.0, "RB2606.SHF", "20260106"),
    )

    out = fst.get_futures_basis_profile.invoke({"query": "螺纹钢"})
    assert "local_futures_price_fallback" in out
    assert "基差源不可用，仅返回期货端价格参考" in out
    assert "3366" in out


def test_basis_profile_uses_snapshot_when_all_external_sources_fail(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_spot_price",
        lambda date, vars_list: pd.DataFrame([{"品种": "M", "日期": "20260403", "现货价格": 3200, "期货价格": 3190, "基差": 10}]),
    )
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_daily", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda *args, **kwargs: pd.DataFrame())
    fst.get_futures_basis_profile.invoke({"query": "豆粕", "date": "2026-04-03"})
    fst._CACHE.clear()

    monkeypatch.setattr(fst.ak, "futures_spot_price", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_daily", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst, "_get_latest_price_for_margin_estimation", lambda *args, **kwargs: (None, "", ""))

    out = fst.get_futures_basis_profile.invoke({"query": "豆粕", "date": "2026-04-05"})
    assert "basis_snapshot_fallback" in out
    assert "历史快照" in out


def test_basis_profile_fallback_to_spot_stock_plus_local_futures(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(fst.ak, "futures_spot_price", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_price_daily", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda *args, **kwargs: pd.DataFrame())

    def _spot_stock(symbol):
        if symbol == "钢铁":
            return pd.DataFrame([{"商品名称": "螺纹钢", "最新价格": 3000}])
        return pd.DataFrame()

    monkeypatch.setattr(fst.ak, "futures_spot_stock", _spot_stock)
    monkeypatch.setattr(
        fst,
        "_get_latest_price_for_margin_estimation",
        lambda code, month=None: (3109.0, "RB2609.SHF", "20260408"),
    )

    out = fst.get_futures_basis_profile.invoke({"query": "螺纹钢", "date": "2026-04-08"})
    assert "futures_spot_stock(钢铁)" in out
    assert "local_futures_price_for_basis" in out
    assert "3109" in out
    assert "3000" in out
    assert "109" in out


def test_inventory_profile_inventory_em_path(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_em",
        lambda symbol: pd.DataFrame(
            [{"品种": "AU", "日期": "2026-04-01", "库存": 1000, "增减": -10}]
        ),
    )
    monkeypatch.setattr(fst.ak, "futures_inventory_99", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(
        fst.ak,
        "futures_shfe_warehouse_receipt",
        lambda date: {"黄金": pd.DataFrame([{"品种": "AU", "仓单": 321, "增减": 5}])},
    )

    out = fst.get_futures_inventory_receipt_profile.invoke({"query": "黄金", "date": "20260401"})
    assert "库存数据来源: futures_inventory_em" in out
    assert "仓单数据来源: futures_shfe_warehouse_receipt" in out
    assert "仓单" in out
    assert "库存命中日期:" in out


def test_inventory_profile_fallback_to_inventory_99(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(fst.ak, "futures_inventory_em", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_99",
        lambda symbol: pd.DataFrame(
            [{"品种": "M", "日期": "2026-04-01", "库存": 2222, "增减": 11}]
        ),
    )
    monkeypatch.setattr(
        fst.ak,
        "futures_warehouse_receipt_dce",
        lambda date: pd.DataFrame([{"品种": "M", "仓单数量": 123, "增减": 7}]),
    )

    out = fst.get_futures_inventory_receipt_profile.invoke({"query": "豆粕"})
    assert "库存数据来源: futures_inventory_99" in out
    assert "仓单数据来源: futures_warehouse_receipt_dce" in out


def test_inventory_profile_column_aliases_are_normalized(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_em",
        lambda symbol: pd.DataFrame([{"品种": "M", "统计日期": "2026-04-01", "库存量": 2222, "日增减": 11}]),
    )
    monkeypatch.setattr(fst.ak, "futures_inventory_99", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_warehouse_receipt_dce", lambda date: pd.DataFrame())

    out = fst.get_futures_inventory_receipt_profile.invoke({"query": "豆粕"})
    assert "库存" in out
    assert "增减" in out


def test_inventory_receipt_supports_dict_and_dataframe(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_em",
        lambda symbol: pd.DataFrame([{"品种": "RB", "库存": 99}]),
    )
    monkeypatch.setattr(fst.ak, "futures_inventory_99", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(
        fst.ak,
        "futures_shfe_warehouse_receipt",
        lambda date: {
            "螺纹": pd.DataFrame([{"品种": "RB", "仓单": 888}]),
            "线材": [{"品种": "WR", "仓单": 66}],
        },
    )
    out_shfe = fst.get_futures_inventory_receipt_profile.invoke({"query": "螺纹钢"})
    assert "仓单数据" in out_shfe
    assert "888" in out_shfe

    _reset_cache()
    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_em",
        lambda symbol: pd.DataFrame([{"品种": "M", "库存": 77}]),
    )
    monkeypatch.setattr(
        fst.ak,
        "futures_warehouse_receipt_dce",
        lambda date: pd.DataFrame([{"品种": "M", "仓单数量": 456}]),
    )
    out_dce = fst.get_futures_inventory_receipt_profile.invoke({"query": "豆粕"})
    assert "456" in out_dce


def test_inventory_receipt_rolls_back_date_when_receipt_source_fails(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_em",
        lambda symbol: pd.DataFrame([{"品种": "M", "日期": "2026-04-05", "库存": 77}]),
    )
    monkeypatch.setattr(fst.ak, "futures_inventory_99", lambda symbol: pd.DataFrame())

    def _receipt(date):
        if date == "20260405":
            raise RuntimeError("No tables found")
        return pd.DataFrame([{"品种": "M", "日期": date, "仓单数量": 456}])

    monkeypatch.setattr(fst.ak, "futures_warehouse_receipt_dce", _receipt)

    out = fst.get_futures_inventory_receipt_profile.invoke({"query": "豆粕", "date": "2026-04-05"})
    assert "仓单数据来源: futures_warehouse_receipt_dce" in out
    assert "仓单命中日期: 20260404" in out
    assert "未取到来源: futures_warehouse_receipt_dce(20260405): No tables found" in out


def test_inventory_partial_success_reports_miss_lines(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_inventory_em",
        lambda symbol: pd.DataFrame([{"品种": "M", "日期": "2026-04-05", "库存": 77}]),
    )
    monkeypatch.setattr(fst.ak, "futures_inventory_99", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_warehouse_receipt_dce", lambda date: (_ for _ in ()).throw(RuntimeError("network")))

    out = fst.get_futures_inventory_receipt_profile.invoke({"query": "豆粕", "date": "2026-04-05"})
    assert "库存数据来源: futures_inventory_em" in out
    assert "仓单数据来源: 无可用来源" in out
    assert "未取到来源:" in out


def test_delivery_tospot_multi_month_partial_failure(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_to_spot_shfe",
        lambda date: pd.DataFrame([{"品种": "RB", "期转现量": 10, "日期": date}]),
    )

    def _dce_fail(date):
        raise RuntimeError("dce down")

    monkeypatch.setattr(fst.ak, "futures_to_spot_dce", _dce_fail)
    monkeypatch.setattr(fst.ak, "futures_to_spot_czce", lambda date: pd.DataFrame())
    monkeypatch.setattr(
        fst.ak,
        "futures_delivery_shfe",
        lambda date: pd.DataFrame([{"品种": "RB", "交割量": 8, "日期": date}]),
    )
    monkeypatch.setattr(fst.ak, "futures_delivery_dce", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_delivery_czce", lambda date: pd.DataFrame())
    monkeypatch.setattr(
        fst.ak,
        "futures_delivery_match_dce",
        lambda symbol: pd.DataFrame([{"品种": "RB", "配对量": 5}]),
    )

    def _czce_match_fail(date):
        raise RuntimeError("czce match down")

    monkeypatch.setattr(fst.ak, "futures_delivery_match_czce", _czce_match_fail)

    out = fst.get_futures_delivery_tospot_profile.invoke({"query": "螺纹钢", "end_month": "202604", "months": 3})
    assert "窗口月份: 202602 ~ 202604 (共 3 个月)" in out
    assert "futures_to_spot_shfe" in out
    assert "futures_delivery_shfe" in out
    assert "未取到来源" in out


def test_outputs_are_data_only_without_strategy_words(monkeypatch):
    _reset_cache()

    monkeypatch.setattr(
        fst.ak,
        "futures_spot_price",
        lambda date, vars_list: pd.DataFrame([{"品种": "RB", "基差": 1}]),
    )
    monkeypatch.setattr(fst.ak, "futures_spot_price_previous", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_spot_sys", lambda symbol, indicator: pd.DataFrame())
    out_basis = fst.get_futures_basis_profile.invoke({"query": "螺纹钢"})

    monkeypatch.setattr(fst.ak, "futures_inventory_em", lambda symbol: pd.DataFrame([{"品种": "RB", "库存": 1}]))
    monkeypatch.setattr(fst.ak, "futures_inventory_99", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_shfe_warehouse_receipt", lambda date: {"x": [{"品种": "RB", "仓单": 1}]})
    out_inv = fst.get_futures_inventory_receipt_profile.invoke({"query": "螺纹钢"})

    monkeypatch.setattr(fst.ak, "futures_to_spot_shfe", lambda date: pd.DataFrame([{"品种": "RB", "期转现量": 1}]))
    monkeypatch.setattr(fst.ak, "futures_to_spot_dce", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_to_spot_czce", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_delivery_shfe", lambda date: pd.DataFrame([{"品种": "RB", "交割量": 1}]))
    monkeypatch.setattr(fst.ak, "futures_delivery_dce", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_delivery_czce", lambda date: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_delivery_match_dce", lambda symbol: pd.DataFrame())
    monkeypatch.setattr(fst.ak, "futures_delivery_match_czce", lambda date: pd.DataFrame())
    out_del = fst.get_futures_delivery_tospot_profile.invoke({"query": "螺纹钢", "months": 1})

    bad_words = ["建议买", "建议卖", "仓位建议", "止损", "止盈"]
    for text in [out_basis, out_inv, out_del]:
        for bad in bad_words:
            assert bad not in text


def test_cache_fetch_temporarily_disables_proxy_env(monkeypatch):
    _reset_cache()
    for k in fst.PROXY_ENV_KEYS:
        monkeypatch.setenv(k, "http://127.0.0.1:7897")

    observed = {}

    def _loader():
        observed["inside"] = {k: fst.os.environ.get(k) for k in fst.PROXY_ENV_KEYS}
        return {"ok": 1}

    out = fst._cache_fetch("proxy_test", ("x",), _loader)
    assert out == {"ok": 1}
    assert all(v is None for v in observed["inside"].values())
    for k in fst.PROXY_ENV_KEYS:
        assert fst.os.environ.get(k) == "http://127.0.0.1:7897"

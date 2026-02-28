import portfolio_analysis_service as svc


def test_compute_position_hash_stable():
    h1 = svc._compute_position_hash(100.0, 12345.678)
    h2 = svc._compute_position_hash(100, 12345.68)
    h3 = svc._compute_position_hash(101, 12345.68)
    assert h1 == h2
    assert h1 != h3


def test_build_industry_allocation_sum_to_100():
    rows = [
        {"industry": "白酒", "market_value": 600.0},
        {"industry": "新能源", "market_value": 300.0},
        {"industry": "白酒", "market_value": 100.0},
    ]
    alloc = svc._build_industry_allocation(rows)
    total_pct = sum(float(x["weight_pct"]) for x in alloc)
    assert abs(total_pct - 100.0) < 1e-6
    assert alloc[0]["industry"] == "白酒"
    assert alloc[0]["weight_pct"] == 70.0


def test_build_weighted_corr_uses_market_value_weights():
    rows = [
        {"market_value": 900.0, "index_corr": {"沪深300 (蓝筹)": 0.8, "上证50 (权重)": 0.6}},
        {"market_value": 100.0, "index_corr": {"沪深300 (蓝筹)": 0.2, "上证50 (权重)": 0.4}},
    ]
    corr = svc._build_weighted_corr(rows)
    # 沪深300 = 0.8*0.9 + 0.2*0.1 = 0.74
    assert abs(corr["沪深300 (蓝筹)"] - 0.74) < 1e-6
    # 上证50 = 0.6*0.9 + 0.4*0.1 = 0.58
    assert abs(corr["上证50 (权重)"] - 0.58) < 1e-6


def test_normalize_positions_corrects_obvious_market_value_mismatch():
    rows = svc._normalize_positions(
        [
            {
                "symbol": "601888",
                "market": "A",
                "name": "中国中免",
                "quantity": 300,
                "price": 80.58,
                "market_value": 450.76,  # OCR 误把盈亏识别为市值
            }
        ]
    )
    assert len(rows) == 1
    assert abs(rows[0]["market_value"] - (300 * 80.58)) < 1e-6


def test_normalize_positions_keeps_market_value_when_close_to_implied():
    rows = svc._normalize_positions(
        [
            {
                "symbol": "601888.SH",
                "market": "A",
                "name": "中国中免",
                "quantity": 300,
                "price": 80.58,
                "market_value": 24170.0,
            }
        ]
    )
    assert len(rows) == 1
    assert abs(rows[0]["market_value"] - 24170.0) < 1e-6


def test_normalize_positions_reconciles_quantity_when_balance_column_is_wrong():
    rows = svc._normalize_positions(
        [
            {
                "symbol": "601126.SH",
                "market": "A",
                "name": "四方股份",
                "quantity": 1000,  # OCR 误读为股票余额
                "price": 44.79,
                "market_value": 53748.0,  # 对应实际数量约 1200
            }
        ]
    )
    assert len(rows) == 1
    assert abs(rows[0]["quantity"] - 1200.0) < 1e-3


def test_normalize_positions_infers_quantity_when_missing():
    rows = svc._normalize_positions(
        [
            {
                "symbol": "600995.SH",
                "market": "A",
                "name": "南网储能",
                "quantity": None,
                "price": 14.93,
                "market_value": 8958.0,  # 对应约 600 股
            }
        ]
    )
    assert len(rows) == 1
    assert abs(rows[0]["quantity"] - 600.0) < 1e-3


def test_normalize_symbol_market_corrects_5digit_a_suffix_to_hk():
    symbol, market = svc._normalize_symbol_market("00988.SZ", "A")
    assert symbol == "00988.HK"
    assert market == "HK"


def test_normalize_positions_dedup_same_identity_multi_symbol():
    rows = svc._normalize_positions(
        [
            {
                "symbol": "00988.SZ",  # 错误后缀，会被纠偏到 HK
                "market": "A",
                "name": "阿里巴巴",
                "quantity": 300,
                "price": 143.8,
                "market_value": 43140.0,
            },
            {
                "symbol": "09988.HK",
                "market": "HK",
                "name": "阿里巴巴",
                "quantity": 300,
                "price": 143.8,
                "market_value": 43140.0,
            },
        ]
    )
    assert len(rows) == 1
    assert rows[0]["symbol"] == "09988.HK"

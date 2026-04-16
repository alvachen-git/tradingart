import vision_tools as vt
from io import BytesIO


def test_parse_position_image_response_stock_json():
    raw = """
    {
      "domain": "stock",
      "stock_positions": [
        {"symbol": "600519", "name": "贵州茅台", "market": "A", "quantity": 100, "market_value": 168000, "cost_price": 1500, "price": 1680}
      ],
      "option_legs": []
    }
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "stock"
    assert len(out["stock_positions"]) == 1
    assert out["option_legs"] == []


def test_parse_position_image_response_option_json():
    raw = """
    {
      "domain": "option",
      "stock_positions": [],
      "option_legs": [
        {"underlying_hint": "159915", "month": 4, "strike": 3.2, "cp": "call", "side": "long", "qty": 23}
      ]
    }
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 1
    assert out["option_legs"][0]["underlying_hint"] == "159915.SZ"
    assert out["option_legs"][0]["signed_qty"] == 23
    assert out["option_legs"][0]["cp_cn"] == "认购"
    assert out["option_legs"][0]["side_cn"] == "买方"
    assert out["option_legs"][0]["direction_cn"] == "买认购"


def test_parse_position_image_response_fallback_option_text():
    raw = "我有创业板4月3.2认购买方23张，还有4月3.3认购卖方50张"
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 2
    signed = sorted([x["signed_qty"] for x in out["option_legs"]])
    assert signed == [-50, 23]
    dir_set = {x["direction_cn"] for x in out["option_legs"]}
    assert "买认购" in dir_set
    assert "卖认购" in dir_set


def test_parse_position_image_response_mixed_sets_mixed_domain():
    raw = """
    {
      "stock_positions": [
        {"symbol": "600519.SH", "name": "茅台", "quantity": 10, "market_value": 16800}
      ],
      "option_legs": [
        {"underlying_hint": "IO", "month": 4, "strike": 4000, "cp": "call", "side": "short", "qty": 2}
      ]
    }
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "mixed"
    assert len(out["stock_positions"]) == 1
    assert len(out["option_legs"]) == 1


def test_parse_position_image_response_option_contract_rows_not_treated_as_stock():
    raw = """
    {
      "domain": "stock",
      "stock_positions": [
        {"symbol": "10011160.SH", "name": "创业板ETF购", "quantity": 30, "market_value": 22200}
      ],
      "option_legs": []
    }
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert out["stock_positions"] == []
    assert len(out["option_legs"]) == 1
    assert out["option_legs"][0]["contract_code"] == "10011160.SH"


def test_parse_position_image_response_option_rows_with_cn_keys():
    raw = """
    {
      "domain": "stock",
      "stock_positions": [
        {"代码": "90007162", "名称": "创业板ETF购4月3400", "类别": "认购", "买卖": "卖", "持仓": 10},
        {"代码": "90007163", "名称": "创业板ETF购4月3500", "类别": "认购", "买卖": "买", "持仓": 5}
      ]
    }
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert out["stock_positions"] == []
    assert len(out["option_legs"]) == 2
    by_code = {x["contract_code"]: x for x in out["option_legs"]}
    assert by_code["90007162"]["underlying_hint"] == "159915.SZ"
    assert by_code["90007162"]["cp"] == "call"
    assert by_code["90007162"]["side"] == "short"
    assert abs(by_code["90007162"]["strike"] - 3.4) < 1e-9
    assert by_code["90007162"]["month"] == 4
    assert by_code["90007162"]["signed_qty"] == -10
    assert by_code["90007163"]["signed_qty"] == 5


def test_parse_position_image_response_fallback_option_table_lines():
    raw = """
    90007162 创业板ETF购4月3400 认购 卖 10
    90007163 创业板ETF购4月3500 认购 买 5
    10011160 科创50购4月1500 认购 买 250
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 3
    by_code = {x["contract_code"]: x for x in out["option_legs"]}
    assert by_code["90007162"]["underlying_hint"] == "159915.SZ"
    assert by_code["90007163"]["underlying_hint"] == "159915.SZ"
    assert by_code["10011160"]["underlying_hint"] == "588000.SH"
    signed = sorted([x["signed_qty"] for x in out["option_legs"]])
    assert signed == [-10, 5, 250]


def test_detect_underlying_prefers_500etf_over_50etf_alias():
    raw = """
    10011145 500ETF购4月9000 认购 卖 2
    10011218 500ETF沽4月7000 认沽 卖 10
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 2
    assert all(x["underlying_hint"] == "510500.SH" for x in out["option_legs"])


def test_detect_underlying_handles_ocr_variant_5ooetf():
    raw = "10011145 5OOETF购4月9000 认购 卖 2"
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 1
    assert out["option_legs"][0]["underlying_hint"] == "510500.SH"
    assert out["option_legs"][0]["side"] == "short"


def test_parse_position_image_response_option_legs_cn_side_and_ss_suffix():
    raw = """
    {
      "domain": "option",
      "option_legs": [
        {"代码": "90007162.SS", "名称": "创业板ETF购4月3400", "类别": "购", "买卖": "卖", "持仓": 10},
        {"代码": "10011145.SS", "名称": "500ETF购4月9000", "类别": "购", "买卖": "卖", "持仓": 2}
      ]
    }
    """
    out = vt.parse_position_image_response(raw)
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 2
    by_code = {x["contract_code"]: x for x in out["option_legs"]}
    assert "90007162.SH" in by_code
    assert "10011145.SH" in by_code
    assert by_code["90007162.SH"]["side"] == "short"
    assert by_code["10011145.SH"]["underlying_hint"] == "510500.SH"


def test_get_dashscope_vision_model_candidates_has_fallback(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_VISION_MODEL", "qwen3-vl-plus")
    monkeypatch.delenv("DASHSCOPE_VISION_STRICT", raising=False)
    models = vt._get_dashscope_vision_model_candidates()
    assert models[0] == "qwen3-vl-plus"
    assert "qwen-vl-plus" in models


def test_get_dashscope_vision_model_candidates_strict_mode(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_VISION_MODEL", "qwen3-vl-plus")
    monkeypatch.setenv("DASHSCOPE_VISION_STRICT", "1")
    models = vt._get_dashscope_vision_model_candidates()
    assert models == ["qwen3-vl-plus"]


def test_analyze_position_image_legacy_fallback_when_two_stage_empty(monkeypatch):
    monkeypatch.setenv("DASHSCOPE_API_KEY", "dummy")
    monkeypatch.setattr(vt, "_get_dashscope_vision_model_candidates", lambda: ["m1"])

    calls = {"n": 0}

    def _fake_call(api_key, prompt, model, image_url=""):
        calls["n"] += 1
        if calls["n"] == 1:
            return {"ok": True, "text": "无有效记录", "error": ""}
        if calls["n"] == 2:
            return {"ok": True, "text": "{}", "error": ""}
        # legacy fallback
        return {
            "ok": True,
            "text": '{"domain":"option","stock_positions":[],"option_legs":[{"underlying_hint":"159915","month":4,"strike":3.2,"cp":"call","side":"long","qty":2,"contract_code":"90007162.SH"}]}',
            "error": "",
        }

    monkeypatch.setattr(vt, "_call_dashscope_vision", _fake_call)

    buf = BytesIO(b"fake-image-bytes")
    out = vt.analyze_position_image(buf)
    assert out["ok"] is True
    assert out["domain"] == "option"
    assert len(out["option_legs"]) == 1
    assert out["option_legs"][0]["contract_code"] == "90007162.SH"


def test_enrich_option_legs_with_contract_metadata_corrects_underlying_and_strike():
    class _FakeLoader:
        def get_contract_by_ts_code(self, ts_code, as_of_yyyymmdd):
            if ts_code == "10011145.SH":
                return {
                    "status": "ok",
                    "ts_code": "10011145.SH",
                    "underlying": "510500.SH",
                    "call_put": "C",
                    "exercise_price": 4.9,
                    "delist_date": "20260424",
                }
            return {"status": "missing_contract", "ts_code": ts_code}

    legs, warnings = vt._enrich_option_legs_with_contract_metadata(
        [
            {
                "contract_code": "10011145.SS",
                "underlying_hint": "510300.SH",
                "month": 4,
                "strike": 9.0,
                "cp": "call",
                "side": "short",
                "qty": 2,
                "signed_qty": -2,
            }
        ],
        loader=_FakeLoader(),
        as_of_yyyymmdd="20260415",
    )
    assert len(legs) == 1
    leg = legs[0]
    assert leg["contract_code"] == "10011145.SH"
    assert leg["underlying_hint"] == "510500.SH"
    assert abs(float(leg["strike"]) - 4.9) < 1e-9
    assert leg["cp"] == "call"
    assert leg["cp_cn"] == "认购"
    assert leg["side_cn"] == "卖方"
    assert leg["direction_cn"] == "卖认购"
    assert any("标的已按元数据修正" in w for w in warnings)

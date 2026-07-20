from types import SimpleNamespace
from unittest.mock import patch

import daily_report_generator as drg


def _commodity_rows() -> str:
    rows = []
    for index in range(0, len(drg.COMMODITY_CARD_LIST), 2):
        left = drg.COMMODITY_CARD_LIST[index]
        right = drg.COMMODITY_CARD_LIST[index + 1]
        rows.append(
            "<tr>"
            f"<td><div>{left} 看多 形态：测试<br>隐含波动率：20.0%（中）</div></td>"
            f"<td><div>{right} 看多 形态：测试<br>隐含波动率：20.0%（中）</div></td>"
            "</tr>"
        )
    return "".join(rows)


def _slots() -> dict:
    return {
        "market-headline": "市场保持震荡。",
        "stock-sector": "主力净流入：医药生物(+10.0亿)；主力净流出：电子(-20.0亿)。",
        "futures-holding": "期货商持仓保持平稳。",
        "commodity-panorama": _commodity_rows(),
        "option-volatility": "<div>沪深300 50.0%（中）</div>",
        "daily-bull": "牛股内容。",
        "risk-warning": "风险内容。",
        "tomorrow-strategy": "明日策略内容。",
        "footer-quote": '💬 "测试点评"',
    }


def _sector_snapshot() -> dict:
    rows = {
        "电力": {"display_name": "电力", "main_flow_yi": 5.5},
        "国有大型银行": {"display_name": "国有大型银行", "main_flow_yi": 5.1},
        "火力发电": {"display_name": "火力发电", "main_flow_yi": 5.0},
        "电子": {"display_name": "电子", "main_flow_yi": -532.1},
        "半导体": {"display_name": "半导体", "main_flow_yi": -250.6},
        "通信": {"display_name": "通信", "main_flow_yi": -200.1},
        "银行": {"display_name": "银行", "main_flow_yi": -11.2},
    }
    return {
        "sectors": rows,
        "sector_top_in": [rows[name] for name in ["电力", "国有大型银行", "火力发电"]],
        "sector_top_out": [rows[name] for name in ["电子", "半导体", "通信"]],
    }


def test_locked_layout_keeps_required_structure_and_ten_cards():
    html = drg._render_locked_report_layout(_slots(), "2026年07月15日", "周三")

    assert drg.validate_report_layout(html) == []
    extracted = drg._extract_report_slots(html)
    assert set(extracted) == set(drg.REPORT_SLOT_ORDER)
    assert "市场保持震荡" in extracted["market-headline"]
    assert extracted["commodity-panorama"].count("<td") == 10


def test_locked_layout_reports_empty_required_slot():
    slots = _slots()
    slots["risk-warning"] = ""
    html = drg._render_locked_report_layout(slots, "2026年07月15日", "周三")

    assert "排版插槽 risk-warning 内容为空" in drg.validate_report_layout(html)


def test_stock_list_uses_valid_div_container_and_readable_inline_styles():
    slots = _slots()
    slots["stock-sector"] = "<ul><li><strong>主力净流入Top3</strong>：医药生物 +99.8亿</li></ul>"
    html = drg._render_locked_report_layout(slots, "2026年07月15日", "周三")
    soup = drg.BeautifulSoup(html, "html.parser")

    container = soup.find(attrs={"data-report-slot": "stock-sector"})
    assert container.name == "div"
    assert "color:#e2e8f0" in container.get("style", "")
    assert "list-style:none" in container.find("ul").get("style", "")
    assert "text-align:left" in container.find("li").get("style", "")
    assert "color:#f8fafc" in container.find("strong").get("style", "")


def test_programmatic_stock_sector_overrides_model_content_without_changing_other_slots():
    original_slots = _slots()
    locked_slots = drg._lock_programmatic_stock_sector(original_slots, _sector_snapshot())
    html = drg._render_locked_report_layout(locked_slots, "2026年07月17日", "周五")
    extracted = drg._extract_report_slots(html)

    assert "医药生物(+10.0亿)" not in extracted["stock-sector"]
    assert "国有大型银行(+5.1亿)" in extracted["stock-sector"]
    assert "电子(-532.1亿)" in extracted["stock-sector"]
    assert 'data-sector-flow-source="programmatic"' in extracted["stock-sector"]
    for slot_name in drg.REPORT_SLOT_ORDER:
        if slot_name != "stock-sector":
            assert extracted[slot_name] == drg._extract_report_slots(
                drg._render_locked_report_layout(original_slots, "2026年07月17日", "周五")
            )[slot_name]
    assert drg.validate_report_layout(html) == []
    assert drg.validate_a_share_report_facts(html, _sector_snapshot()) == []


def test_llm_stock_repair_is_overridden_by_programmatic_snapshot():
    original = drg._render_locked_report_layout(_slots(), "2026年07月17日", "周五")
    response = SimpleNamespace(
        content=(
            '<report-repair><section data-slot="stock-sector">'
            "主力净流入：银行(+5.1亿)。"
            "</section></report-repair>"
        )
    )

    with patch.object(drg, "invoke_report_llm_with_fallback", return_value=response):
        repaired = drg._rewrite_report_slots_after_validation(
            "记者素材",
            original,
            ["股票板块测试异常"],
            ["股票板块测试异常"],
            ["stock-sector"],
            1,
            "2026年07月17日",
            "周五",
            a_share_snapshot_text="程序真值",
            a_share_snapshot=_sector_snapshot(),
        )

    stock_slot = drg._extract_report_slots(repaired)["stock-sector"]
    stock_soup = drg.BeautifulSoup(stock_slot, "html.parser")
    assert stock_soup.find(attrs={"data-sector-name": "银行"}) is None
    assert "国有大型银行(+5.1亿)" in stock_slot
    assert "电子(-532.1亿)" in stock_slot


def test_local_repair_changes_only_target_slot_and_preserves_shell():
    original = drg._render_locked_report_layout(_slots(), "2026年07月15日", "周三")
    response = SimpleNamespace(
        content=(
            '<report-repair><section data-slot="stock-sector">'
            "主力净流入：医药生物(+99.8亿)；主力净流出：电子(-447.7亿)。"
            "</section></report-repair>"
        )
    )

    with patch.object(drg, "invoke_report_llm_with_fallback", return_value=response):
        repaired = drg._rewrite_report_slots_after_validation(
            "记者素材",
            original,
            ["电子真实主力净流出 -447.7亿，但页面写成资金流入"],
            ["电子真实主力净流出 -447.7亿，但页面写成资金流入"],
            ["stock-sector"],
            1,
            "2026年07月15日",
            "周三",
            a_share_snapshot_text="电子 -447.7亿",
        )

    before_slots = drg._extract_report_slots(original)
    after_slots = drg._extract_report_slots(repaired)
    assert "电子(-447.7亿)" in after_slots["stock-sector"]
    for slot_name in drg.REPORT_SLOT_ORDER:
        if slot_name != "stock-sector":
            assert after_slots[slot_name] == before_slots[slot_name]
    assert drg.validate_report_layout(repaired) == []


def test_repair_response_rejects_unknown_slots_and_page_markup():
    page_response = """
    <html><head><style>body{color:red}</style></head><body>
      <report-repair><section data-slot="stock-sector">错误内容</section></report-repair>
    </body></html>
    """
    repairs, rejected = drg._parse_slot_repair_response(page_response, ["stock-sector"])
    assert repairs == {}
    assert rejected == ["<page-markup>"]

    unknown_response = """
    <report-repair><section data-slot="unknown-slot">未知内容</section></report-repair>
    """
    repairs, rejected = drg._parse_slot_repair_response(unknown_response, ["stock-sector"])

    assert repairs == {}
    assert rejected == ["unknown-slot"]


def test_direction_error_targets_only_slots_containing_the_entity():
    slots = _slots()
    slots["tomorrow-strategy"] = "上证50ETF大阴线后注意风险。"
    html = drg._render_locked_report_layout(slots, "2026年07月15日", "周三")

    targets = drg._classify_repair_slots(
        ["上证50ETF当日涨跌幅 +0.20%，但页面出现相反方向表述“大阴线”"],
        html,
    )

    assert targets == ["tomorrow-strategy"]


def test_candle_error_targets_only_slots_containing_the_entity():
    slots = _slots()
    slots["tomorrow-strategy"] = "上证50ETF大阴线后注意风险。"
    html = drg._render_locked_report_layout(slots, "2026年07月15日", "周三")

    targets = drg._classify_repair_slots(
        ["上证50ETF当日K线形态为“大阳线”，但页面出现相反K线表述“大阴线”"],
        html,
    )

    assert targets == ["tomorrow-strategy"]


def test_draft_report_can_succeed_on_third_local_repair():
    initial_html = drg._render_locked_report_layout(_slots(), "2026年07月15日", "周三")
    commodity_results = [
        (False, ["电子资金方向错误"]),
        (False, ["电子资金方向错误"]),
        (False, ["电子资金方向错误"]),
        (True, []),
    ]

    with patch.object(drg, "MAX_REWRITE_ROUNDS", 4), patch.object(
        drg,
        "_fetch_programmatic_commodity_iv_snapshot",
        return_value=({}, "商品IV真值"),
    ), patch.object(
        drg,
        "_fetch_programmatic_commodity_kline_snapshot",
        return_value=({}, "商品K线真值"),
    ), patch.object(
        drg,
        "invoke_report_llm_with_fallback",
        return_value=SimpleNamespace(content=initial_html),
    ), patch.object(
        drg,
        "validate_commodity_cards",
        side_effect=commodity_results,
    ), patch.object(
        drg,
        "validate_a_share_report_facts",
        return_value=[],
    ), patch.object(
        drg,
        "validate_report_layout",
        return_value=[],
    ), patch.object(
        drg,
        "_classify_repair_slots",
        return_value=["commodity-panorama"],
    ), patch.object(
        drg,
        "_rewrite_report_slots_after_validation",
        side_effect=lambda _material, html, *_args, **_kwargs: html,
    ) as rewrite:
        report = drg.draft_report(
            "有效记者素材",
            {"report_date": "20260715"},
            "A股真值",
            "20260715",
        )

    assert report
    assert rewrite.call_count == 3


def test_programmatic_stock_failure_never_calls_llm_repair():
    initial_html = drg._render_locked_report_layout(_slots(), "2026年07月17日", "周五")
    snapshot = {"report_date": "20260717", **_sector_snapshot()}

    with patch.object(
        drg,
        "_fetch_programmatic_commodity_iv_snapshot",
        return_value=({}, "商品IV真值"),
    ), patch.object(
        drg,
        "_fetch_programmatic_commodity_kline_snapshot",
        return_value=({}, "商品K线真值"),
    ), patch.object(
        drg,
        "invoke_report_llm_with_fallback",
        return_value=SimpleNamespace(content=initial_html),
    ), patch.object(
        drg,
        "validate_commodity_cards",
        return_value=(True, []),
    ), patch.object(
        drg,
        "validate_a_share_report_facts",
        return_value=["银行主力净额与真值不一致：页面=+5.1亿，真值=-11.2亿"],
    ), patch.object(
        drg,
        "validate_report_layout",
        return_value=[],
    ), patch.object(
        drg,
        "_rewrite_report_slots_after_validation",
    ) as rewrite:
        report = drg.draft_report("有效记者素材", snapshot, "A股真值", "20260717")

    assert report == ""
    rewrite.assert_not_called()


def test_draft_report_fails_closed_and_archives_after_four_repairs():
    initial_html = drg._render_locked_report_layout(_slots(), "2026年07月15日", "周三")

    with patch.object(drg, "MAX_REWRITE_ROUNDS", 4), patch.object(
        drg,
        "_fetch_programmatic_commodity_iv_snapshot",
        return_value=({}, "商品IV真值"),
    ), patch.object(
        drg,
        "_fetch_programmatic_commodity_kline_snapshot",
        return_value=({}, "商品K线真值"),
    ), patch.object(
        drg,
        "invoke_report_llm_with_fallback",
        return_value=SimpleNamespace(content=initial_html),
    ), patch.object(
        drg,
        "validate_commodity_cards",
        return_value=(False, ["商品卡片仍有错误"]),
    ), patch.object(
        drg,
        "validate_a_share_report_facts",
        return_value=[],
    ), patch.object(
        drg,
        "validate_report_layout",
        return_value=[],
    ), patch.object(
        drg,
        "_classify_repair_slots",
        return_value=["commodity-panorama"],
    ), patch.object(
        drg,
        "_rewrite_report_slots_after_validation",
        side_effect=lambda _material, html, *_args, **_kwargs: html,
    ) as rewrite, patch.object(
        drg,
        "_write_failed_daily_report",
        return_value="outputs/failed.html",
    ) as archive:
        report = drg.draft_report(
            "有效记者素材",
            {"report_date": "20260715"},
            "A股真值",
            "20260715",
        )

    assert report == ""
    assert rewrite.call_count == 4
    archive.assert_called_once()

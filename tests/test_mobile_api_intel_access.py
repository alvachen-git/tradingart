import unittest
from unittest.mock import patch

_IMPORT_ERROR = None
try:
    from fastapi import HTTPException
    import mobile_api
except Exception as exc:  # pragma: no cover
    HTTPException = Exception
    mobile_api = None
    _IMPORT_ERROR = exc


@unittest.skipIf(mobile_api is None, f"mobile_api import failed: {_IMPORT_ERROR}")
class TestMobileApiIntelAccess(unittest.TestCase):
    SAFE_STOCK_HTML = """
    <html><body>
      <div class="wrap">
        <section class="hero">
          <h1>小爱选股晚报</h1>
          <div class="muted">交易日：20260601 · 生成时间：2026-06-01 20:57:12</div>
          <div class="market-note">今天盘面不是一条线猛冲，重点找刚从底部转强的机会。</div>
        </section>
        <h2>资金回流</h2>
        <div class="table-scroll"><table><thead><tr>
          <th>排名</th><th>板块</th><th>类型</th><th>分数</th><th>资金改善</th><th>流入天数</th><th>近窗涨幅</th>
        </tr></thead><tbody><tr>
          <td>1</td><td>大众出版</td><td>行业</td><td>0.90</td><td>0.0501</td><td>5</td><td>+1.16%</td>
        </tr></tbody></table></div>
        <h2>可买标的</h2>
        <div class="table-scroll"><table><thead><tr>
          <th>代码</th><th>名称</th><th>板块</th><th>板块排名</th><th>价格</th><th>信号/说明</th>
        </tr></thead><tbody><tr>
          <td>688111.SH</td><td>金山办公</td><td>软件服务</td><td>1</td><td>256.10</td><td>底部转折100.0；信号:破底翻</td>
        </tr></tbody></table></div>
        <h2>观察标的</h2>
        <div class="table-scroll"><table><thead><tr>
          <th>代码</th><th>名称</th><th>板块</th><th>板块排名</th><th>价格</th><th>信号/说明</th>
        </tr></thead><tbody><tr>
          <td>002410.SZ</td><td>广联达</td><td>软件服务</td><td>1</td><td>10.11</td><td>底部转折100.0；继续观察。</td>
        </tr></tbody></table></div>
        <h2>已买跟踪</h2>
        <div class="table-scroll"><table><thead><tr>
          <th>代码</th><th>名称</th><th>当前状态</th><th>已持有天数</th><th>今日操作</th><th>收益</th><th>原因</th>
        </tr></thead><tbody><tr>
          <td>600996.SH</td><td>贵广网络</td><td><span class="badge badge-hold">持有</span></td><td>15</td><td><span class="badge badge-hold">持有</span></td><td>3.74%</td><td>底部区间或动态止损，继续跟踪。</td>
        </tr></tbody></table></div>
      </div>
    </body></html>
    """

    EXPIRY_OPTION_HTML = """
    <html><body>
      <div class="header">
        <h1>📅 末日期权晚报</h1>
        <p>交易日：20260601 · 生成时间：2026-06-01 20:20</p>
      </div>
      <div class="card"><p>临近到期，优先做低成本、风险可控的方向性策略。</p></div>
      <div style="margin-bottom:20px;">
        <h2 class="section-title">沪深300ETF <span class="badge">剩余 3 天</span> <span class="strategy">买看涨</span></h2>
        <div class="card">
          <p><span class="label">趋势研判</span><span class="value">震荡偏强</span></p>
          <p><span class="label">策略理由</span><span class="value">波动收敛后尝试突破。</span></p>
          <p><span class="label">标的现价</span><span class="value">3.510</span></p>
          <div class="contract-box">
            <div class="contract-row">
              <div class="contract-name">IO2506-C-3600</div>
              <div class="contract-meta"><span>权利金 120</span><span>持仓 1200</span></div>
            </div>
          </div>
        </div>
      </div>
      <div class="risk-box"><p>临近到期，时间价值衰减较快。仓位不宜过重。</p></div>
    </body></html>
    """

    BROKER_POSITION_HTML = """
    <html><body>
      <h1>爱波塔-持仓数据流晚报</h1>
      <p class="sub-text">交易日：20260601 · 期货商持仓扫描</p>
      <h2 class="section-title">今日核心信号</h2>
      <div class="signal-item"><strong>PTA 多头共振</strong><br>海通、东证同步增多。</div>
      <h2 class="section-title">机构当日动向</h2>
      <table><thead><tr><th>品种</th><th>合计</th><th>明细</th></tr></thead><tbody>
        <tr><td>PTA</td><td>+12,300手</td><td>海通 +4,000；东证 +3,500</td></tr>
      </tbody></table>
      <table><thead><tr><th>品种</th><th>合计</th><th>明细</th></tr></thead><tbody>
        <tr><td>豆粕</td><td>-9,800手</td><td>国泰君安 -3,000</td></tr>
      </tbody></table>
      <h2 class="section-title">机构5日累计布局</h2>
      <div>
        <strong>累计做多</strong><br>
        1. PTA +144,621手 (约+19.96亿)<br>
        <strong>累计做空</strong><br>
        1. 豆粕 -91,200手 (约-12.30亿)<br>
      </div>
      <h2 class="section-title">外资风向标</h2>
      <div>外资集中加多 PTA。<br>空头集中在豆粕。</div>
      <h2 class="section-title">反指标信号</h2>
      <table><thead><tr><th>品种</th><th>合计净多</th><th>潜在信号</th></tr></thead><tbody>
        <tr><td>甲醇</td><td>+5,000手</td><td>反向观察</td></tr>
      </tbody></table>
      <table><thead><tr><th>品种</th><th>合计净空</th><th>潜在信号</th></tr></thead><tbody>
        <tr><td>螺纹</td><td>-6,000手</td><td>空头拥挤</td></tr>
      </tbody></table>
      <h2 class="section-title">AI毒舌点评</h2>
      <div class="highlight-box">多空都别上头，先看资金连续性。</div>
    </body></html>
    """

    def test_intel_reports_normalizes_channel_alias_and_maps_published_at(self):
        fake_rows = [
            {
                "id": 11,
                "title": "t",
                "channel_name": "末日期权晚报",
                "channel_code": "expiry_option_radar",
                "content": "<p>hello</p>",
                "publish_time": "2026-03-27 18:30:00",
            }
        ]
        with patch.object(mobile_api.sub_svc, "get_channel_contents", return_value=fake_rows) as mocked_get:
            out = mobile_api.intel_reports(channel_code="expiry_option_report", username="u1")
        self.assertEqual(len(out["items"]), 1)
        self.assertEqual(out["items"][0]["published_at"], "2026-03-27 18:30:00")
        self.assertEqual(mocked_get.call_args.kwargs["channel_code"], "expiry_option_radar")

    def test_safe_stock_mobile_render_parses_sections(self):
        out = mobile_api._build_safe_stock_mobile_render(self.SAFE_STOCK_HTML)

        self.assertIsNotNone(out)
        self.assertEqual(out["type"], "safe_stock_report")
        self.assertEqual(out["hero"]["trade_date"], "20260601")
        self.assertEqual(out["hero"]["generated_at"], "2026-06-01 20:57:12")
        self.assertIn("底部转强", out["hero"]["market_note"])
        self.assertEqual(out["sectors"][0]["sector"], "大众出版")
        self.assertEqual(out["buys"][0]["symbol"], "688111.SH")
        self.assertEqual(out["watches"][0]["name"], "广联达")
        self.assertEqual(out["tracking"][0]["action"], "持有")

    def test_expiry_option_mobile_render_parses_strategy_cards(self):
        out = mobile_api._build_expiry_option_mobile_render(self.EXPIRY_OPTION_HTML)

        self.assertIsNotNone(out)
        self.assertEqual(out["type"], "expiry_option_radar")
        self.assertIn("低成本", out["hero"]["intro"])
        self.assertEqual(out["items"][0]["name"], "沪深300ETF")
        self.assertEqual(out["items"][0]["days_left"], "3")
        self.assertEqual(out["items"][0]["strategy"], "买看涨")
        self.assertEqual(out["items"][0]["contracts"][0]["name"], "IO2506-C-3600")
        self.assertIn("时间价值", out["risks"][0])

    def test_broker_position_mobile_render_parses_sections(self):
        out = mobile_api._build_broker_position_mobile_render(self.BROKER_POSITION_HTML)

        self.assertIsNotNone(out)
        self.assertEqual(out["type"], "broker_position_report")
        self.assertEqual(out["core_signals"][0]["title"], "PTA 多头共振")
        self.assertEqual(out["institution_day"]["longs"][0]["product"], "PTA")
        self.assertEqual(out["institution_day"]["shorts"][0]["product"], "豆粕")
        self.assertEqual(out["institution_5d"]["longs"][0]["product"], "PTA")
        self.assertEqual(out["institution_5d"]["shorts"][0]["product"], "豆粕")
        self.assertIn("外资集中加多", out["foreign_notes"][0])
        self.assertEqual(out["contra"]["longs"][0]["product"], "甲醇")
        self.assertIn("别上头", out["commentary"][0])

    def test_safe_stock_report_detail_attaches_mobile_render(self):
        with patch.object(
            mobile_api.sub_svc,
            "get_content_by_id",
            return_value={
                "id": 12,
                "channel_id": 6,
                "channel_code": "safe_stock_report",
                "channel_name": "小爱选股晚报",
                "is_premium": False,
                "title": "20260601 小爱选股晚报",
                "content": self.SAFE_STOCK_HTML,
                "publish_time": "2026-06-01 20:57:12",
            },
        ):
            out = mobile_api.intel_report_detail(12, username="u1")

        self.assertEqual(out["published_at"], "2026-06-01 20:57:12")
        self.assertIn("mobile_render", out)
        self.assertEqual(out["mobile_render"]["buys"][0]["symbol"], "688111.SH")

    def test_expiry_option_report_detail_attaches_mobile_render(self):
        with patch.object(
            mobile_api.sub_svc,
            "get_content_by_id",
            return_value={
                "id": 13,
                "channel_id": 7,
                "channel_code": "expiry_option_radar",
                "channel_name": "末日期权晚报",
                "is_premium": False,
                "title": "20260601 末日期权晚报",
                "content": self.EXPIRY_OPTION_HTML,
                "publish_time": "2026-06-01 20:20:00",
            },
        ):
            out = mobile_api.intel_report_detail(13, username="u1")

        self.assertIn("mobile_render", out)
        self.assertEqual(out["mobile_render"]["type"], "expiry_option_radar")
        self.assertEqual(out["mobile_render"]["items"][0]["strategy"], "买看涨")

    def test_broker_position_report_detail_attaches_mobile_render(self):
        with patch.object(
            mobile_api.sub_svc,
            "get_content_by_id",
            return_value={
                "id": 14,
                "channel_id": 8,
                "channel_code": "broker_position_report",
                "channel_name": "期货商持仓晚报",
                "is_premium": False,
                "title": "20260601 期货商持仓晚报",
                "content": self.BROKER_POSITION_HTML,
                "publish_time": "2026-06-01 20:20:00",
            },
        ):
            out = mobile_api.intel_report_detail(14, username="u1")

        self.assertIn("mobile_render", out)
        self.assertEqual(out["mobile_render"]["type"], "broker_position_report")
        self.assertEqual(out["mobile_render"]["institution_day"]["longs"][0]["product"], "PTA")

    def test_intel_report_detail_adds_published_at_fallback(self):
        with patch.object(
            mobile_api.sub_svc,
            "get_content_by_id",
            return_value={
                "id": 9,
                "channel_id": 1,
                "is_premium": False,
                "publish_time": "2026-03-27 18:30:00",
            },
        ):
            out = mobile_api.intel_report_detail(9, username="u1")
        self.assertEqual(out["published_at"], "2026-03-27 18:30:00")

    def test_premium_report_requires_subscription(self):
        with patch.object(
            mobile_api.sub_svc,
            "get_content_by_id",
            return_value={"id": 1, "channel_id": 100, "is_premium": True},
        ), patch.object(
            mobile_api.sub_svc,
            "check_subscription_access",
            return_value={"has_access": False, "reason": "not_subscribed"},
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_report_detail(1, username="u1")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subscribe_reject_when_api_disabled(self):
        body = mobile_api.SubscribeRequest(channel_code="daily_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", False):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_subscribe(body=body, username="u1")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subscribe_reject_when_not_in_whitelist(self):
        body = mobile_api.SubscribeRequest(channel_code="daily_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            set(),
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 1, "code": "daily_report"},
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_subscribe(body=body, username="u1")
        self.assertEqual(cm.exception.status_code, 403)

    def test_subscribe_ok_when_enabled_and_whitelisted(self):
        body = mobile_api.SubscribeRequest(channel_code="daily_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            {"daily_report"},
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 1, "code": "daily_report"},
        ), patch.object(
            mobile_api.sub_svc,
            "add_subscription",
            return_value=(True, "ok"),
        ) as mocked_add:
            out = mobile_api.intel_subscribe(body=body, username="u1")

        self.assertEqual(out["message"], "ok")
        kwargs = mocked_add.call_args.kwargs
        self.assertEqual(kwargs["source_type"], "self_subscribe_whitelist")

    def test_subscribe_accepts_legacy_channel_alias(self):
        body = mobile_api.SubscribeRequest(channel_code="expiry_option_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            {"expiry_option_radar"},
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 2, "code": "expiry_option_radar"},
        ), patch.object(
            mobile_api.sub_svc,
            "add_subscription",
            return_value=(True, "ok"),
        ) as mocked_add:
            out = mobile_api.intel_subscribe(body=body, username="u1")

        self.assertEqual(out["message"], "ok")
        kwargs = mocked_add.call_args.kwargs
        self.assertEqual(kwargs["source_ref"], "api:intel_subscribe:expiry_option_radar")

    def test_subscribe_reject_for_force_paid_channel_even_if_whitelisted(self):
        body = mobile_api.SubscribeRequest(channel_code="fund_flow_report")
        with patch.object(mobile_api, "_INTEL_SELF_SUBSCRIBE_API_ENABLED", True), patch.object(
            mobile_api, "_ALLOW_SELF_SUB_IN_PROD", True
        ), patch.object(
            mobile_api,
            "_is_production_env",
            return_value=False,
        ), patch.object(
            mobile_api,
            "_EFFECTIVE_FREE_CHANNEL_CODES",
            {"daily_report"},
        ), patch.object(
            mobile_api.sub_svc,
            "get_channel_by_code",
            return_value={"id": 3, "code": "fund_flow_report"},
        ):
            with self.assertRaises(HTTPException) as cm:
                mobile_api.intel_subscribe(body=body, username="u1")
        self.assertEqual(cm.exception.status_code, 403)


if __name__ == "__main__":
    unittest.main()

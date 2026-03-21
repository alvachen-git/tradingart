import os
import sys
import types
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, text

if "data_engine" not in sys.modules:
    fake_data_engine = types.ModuleType("data_engine")
    fake_data_engine.engine = None
    sys.modules["data_engine"] = fake_data_engine

import payment_service as pay


class _FakeAliPay:
    def __init__(self, verify_ok=True):
        self.verify_ok = verify_ok

    def api_alipay_trade_page_pay(self, **kwargs):
        return "mock_query=1"

    def verify(self, payload, sign):
        return self.verify_ok


class TestPaymentService(unittest.TestCase):
    def setUp(self):
        self.orig_engine = pay.engine
        self.orig_app_id = os.environ.get("ALIPAY_APP_ID")
        self.orig_payment_enabled = os.environ.get("POINTS_PAYMENT_ENABLED")
        os.environ["ALIPAY_APP_ID"] = "test_app_id"
        os.environ["POINTS_PAYMENT_ENABLED"] = "true"

        self.engine = create_engine("sqlite:///:memory:", future=True)
        pay.engine = self.engine

        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    CREATE TABLE user_points (
                        user_id TEXT PRIMARY KEY,
                        balance INTEGER NOT NULL DEFAULT 0,
                        total_earned INTEGER NOT NULL DEFAULT 0,
                        total_spent INTEGER NOT NULL DEFAULT 0,
                        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE points_transactions (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id TEXT NOT NULL,
                        type TEXT NOT NULL,
                        amount INTEGER NOT NULL,
                        balance_after INTEGER NOT NULL,
                        ref_id TEXT,
                        description TEXT,
                        biz_id TEXT,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(
                text(
                    "CREATE UNIQUE INDEX uq_points_txn_user_type_biz "
                    "ON points_transactions(user_id, type, biz_id)"
                )
            )
            conn.execute(
                text(
                    """
                    CREATE TABLE points_orders (
                        id TEXT PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        package_name TEXT NOT NULL,
                        points_amount INTEGER NOT NULL,
                        rmb_amount NUMERIC NOT NULL,
                        paid_rmb_amount NUMERIC,
                        alipay_trade_no TEXT,
                        notify_payload_hash TEXT,
                        notified_at DATETIME,
                        status TEXT NOT NULL DEFAULT 'pending',
                        paid_at DATETIME,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            )
            conn.execute(text("CREATE UNIQUE INDEX uq_alipay_trade_no ON points_orders(alipay_trade_no)"))
            conn.execute(
                text(
                    """
                    CREATE TABLE content_channels (
                        id INTEGER PRIMARY KEY,
                        code TEXT,
                        name TEXT,
                        icon TEXT,
                        is_active INTEGER DEFAULT 1,
                        is_premium INTEGER DEFAULT 1,
                        sort_order INTEGER DEFAULT 1,
                        price_points_monthly INTEGER
                    )
                    """
                )
            )
            conn.execute(
                text(
                    """
                    INSERT INTO content_channels(id, code, name, icon, is_active, is_premium, sort_order, price_points_monthly)
                    VALUES
                    (1, 'daily_report', '复盘晚报', '📪', 1, 1, 1, 500),
                    (2, 'expiry_option_radar', '末日期权晚报', '📅', 1, 1, 2, 500),
                    (3, 'broker_position_report', '持仓密报', '🔥', 1, 1, 3, 500),
                    (4, 'fund_flow_report', '资金流晚报', '💰', 1, 1, 4, 500)
                    """
                )
            )

    def tearDown(self):
        pay.engine = self.orig_engine
        if self.orig_app_id is None:
            os.environ.pop("ALIPAY_APP_ID", None)
        else:
            os.environ["ALIPAY_APP_ID"] = self.orig_app_id

        if self.orig_payment_enabled is None:
            os.environ.pop("POINTS_PAYMENT_ENABLED", None)
        else:
            os.environ["POINTS_PAYMENT_ENABLED"] = self.orig_payment_enabled

    def test_create_topup_order_success(self):
        with patch.object(pay, "get_alipay_client", return_value=_FakeAliPay()):
            out = pay.create_topup_order("u1", "标准包")
        self.assertIsNotNone(out)
        self.assertIn("order_id", out)
        self.assertIn("pay_url", out)

        with self.engine.connect() as conn:
            row = conn.execute(
                text("SELECT status, points_amount FROM points_orders WHERE id = :id"),
                {"id": out["order_id"]},
            ).fetchone()
        self.assertEqual(row[0], "pending")
        self.assertEqual(int(row[1]), 1000)

    def test_create_topup_order_invalid_package(self):
        out = pay.create_topup_order("u1", "不存在套餐")
        self.assertIsNone(out)

    def test_process_alipay_notify_success_and_idempotent(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO points_orders(id, user_id, package_name, points_amount, rmb_amount, status)
                    VALUES ('PAY001', 'u1', '标准包', 1000, 100.00, 'pending')
                    """
                )
            )

        payload = {
            "sign": "mock_sign",
            "trade_status": "TRADE_SUCCESS",
            "out_trade_no": "PAY001",
            "trade_no": "ALI001",
            "app_id": "test_app_id",
            "total_amount": "100.00",
        }
        with patch.object(pay, "get_alipay_client", return_value=_FakeAliPay(verify_ok=True)):
            ok1, reason1 = pay.process_alipay_notify(payload)
            ok2, reason2 = pay.process_alipay_notify(payload)

        self.assertTrue(ok1)
        self.assertIn(reason1, {"ok", "already_processed"})
        self.assertTrue(ok2)
        self.assertEqual(reason2, "already_processed")

        points = pay.get_user_points("u1")
        self.assertEqual(points["balance"], 1000)
        with self.engine.connect() as conn:
            tx_count = conn.execute(
                text("SELECT COUNT(*) FROM points_transactions WHERE user_id='u1' AND type='topup'")
            ).scalar_one()
        self.assertEqual(int(tx_count), 1)

    def test_process_alipay_notify_amount_mismatch(self):
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO points_orders(id, user_id, package_name, points_amount, rmb_amount, status)
                    VALUES ('PAY002', 'u2', '标准包', 1000, 100.00, 'pending')
                    """
                )
            )

        payload = {
            "sign": "mock_sign",
            "trade_status": "TRADE_SUCCESS",
            "out_trade_no": "PAY002",
            "trade_no": "ALI002",
            "app_id": "test_app_id",
            "total_amount": "99.99",
        }
        with patch.object(pay, "get_alipay_client", return_value=_FakeAliPay(verify_ok=True)):
            ok, reason = pay.process_alipay_notify(payload)
        self.assertFalse(ok)
        self.assertEqual(reason, "amount_mismatch")
        self.assertEqual(pay.get_user_points("u2")["balance"], 0)

    def test_deduct_points_insufficient_no_transaction(self):
        ok, reason = pay.deduct_points(
            "u3",
            100,
            ref_id="1",
            description="购买测试",
            biz_id="biz_insufficient",
        )
        self.assertFalse(ok)
        self.assertEqual(reason, "余额不足")
        with self.engine.connect() as conn:
            cnt = conn.execute(text("SELECT COUNT(*) FROM points_transactions WHERE user_id='u3'"))
            cnt = cnt.scalar_one()
        self.assertEqual(int(cnt), 0)

    def test_purchase_subscription_refund_when_subscription_fails(self):
        pay.credit_points("u4", 500, ref_id="seed", description="初始化", tx_type="admin_grant", biz_id="seed_u4")
        with patch.object(pay.sub_svc, "add_subscription", return_value=(False, "mock_failed")):
            ok, msg = pay.purchase_subscription_with_points("u4", 1, months=1, biz_id="biz_refund_1")

        self.assertFalse(ok)
        self.assertIn("mock_failed", msg)
        self.assertEqual(pay.get_user_points("u4")["balance"], 500)

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT type, amount
                    FROM points_transactions
                    WHERE user_id='u4' AND biz_id='biz_refund_1'
                    ORDER BY id
                    """
                )
            ).fetchall()
        self.assertEqual([r[0] for r in rows], ["spend", "refund"])
        self.assertEqual(int(rows[0][1]), -500)
        self.assertEqual(int(rows[1][1]), 500)

        wallet = pay.get_user_points("u4")
        self.assertEqual(wallet["balance"], 500)
        self.assertEqual(wallet["total_spent"], 0)

    def test_purchase_intel_package_rolls_back_on_partial_failure(self):
        pay.credit_points("u5", 999, ref_id="seed", description="初始点数", tx_type="admin_grant", biz_id="seed_u5")

        call_count = {"n": 0}

        def _fake_add_subscription_in_tx(conn, user_id, channel_id, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 2:
                return False, "mock_channel_fail"
            return True, "ok"

        with patch.object(pay.sub_svc, "add_subscription_in_tx", side_effect=_fake_add_subscription_in_tx):
            ok, msg = pay.purchase_intel_package_with_points("u5", months=1, biz_id="biz_pkg_rb_1")

        self.assertFalse(ok)
        self.assertIn("回滚", msg)
        self.assertEqual(call_count["n"], 2)

        wallet = pay.get_user_points("u5")
        self.assertEqual(wallet["balance"], 999)
        self.assertEqual(wallet["total_spent"], 0)

        with self.engine.connect() as conn:
            tx_cnt = conn.execute(
                text("SELECT COUNT(*) FROM points_transactions WHERE user_id='u5' AND biz_id='biz_pkg_rb_1'")
            ).scalar_one()
        self.assertEqual(int(tx_cnt), 0)


if __name__ == "__main__":
    unittest.main()

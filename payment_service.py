"""
点数支付服务层

能力：
1. 充值订单创建（支付宝跳转）
2. 支付宝回调验签与到账（幂等）
3. 点数账户管理（余额/流水/扣款）
4. 点数购买订阅（失败补偿）
5. 每日对账报告与告警
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import secrets
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from data_engine import engine
import subscription_service as sub_svc


POINTS_PACKAGES = [
    {"name": "体验包", "rmb": Decimal("50.00"), "points": 500},
    {"name": "标准包", "rmb": Decimal("100.00"), "points": 1000},
    {"name": "超值包", "rmb": Decimal("500.00"), "points": 6000},
    {"name": "富豪包", "rmb": Decimal("5000.00"), "points": 80000},
]
_PACKAGES_MAP = {item["name"]: item for item in POINTS_PACKAGES}

# 晚报类付费产品（频道）
REPORT_PRODUCTS = [
    {"code": "daily_report", "name": "复盘晚报", "icon": "📊", "points_monthly": 500},
    {"code": "expiry_option_radar", "name": "末日期权晚报", "icon": "🗓️", "points_monthly": 500},
    {"code": "broker_position_report", "name": "期货商持仓晚报", "icon": "🏦", "points_monthly": 500},
    {"code": "fund_flow_report", "name": "资金流晚报", "icon": "💸", "points_monthly": 500},
]
_REPORT_PRICE_DEFAULTS = {item["code"]: int(item["points_monthly"]) for item in REPORT_PRODUCTS}

# 套餐产品：包含全部四个晚报
INTEL_PACKAGE_PRODUCT = {
    "code": "intel_package",
    "name": "情报套餐",
    "icon": "🧠",
    "points_monthly": 999,
    "includes": [item["code"] for item in REPORT_PRODUCTS],
}

ALIPAY_NOTIFY_URL = os.getenv("ALIPAY_NOTIFY_URL", "https://www.aiprota.com/api/alipay/notify")
ALIPAY_RETURN_URL = os.getenv("ALIPAY_RETURN_URL", "https://www.aiprota.com")
ALIPAY_SANDBOX = str(os.getenv("ALIPAY_SANDBOX", "true")).lower() == "true"
ALIPAY_GATEWAY = str(os.getenv("ALIPAY_GATEWAY", "")).strip()
if not ALIPAY_GATEWAY:
    ALIPAY_GATEWAY = (
        "https://openapi-sandbox.dl.alipaydev.com/gateway.do?"
        if ALIPAY_SANDBOX
        else "https://openapi.alipay.com/gateway.do?"
    )


def is_points_payment_enabled() -> bool:
    """
    支付开关（默认关闭）：
    - true/1/on/yes 才允许创建支付订单
    """
    flag = str(os.getenv("POINTS_PAYMENT_ENABLED", "false")).strip().lower()
    return flag in {"1", "true", "on", "yes"}


def _as_decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal("0")


def _payload_hash(payload: Dict[str, Any]) -> str:
    text_payload = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text_payload.encode("utf-8")).hexdigest()


def _ensure_points_row(conn, user_id: str) -> None:
    if conn.dialect.name == "sqlite":
        conn.execute(
            text(
                """
                INSERT OR IGNORE INTO user_points (user_id, balance, total_earned, total_spent, updated_at)
                VALUES (:uid, 0, 0, 0, CURRENT_TIMESTAMP)
                """
            ),
            {"uid": user_id},
        )
        return

    conn.execute(
        text(
            """
            INSERT INTO user_points (user_id, balance, total_earned, total_spent)
            VALUES (:uid, 0, 0, 0)
            ON DUPLICATE KEY UPDATE user_id = user_id
            """
        ),
        {"uid": user_id},
    )


def _tx_exists(conn, user_id: str, tx_type: str, biz_id: Optional[str]) -> bool:
    if not biz_id:
        return False
    row = conn.execute(
        text(
            """
            SELECT id
            FROM points_transactions
            WHERE user_id = :uid AND type = :t AND biz_id = :biz
            LIMIT 1
            """
        ),
        {"uid": user_id, "t": tx_type, "biz": biz_id},
    ).fetchone()
    return bool(row)


def _get_balance(conn, user_id: str, for_update: bool) -> Optional[int]:
    sql = """
        SELECT balance
        FROM user_points
        WHERE user_id = :uid
    """
    if for_update and conn.dialect.name != "sqlite":
        sql += " FOR UPDATE"
    row = conn.execute(text(sql), {"uid": user_id}).fetchone()
    return int(row[0]) if row else None


def _insert_tx(
    conn,
    *,
    user_id: str,
    tx_type: str,
    amount: int,
    balance_after: int,
    ref_id: Optional[str],
    description: Optional[str],
    biz_id: Optional[str],
) -> None:
    conn.execute(
        text(
            """
            INSERT INTO points_transactions
            (user_id, type, amount, balance_after, ref_id, description, biz_id)
            VALUES (:uid, :t, :amt, :bal, :ref_id, :desc, :biz)
            """
        ),
        {
            "uid": user_id,
            "t": tx_type,
            "amt": amount,
            "bal": balance_after,
            "ref_id": ref_id,
            "desc": description,
            "biz": biz_id,
        },
    )


def _credit_points_in_tx(
    conn,
    *,
    user_id: str,
    amount: int,
    ref_id: Optional[str],
    description: Optional[str],
    tx_type: str,
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if amount <= 0:
        return False, "amount_must_be_positive"

    if _tx_exists(conn, user_id, tx_type, biz_id):
        return True, "already_processed"

    _ensure_points_row(conn, user_id)

    # 口径修正：
    # - topup: 累计充值 +amount
    # - refund: 累计消费 -amount（净消费口径）
    # - admin_grant: 仅加余额，不计入累计充值
    if tx_type == "topup":
        conn.execute(
            text(
                """
                UPDATE user_points
                SET balance = balance + :amt,
                    total_earned = total_earned + :amt,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = :uid
                """
            ),
            {"uid": user_id, "amt": amount},
        )
    elif tx_type == "refund":
        if conn.dialect.name == "sqlite":
            conn.execute(
                text(
                    """
                    UPDATE user_points
                    SET balance = balance + :amt,
                        total_spent = CASE WHEN total_spent > :amt THEN total_spent - :amt ELSE 0 END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = :uid
                    """
                ),
                {"uid": user_id, "amt": amount},
            )
        else:
            conn.execute(
                text(
                    """
                    UPDATE user_points
                    SET balance = balance + :amt,
                        total_spent = GREATEST(total_spent - :amt, 0),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = :uid
                    """
                ),
                {"uid": user_id, "amt": amount},
            )
    else:  # admin_grant
        conn.execute(
            text(
                """
                UPDATE user_points
                SET balance = balance + :amt,
                    updated_at = CURRENT_TIMESTAMP
                WHERE user_id = :uid
                """
            ),
            {"uid": user_id, "amt": amount},
        )
    balance_after = _get_balance(conn, user_id, for_update=False) or 0
    _insert_tx(
        conn,
        user_id=user_id,
        tx_type=tx_type,
        amount=amount,
        balance_after=balance_after,
        ref_id=ref_id,
        description=description,
        biz_id=biz_id,
    )
    return True, "ok"


def get_alipay_client():
    app_id = str(os.getenv("ALIPAY_APP_ID", "")).strip()
    private_key = str(os.getenv("ALIPAY_PRIVATE_KEY", "")).replace("\\n", "\n").strip()
    public_key = str(os.getenv("ALIPAY_PUBLIC_KEY", "")).replace("\\n", "\n").strip()
    sandbox = str(os.getenv("ALIPAY_SANDBOX", "true")).lower() == "true"

    if not app_id or not private_key or not public_key:
        raise ValueError("ALIPAY_APP_ID/ALIPAY_PRIVATE_KEY/ALIPAY_PUBLIC_KEY 缺失")

    try:
        from alipay import AliPay
    except Exception as exc:  # pragma: no cover - 依赖缺失时兜底
        raise RuntimeError("python-alipay-sdk 未安装") from exc

    return AliPay(
        appid=app_id,
        app_notify_url=None,
        app_private_key_string=private_key,
        alipay_public_key_string=public_key,
        sign_type="RSA2",
        debug=sandbox,
        verbose=False,
    )


def generate_order_id() -> str:
    return f"PAY{datetime.now().strftime('%Y%m%d%H%M%S')}{secrets.token_hex(3).upper()}"


def create_topup_order(user_id: str, package_name: str) -> Optional[Dict[str, Any]]:
    if not is_points_payment_enabled():
        print("[payment][create_topup_order] blocked: POINTS_PAYMENT_ENABLED is not true")
        return None

    pkg = _PACKAGES_MAP.get(package_name)
    if not pkg or not user_id:
        return None

    try:
        alipay = get_alipay_client()
    except Exception as exc:
        print(f"[payment][create_topup_order] alipay_not_ready error={exc}")
        return None

    order_id = generate_order_id()
    rmb_amount = _as_decimal(pkg["rmb"]).quantize(Decimal("0.00"))
    points_amount = int(pkg["points"])
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO points_orders
                    (id, user_id, package_name, points_amount, rmb_amount, status)
                    VALUES (:id, :uid, :pkg, :points, :rmb, 'pending')
                    """
                ),
                {
                    "id": order_id,
                    "uid": user_id,
                    "pkg": package_name,
                    "points": points_amount,
                    "rmb": str(rmb_amount),
                },
            )

        order_string = alipay.api_alipay_trade_page_pay(
            out_trade_no=order_id,
            total_amount=f"{rmb_amount:.2f}",
            subject=f"爱波塔充值 - {package_name}",
            return_url=ALIPAY_RETURN_URL,
            notify_url=ALIPAY_NOTIFY_URL,
        )
        if str(order_string).startswith("http"):
            pay_url = str(order_string)
        else:
            gateway = ALIPAY_GATEWAY
            if "?" not in gateway:
                gateway = gateway + "?"
            elif not gateway.endswith(("?", "&")):
                gateway = gateway + "&"
            pay_url = gateway + str(order_string).lstrip("?")
        return {
            "order_id": order_id,
            "pay_url": pay_url,
            "points": points_amount,
            "rmb": float(rmb_amount),
        }
    except Exception as exc:
        print(f"[payment][create_topup_order] failed user={user_id} package={package_name} error={exc}")
        return None


def process_alipay_notify(data: Dict[str, Any]) -> tuple[bool, str]:
    raw = {str(k): str(v) for k, v in (data or {}).items()}
    sign = raw.get("sign", "")
    if not sign:
        return False, "missing_sign"

    verify_payload = dict(raw)
    verify_payload.pop("sign", None)
    verify_payload.pop("sign_type", None)

    try:
        alipay = get_alipay_client()
        if not alipay.verify(verify_payload, sign):
            return False, "invalid_signature"
    except Exception as exc:
        print(f"[payment][notify] verify_error={exc}")
        return False, "verify_error"

    trade_status = raw.get("trade_status", "")
    if trade_status not in {"TRADE_SUCCESS", "TRADE_FINISHED"}:
        return False, f"ignored_status:{trade_status}"

    order_id = raw.get("out_trade_no", "").strip()
    trade_no = raw.get("trade_no", "").strip()
    app_id = raw.get("app_id", "").strip()
    notify_amount = _as_decimal(raw.get("total_amount")).quantize(Decimal("0.00"))
    payload_hash = _payload_hash(raw)

    try:
        with engine.begin() as conn:
            sql = """
                SELECT id, user_id, package_name, points_amount, rmb_amount, status
                FROM points_orders
                WHERE id = :id
            """
            if conn.dialect.name != "sqlite":
                sql += " FOR UPDATE"
            row = conn.execute(text(sql), {"id": order_id}).fetchone()
            if not row:
                return False, "order_not_found"

            user_id = str(row[1])
            package_name = str(row[2])
            points_amount = int(row[3])
            order_rmb = _as_decimal(row[4]).quantize(Decimal("0.00"))
            status = str(row[5] or "")

            if status == "paid":
                return True, "already_processed"

            expected_app_id = str(os.getenv("ALIPAY_APP_ID", "")).strip()
            if expected_app_id and app_id != expected_app_id:
                return False, "app_id_mismatch"

            if notify_amount != order_rmb:
                return False, "amount_mismatch"

            conn.execute(
                text(
                    """
                    UPDATE points_orders
                    SET status = 'paid',
                        alipay_trade_no = :trade_no,
                        paid_rmb_amount = :paid_rmb,
                        notify_payload_hash = :payload_hash,
                        notified_at = CURRENT_TIMESTAMP,
                        paid_at = COALESCE(paid_at, CURRENT_TIMESTAMP)
                    WHERE id = :id
                    """
                ),
                {
                    "trade_no": trade_no or None,
                    "paid_rmb": str(notify_amount),
                    "payload_hash": payload_hash,
                    "id": order_id,
                },
            )

            ok, reason = _credit_points_in_tx(
                conn,
                user_id=user_id,
                amount=points_amount,
                ref_id=order_id,
                description=f"充值 {package_name}",
                tx_type="topup",
                biz_id=f"topup:{order_id}",
            )
            if not ok:
                return False, reason
            return True, reason
    except Exception as exc:
        print(f"[payment][notify] order={order_id} error={exc}")
        return False, "db_error"


def _legacy_deduct_points(
    user_id: str,
    amount: int,
    *,
    ref_id: Optional[str],
    description: Optional[str],
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if amount <= 0:
        return False, "amount_must_be_positive"
    try:
        with engine.begin() as conn:
            if _tx_exists(conn, user_id, "spend", biz_id):
                return True, "already_processed"

            _ensure_points_row(conn, user_id)
            balance = _get_balance(conn, user_id, for_update=True) or 0
            if balance < amount:
                return False, "余额不足"

            conn.execute(
                text(
                    """
                    UPDATE user_points
                    SET balance = balance - :amt,
                        total_spent = total_spent + :amt,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = :uid
                    """
                ),
                {"uid": user_id, "amt": amount},
            )
            balance_after = _get_balance(conn, user_id, for_update=False) or 0
            _insert_tx(
                conn,
                user_id=user_id,
                tx_type="spend",
                amount=-amount,
                balance_after=balance_after,
                ref_id=ref_id,
                description=description,
                biz_id=biz_id,
            )
            return True, "ok"
    except IntegrityError as exc:
        print(f"[payment][deduct] integrity user={user_id} biz={biz_id} err={exc}")
        return True, "already_processed"
    except Exception as exc:
        print(f"[payment][deduct] user={user_id} err={exc}")
        return False, "扣点失败"


def _deduct_points_in_tx(
    conn,
    user_id: str,
    amount: int,
    *,
    ref_id: Optional[str],
    description: Optional[str],
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if amount <= 0:
        return False, "amount_must_be_positive"

    if _tx_exists(conn, user_id, "spend", biz_id):
        return True, "already_processed"

    _ensure_points_row(conn, user_id)
    balance = _get_balance(conn, user_id, for_update=True) or 0
    if balance < amount:
        return False, "余额不足"

    conn.execute(
        text(
            """
            UPDATE user_points
            SET balance = balance - :amt,
                total_spent = total_spent + :amt,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = :uid
            """
        ),
        {"uid": user_id, "amt": amount},
    )
    balance_after = _get_balance(conn, user_id, for_update=False) or 0
    _insert_tx(
        conn,
        user_id=user_id,
        tx_type="spend",
        amount=-amount,
        balance_after=balance_after,
        ref_id=ref_id,
        description=description,
        biz_id=biz_id,
    )
    return True, "ok"


def deduct_points(
    user_id: str,
    amount: int,
    *,
    ref_id: Optional[str],
    description: Optional[str],
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    try:
        with engine.begin() as conn:
            return _deduct_points_in_tx(
                conn,
                user_id,
                amount,
                ref_id=ref_id,
                description=description,
                biz_id=biz_id,
            )
    except IntegrityError as exc:
        print(f"[payment][deduct] integrity user={user_id} biz={biz_id} err={exc}")
        return True, "already_processed"
    except Exception as exc:
        print(f"[payment][deduct] user={user_id} err={exc}")
        return False, "扣点失败"


def credit_points(
    user_id: str,
    amount: int,
    *,
    ref_id: Optional[str],
    description: Optional[str],
    tx_type: str = "topup",
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if tx_type not in {"topup", "refund", "admin_grant"}:
        return False, "invalid_tx_type"
    try:
        with engine.begin() as conn:
            return _credit_points_in_tx(
                conn,
                user_id=user_id,
                amount=amount,
                ref_id=ref_id,
                description=description,
                tx_type=tx_type,
                biz_id=biz_id,
            )
    except IntegrityError as exc:
        print(f"[payment][credit] integrity user={user_id} biz={biz_id} err={exc}")
        return True, "already_processed"
    except Exception as exc:
        print(f"[payment][credit] user={user_id} err={exc}")
        return False, "加点失败"


def get_user_points(user_id: str) -> Dict[str, Any]:
    if not user_id:
        return {"balance": 0, "total_earned": 0, "total_spent": 0, "updated_at": None}
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT balance, total_earned, total_spent, updated_at
                    FROM user_points
                    WHERE user_id = :uid
                    """
                ),
                {"uid": user_id},
            ).fetchone()
        if not row:
            return {"balance": 0, "total_earned": 0, "total_spent": 0, "updated_at": None}
        return {
            "balance": int(row[0] or 0),
            "total_earned": int(row[1] or 0),
            "total_spent": int(row[2] or 0),
            "updated_at": row[3],
        }
    except Exception as exc:
        print(f"[payment][get_user_points] user={user_id} err={exc}")
        return {"balance": 0, "total_earned": 0, "total_spent": 0, "updated_at": None}


def get_points_history(user_id: str, limit: int = 20) -> list[Dict[str, Any]]:
    limit = max(1, min(int(limit or 20), 100))
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, type, amount, balance_after, ref_id, description, biz_id, created_at
                    FROM points_transactions
                    WHERE user_id = :uid
                    ORDER BY id DESC
                    LIMIT :limit
                    """
                ),
                {"uid": user_id, "limit": limit},
            ).fetchall()
        return [
            {
                "id": int(row[0]),
                "type": str(row[1]),
                "amount": int(row[2] or 0),
                "balance_after": int(row[3] or 0),
                "ref_id": row[4],
                "description": row[5],
                "biz_id": row[6],
                "created_at": row[7],
            }
            for row in rows
        ]
    except Exception as exc:
        print(f"[payment][history] user={user_id} err={exc}")
        return []


def get_points_channels() -> list[Dict[str, Any]]:
    meta_map = {item["code"]: item for item in REPORT_PRODUCTS}
    if not meta_map:
        return []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, code, name, icon, is_premium, price_points_monthly
                    FROM content_channels
                    WHERE is_active = 1
                    ORDER BY sort_order, id
                    """
                )
            ).fetchall()

        by_code = {}
        for row in rows:
            code = str(row[1] or "")
            if code not in meta_map:
                continue
            by_code[code] = {
                "id": int(row[0]),
                "code": code,
                "name": row[2] or meta_map.get(code, {}).get("name"),
                "icon": row[3] or meta_map.get(code, {}).get("icon"),
                "is_premium": bool(row[4]),
                "price_points_monthly": int(row[5]) if row[5] is not None else _REPORT_PRICE_DEFAULTS.get(code),
            }

        # 按固定产品顺序输出
        result: list[Dict[str, Any]] = []
        for item in REPORT_PRODUCTS:
            code = item["code"]
            if code in by_code:
                result.append(by_code[code])
        return result
    except Exception as exc:
        print(f"[payment][get_points_channels] err={exc}")
        return []


def get_paid_products() -> list[Dict[str, Any]]:
    channels = get_points_channels()
    products = [
        {
            "product_type": "channel",
            "code": item["code"],
            "id": int(item["id"]),
            "name": item["name"],
            "icon": item.get("icon") or "📰",
            "points_monthly": int(item.get("price_points_monthly") or 0),
            "months_options": [1, 3, 6, 12],
        }
        for item in channels
    ]

    # 套餐：仅在四个频道都存在时展示
    channel_map = {item["code"]: item for item in channels}
    include_codes = list(INTEL_PACKAGE_PRODUCT["includes"])
    if all(code in channel_map for code in include_codes):
        products.append(
            {
                "product_type": "package",
                "code": INTEL_PACKAGE_PRODUCT["code"],
                "id": None,
                "name": INTEL_PACKAGE_PRODUCT["name"],
                "icon": INTEL_PACKAGE_PRODUCT["icon"],
                "points_monthly": int(INTEL_PACKAGE_PRODUCT["points_monthly"]),
                "months_options": [1, 3, 6, 12],
                "includes": include_codes,
                "includes_names": [channel_map[code]["name"] for code in include_codes],
            }
        )
    return products


def _legacy_purchase_subscription_with_points(
    user_id: str,
    channel_id: int,
    *,
    months: int = 1,
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if months <= 0:
        return False, "months_invalid"
    biz_id = biz_id or f"purchase:{user_id}:{channel_id}:{months}:{uuid4().hex[:10]}"

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT code, name, price_points_monthly
                    FROM content_channels
                    WHERE id = :cid AND is_active = 1
                    """
                ),
                {"cid": channel_id},
            ).fetchone()
        if not row:
            return False, "频道不存在"

        channel_code = str(row[0] or "")
        channel_name = str(row[1])
        price_points_monthly = row[2]
        if price_points_monthly is None:
            fallback = _REPORT_PRICE_DEFAULTS.get(channel_code)
            if fallback is None:
                return False, "该频道不支持点数购买"
            price_points_monthly = fallback

        total_cost = int(price_points_monthly) * int(months)
        ok, reason = deduct_points(
            user_id,
            total_cost,
            ref_id=str(channel_id),
            description=f"购买 {channel_name} {months}个月",
            biz_id=biz_id,
        )
        if not ok:
            return False, reason

        result = sub_svc.add_subscription(user_id, channel_id, days=months * 30)
        if isinstance(result, tuple):
            sub_ok = bool(result[0])
            sub_msg = str(result[1]) if len(result) > 1 else ""
        else:
            sub_ok = bool(result)
            sub_msg = ""

        if sub_ok:
            return True, "购买成功，有效期已更新"

        refund_ok, refund_reason = credit_points(
            user_id,
            total_cost,
            ref_id=str(channel_id),
            description=f"购买失败退款 {channel_name} {months}个月",
            tx_type="refund",
            biz_id=biz_id,
        )
        if not refund_ok:
            print(
                f"[payment][purchase] refund_failed user={user_id} channel={channel_id} "
                f"biz={biz_id} reason={refund_reason}"
            )
        return False, sub_msg or "订阅开通失败，已自动退款"
    except Exception as exc:
        print(f"[payment][purchase] user={user_id} channel={channel_id} err={exc}")
        return False, "购买失败，请稍后重试"


def _legacy_purchase_intel_package_with_points(
    user_id: str,
    *,
    months: int = 1,
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if months <= 0:
        return False, "months_invalid"

    include_codes = list(INTEL_PACKAGE_PRODUCT["includes"])
    biz_id = biz_id or f"purchase:{user_id}:{INTEL_PACKAGE_PRODUCT['code']}:{months}:{uuid4().hex[:10]}"

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, code, name
                    FROM content_channels
                    WHERE is_active = 1
                    """
                )
            ).fetchall()

        rows = [row for row in rows if str(row[1] or "") in include_codes]

        if len(rows) != len(include_codes):
            found = {str(x[1]) for x in rows}
            missing = [code for code in include_codes if code not in found]
            return False, f"套餐频道缺失：{','.join(missing)}"

        code_to_row = {str(row[1]): row for row in rows}
        ordered_rows = [code_to_row[code] for code in include_codes]

        total_cost = int(INTEL_PACKAGE_PRODUCT["points_monthly"]) * int(months)
        ok, reason = deduct_points(
            user_id,
            total_cost,
            ref_id=INTEL_PACKAGE_PRODUCT["code"],
            description=f"购买 {INTEL_PACKAGE_PRODUCT['name']} {months}个月",
            biz_id=biz_id,
        )
        if not ok:
            return False, reason

        opened_names = []
        for row in ordered_rows:
            channel_id = int(row[0])
            channel_name = str(row[2])
            result = sub_svc.add_subscription(user_id, channel_id, days=months * 30)
            sub_ok = bool(result[0]) if isinstance(result, tuple) else bool(result)
            if not sub_ok:
                refund_ok, refund_reason = credit_points(
                    user_id,
                    total_cost,
                    ref_id=INTEL_PACKAGE_PRODUCT["code"],
                    description=f"购买失败退款 {INTEL_PACKAGE_PRODUCT['name']} {months}个月",
                    tx_type="refund",
                    biz_id=biz_id,
                )
                if not refund_ok:
                    print(
                        f"[payment][package] refund_failed user={user_id} "
                        f"biz={biz_id} reason={refund_reason}"
                    )
                return False, "套餐开通失败，已自动退款"
            opened_names.append(channel_name)

        return True, f"购买成功：已开通 {', '.join(opened_names)}"
    except Exception as exc:
        print(f"[payment][package] user={user_id} err={exc}")
        return False, "套餐购买失败，请稍后重试"


def purchase_subscription_with_points(
    user_id: str,
    channel_id: int,
    *,
    months: int = 1,
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if months <= 0:
        return False, "months_invalid"
    biz_id = biz_id or f"purchase:{user_id}:{channel_id}:{months}:{uuid4().hex[:10]}"

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    """
                    SELECT code, name, price_points_monthly
                    FROM content_channels
                    WHERE id = :cid AND is_active = 1
                    """
                ),
                {"cid": channel_id},
            ).fetchone()
        if not row:
            return False, "频道不存在"

        channel_code = str(row[0] or "")
        channel_name = str(row[1] or channel_code)
        price_points_monthly = row[2]
        if price_points_monthly is None:
            fallback = _REPORT_PRICE_DEFAULTS.get(channel_code)
            if fallback is None:
                return False, "该频道不支持点数购买"
            price_points_monthly = fallback

        total_cost = int(price_points_monthly) * int(months)
        ok, reason = deduct_points(
            user_id,
            total_cost,
            ref_id=str(channel_id),
            description=f"购买 {channel_name} {months}个月",
            biz_id=biz_id,
        )
        if not ok:
            return False, reason
        if reason == "already_processed":
            return True, "already_processed"

        result = sub_svc.add_subscription(
            user_id,
            channel_id,
            days=months * 30,
            source_type="points_purchase",
            source_ref=biz_id,
            source_note=f"channel={channel_code};months={months}",
            operator="system_points",
        )
        if isinstance(result, tuple):
            sub_ok = bool(result[0])
            sub_msg = str(result[1]) if len(result) > 1 else ""
        else:
            sub_ok = bool(result)
            sub_msg = ""

        if sub_ok:
            print(
                f"[payment][purchase] success user_id={user_id} biz_id={biz_id} "
                f"channel_id={channel_id} channel_code={channel_code}"
            )
            return True, "购买成功，有效期已更新"

        refund_ok, refund_reason = credit_points(
            user_id,
            total_cost,
            ref_id=str(channel_id),
            description=f"购买失败退款 {channel_name} {months}个月",
            tx_type="refund",
            biz_id=biz_id,
        )
        if not refund_ok:
            print(
                f"[payment][purchase] refund_failed user_id={user_id} "
                f"channel_id={channel_id} biz_id={biz_id} reason={refund_reason}"
            )
        return False, sub_msg or "订阅开通失败，已自动退款"
    except Exception as exc:
        print(f"[payment][purchase] user_id={user_id} channel_id={channel_id} biz_id={biz_id} err={exc}")
        return False, "购买失败，请稍后重试"


def purchase_intel_package_with_points(
    user_id: str,
    *,
    months: int = 1,
    biz_id: Optional[str] = None,
) -> tuple[bool, str]:
    if months <= 0:
        return False, "months_invalid"

    include_codes = list(INTEL_PACKAGE_PRODUCT["includes"])
    package_code = str(INTEL_PACKAGE_PRODUCT["code"])
    package_name = str(INTEL_PACKAGE_PRODUCT["name"])
    biz_id = biz_id or f"purchase:{user_id}:{package_code}:{months}:{uuid4().hex[:10]}"
    total_cost = int(INTEL_PACKAGE_PRODUCT["points_monthly"]) * int(months)

    try:
        with engine.begin() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT id, code, name
                    FROM content_channels
                    WHERE is_active = 1
                    """
                )
            ).fetchall()
            rows = [row for row in rows if str(row[1] or "") in include_codes]
            if len(rows) != len(include_codes):
                found = {str(x[1]) for x in rows}
                missing = [code for code in include_codes if code not in found]
                return False, f"套餐频道缺失：{','.join(missing)}"

            code_to_row = {str(row[1]): row for row in rows}
            ordered_rows = [code_to_row[code] for code in include_codes]

            ok, reason = _deduct_points_in_tx(
                conn,
                user_id,
                total_cost,
                ref_id=package_code,
                description=f"购买 {package_name} {months}个月",
                biz_id=biz_id,
            )
            if not ok:
                return False, reason
            if reason == "already_processed":
                return True, "already_processed"

            opened_names: list[str] = []
            for row in ordered_rows:
                channel_id = int(row[0])
                channel_code = str(row[1] or "")
                channel_name = str(row[2] or channel_code)
                print(
                    f"[payment][package] grant_start user_id={user_id} biz_id={biz_id} "
                    f"package_code={package_code} channel_id={channel_id} channel_code={channel_code}"
                )
                sub_ok, sub_msg = sub_svc.add_subscription_in_tx(
                    conn,
                    user_id,
                    channel_id,
                    days=months * 30,
                    source_type="points_package",
                    source_ref=biz_id,
                    source_note=f"package={package_code};months={months};channel={channel_code}",
                    operator="system_points",
                )
                if not sub_ok:
                    raise RuntimeError(sub_msg or f"channel_grant_failed:{channel_code}")
                opened_names.append(channel_name)

            print(
                f"[payment][package] success user_id={user_id} biz_id={biz_id} package_code={package_code} "
                f"channels={','.join(include_codes)}"
            )
            return True, f"购买成功：已开通 {', '.join(opened_names)}"
    except RuntimeError as exc:
        print(
            f"[payment][package] rollback user_id={user_id} biz_id={biz_id} "
            f"package_code={package_code} err={exc}"
        )
        return False, "套餐开通失败，已回滚扣点与权限"
    except IntegrityError as exc:
        print(
            f"[payment][package] integrity user_id={user_id} biz_id={biz_id} "
            f"package_code={package_code} err={exc}"
        )
        return True, "already_processed"
    except Exception as exc:
        print(f"[payment][package] user_id={user_id} biz_id={biz_id} package_code={package_code} err={exc}")
        return False, "套餐购买失败，请稍后重试"


def reconcile_points_orders(trade_date: Optional[date] = None) -> Dict[str, Any]:
    target_date = trade_date or date.today()
    rows_out: list[Dict[str, Any]] = []
    anomalies: list[Dict[str, Any]] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    """
                    SELECT o.id,
                           o.user_id,
                           o.points_amount,
                           o.rmb_amount,
                           o.paid_rmb_amount,
                           o.status,
                           o.alipay_trade_no,
                           o.paid_at,
                           COUNT(t.id) AS topup_tx_count,
                           COALESCE(MAX(t.amount), 0) AS topup_amount
                    FROM points_orders o
                    LEFT JOIN points_transactions t
                      ON t.ref_id = o.id AND t.type = 'topup'
                    WHERE o.status = 'paid'
                      AND DATE(o.paid_at) = :d
                    GROUP BY o.id, o.user_id, o.points_amount, o.rmb_amount, o.paid_rmb_amount,
                             o.status, o.alipay_trade_no, o.paid_at
                    ORDER BY o.paid_at, o.id
                    """
                ),
                {"d": target_date},
            ).fetchall()

        for row in rows:
            rec = {
                "order_id": row[0],
                "user_id": row[1],
                "points_amount": int(row[2] or 0),
                "rmb_amount": float(_as_decimal(row[3])),
                "paid_rmb_amount": float(_as_decimal(row[4])),
                "status": row[5],
                "alipay_trade_no": row[6],
                "paid_at": str(row[7] or ""),
                "topup_tx_count": int(row[8] or 0),
                "topup_amount": int(row[9] or 0),
                "issues": "",
            }
            issues = []
            if rec["topup_tx_count"] == 0:
                issues.append("missing_topup_tx")
            if rec["topup_tx_count"] > 1:
                issues.append("duplicate_topup_tx")
            if rec["topup_tx_count"] >= 1 and rec["topup_amount"] != rec["points_amount"]:
                issues.append("points_mismatch")
            if rec["paid_rmb_amount"] and abs(rec["paid_rmb_amount"] - rec["rmb_amount"]) > 0.0001:
                issues.append("rmb_mismatch")
            rec["issues"] = ",".join(issues)
            rows_out.append(rec)
            if issues:
                anomalies.append(rec)

        return {
            "ok": True,
            "date": str(target_date),
            "total_paid_orders": len(rows_out),
            "anomaly_count": len(anomalies),
            "rows": rows_out,
            "anomalies": anomalies,
        }
    except Exception as exc:
        print(f"[payment][reconcile] date={target_date} err={exc}")
        return {
            "ok": False,
            "date": str(target_date),
            "total_paid_orders": 0,
            "anomaly_count": 0,
            "rows": [],
            "anomalies": [],
            "error": str(exc),
        }


def write_reconcile_report(trade_date: Optional[date] = None, base_dir: str = "static/reports") -> Dict[str, Any]:
    result = reconcile_points_orders(trade_date)
    report_date = str(result.get("date") or date.today())
    report_name = f"points_reconcile_{report_date.replace('-', '')}.csv"
    out_dir = Path(base_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / report_name

    fieldnames = [
        "order_id",
        "user_id",
        "points_amount",
        "rmb_amount",
        "paid_rmb_amount",
        "status",
        "alipay_trade_no",
        "paid_at",
        "topup_tx_count",
        "topup_amount",
        "issues",
    ]
    with report_path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in result.get("rows", []):
            writer.writerow(row)

    result["report_path"] = str(report_path)
    return result


def send_reconcile_alert(result: Dict[str, Any]) -> bool:
    alert_email = str(os.getenv("POINTS_RECONCILE_ALERT_EMAIL", "")).strip()
    if not alert_email:
        return False
    if not result.get("ok"):
        subject = f"[AiBota] 点数对账失败 {result.get('date')}"
        body = f"<p>对账执行失败：{result.get('error', 'unknown')}</p>"
    elif int(result.get("anomaly_count", 0)) <= 0:
        return False
    else:
        subject = f"[AiBota] 点数对账异常 {result.get('date')} (异常{result.get('anomaly_count')})"
        items = ""
        for row in (result.get("anomalies") or [])[:20]:
            items += f"<li>{row['order_id']} / {row['user_id']} / {row['issues']}</li>"
        body = (
            f"<p>检测到点数对账异常：{result.get('anomaly_count')} 条。</p>"
            f"<p>报告：{result.get('report_path', '')}</p>"
            f"<ul>{items}</ul>"
        )

    try:
        from email_utils2 import send_email

        return bool(send_email(alert_email, subject, body))
    except Exception as exc:
        print(f"[payment][reconcile_alert] send_failed err={exc}")
        return False


def run_daily_reconcile_and_alert(trade_date: Optional[date] = None) -> Dict[str, Any]:
    result = write_reconcile_report(trade_date=trade_date)
    alerted = send_reconcile_alert(result)
    result["alerted"] = alerted
    print(
        f"[payment][reconcile_daily] date={result.get('date')} total={result.get('total_paid_orders')} "
        f"anomaly={result.get('anomaly_count')} alerted={alerted} report={result.get('report_path')}"
    )
    return result

"""
统一订阅策略（上线前一次性执行）：
1) 老用户（按 users.created_at）现有权限统一封顶到 2026-03-31 23:59:59
2) 四个晚报频道强制标记为付费频道（is_premium=1）
3) 四个晚报补齐 points 月价（默认 500）

用法：
  python -m scripts.unify_subscription_policy_20260331 --dry-run
  python -m scripts.unify_subscription_policy_20260331 --apply
  python -m scripts.unify_subscription_policy_20260331 --apply --legacy-user-before "2026-03-21 00:00:00"
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_engine import engine


PAID_REPORT_CODES = (
    "daily_report",
    "expiry_option_radar",
    "broker_position_report",
    "fund_flow_report",
)


def _parse_dt(value: str) -> datetime:
    value = str(value or "").strip()
    fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in fmts:
        try:
            dt = datetime.strptime(value, fmt)
            if fmt == "%Y-%m-%d":
                return dt.replace(hour=23, minute=59, second=59)
            return dt
        except ValueError:
            continue
    raise ValueError(f"invalid datetime: {value}")


def _has_source_columns(conn) -> bool:
    rows = conn.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = 'user_subscriptions'
              AND column_name IN ('source_type','source_ref','source_note','granted_at','operator')
            """
        )
    ).fetchall()
    return len(rows) >= 5


def run(apply: bool, legacy_expire_at: datetime, legacy_user_before: datetime) -> int:
    policy_ref = "policy_unify_20260331"
    policy_note = f"legacy_cutoff_to_{legacy_expire_at.strftime('%Y-%m-%d')}"

    codes_sql = ",".join([f"'{c}'" for c in PAID_REPORT_CODES])

    with engine.begin() as conn:
        source_cols = _has_source_columns(conn)

        old_user_count = int(
            conn.execute(
                text("SELECT COUNT(*) FROM users WHERE created_at < :legacy_before"),
                {"legacy_before": legacy_user_before},
            ).scalar_one()
            or 0
        )

        to_cap_count = int(
            conn.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM user_subscriptions us
                    JOIN users u ON u.username = us.user_id
                    WHERE u.created_at < :legacy_before
                      AND us.is_active = 1
                      AND (us.expire_at IS NULL OR us.expire_at > :legacy_expire)
                    """
                ),
                {"legacy_before": legacy_user_before, "legacy_expire": legacy_expire_at},
            ).scalar_one()
            or 0
        )

        premium_fix_count = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM content_channels
                    WHERE code IN ({codes_sql})
                      AND (is_premium = 0 OR is_premium IS NULL)
                    """
                )
            ).scalar_one()
            or 0
        )

        points_price_fix_count = int(
            conn.execute(
                text(
                    f"""
                    SELECT COUNT(*)
                    FROM content_channels
                    WHERE code IN ({codes_sql})
                      AND (price_points_monthly IS NULL OR price_points_monthly <= 0)
                    """
                )
            ).scalar_one()
            or 0
        )

        capped_rows = 0
        premium_rows = 0
        price_rows = 0
        if apply:
            if source_cols:
                capped_rows = int(
                    conn.execute(
                        text(
                            """
                            UPDATE user_subscriptions us
                            JOIN users u ON u.username = us.user_id
                            SET us.expire_at = :legacy_expire,
                                us.updated_at = CURRENT_TIMESTAMP,
                                us.source_type = COALESCE(us.source_type, 'policy_legacy_cutoff'),
                                us.source_ref = COALESCE(us.source_ref, :policy_ref),
                                us.source_note = COALESCE(us.source_note, :policy_note),
                                us.operator = COALESCE(us.operator, 'policy_script'),
                                us.granted_at = COALESCE(us.granted_at, CURRENT_TIMESTAMP)
                            WHERE u.created_at < :legacy_before
                              AND us.is_active = 1
                              AND (us.expire_at IS NULL OR us.expire_at > :legacy_expire)
                            """
                        ),
                        {
                            "legacy_expire": legacy_expire_at,
                            "legacy_before": legacy_user_before,
                            "policy_ref": policy_ref,
                            "policy_note": policy_note,
                        },
                    ).rowcount
                    or 0
                )
            else:
                capped_rows = int(
                    conn.execute(
                        text(
                            """
                            UPDATE user_subscriptions us
                            JOIN users u ON u.username = us.user_id
                            SET us.expire_at = :legacy_expire,
                                us.updated_at = CURRENT_TIMESTAMP
                            WHERE u.created_at < :legacy_before
                              AND us.is_active = 1
                              AND (us.expire_at IS NULL OR us.expire_at > :legacy_expire)
                            """
                        ),
                        {"legacy_expire": legacy_expire_at, "legacy_before": legacy_user_before},
                    ).rowcount
                    or 0
                )

            premium_rows = int(
                conn.execute(
                    text(
                        f"""
                        UPDATE content_channels
                        SET is_premium = 1
                        WHERE code IN ({codes_sql})
                          AND (is_premium = 0 OR is_premium IS NULL)
                        """
                    )
                ).rowcount
                or 0
            )
            price_rows = int(
                conn.execute(
                    text(
                        f"""
                        UPDATE content_channels
                        SET price_points_monthly = 500
                        WHERE code IN ({codes_sql})
                          AND (price_points_monthly IS NULL OR price_points_monthly <= 0)
                        """
                    )
                ).rowcount
                or 0
            )

    print("[policy-unify] mode=", "apply" if apply else "dry-run")
    print("[policy-unify] legacy_user_before=", legacy_user_before.strftime("%Y-%m-%d %H:%M:%S"))
    print("[policy-unify] legacy_expire_at=", legacy_expire_at.strftime("%Y-%m-%d %H:%M:%S"))
    print("[policy-unify] old_user_count=", old_user_count)
    print("[policy-unify] subscriptions_need_cap=", to_cap_count)
    print("[policy-unify] channels_need_premium_fix=", premium_fix_count)
    print("[policy-unify] channels_need_price_fix=", points_price_fix_count)
    if apply:
        print("[policy-unify] subscriptions_capped=", capped_rows)
        print("[policy-unify] channels_premium_fixed=", premium_rows)
        print("[policy-unify] channels_price_fixed=", price_rows)
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Unify legacy subscription policy for recharge launch")
    parser.add_argument("--apply", action="store_true", help="Apply changes to database")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default)")
    parser.add_argument("--legacy-expire-date", default="2026-03-31", help="Legacy subscription cap date")
    parser.add_argument(
        "--legacy-user-before",
        default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        help="Users created before this datetime are treated as legacy users",
    )
    args = parser.parse_args()

    # dry-run as default if both flags omitted
    apply_mode = bool(args.apply) and not bool(args.dry_run)
    legacy_expire_dt = _parse_dt(args.legacy_expire_date)
    legacy_user_before_dt = _parse_dt(args.legacy_user_before)
    raise SystemExit(
        run(
            apply=apply_mode,
            legacy_expire_at=legacy_expire_dt,
            legacy_user_before=legacy_user_before_dt,
        )
    )

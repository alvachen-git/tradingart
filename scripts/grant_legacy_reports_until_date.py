"""
Grant legacy users report-channel access until a fixed date.

Use cases:
1) Backfill missing subscriptions for legacy users.
2) Reactivate inactive subscriptions.
3) Ensure expiry is at least the target datetime (without shortening longer expiry).

Examples:
  python scripts/grant_legacy_reports_until_date.py --dry-run
  python scripts/grant_legacy_reports_until_date.py --apply
  python scripts/grant_legacy_reports_until_date.py --apply --expire-at "2026-04-05 23:59:59"
  python scripts/grant_legacy_reports_until_date.py --apply --legacy-user-before "2026-03-24 00:00:00"
  python scripts/grant_legacy_reports_until_date.py --apply --channel-codes "macro_risk_radar" --expire-at "2026-06-30 23:59:59"
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_engine import engine


TARGET_CHANNEL_CODES: Tuple[str, ...] = (
    "fund_flow_report",
    "broker_position_report",
    "daily_report",
    "expiry_option_radar",
)


def _parse_dt(value: str) -> datetime:
    value = str(value or "").strip()
    formats = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")
    for fmt in formats:
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


def _build_codes_sql(codes: Tuple[str, ...]) -> str:
    return ",".join([f"'{code}'" for code in codes])


def _parse_channel_codes(raw: str) -> Tuple[str, ...]:
    seen = set()
    out: List[str] = []
    for item in str(raw or "").split(","):
        code = str(item or "").strip().lower()
        if not code or code in seen:
            continue
        seen.add(code)
        out.append(code)
    return tuple(out)


def _collect_preview(
    conn,
    legacy_user_before: datetime,
    target_expire_at: datetime,
    codes_sql: str,
) -> Tuple[int, List[Tuple[str, int, int]]]:
    legacy_user_count = int(
        conn.execute(
            text("SELECT COUNT(*) FROM users WHERE created_at < :legacy_before"),
            {"legacy_before": legacy_user_before},
        ).scalar_one()
        or 0
    )

    per_channel_rows = conn.execute(
        text(
            f"""
            SELECT c.code,
                   SUM(CASE WHEN us.user_id IS NULL THEN 1 ELSE 0 END) AS to_insert,
                   SUM(
                     CASE
                       WHEN us.user_id IS NOT NULL
                        AND (us.is_active = 0 OR us.expire_at IS NULL OR us.expire_at < :target_expire)
                       THEN 1 ELSE 0
                     END
                   ) AS to_update
            FROM users u
            JOIN content_channels c
              ON c.code IN ({codes_sql})
             AND c.is_active = 1
            LEFT JOIN user_subscriptions us
              ON us.user_id = u.username
             AND us.channel_id = c.id
            WHERE u.created_at < :legacy_before
            GROUP BY c.code
            ORDER BY c.code
            """
        ),
        {
            "legacy_before": legacy_user_before,
            "target_expire": target_expire_at,
        },
    ).fetchall()

    channel_plan: List[Tuple[str, int, int]] = []
    for row in per_channel_rows:
        code = str(row[0] or "").strip()
        to_insert = int(row[1] or 0)
        to_update = int(row[2] or 0)
        channel_plan.append((code, to_insert, to_update))

    return legacy_user_count, channel_plan


def run(
    apply: bool,
    target_expire_at: datetime,
    legacy_user_before: datetime,
    target_channel_codes: Tuple[str, ...],
    operator: str,
    source_note: str,
) -> int:
    if not target_channel_codes:
        print("[legacy-grant] target_channel_codes is empty")
        return 1

    codes_sql = _build_codes_sql(target_channel_codes)
    source_ref = f"legacy_grant_until_{target_expire_at.strftime('%Y%m%d')}"

    with engine.begin() as conn:
        if conn.dialect.name == "sqlite":
            print("[legacy-grant] this script is intended for MySQL environments.")
            return 1

        source_cols = _has_source_columns(conn)
        legacy_user_count, plan_rows = _collect_preview(
            conn=conn,
            legacy_user_before=legacy_user_before,
            target_expire_at=target_expire_at,
            codes_sql=codes_sql,
        )

        present_codes = {row[0] for row in plan_rows}
        missing_codes = [code for code in target_channel_codes if code not in present_codes]
        total_to_insert = sum(row[1] for row in plan_rows)
        total_to_update = sum(row[2] for row in plan_rows)

        print("[legacy-grant] mode=", "apply" if apply else "dry-run")
        print("[legacy-grant] legacy_user_before=", legacy_user_before.strftime("%Y-%m-%d %H:%M:%S"))
        print("[legacy-grant] target_expire_at=", target_expire_at.strftime("%Y-%m-%d %H:%M:%S"))
        print("[legacy-grant] target_channel_codes=", ",".join(target_channel_codes))
        print("[legacy-grant] legacy_user_count=", legacy_user_count)
        print("[legacy-grant] preview_to_insert=", total_to_insert)
        print("[legacy-grant] preview_to_update=", total_to_update)
        for code, to_insert, to_update in plan_rows:
            print(f"[legacy-grant] channel={code} to_insert={to_insert} to_update={to_update}")
        if missing_codes:
            print("[legacy-grant] missing_active_channels=", ",".join(missing_codes))
            print("[legacy-grant] abort: fix channel config first.")
            return 1

        if not apply:
            return 0

        if source_cols:
            inserted = int(
                conn.execute(
                    text(
                        f"""
                        INSERT INTO user_subscriptions
                        (user_id, channel_id, is_active, expire_at, source_type, source_ref, source_note, granted_at, operator)
                        SELECT u.username,
                               c.id,
                               1,
                               :target_expire,
                               'legacy_manual_grant',
                               :source_ref,
                               :source_note,
                               CURRENT_TIMESTAMP,
                               :operator
                        FROM users u
                        JOIN content_channels c
                          ON c.code IN ({codes_sql})
                         AND c.is_active = 1
                        LEFT JOIN user_subscriptions us
                          ON us.user_id = u.username
                         AND us.channel_id = c.id
                        WHERE u.created_at < :legacy_before
                          AND us.user_id IS NULL
                        """
                    ),
                    {
                        "target_expire": target_expire_at,
                        "legacy_before": legacy_user_before,
                        "source_ref": source_ref,
                        "source_note": source_note,
                        "operator": operator,
                    },
                ).rowcount
                or 0
            )
            updated = int(
                conn.execute(
                    text(
                        f"""
                        UPDATE user_subscriptions us
                        JOIN users u ON u.username = us.user_id
                        JOIN content_channels c
                          ON c.id = us.channel_id
                         AND c.code IN ({codes_sql})
                        SET us.is_active = 1,
                            us.expire_at = CASE
                                WHEN us.expire_at IS NULL OR us.expire_at < :target_expire
                                THEN :target_expire
                                ELSE us.expire_at
                            END,
                            us.updated_at = CURRENT_TIMESTAMP,
                            us.source_type = COALESCE(us.source_type, 'legacy_manual_grant'),
                            us.source_ref = COALESCE(us.source_ref, :source_ref),
                            us.source_note = COALESCE(us.source_note, :source_note),
                            us.operator = COALESCE(us.operator, :operator),
                            us.granted_at = COALESCE(us.granted_at, CURRENT_TIMESTAMP)
                        WHERE u.created_at < :legacy_before
                          AND (us.is_active = 0 OR us.expire_at IS NULL OR us.expire_at < :target_expire)
                        """
                    ),
                    {
                        "target_expire": target_expire_at,
                        "legacy_before": legacy_user_before,
                        "source_ref": source_ref,
                        "source_note": source_note,
                        "operator": operator,
                    },
                ).rowcount
                or 0
            )
        else:
            inserted = int(
                conn.execute(
                    text(
                        f"""
                        INSERT INTO user_subscriptions
                        (user_id, channel_id, is_active, expire_at)
                        SELECT u.username,
                               c.id,
                               1,
                               :target_expire
                        FROM users u
                        JOIN content_channels c
                          ON c.code IN ({codes_sql})
                         AND c.is_active = 1
                        LEFT JOIN user_subscriptions us
                          ON us.user_id = u.username
                         AND us.channel_id = c.id
                        WHERE u.created_at < :legacy_before
                          AND us.user_id IS NULL
                        """
                    ),
                    {"target_expire": target_expire_at, "legacy_before": legacy_user_before},
                ).rowcount
                or 0
            )
            updated = int(
                conn.execute(
                    text(
                        f"""
                        UPDATE user_subscriptions us
                        JOIN users u ON u.username = us.user_id
                        JOIN content_channels c
                          ON c.id = us.channel_id
                         AND c.code IN ({codes_sql})
                        SET us.is_active = 1,
                            us.expire_at = CASE
                                WHEN us.expire_at IS NULL OR us.expire_at < :target_expire
                                THEN :target_expire
                                ELSE us.expire_at
                            END,
                            us.updated_at = CURRENT_TIMESTAMP
                        WHERE u.created_at < :legacy_before
                          AND (us.is_active = 0 OR us.expire_at IS NULL OR us.expire_at < :target_expire)
                        """
                    ),
                    {"target_expire": target_expire_at, "legacy_before": legacy_user_before},
                ).rowcount
                or 0
            )

        print("[legacy-grant] apply_inserted=", inserted)
        print("[legacy-grant] apply_updated=", updated)
        print("[legacy-grant] done")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Grant legacy users selected report channels until a fixed date.")
    parser.add_argument("--apply", action="store_true", help="Apply DB changes.")
    parser.add_argument("--dry-run", action="store_true", help="Preview only (default).")
    parser.add_argument(
        "--expire-at",
        default="2026-04-05 23:59:59",
        help="Target expire datetime. Supports YYYY-MM-DD or YYYY-MM-DD HH:MM:SS.",
    )
    parser.add_argument(
        "--legacy-user-before",
        default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        help="Users created before this datetime are treated as legacy users.",
    )
    parser.add_argument(
        "--operator",
        default="ops_manual",
        help="Operator name for audit fields.",
    )
    parser.add_argument(
        "--channel-codes",
        default=",".join(TARGET_CHANNEL_CODES),
        help="Comma-separated channel codes to grant. e.g. macro_risk_radar",
    )
    parser.add_argument(
        "--source-note",
        default="legacy_user_report_access_backfill",
        help="Source note for audit fields.",
    )
    args = parser.parse_args()

    apply_mode = bool(args.apply) and not bool(args.dry_run)
    expire_at_dt = _parse_dt(args.expire_at)
    legacy_before_dt = _parse_dt(args.legacy_user_before)
    channel_codes = _parse_channel_codes(args.channel_codes)

    raise SystemExit(
        run(
            apply=apply_mode,
            target_expire_at=expire_at_dt,
            legacy_user_before=legacy_before_dt,
            target_channel_codes=channel_codes,
            operator=str(args.operator or "").strip() or "ops_manual",
            source_note=str(args.source_note or "").strip() or "legacy_user_report_access_backfill",
        )
    )

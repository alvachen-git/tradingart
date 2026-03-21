"""
一次性迁移：为 user_subscriptions 回填来源字段，并输出审计报表。

用法:
  python scripts/migrate_subscription_sources.py
  python scripts/migrate_subscription_sources.py --dry-run
  python scripts/migrate_subscription_sources.py --report-dir static/reports
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from data_engine import engine


SOURCE_COLUMNS = {
    "source_type": "VARCHAR(50) DEFAULT NULL",
    "source_ref": "VARCHAR(100) DEFAULT NULL",
    "source_note": "VARCHAR(255) DEFAULT NULL",
    "granted_at": "DATETIME DEFAULT NULL",
    "operator": "VARCHAR(100) DEFAULT NULL",
}


def _column_exists(conn, table_name: str, column_name: str) -> bool:
    if conn.dialect.name == "sqlite":
        rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        cols = {str(row[1]).lower() for row in rows}
        return column_name.lower() in cols

    row = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = DATABASE()
              AND table_name = :table_name
              AND column_name = :column_name
            LIMIT 1
            """
        ),
        {"table_name": table_name, "column_name": column_name},
    ).fetchone()
    return bool(row)


def _index_exists(conn, table_name: str, index_name: str) -> bool:
    if conn.dialect.name == "sqlite":
        rows = conn.execute(text(f"PRAGMA index_list({table_name})")).fetchall()
        names = {str(row[1]) for row in rows}
        return index_name in names

    row = conn.execute(
        text(
            """
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = DATABASE()
              AND table_name = :table_name
              AND index_name = :index_name
            LIMIT 1
            """
        ),
        {"table_name": table_name, "index_name": index_name},
    ).fetchone()
    return bool(row)


def _ensure_schema(conn) -> None:
    for col, ddl in SOURCE_COLUMNS.items():
        if _column_exists(conn, "user_subscriptions", col):
            continue
        conn.execute(text(f"ALTER TABLE user_subscriptions ADD COLUMN {col} {ddl}"))
        print(f"[migrate] added column user_subscriptions.{col}")

    if conn.dialect.name == "sqlite":
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_trial_grants (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT NOT NULL,
                    trial_code TEXT NOT NULL,
                    channel_id INTEGER NOT NULL,
                    days INTEGER NOT NULL,
                    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source_note TEXT DEFAULT NULL,
                    operator TEXT DEFAULT NULL,
                    UNIQUE(user_id, trial_code)
                )
                """
            )
        )
    else:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS user_trial_grants (
                    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(50) NOT NULL,
                    trial_code VARCHAR(100) NOT NULL,
                    channel_id INT NOT NULL,
                    days INT NOT NULL,
                    granted_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    source_note VARCHAR(255) DEFAULT NULL,
                    operator VARCHAR(100) DEFAULT NULL,
                    UNIQUE KEY uq_trial_user_code (user_id, trial_code),
                    INDEX idx_trial_user (user_id),
                    INDEX idx_trial_channel (channel_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )
    print("[migrate] ensured table user_trial_grants")

    index_name = "uq_user_subscriptions_user_channel"
    if _index_exists(conn, "user_subscriptions", index_name):
        return

    duplicated = conn.execute(
        text(
            """
            SELECT user_id, channel_id, COUNT(*) AS cnt
            FROM user_subscriptions
            GROUP BY user_id, channel_id
            HAVING COUNT(*) > 1
            LIMIT 10
            """
        )
    ).fetchall()
    if duplicated:
        print("[migrate] skip unique index due duplicated (user_id, channel_id):")
        for row in duplicated:
            print(f"  - user_id={row[0]} channel_id={row[1]} cnt={row[2]}")
        return

    conn.execute(
        text(
            f"CREATE UNIQUE INDEX {index_name} ON user_subscriptions(user_id, channel_id)"
        )
    )
    print(f"[migrate] created unique index {index_name}")


def _write_report(rows: list[dict], report_dir: str) -> str:
    out_dir = Path(report_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"subscription_migration_audit_{ts}.csv"
    with out_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["channel_code", "channel_name", "is_active", "source_type", "count"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return str(out_path)


def run(dry_run: bool = False, report_dir: str = "static/reports") -> int:
    with engine.begin() as conn:
        _ensure_schema(conn)

        before_total = int(conn.execute(text("SELECT COUNT(*) FROM user_subscriptions")).scalar_one() or 0)
        before_active = int(
            conn.execute(text("SELECT COUNT(*) FROM user_subscriptions WHERE is_active = 1")).scalar_one() or 0
        )

        updated = 0
        if not dry_run:
            updated = int(
                conn.execute(
                    text(
                        """
                        UPDATE user_subscriptions
                        SET source_type = 'legacy_migrated',
                            source_ref = COALESCE(source_ref, 'migration_20260320'),
                            source_note = COALESCE(source_note, 'legacy subscription migrated'),
                            granted_at = COALESCE(granted_at, updated_at, CURRENT_TIMESTAMP),
                            operator = COALESCE(operator, 'migration_script')
                        WHERE source_type IS NULL OR source_type = ''
                        """
                    )
                ).rowcount
                or 0
            )

        rows = conn.execute(
            text(
                """
                SELECT
                    COALESCE(c.code, CONCAT('channel_', us.channel_id)) AS channel_code,
                    COALESCE(c.name, 'unknown') AS channel_name,
                    us.is_active,
                    COALESCE(NULLIF(us.source_type, ''), 'empty') AS source_type,
                    COUNT(*) AS cnt
                FROM user_subscriptions us
                LEFT JOIN content_channels c ON c.id = us.channel_id
                GROUP BY channel_code, channel_name, us.is_active, source_type
                ORDER BY channel_code, us.is_active DESC, source_type
                """
            )
        ).fetchall()

        report_rows = [
            {
                "channel_code": row[0],
                "channel_name": row[1],
                "is_active": int(row[2] or 0),
                "source_type": row[3],
                "count": int(row[4] or 0),
            }
            for row in rows
        ]
        report_path = _write_report(report_rows, report_dir)

        after_total = int(conn.execute(text("SELECT COUNT(*) FROM user_subscriptions")).scalar_one() or 0)
        after_active = int(
            conn.execute(text("SELECT COUNT(*) FROM user_subscriptions WHERE is_active = 1")).scalar_one() or 0
        )

    print(
        f"[migrate] done dry_run={dry_run} updated={updated} "
        f"before_total={before_total} after_total={after_total} "
        f"before_active={before_active} after_active={after_active}"
    )
    print(f"[migrate] audit_report={report_path}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migrate subscription source metadata")
    parser.add_argument("--dry-run", action="store_true", help="Only check schema/report, do not update source_type")
    parser.add_argument("--report-dir", default="static/reports", help="Audit csv output directory")
    args = parser.parse_args()
    raise SystemExit(run(dry_run=args.dry_run, report_dir=args.report_dir))

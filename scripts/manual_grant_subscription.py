"""
手工开通订阅（受控脚本，无后台 UI）。

必填参数:
  --user-id
  --channel-code
  --days
  --source-note
  --operator
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
import subscription_service as sub_svc


def run(user_id: str, channel_code: str, days: int, source_note: str, operator: str) -> int:
    channel_code = str(channel_code or "").strip()
    if not user_id:
        print("[manual-grant] user_id is required")
        return 1
    if not channel_code:
        print("[manual-grant] channel_code is required")
        return 1
    if days <= 0:
        print("[manual-grant] days must be > 0")
        return 1
    if not source_note:
        print("[manual-grant] source_note is required")
        return 1
    if not operator:
        print("[manual-grant] operator is required")
        return 1

    with engine.connect() as conn:
        channel = conn.execute(
            text(
                """
                SELECT id, name
                FROM content_channels
                WHERE code = :code AND is_active = 1
                LIMIT 1
                """
            ),
            {"code": channel_code},
        ).fetchone()
    if not channel:
        print(f"[manual-grant] channel not found: {channel_code}")
        return 1

    channel_id = int(channel[0])
    channel_name = str(channel[1] or channel_code)
    source_ref = f"manual:{operator}:{datetime.now().strftime('%Y%m%d%H%M%S')}"
    ok, msg = sub_svc.add_subscription(
        user_id=user_id,
        channel_id=channel_id,
        days=days,
        source_type="manual",
        source_ref=source_ref,
        source_note=source_note,
        operator=operator,
    )
    if not ok:
        print(f"[manual-grant] failed user_id={user_id} channel={channel_code} err={msg}")
        return 1

    print(
        f"[manual-grant] success user_id={user_id} channel={channel_code}({channel_name}) "
        f"days={days} source_ref={source_ref}"
    )
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manual grant subscription")
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--channel-code", required=True)
    parser.add_argument("--days", required=True, type=int)
    parser.add_argument("--source-note", required=True)
    parser.add_argument("--operator", required=True)
    args = parser.parse_args()
    raise SystemExit(
        run(
            user_id=args.user_id,
            channel_code=args.channel_code,
            days=args.days,
            source_note=args.source_note,
            operator=args.operator,
        )
    )

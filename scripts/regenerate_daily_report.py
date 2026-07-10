"""Safely regenerate a historical daily report without sending duplicate email."""

import argparse
import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from sqlalchemy import text

from daily_report_generator import (
    _fetch_programmatic_a_share_snapshot,
    _normalize_report_trade_date,
    _write_daily_report_audit,
    collect_data_via_agent,
    draft_report,
    engine,
    extract_summary,
)


def _load_material(report_date: str, audit_file: str = None) -> tuple[str, Path]:
    audit_path = Path(audit_file or f"outputs/daily_report_audit_{report_date}.json")
    if not audit_path.is_absolute():
        audit_path = ROOT_DIR / audit_path
    if not audit_path.exists():
        raise FileNotFoundError(
            f"未找到审计素材 {audit_path}；如确需重新采集，请增加 --collect-new-material"
        )
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    material = str(payload.get("reporter_material") or "").strip()
    if len(material) <= 100:
        raise ValueError(f"审计文件中的记者素材无效: {audit_path}")
    return material, audit_path


def _replace_existing_content(content_id: int, report_date: str, html: str) -> Path:
    query = text("""
        SELECT ci.id, ci.title, ci.content, c.code AS channel_code
        FROM content_items ci
        JOIN content_channels c ON ci.channel_id = c.id
        WHERE ci.id = :content_id
    """)
    with engine.connect() as conn:
        row = conn.execute(query, {"content_id": content_id}).mappings().first()
    if not row:
        raise ValueError(f"未找到待替换报告 content_id={content_id}")
    if row["channel_code"] != "daily_report":
        raise ValueError(
            f"content_id={content_id} 频道为 {row['channel_code']}，不是 daily_report"
        )

    title_date = f"{report_date[4:6]}月{report_date[6:8]}日"
    if title_date not in str(row["title"] or ""):
        raise ValueError(
            f"content_id={content_id} 标题日期不匹配: {row['title']}，期望包含 {title_date}"
        )

    backup_path = ROOT_DIR / "outputs" / f"daily_report_{content_id}_before_{report_date}.html"
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    backup_path.write_text(str(row["content"] or ""), encoding="utf-8")

    with engine.begin() as conn:
        result = conn.execute(
            text("""
                UPDATE content_items
                SET content = :content,
                    summary = :summary,
                    is_published = 1
                WHERE id = :content_id
            """),
            {
                "content": html,
                "summary": extract_summary(html),
                "content_id": content_id,
            },
        )
        if result.rowcount != 1:
            raise RuntimeError(f"替换报告行数异常: {result.rowcount}")
    return backup_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="按指定交易日重新生成复盘晚报；默认仅生成预览，不发邮件、不新增内容。"
    )
    parser.add_argument("--report-date", required=True, help="交易日 YYYYMMDD")
    parser.add_argument("--content-id", type=int, help="原位替换指定 content_items.id")
    parser.add_argument("--audit-file", help="记者素材审计JSON；默认按交易日读取 outputs")
    parser.add_argument(
        "--collect-new-material",
        action="store_true",
        help="忽略旧审计素材并重新调用AI记者；历史报告一般不建议使用",
    )
    parser.add_argument("--output", help="修正版HTML输出路径")
    args = parser.parse_args()

    report_date = _normalize_report_trade_date(args.report_date)
    if report_date != str(args.report_date).strip():
        raise SystemExit("--report-date 必须是8位 YYYYMMDD")

    snapshot, snapshot_text = _fetch_programmatic_a_share_snapshot(report_date)
    if args.collect_new_material:
        material = collect_data_via_agent(report_date)
        if len(material) <= 100:
            raise SystemExit("AI记者素材采集失败")
        audit_path = Path(_write_daily_report_audit(snapshot, material)).resolve()
    else:
        material, audit_path = _load_material(report_date, args.audit_file)

    html = draft_report(
        material,
        snapshot,
        snapshot_text,
        report_trade_date=report_date,
    )
    if len(html) <= 300:
        raise SystemExit("报告事实校验未通过，未生成、未替换数据库内容")

    output_path = Path(args.output or f"outputs/corrected_daily_report_{report_date}.html")
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")

    print(f"✅ 修正版已生成: {output_path}")
    print(f"🧾 使用审计素材: {audit_path}")

    if args.content_id is not None:
        backup_path = _replace_existing_content(args.content_id, report_date, html)
        print(f"✅ 已原位替换报告 #{args.content_id}")
        print(f"💾 原报告备份: {backup_path}")
        print("📧 本命令不会发送邮件，也不会创建新的订阅内容或通知")
    else:
        print("ℹ️ 未传 --content-id，仅生成预览，数据库未修改")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
SMS verification utilities (Aliyun SMS + risk controls).

Primary usage:
1. send_register_sms_code / verify_register_sms_code
2. send_login_sms_code / verify_login_sms_code
"""

from __future__ import annotations

import json
import os
import random
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple

from sqlalchemy import text

from data_engine import engine


PHONE_REGEX = re.compile(r"^(?:\+?86)?(1[3-9]\d{9})$")

SMS_CODE_EXPIRE_SECONDS = int(os.getenv("SMS_CODE_EXPIRE_SECONDS", "300"))
SMS_SEND_INTERVAL_SECONDS = int(os.getenv("SMS_SEND_INTERVAL_SECONDS", "60"))
SMS_PHONE_DAILY_LIMIT = int(os.getenv("SMS_PHONE_DAILY_LIMIT", "20"))
SMS_IP_DAILY_LIMIT = int(os.getenv("SMS_IP_DAILY_LIMIT", "60"))
SMS_VERIFY_MAX_ATTEMPTS = int(os.getenv("SMS_VERIFY_MAX_ATTEMPTS", "5"))


def _env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return str(val).strip().lower() in {"1", "true", "yes", "on"}


def _should_force_local_mock_mode() -> bool:
    current_path = str(Path(__file__).resolve())
    in_worktree = "/.worktrees/" in current_path
    no_sms_toggle = os.getenv("SMS_ENABLED") is None and os.getenv("SMS_MOCK_MODE") is None
    no_aliyun_sms = not any(
        os.getenv(name)
        for name in [
            "ALIYUN_SMS_ACCESS_KEY_ID",
            "ALIYUN_SMS_ACCESS_KEY_SECRET",
            "ALIYUN_SMS_SIGN_NAME",
            "ALIYUN_SMS_TEMPLATE_CODE_REGISTER",
            "ALIYUN_SMS_TEMPLATE_CODE_LOGIN",
        ]
    )
    return in_worktree and no_sms_toggle and no_aliyun_sms


SMS_ENABLED = _env_bool("SMS_ENABLED", False)
SMS_MOCK_MODE = _env_bool("SMS_MOCK_MODE", False)
if _should_force_local_mock_mode():
    SMS_ENABLED = True
    SMS_MOCK_MODE = True
    print("[sms] local worktree detected without SMS config; fallback to mock mode")


def normalize_cn_phone(phone: str) -> Tuple[bool, str, str]:
    """
    Accept +86/mobile formats and normalize to mainland 11-digit phone.
    """
    raw = str(phone or "").strip()
    clean = raw.replace(" ", "").replace("-", "")
    matched = PHONE_REGEX.match(clean)
    if not matched:
        return False, "", "手机号格式错误，仅支持 +86 中国大陆手机号"
    return True, matched.group(1), "ok"


def _ensure_sms_table() -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS sms_verification_codes (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    phone VARCHAR(20) NOT NULL,
                    purpose VARCHAR(20) NOT NULL,
                    code VARCHAR(10) NOT NULL,
                    client_ip VARCHAR(64) NULL,
                    sent_at DATETIME NOT NULL,
                    expires_at DATETIME NOT NULL,
                    used_at DATETIME NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'sent',
                    verify_attempts INT NOT NULL DEFAULT 0,
                    provider_msg VARCHAR(255) NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_sms_phone_purpose_time (phone, purpose, sent_at),
                    INDEX idx_sms_ip_purpose_time (client_ip, purpose, sent_at),
                    INDEX idx_sms_expires (expires_at)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
        )


def _purpose_template_code(purpose: str) -> str:
    generic_code = os.getenv("ALIYUN_SMS_TEMPLATE_CODE", "").strip()
    if purpose == "register":
        return os.getenv("ALIYUN_SMS_TEMPLATE_CODE_REGISTER", "").strip() or generic_code
    if purpose == "login":
        return (
            os.getenv("ALIYUN_SMS_TEMPLATE_CODE_LOGIN", "").strip()
            or generic_code
            or os.getenv("ALIYUN_SMS_TEMPLATE_CODE_REGISTER", "").strip()
        )
    return ""


def _send_via_aliyun(phone: str, code: str, purpose: str) -> Tuple[bool, str]:
    access_key_id = os.getenv("ALIYUN_SMS_ACCESS_KEY_ID", "").strip()
    access_key_secret = os.getenv("ALIYUN_SMS_ACCESS_KEY_SECRET", "").strip()
    sign_name = os.getenv("ALIYUN_SMS_SIGN_NAME", "").strip()
    template_code = _purpose_template_code(purpose)

    if not access_key_id or not access_key_secret:
        return False, "短信配置缺失：未配置阿里云 AK/SK"
    if not sign_name or not template_code:
        return False, "短信配置缺失：未配置签名或模板 CODE"

    try:
        from aliyunsdkcore.client import AcsClient
        from aliyunsdkdysmsapi.request.v20170525.SendSmsRequest import SendSmsRequest
    except Exception:
        return (
            False,
            "缺少阿里云短信 SDK 依赖，请安装 aliyun-python-sdk-core 和 "
            "aliyun-python-sdk-dysmsapi",
        )

    try:
        client = AcsClient(access_key_id, access_key_secret, "cn-hangzhou")
        request = SendSmsRequest()
        request.set_accept_format("json")
        request.set_PhoneNumbers(phone)
        request.set_SignName(sign_name)
        request.set_TemplateCode(template_code)
        request.set_TemplateParam(json.dumps({"code": code}, ensure_ascii=False))

        response = client.do_action_with_exception(request)
        payload = json.loads(response.decode("utf-8"))
        if payload.get("Code") == "OK":
            return True, "短信发送成功"
        return False, f"短信发送失败：{payload.get('Message', 'unknown')}"
    except Exception as exc:
        return False, f"短信发送异常：{exc}"


def _send_sms(phone: str, code: str, purpose: str) -> Tuple[bool, str]:
    if SMS_MOCK_MODE:
        return True, "mock_sent"
    if not SMS_ENABLED:
        return False, "短信服务未启用，请联系管理员"
    return _send_via_aliyun(phone, code, purpose)


def _check_send_limit(
    conn,
    phone: str,
    purpose: str,
    client_ip: str | None,
) -> Tuple[bool, str]:
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    latest = conn.execute(
        text(
            """
            SELECT sent_at
            FROM sms_verification_codes
            WHERE phone = :phone AND purpose = :purpose AND status IN ('sent', 'used')
            ORDER BY sent_at DESC
            LIMIT 1
            """
        ),
        {"phone": phone, "purpose": purpose},
    ).fetchone()

    if latest and latest[0]:
        elapsed = (now - latest[0]).total_seconds()
        if elapsed < SMS_SEND_INTERVAL_SECONDS:
            wait_sec = int(SMS_SEND_INTERVAL_SECONDS - elapsed)
            return False, f"发送过于频繁，请 {wait_sec}s 后重试"

    phone_count = conn.execute(
        text(
            """
            SELECT COUNT(1)
            FROM sms_verification_codes
            WHERE phone = :phone AND purpose = :purpose AND sent_at >= :today_start
            """
        ),
        {"phone": phone, "purpose": purpose, "today_start": today_start},
    ).scalar() or 0
    if int(phone_count) >= SMS_PHONE_DAILY_LIMIT:
        return False, "该手机号今日发送次数已达上限"

    if client_ip:
        ip_count = conn.execute(
            text(
                """
                SELECT COUNT(1)
                FROM sms_verification_codes
                WHERE client_ip = :ip AND purpose = :purpose AND sent_at >= :today_start
                """
            ),
            {"ip": client_ip, "purpose": purpose, "today_start": today_start},
        ).scalar() or 0
        if int(ip_count) >= SMS_IP_DAILY_LIMIT:
            return False, "当前网络请求过于频繁，请稍后再试"

    return True, "ok"


def _send_code(phone: str, purpose: str, client_ip: str | None = None) -> Tuple[bool, str]:
    ok, normalized_phone, msg = normalize_cn_phone(phone)
    if not ok:
        return False, msg

    _ensure_sms_table()
    now = datetime.now()
    code = f"{random.randint(100000, 999999)}"

    with engine.begin() as conn:
        limit_ok, limit_msg = _check_send_limit(conn, normalized_phone, purpose, client_ip)
        if not limit_ok:
            return False, limit_msg

        sent_ok, sent_msg = _send_sms(normalized_phone, code, purpose)
        status = "sent" if sent_ok else "failed"
        conn.execute(
            text(
                """
                INSERT INTO sms_verification_codes
                (phone, purpose, code, client_ip, sent_at, expires_at, status, provider_msg)
                VALUES
                (:phone, :purpose, :code, :client_ip, :sent_at, :expires_at, :status, :provider_msg)
                """
            ),
            {
                "phone": normalized_phone,
                "purpose": purpose,
                "code": code,
                "client_ip": client_ip,
                "sent_at": now,
                "expires_at": now + timedelta(seconds=SMS_CODE_EXPIRE_SECONDS),
                "status": status,
                "provider_msg": sent_msg[:255],
            },
        )

    if sent_ok:
        print(
            f"[sms] send_ok purpose={purpose} phone={normalized_phone} "
            f"ip={client_ip or '-'} mode={'mock' if SMS_MOCK_MODE else 'aliyun'}"
        )
        if SMS_MOCK_MODE:
            return True, f"开发环境验证码：{code}"
        return True, "验证码已发送"

    print(
        f"[sms] send_failed purpose={purpose} phone={normalized_phone} "
        f"ip={client_ip or '-'} reason={sent_msg}"
    )
    return False, sent_msg


def _verify_code(phone: str, code: str, purpose: str) -> Tuple[bool, str]:
    ok, normalized_phone, msg = normalize_cn_phone(phone)
    if not ok:
        return False, msg

    code_str = str(code or "").strip()
    if not re.fullmatch(r"\d{6}", code_str):
        return False, "验证码格式错误"

    _ensure_sms_table()
    now = datetime.now()
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                SELECT id, code, expires_at, used_at, verify_attempts
                FROM sms_verification_codes
                WHERE phone = :phone AND purpose = :purpose AND status IN ('sent', 'used')
                ORDER BY sent_at DESC
                LIMIT 1
                """
            ),
            {"phone": normalized_phone, "purpose": purpose},
        ).fetchone()

        if not row:
            return False, "请先获取短信验证码"

        record_id, real_code, expires_at, used_at, attempts = row
        if used_at is not None:
            return False, "验证码已使用，请重新获取"
        if expires_at is None or expires_at < now:
            conn.execute(
                text(
                    "UPDATE sms_verification_codes SET status='expired' WHERE id=:id AND used_at IS NULL"
                ),
                {"id": record_id},
            )
            return False, "验证码已过期，请重新获取"
        if int(attempts or 0) >= SMS_VERIFY_MAX_ATTEMPTS:
            return False, "验证码错误次数过多，请重新获取"

        if code_str != str(real_code):
            conn.execute(
                text(
                    """
                    UPDATE sms_verification_codes
                    SET verify_attempts = verify_attempts + 1
                    WHERE id = :id AND used_at IS NULL
                    """
                ),
                {"id": record_id},
            )
            return False, "验证码错误"

        updated = conn.execute(
            text(
                """
                UPDATE sms_verification_codes
                SET used_at = :now, status='used'
                WHERE id = :id AND used_at IS NULL
                """
            ),
            {"id": record_id, "now": now},
        ).rowcount

        if int(updated or 0) <= 0:
            return False, "验证码状态异常，请重试"

    return True, "ok"


def send_register_sms_code(phone: str, client_ip: str | None = None) -> Tuple[bool, str]:
    return _send_code(phone, "register", client_ip=client_ip)


def verify_register_sms_code(phone: str, code: str) -> Tuple[bool, str]:
    return _verify_code(phone, code, "register")


def send_login_sms_code(phone: str, client_ip: str | None = None) -> Tuple[bool, str]:
    return _send_code(phone, "login", client_ip=client_ip)


def verify_login_sms_code(phone: str, code: str) -> Tuple[bool, str]:
    return _verify_code(phone, code, "login")

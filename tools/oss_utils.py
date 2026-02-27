import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict


DEFAULT_SIGNED_URL_TTL_SEC = 21600


def _get_oss_config() -> Dict[str, str]:
    return {
        "endpoint": os.getenv("OSS_ENDPOINT", "").strip(),
        "bucket": os.getenv("OSS_BUCKET", "").strip(),
        "access_key_id": os.getenv("OSS_ACCESS_KEY_ID", "").strip(),
        "access_key_secret": os.getenv("OSS_ACCESS_KEY_SECRET", "").strip(),
    }


def is_oss_configured() -> bool:
    cfg = _get_oss_config()
    return all(cfg.values())


def _build_bucket():
    try:
        import oss2
    except Exception as e:
        raise RuntimeError(f"未安装 oss2，请先安装依赖: {e}")

    cfg = _get_oss_config()
    if not all(cfg.values()):
        raise RuntimeError("OSS 配置不完整，请检查 OSS_ENDPOINT/OSS_BUCKET/OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET")

    auth = oss2.Auth(cfg["access_key_id"], cfg["access_key_secret"])
    bucket = oss2.Bucket(auth, cfg["endpoint"], cfg["bucket"])
    return bucket


def upload_bytes(object_key: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """
    上传字节流到 OSS。若对象已存在则跳过上传并返回 True。
    """
    try:
        bucket = _build_bucket()
        if bucket.object_exists(object_key):
            return True
        result = bucket.put_object(object_key, data, headers={"Content-Type": content_type})
        return 200 <= int(getattr(result, "status", 0)) < 300
    except Exception as e:
        print(f"[OSS] 上传失败 key={object_key}: {e}")
        return False


def generate_signed_get_url(object_key: str, expires_sec: Optional[int] = None) -> Optional[Dict[str, str]]:
    """
    生成 GET 签名 URL。
    返回 {"url": "...", "expires_at": "..."}；失败返回 None。
    """
    ttl = expires_sec
    if ttl is None:
        try:
            ttl = int(os.getenv("OSS_SIGNED_URL_TTL_SEC", str(DEFAULT_SIGNED_URL_TTL_SEC)))
        except Exception:
            ttl = DEFAULT_SIGNED_URL_TTL_SEC

    try:
        bucket = _build_bucket()
        url = bucket.sign_url("GET", object_key, ttl)
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl)).isoformat()
        return {"url": url, "expires_at": expires_at}
    except Exception as e:
        print(f"[OSS] 签名失败 key={object_key}: {e}")
        return None

from __future__ import annotations

import json
import os
from pathlib import Path

from sqlalchemy import create_engine

from risk_index_service import refresh_geopolitical_risk_snapshot


def _load_env() -> None:
    current_dir = Path(__file__).resolve().parent
    candidates = [current_dir / ".env", current_dir.parent / ".env"]
    env_path = next((path for path in candidates if path.exists()), None)
    if env_path is None:
        return

    try:
        from dotenv import load_dotenv

        load_dotenv(dotenv_path=env_path, override=True)
        return
    except Exception:
        pass

    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


def _build_engine():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')
    if not all([db_user, db_password, db_host, db_name]):
        raise ValueError('数据库配置缺失，请检查 .env 中的 DB_HOST / DB_USER / DB_PASSWORD / DB_NAME。')
    db_url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(db_url, pool_recycle=3600, pool_pre_ping=True)


def main() -> None:
    _load_env()
    engine = _build_engine()
    snapshot = refresh_geopolitical_risk_snapshot(engine=engine, persist=True, use_news_explainer=True)
    print(json.dumps(snapshot, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()

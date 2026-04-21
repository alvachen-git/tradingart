from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict


class JsonDiskCache:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path_for_key(self, key: Dict[str, Any]) -> Path:
        key_str = json.dumps(key, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(key_str.encode("utf-8")).hexdigest()
        return self.root / f"{digest}.json"

    def get(self, key: Dict[str, Any]) -> Dict[str, Any] | None:
        path = self._path_for_key(key)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def set(self, key: Dict[str, Any], value: Dict[str, Any]) -> None:
        path = self._path_for_key(key)
        payload = json.dumps(value, ensure_ascii=False, indent=2)
        path.write_text(payload, encoding="utf-8")

    def invalidate(self, symbol: str | None = None) -> int:
        removed = 0
        for path in self.root.glob("*.json"):
            if symbol is None:
                path.unlink(missing_ok=True)
                removed += 1
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                path.unlink(missing_ok=True)
                removed += 1
                continue
            if str(data.get("symbol", "")).strip() == symbol:
                path.unlink(missing_ok=True)
                removed += 1
        return removed


#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_GODOT_BIN="/Applications/Godot.app/Contents/MacOS/Godot"

resolve_godot_bin() {
  if [[ -n "${GODOT_BIN:-}" ]]; then
    echo "$GODOT_BIN"
    return 0
  fi
  if [[ -x "$DEFAULT_GODOT_BIN" ]]; then
    echo "$DEFAULT_GODOT_BIN"
    return 0
  fi
  local cmd=""
  cmd="$(command -v godot4 || true)"
  if [[ -n "$cmd" ]]; then
    echo "$cmd"
    return 0
  fi
  cmd="$(command -v godot || true)"
  if [[ -n "$cmd" ]]; then
    echo "$cmd"
    return 0
  fi
  return 1
}

GODOT_BIN="$(resolve_godot_bin || true)"
if [[ -z "$GODOT_BIN" ]]; then
  echo "[ERROR] Godot binary not found."
  echo "Set GODOT_BIN or install Godot to /Applications/Godot.app."
  exit 1
fi

ensure_writable_home() {
  local target_home="${HOME:-}"
  if [[ -z "$target_home" || ! -w "$target_home" ]]; then
    target_home="/tmp/tradingart_godot_home"
    export HOME="$target_home"
  fi
  mkdir -p "$HOME/Library/Caches/Godot" "$HOME/Library/Application Support/Godot" >/dev/null 2>&1 || true
}

ensure_writable_home

if [[ "${1:-}" == "--import-only" ]]; then
  "$GODOT_BIN" --headless --path "$ROOT_DIR" --import --quit
  echo "[OK] Assets imported."
  exit 0
fi

echo "[INFO] Importing assets..."
"$GODOT_BIN" --headless --path "$ROOT_DIR" --import --quit

echo "[INFO] Launching Godot client..."
exec "$GODOT_BIN" --path "$ROOT_DIR"

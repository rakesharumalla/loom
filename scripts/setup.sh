#!/usr/bin/env bash
# Idempotent setup. Safe to re-run. Exits 3 if Lakebase Search must be enabled in the UI first.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
uv sync -q
uv run python scripts/setup.py --phase all "$@"

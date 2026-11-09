#!/usr/bin/env bash
# Removes everything the demo created. Pass --yes to skip the prompt.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] && set -a && . ./.env && set +a
uv run python scripts/teardown.py "$@"

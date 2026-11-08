#!/usr/bin/env bash

set -euo pipefail

# Work from the project directory so relative paths are always valid.
cd "$(dirname "$0")"

SCRIPT_NAME="${1:-parser.py}"

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: 'uv' was not found in PATH." >&2
    exit 1
fi

uv run "$SCRIPT_NAME"

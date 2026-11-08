#!/usr/bin/env bash

set -euo pipefail

# Work from the project directory so relative paths are always valid.
cd "$(dirname "$0")"

HOST="${1:-127.0.0.1}"
PORT="${2:-3000}"

if ! command -v uv >/dev/null 2>&1; then
    echo "Error: 'uv' was not found in PATH." >&2
    exit 1
fi

# Run the parser (or the requested script) through the project environment.
uv run parser.py

# Then serve the repository (index.html and cfp.db) over HTTP so the viewer can auto-load it.
echo "Serving index.html at http://${HOST}:${PORT}/"
exec uv run python -m http.server "$PORT" --bind "$HOST"

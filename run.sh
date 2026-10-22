#!/usr/bin/env bash

set -euo pipefail

# Work from the project directory so relative paths are always valid.
cd "$(dirname "$0")"

VENV_NAME="${1:-parser}"
SCRIPT_NAME="${2:-parser.py}"

if command -v python >/dev/null 2>&1; then
    PYTHON=python
elif command -v python3 >/dev/null 2>&1; then
    PYTHON=python3
else
    echo "Error: neither 'python' nor 'python3' was found in PATH." >&2
    exit 1
fi

if [ ! -f "$VENV_NAME/bin/activate" ]; then
    echo "Virtual environment '$VENV_NAME' not found. Creating it..."
    "$PYTHON" -m venv "$VENV_NAME"
    source "$VENV_NAME/bin/activate"
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    echo "Activating existing virtual environment '$VENV_NAME'"
    source "$VENV_NAME/bin/activate"
fi

echo "Running script '$SCRIPT_NAME' in '$VENV_NAME' virtual environment with '$PYTHON' command"
"$PYTHON" "$SCRIPT_NAME"
echo "Deactivating virtual environment '$VENV_NAME'"
deactivate


#!/usr/bin/env bash

set -euo pipefail

# Work from the project directory so relative paths are always valid.
cd "$(dirname "$0")"

VENV_NAME="${1:-parser}"

if [ ! -f "$VENV_NAME/bin/activate" ]; then
    echo "Virtual environment '$VENV_NAME' not found. Creating it..."
    python3 -m venv "$VENV_NAME"
    source "$VENV_NAME/bin/activate"
    echo "Installing dependencies..."
    pip install -r requirements.txt
else
    echo "Activating existing virtual environment '$VENV_NAME'"
    source "$VENV_NAME/bin/activate"
fi

python parser.py
deactivate


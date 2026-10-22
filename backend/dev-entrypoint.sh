#!/bin/bash
set -e

# Resolve the virtualenv relative to this script so the entrypoint works on any
# checkout (it used to contain one developer's absolute macOS path).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PATH="${VENV_PATH:-$SCRIPT_DIR/venv312}"

echo "Starting application (Development Mode)..."

if [ ! -d "$VENV_PATH" ]; then
    echo "ERROR: Virtual environment not found at $VENV_PATH"
    echo "Please create it first:"
    echo "  python3 -m venv $VENV_PATH"
    exit 1
fi

echo "Activating virtual environment: $VENV_PATH"
# shellcheck disable=SC1091
source "$VENV_PATH/bin/activate"

echo "Starting Flask application..."
exec python main.py

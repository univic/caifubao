#!/usr/bin/env bash
set -euo pipefail

ENV_FILE="${1:-.env.local}"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT_DIR"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
else
  echo "No $ENV_FILE found. Copy .env.example to .env.local for DB-backed development."
  echo "Continuing with the current shell environment."
fi

echo "Caifubao dev environment"
echo "  APP_ENV=${APP_ENV:-DEV}"
echo "  Mongo=${MONGODB_HOST:-<unset>}:${MONGODB_PORT:-<unset>}/${MONGODB_NAME:-<unset>}"
echo "  Frontend mock API=${VITE_USE_MOCK_API:-false}"
echo

cleanup() {
  jobs -p | xargs -r kill
}
trap cleanup EXIT

(
  cd backend
  ./venv312/bin/python main.py
) &

(
  cd frontend
  npm run dev -- --host 127.0.0.1
) &

wait

#!/usr/bin/env bash
# run-step1.sh
# Runs only Step 1 (Work Queue Serializer) of the price pipeline.
#
# Usage:
#   ./scripts/run-step1.sh                  # default: tick data
#   ./scripts/run-step1.sh --data-type ohlc
#   DATA_TYPE=ohlc ./scripts/run-step1.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$REPO_ROOT/.venv"
DATA_TYPE="${DATA_TYPE:-tick}"

# Allow overriding data type via CLI flag
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-type)
      DATA_TYPE="$2"
      shift 2
      ;;
    *)
      echo "[ERROR] Unknown argument: $1" >&2
      echo "Usage: $0 [--data-type tick|ohlc]" >&2
      exit 1
      ;;
  esac
done

# Activate virtualenv if present
if [[ -f "$VENV/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
else
  echo "[WARN]  No .venv found at $VENV — using system Python."
fi

# Use sudo for docker if needed
DOCKER=$(docker info >/dev/null 2>&1 && echo docker || echo "sudo docker")

# Wait for Redis to be reachable (max 30s)
echo "[INFO]  Waiting for Redis to be ready..."
for i in $(seq 1 30); do
  if $DOCKER compose -f "$REPO_ROOT/docker/docker-compose.yml" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "[INFO]  Redis is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "[ERROR] Redis did not become ready in time." >&2
    exit 1
  fi
  sleep 1
done

echo "[INFO]  Running Step 1 (Work Queue Serializer) — data_type=$DATA_TYPE"
cd "$REPO_ROOT"
python -m pipeline.main --serialize-only --data-type "$DATA_TYPE"

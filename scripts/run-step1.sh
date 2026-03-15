#!/usr/bin/env bash
# run-step1.sh
# Runs only Step 1 (Work Queue Serializer) of the price pipeline.
# The worker listens on pipeline_trigger_queue for trigger messages.
#
# Usage:
#   ./scripts/run-step1.sh
#
# To trigger it, publish a message to the Redis stream from another terminal:
#   redis-cli XADD pipeline_trigger_queue '*' data \
#     '{"storage_path":"tests/data","data_type":"tick"}'

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
VENV="$REPO_ROOT/.venv"

# Activate virtualenv if present
if [[ -f "$VENV/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source "$VENV/bin/activate"
else
  echo "[WARN]  No .venv found at $VENV — using system Python."
fi

# Use sudo for docker if needed
DOCKER=$(docker info >/dev/null 2>&1 && echo docker || echo "sudo docker")

# Ensure Redis is running
echo "[INFO]  Ensuring Redis is running..."
$DOCKER compose -f "$REPO_ROOT/docker/docker-compose.yml" up -d redis

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

echo "[INFO]  Starting Step 1 (Serializer) — listening on pipeline_trigger_queue..."
echo "[INFO]  Send a trigger with:"
echo "          redis-cli XADD pipeline_trigger_queue '*' data '{\"storage_path\":\"tests/data\",\"data_type\":\"tick\"}'"
echo ""

cd "$REPO_ROOT"
python -m pipeline.main --serialize-only

#!/usr/bin/env bash
# deploy/setup-vm.sh
#
# Bootstraps a fresh Linux VM (Ubuntu/Debian) for running the price pipeline.
# Works on AWS EC2, GCP Compute Engine, DigitalOcean Droplets, or any Linux box.
#
# Usage:
#   ssh user@vm 'bash -s' < deploy/setup-vm.sh
#   # or copy repo to VM and run:
#   bash deploy/setup-vm.sh

set -euo pipefail

log()  { echo "[INFO]  $(date +%H:%M:%S) $*"; }
warn() { echo "[WARN]  $(date +%H:%M:%S) $*"; }

# ─── 1. Install Docker if missing ────────────────────────────────────────────

if ! command -v docker &>/dev/null; then
  log "Installing Docker..."
  curl -fsSL https://get.docker.com | sh
  sudo systemctl enable --now docker
  sudo usermod -aG docker "$USER"
  log "Docker installed. You may need to re-login for group membership."
else
  log "Docker already installed: $(docker --version)"
fi

# ─── 2. Install Docker Compose plugin if missing ─────────────────────────────

if ! docker compose version &>/dev/null; then
  log "Installing Docker Compose plugin..."
  sudo apt-get update -qq && sudo apt-get install -y -qq docker-compose-plugin
  log "Docker Compose plugin installed."
else
  log "Docker Compose already installed: $(docker compose version)"
fi

# ─── 3. Create data directories ──────────────────────────────────────────────

DATA_DIR="/data"
log "Creating data directories under $DATA_DIR..."

sudo mkdir -p "$DATA_DIR/raw"
sudo mkdir -p "$DATA_DIR/processed"
sudo chown -R "$USER:$USER" "$DATA_DIR"

log "  $DATA_DIR/raw       — drop CSV files here"
log "  $DATA_DIR/processed — pipeline output (Parquet, abstractions)"

# ─── 4. Create environment file template ─────────────────────────────────────

ENV_FILE="/opt/pipeline/.env"
sudo mkdir -p /opt/pipeline
if [ ! -f "$ENV_FILE" ]; then
  log "Creating environment template at $ENV_FILE"
  sudo tee "$ENV_FILE" > /dev/null <<'ENVEOF'
# ── Pipeline settings ─────────────────────────────────────────────
# Adjust these for your deployment. All are read via PIPELINE_ prefix.

PIPELINE_REDIS_URL=redis://redis:6379/0
PIPELINE_STORAGE_BACKEND=local
PIPELINE_INPUT_BASE_PATH=/data/raw
PIPELINE_OUTPUT_BASE_PATH=/data/processed
PIPELINE_CONFIG_PATH=/app/config/pipeline_config.yaml
PIPELINE_WORKER_COUNT=2

# ── Object storage (optional — uncomment for S3/GCS/Spaces) ──────
# PIPELINE_STORAGE_BACKEND=s3
# PIPELINE_INPUT_BASE_PATH=s3://my-bucket/raw
# PIPELINE_OUTPUT_BASE_PATH=s3://my-bucket/processed
# PIPELINE_AWS_ACCESS_KEY_ID=
# PIPELINE_AWS_SECRET_ACCESS_KEY=
# PIPELINE_AWS_REGION=us-east-1
#
# For GCS:
# PIPELINE_STORAGE_BACKEND=gcs
# PIPELINE_INPUT_BASE_PATH=gcs://my-bucket/raw
# PIPELINE_OUTPUT_BASE_PATH=gcs://my-bucket/processed
# GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/gcp-sa-key.json
#
# For DigitalOcean Spaces (S3-compatible):
# PIPELINE_STORAGE_BACKEND=s3
# PIPELINE_INPUT_BASE_PATH=s3://my-space/raw
# PIPELINE_OUTPUT_BASE_PATH=s3://my-space/processed
# PIPELINE_AWS_ACCESS_KEY_ID=<spaces-key>
# PIPELINE_AWS_SECRET_ACCESS_KEY=<spaces-secret>
# PIPELINE_S3_ENDPOINT_URL=https://nyc3.digitaloceanspaces.com
ENVEOF
  sudo chown "$USER:$USER" "$ENV_FILE"
  log "Edit $ENV_FILE before first run."
else
  warn "$ENV_FILE already exists — skipping."
fi

# ─── 5. Firewall: only SSH ───────────────────────────────────────────────────

if command -v ufw &>/dev/null; then
  log "Configuring UFW firewall (SSH only, Redis not exposed externally)..."
  sudo ufw allow OpenSSH
  sudo ufw --force enable
  log "Firewall enabled. Redis (6379) is only accessible inside Docker network."
fi

# ─── Summary ──────────────────────────────────────────────────────────────────

log ""
log "✅ VM is ready."
log ""
log "Next steps:"
log "  1. Clone your repo:  git clone <repo-url> /opt/pipeline/app"
log "  2. Edit env file:    nano $ENV_FILE"
log "  3. Start pipeline:   cd /opt/pipeline/app && docker compose -f deploy/docker-compose.prod.yml up -d"
log "  4. Send trigger:     docker compose -f deploy/docker-compose.prod.yml exec redis redis-cli XADD pipeline_trigger_queue '*' data '{\"storage_path\":\"/data/raw\",\"data_type\":\"tick\",\"config_path\":\"/app/config/pipeline_config.yaml\"}'"

#!/usr/bin/env bash
# minikube-start.sh
# Sets up Minikube if not installed, then starts it and applies k8s manifests.

set -euo pipefail

K8S_DIR="$(cd "$(dirname "$0")/../k8s" && pwd)"

# ─── Helpers ──────────────────────────────────────────────────────────────────

log()  { echo "[INFO]  $*"; }
warn() { echo "[WARN]  $*"; }
die()  { echo "[ERROR] $*" >&2; exit 1; }

# ─── 1. Install kubectl if missing ────────────────────────────────────────────

if ! command -v kubectl &>/dev/null; then
  log "kubectl not found — installing..."
  KUBECTL_VERSION=$(curl -fsSL https://dl.k8s.io/release/stable.txt)
  curl -fsSLo /tmp/kubectl \
    "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl"
  chmod +x /tmp/kubectl
  sudo install -o root -g root -m 0755 /tmp/kubectl /usr/local/bin/kubectl
  rm /tmp/kubectl
  log "kubectl $(kubectl version --client -o json | grep -m1 gitVersion | tr -d '", ' | cut -d: -f2) installed."
else
  log "kubectl already installed: $(kubectl version --client --short 2>/dev/null || true)"
fi

# ─── 2. Install Minikube if missing ───────────────────────────────────────────

if ! command -v minikube &>/dev/null; then
  log "Minikube not found — installing..."
  curl -fsSLo /tmp/minikube \
    "https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64"
  sudo install /tmp/minikube /usr/local/bin/minikube
  rm /tmp/minikube
  log "Minikube $(minikube version --short) installed."
else
  log "Minikube already installed: $(minikube version --short)"
fi

# ─── 3. Ensure current user is in the docker group ───────────────────────────

if ! docker version &>/dev/null; then
  warn "Cannot reach Docker daemon. Adding '$USER' to the 'docker' group..."
  sudo usermod -aG docker "$USER"
  log "User added to 'docker' group."
  log "Re-launching this script under the new group membership..."
  exec newgrp docker <<EOF
bash "$(realpath "$0")"
EOF
  exit 0
fi

# ─── 4. Start Minikube (skip if already running) ──────────────────────────────

MINIKUBE_STATUS=$(minikube status --format='{{.Host}}' 2>/dev/null || echo "Stopped")

if [ "$MINIKUBE_STATUS" = "Running" ]; then
  log "Minikube is already running — skipping start."
else
  log "Starting Minikube..."
  minikube start
fi

# ─── 5. Point Docker CLI at Minikube's daemon ─────────────────────────────────

log "Configuring Docker to use Minikube's daemon..."
eval "$(minikube docker-env)"
log "Docker now points to Minikube. Build images here to avoid a registry push."

# ─── 6. Apply k8s manifests ───────────────────────────────────────────────────

log "Applying Kubernetes manifests from: $K8S_DIR"

for manifest in namespace.yaml configmap.yaml secret.yaml deployment.yaml service.yaml hpa.yaml; do
  FILE="$K8S_DIR/$manifest"
  if [ -f "$FILE" ]; then
    log "  kubectl apply -f $manifest"
    kubectl apply -f "$FILE"
  else
    warn "  $manifest not found — skipping."
  fi
done

# ─── 7. Summary ───────────────────────────────────────────────────────────────

log ""
log "✅ Minikube is up and manifests are applied."
log ""
log "Useful commands:"
log "  minikube dashboard          # open the web UI"
log "  minikube tunnel             # expose LoadBalancer services locally"
log "  kubectl get pods -n price-server"
log "  minikube stop               # stop the cluster"

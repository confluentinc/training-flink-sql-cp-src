#!/usr/bin/env bash
# setup-s3proxy-rclone.sh — Deploy S3proxy from cp-flink-sql and configure rclone on Ubuntu
#
# Based on: https://github.com/rjmfernandes/cp-flink-sql#start-s3proxy
#
# Usage:
#   ./setup-s3proxy-rclone.sh [--repo-dir /path/to/cp-flink-sql] [--no-port-forward] [--skip-rclone]
#   ./setup-s3proxy-rclone.sh --stop   # Stop port-forward only
#
# Prerequisites:
#   - kubectl, rclone
#   - Kubernetes cluster with confluent namespace
#   - Run from cp-flink-sql repo dir, or pass --repo-dir

set -euo pipefail

REPO_DIR="${CPFLINK_SQL_DIR:-}"
DO_PORT_FORWARD=true
DO_RCLONE=true
DO_STOP=false
NAMESPACE="${NAMESPACE:-confluent}"
S3PROXY_PORT=8000
RCLONE_REMOTE="s3proxy"

die() { echo "ERROR: $*" >&2; exit 1; }
log() { echo ">> $*"; }
have() { command -v "$1" >/dev/null 2>&1; }

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Deploy S3proxy from cp-flink-sql and configure rclone for browsing the warehouse bucket.

Options:
  --repo-dir DIR    Path to cp-flink-sql repo (default: . if s3proxy/ exists, else \$HOME/cp-flink-sql)
  --no-port-forward Skip starting port-forward (use if already running)
  --skip-rclone     Skip rclone configuration
  --stop            Stop port-forward on port 8000 and exit
  --namespace NS    Kubernetes namespace (default: confluent)
  -h, --help        Show this help

Examples:
  cd ~/cp-flink-sql && $0
  $0 --repo-dir /home/training/cp-flink-sql
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo-dir)     REPO_DIR="$2"; shift 2 ;;
    --no-port-forward) DO_PORT_FORWARD=false; shift ;;
    --skip-rclone)  DO_RCLONE=false; shift ;;
    --stop)         DO_STOP=true; shift ;;
    --namespace)    NAMESPACE="$2"; shift 2 ;;
    -h|--help)      usage; exit 0 ;;
    *)              die "Unknown option: $1" ;;
  esac
done

# Handle --stop
if [[ "$DO_STOP" == true ]]; then
  if have lsof; then
    pids=$(lsof -ti "tcp:${S3PROXY_PORT}" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
      echo "$pids" | xargs -r kill -9 2>/dev/null || true
      log "Stopped port-forward on port ${S3PROXY_PORT}"
    else
      log "No process found on port ${S3PROXY_PORT}"
    fi
  else
    die "lsof not found, cannot stop port-forward"
  fi
  exit 0
fi

# Resolve s3proxy location (training project: labs-src/s3proxy, cp-flink-sql: s3proxy/)
if [[ -z "${REPO_DIR}" ]]; then
  if [[ -d "labs-src/k8s/s3proxy" ]] && [[ -f "labs-src/k8s/s3proxy/s3proxy-deployment.yaml" ]]; then
    S3PROXY_DIR="$(pwd)/labs-src/k8s/s3proxy"
  elif [[ -d "k8s/s3proxy" ]] && [[ -f "k8s/s3proxy/s3proxy-deployment.yaml" ]]; then
    S3PROXY_DIR="$(pwd)/k8s/s3proxy"
  elif [[ -d "$HOME/confluent-flink/k8s/s3proxy" ]]; then
    S3PROXY_DIR="$HOME/confluent-flink/k8s/s3proxy"
  elif [[ -d "$HOME/cp-flink-sql/s3proxy" ]]; then
    S3PROXY_DIR="$HOME/cp-flink-sql/s3proxy"
  else
    die "Cannot find s3proxy. Run from project root (confluent-flink) or pass --repo-dir /path/to/dir/containing/s3proxy"
  fi
else
  if [[ -d "$REPO_DIR/labs-src/k8s/s3proxy" ]]; then
    S3PROXY_DIR="$REPO_DIR/labs-src/k8s/s3proxy"
  elif [[ -d "$REPO_DIR/k8s/s3proxy" ]]; then
    S3PROXY_DIR="$REPO_DIR/k8s/s3proxy"
  elif [[ -d "$REPO_DIR/s3proxy" ]]; then
    S3PROXY_DIR="$REPO_DIR/s3proxy"
  else
    die "s3proxy directory not found in $REPO_DIR"
  fi
fi

if [[ ! -f "$S3PROXY_DIR/s3proxy-deployment.yaml" ]]; then
  die "s3proxy-deployment.yaml not found in $S3PROXY_DIR"
fi

have kubectl || die "kubectl not found. Install: https://kubernetes.io/docs/tasks/tools/"
[[ "$DO_RCLONE" == true ]] && { have rclone || die "rclone not found. Install: sudo apt install rclone"; }

# --- Deploy S3proxy ---
log "Deploying S3proxy from $S3PROXY_DIR"
kubectl apply -f "$S3PROXY_DIR/s3proxy-deployment.yaml" -n "$NAMESPACE"
kubectl delete job s3proxy-init -n "$NAMESPACE" 2>/dev/null || true
kubectl apply -f "$S3PROXY_DIR/s3proxy-init-job.yaml" -n "$NAMESPACE"

# --- Wait for S3proxy pod ---
log "Waiting for S3proxy pod to be ready..."
if ! kubectl wait --for=condition=ready pod -l app=s3proxy -n "$NAMESPACE" --timeout=120s 2>/dev/null; then
  log "Waiting for any s3proxy pod..."
  sleep 5
  until kubectl get pods -n "$NAMESPACE" -l app=s3proxy -o jsonpath='{.items[0].status.phase}' 2>/dev/null | grep -q Running; do
    echo -n "."
    sleep 2
  done
  echo
fi

# --- Wait for init job (optional, may already be complete) ---
log "Waiting for s3proxy-init job..."
kubectl wait --for=condition=complete job/s3proxy-init -n "$NAMESPACE" --timeout=60s 2>/dev/null || {
  log "Init job may have failed or already completed. Continuing..."
}

# --- Port forward ---
if [[ "$DO_PORT_FORWARD" == true ]]; then
  # Kill existing forward on port 8000 if any
  if have lsof; then
    pids=$(lsof -ti "tcp:${S3PROXY_PORT}" 2>/dev/null || true)
    if [[ -n "$pids" ]]; then
      log "Stopping existing process on port ${S3PROXY_PORT}"
      echo "$pids" | xargs -r kill -9 2>/dev/null || true
      sleep 1
    fi
  fi

  log "Starting port-forward s3proxy ${S3PROXY_PORT}:8000 (background)"
  kubectl -n "$NAMESPACE" port-forward "service/s3proxy" "${S3PROXY_PORT}:8000" >/dev/null 2>&1 &
  PF_PID=$!
  sleep 2
  if kill -0 "$PF_PID" 2>/dev/null; then
    log "Port-forward running (PID $PF_PID)"
  else
    die "Port-forward failed to start"
  fi
fi

# --- Verify warehouse bucket was created by init job ---
log "Verifying warehouse bucket (init job creates it via mc)..."
if kubectl get job s3proxy-init -n "$NAMESPACE" -o jsonpath='{.status.succeeded}' 2>/dev/null | grep -q "1"; then
  log "Warehouse bucket ready (init job succeeded)."
else
  log "Init job may still be running or failed. Re-run setup-lab.sh --lab 3 to verify."
fi

# --- Configure rclone ---
if [[ "$DO_RCLONE" == true ]]; then
  RCLONE_CONF="${RCLONE_CONFIG:-$HOME/.config/rclone/rclone.conf}"
  mkdir -p "$(dirname "$RCLONE_CONF")"

  # Remove existing [s3proxy] section if present
  if [[ -f "$RCLONE_CONF" ]] && grep -q "^\[${RCLONE_REMOTE}\]" "$RCLONE_CONF" 2>/dev/null; then
    log "Updating existing rclone remote [${RCLONE_REMOTE}]"
    awk -v section="[${RCLONE_REMOTE}]" '
      $0 == section { skip=1; next }
      skip && /^\[/ { skip=0 }
      skip { next }
      { print }
    ' "$RCLONE_CONF" > "${RCLONE_CONF}.tmp" && mv "${RCLONE_CONF}.tmp" "$RCLONE_CONF"
  fi

  log "Adding rclone remote [${RCLONE_REMOTE}]"
  cat >> "$RCLONE_CONF" << EOF

[${RCLONE_REMOTE}]
type = s3
provider = Other
env_auth = false
access_key_id = admin
secret_access_key = password
endpoint = http://localhost:${S3PROXY_PORT}
EOF
  log "rclone config written to $RCLONE_CONF"

  # --- Mount warehouse for GUI browsing ---
  log "Mounting warehouse bucket at ~/warehouse-mount"
  fusermount -u ~/warehouse-mount 2>/dev/null || true
  mkdir -p ~/warehouse-mount
  rclone mount ${RCLONE_REMOTE}:warehouse ~/warehouse-mount --daemon
fi

# --- Summary ---
echo
log "Done."
echo
echo "  S3proxy:     http://localhost:${S3PROXY_PORT}"
echo "  Bucket:      warehouse"
echo "  Credentials: admin / password"
if [[ "$DO_RCLONE" == true ]]; then
  echo
  echo "  Browse with rclone:"
  echo "    rclone lsd ${RCLONE_REMOTE}:"
  echo "    rclone ls ${RCLONE_REMOTE}:warehouse"
  echo
  echo "  Browse in file manager (already mounted):"
  echo "    nautilus ~/warehouse-mount   # or thunar, dolphin"
  echo
  echo "  Unmount: fusermount -u ~/warehouse-mount"
fi
echo


#!/usr/bin/env bash
# setup-all.sh — Provision the entire training environment from scratch
#
# Creates: Kind cluster, CFK operator, Confluent Platform (Kafka, SR, C3),
#          cert-manager, Flink Kubernetes Operator, CMF, S3proxy, port-forwards,
#          Flink environment, catalog, database, compute pool, rclone mount.
#
# Usage:
#   ./setup-all.sh                      # full setup
#   ./setup-all.sh --skip-kind          # skip Kind cluster creation (use existing)
#   ./setup-all.sh --skip-rclone        # skip rclone + FUSE mount
#   ./setup-all.sh --teardown           # delete everything
#   ./setup-all.sh --name my-cluster    # use a custom Kind cluster name
#
# Prerequisites: kind, kubectl, helm, confluent CLI, curl, jq

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-confluent-flink}"
KIND_IMAGE="kindest/node:v1.35.1"
NAMESPACE="confluent"
CMF_URL="http://localhost:8080"
CERT_MANAGER_VER="v1.20.2"
FLINK_OPERATOR_CHART_VER="1.140.1"

# Local Docker images (pre-built, loaded into Kind instead of pulling from ECR)
CMF_IMAGE="519856050701.dkr.ecr.us-west-2.amazonaws.com/docker/dev/confluentinc/cp-cmf:2.3.0"
FLINK_SQL_IMAGE="confluentinc/cp-flink-sql:1.19-cp7"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LABS_SRC="$(dirname "$SCRIPT_DIR")"
REPO_ROOT="$(dirname "$LABS_SRC")"
CMF_LOCAL_CHART=""
for _chart_path in \
    "${REPO_ROOT}/FlinkSQL-CP-IMAGES/confluent-manager-for-apache-flink" \
    "/opt/cp-flink-sql-deploy/cmf-chart/confluent-manager-for-apache-flink" \
    "${REPO_ROOT}/confluent-manager-for-apache-flink"; do
  if [[ -d "$_chart_path" ]]; then
    CMF_LOCAL_CHART="$_chart_path"
    break
  fi
done

SKIP_KIND=false
SKIP_RCLONE=false
DO_TEARDOWN=false

# ── Helpers ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'

log()  { echo -e "${GREEN}>> $*${NC}"; }
warn() { echo -e "${YELLOW}⚠  $*${NC}"; }
err()  { echo -e "${RED}ERROR: $*${NC}" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

banner() {
  echo
  echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
  echo -e "${CYAN}  $*${NC}"
  echo -e "${CYAN}════════════════════════════════════════════════════════════${NC}"
  echo
}

wait_for_pods() {
  local label="${1:-}" timeout="${2:-300}"
  log "Waiting for pods to be ready (timeout ${timeout}s)..."
  if [[ -n "$label" ]]; then
    kubectl wait --for=condition=ready pod -l "$label" -n "$NAMESPACE" --timeout="${timeout}s" 2>/dev/null || true
  fi
  local elapsed=0
  while true; do
    local not_ready
    not_ready=$(kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null \
      | grep -v Completed \
      | awk '{split($2,a,"/"); if (a[1]!=a[2] || $3!="Running") print $1}' || true)
    if [[ -z "$not_ready" ]]; then
      log "All pods are ready."
      return 0
    fi
    if (( elapsed >= timeout )); then
      warn "Timeout waiting for pods. Still not ready: ${not_ready}"
      return 1
    fi
    sleep 5
    elapsed=$((elapsed + 5))
    echo -n "."
  done
}

wait_for_endpoint() {
  local ns="$1" name="$2" timeout="${3:-120}"
  log "Waiting for endpoint ${name} in ${ns}..."
  local elapsed=0
  while true; do
    local addr
    addr=$(kubectl get endpoints -n "$ns" "$name" -o jsonpath='{.subsets[0].addresses[0].ip}' 2>/dev/null || true)
    if [[ -n "$addr" ]]; then
      log "Endpoint ${name} is ready (${addr})."
      return 0
    fi
    if (( elapsed >= timeout )); then
      warn "Timeout waiting for endpoint ${name}."
      return 1
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
}

# ── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-kind)   SKIP_KIND=true;   shift ;;
    --skip-rclone) SKIP_RCLONE=true; shift ;;
    --teardown)    DO_TEARDOWN=true; shift ;;
    --name)        KIND_CLUSTER_NAME="${2:-}"; shift 2 ;;
    -h|--help)
      sed -n '2,/^$/s/^# //p' "$0"
      exit 0 ;;
    *) err "Unknown option: $1" ;;
  esac
done

# ── Teardown ─────────────────────────────────────────────────────────────────
if [[ "$DO_TEARDOWN" == true ]]; then
  banner "Tearing down environment"
  "${LABS_SRC}/scripts/portfw.sh" --stop 2>/dev/null || true
  "${LABS_SRC}/scripts/setup-s3proxy-rclone.sh" --stop 2>/dev/null || true
  fusermount -u ~/warehouse-mount 2>/dev/null || umount ~/warehouse-mount 2>/dev/null || true
  if have kind && kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    kind delete cluster --name "${KIND_CLUSTER_NAME}"
    log "Kind cluster '${KIND_CLUSTER_NAME}' deleted."
  fi
  log "Teardown complete."
  exit 0
fi

# ── Preflight checks ────────────────────────────────────────────────────────
banner "Preflight checks"
for cmd in kubectl helm curl jq; do
  have "$cmd" || err "${cmd} is required but not found in PATH"
done
[[ "$SKIP_KIND" == true ]] || have kind || err "kind is required (use --skip-kind to skip)"
have confluent || warn "confluent CLI not found — Flink environment/catalog/compute-pool steps will be skipped"

# ════════════════════════════════════════════════════════════════════════════
# PHASE 1: Kubernetes cluster
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 1: Kubernetes cluster"

if [[ "$SKIP_KIND" == true ]]; then
  log "Skipping Kind cluster creation (--skip-kind)."
else
  if kind get clusters 2>/dev/null | grep -qx "${KIND_CLUSTER_NAME}"; then
    log "Kind cluster '${KIND_CLUSTER_NAME}' already exists."
  else
    log "Creating Kind cluster '${KIND_CLUSTER_NAME}' ..."
    kind create cluster --name "${KIND_CLUSTER_NAME}" --image "${KIND_IMAGE}"
  fi
fi

kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}" >/dev/null 2>&1 \
  || err "Cannot reach Kubernetes cluster. Check your kubeconfig."

# Load local Docker images into Kind
log "Loading local Docker images into Kind cluster..."
for img in "$CMF_IMAGE" "$FLINK_SQL_IMAGE"; do
  if docker image inspect "$img" >/dev/null 2>&1; then
    log "Loading ${img##*/} into Kind..."
    kind load docker-image "$img" --name "${KIND_CLUSTER_NAME}"
  else
    warn "Image not found locally: $img (will attempt pull from registry)"
  fi
done

# ════════════════════════════════════════════════════════════════════════════
# PHASE 2: CFK Operator
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 2: Confluent for Kubernetes (CFK) Operator"

if ! kubectl get namespace "$NAMESPACE" >/dev/null 2>&1; then
  kubectl create namespace "$NAMESPACE"
fi
kubectl config set-context --current --namespace="$NAMESPACE"

helm repo add confluentinc https://packages.confluent.io/helm 2>/dev/null || true
helm repo update

if helm status operator -n "$NAMESPACE" >/dev/null 2>&1; then
  log "CFK operator already installed."
else
  log "Installing CFK operator..."
  helm upgrade --install operator confluentinc/confluent-for-kubernetes -n "$NAMESPACE"
fi

log "Waiting for CFK operator pod..."
sleep 10
kubectl wait --for=condition=ready pod -l app=confluent-operator -n "$NAMESPACE" --timeout=180s

# ════════════════════════════════════════════════════════════════════════════
# PHASE 3: Confluent Platform (Kafka, SR, Control Center)
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 3: Confluent Platform components"

log "Applying confluent-platform.yaml..."
kubectl apply -f "${LABS_SRC}/k8s/confluent-platform.yaml" -n "$NAMESPACE"

log "Waiting for Confluent Platform pods (this may take 3-5 minutes)..."
wait_for_pods "" 600

# ════════════════════════════════════════════════════════════════════════════
# PHASE 4: cert-manager
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 4: cert-manager"

if kubectl get namespace cert-manager >/dev/null 2>&1; then
  log "cert-manager namespace exists, assuming already installed."
else
  log "Installing cert-manager ${CERT_MANAGER_VER}..."
  kubectl create -f "https://github.com/jetstack/cert-manager/releases/download/${CERT_MANAGER_VER}/cert-manager.yaml"
fi

wait_for_endpoint "cert-manager" "cert-manager-webhook" 180

# ════════════════════════════════════════════════════════════════════════════
# PHASE 5: Flink Kubernetes Operator
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 5: Flink Kubernetes Operator"

if helm status cp-flink-kubernetes-operator -n "$NAMESPACE" >/dev/null 2>&1; then
  log "Flink Kubernetes Operator already installed."
else
  log "Installing Flink Kubernetes Operator (chart ${FLINK_OPERATOR_CHART_VER})..."
  kubectl config set-context --current --namespace="$NAMESPACE"
  helm upgrade --install cp-flink-kubernetes-operator \
    --version "${FLINK_OPERATOR_CHART_VER}" \
    confluentinc/flink-kubernetes-operator \
    --set watchNamespaces="{${NAMESPACE}}"
fi

log "Waiting for Flink operator pod..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=flink-kubernetes-operator \
  -n "$NAMESPACE" --timeout=180s 2>/dev/null || wait_for_pods "" 180

# ════════════════════════════════════════════════════════════════════════════
# PHASE 6: Confluent Manager for Apache Flink (CMF)
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 6: Confluent Manager for Apache Flink (CMF)"

CMF_TAG="${CMF_IMAGE##*:}"
CMF_FULL="${CMF_IMAGE%%:*}"
CMF_REPO="${CMF_FULL%/*}"
CMF_NAME="${CMF_FULL##*/}"

if helm status cmf -n "$NAMESPACE" >/dev/null 2>&1; then
  log "CMF already installed."
elif [[ -n "$CMF_LOCAL_CHART" && -d "$CMF_LOCAL_CHART" ]]; then
  log "Installing CMF from local chart (${CMF_LOCAL_CHART})..."
  helm upgrade --install cmf "$CMF_LOCAL_CHART" \
    --set image.repository="${CMF_REPO}" \
    --set image.name="${CMF_NAME}" \
    --set image.tag="${CMF_TAG}" \
    --set image.pullPolicy=Never \
    --set cmf.sql.production=false \
    --namespace "$NAMESPACE"
else
  warn "Local CMF chart not found, falling back to remote chart..."
  helm upgrade --install cmf confluentinc/confluent-manager-for-apache-flink \
    --version "~2.3.0" --set cmf.sql.production=false \
    --namespace "$NAMESPACE"
fi

log "Waiting for CMF pod..."
wait_for_pods "" 300

# ════════════════════════════════════════════════════════════════════════════
# PHASE 7: S3proxy (checkpoints + HA storage)
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 7: S3proxy for Flink state storage"

RCLONE_FLAG=""
[[ "$SKIP_RCLONE" == true ]] && RCLONE_FLAG="--skip-rclone"

"${LABS_SRC}/scripts/setup-s3proxy-rclone.sh" ${RCLONE_FLAG}

# ════════════════════════════════════════════════════════════════════════════
# PHASE 8: Port forwarding
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 8: Port forwarding"

"${LABS_SRC}/scripts/portfw.sh"

log "Waiting for CMF API to become reachable..."
RETRIES=0
until curl -sf "${CMF_URL}/cmf/api/v1/environments" >/dev/null 2>&1; do
  RETRIES=$((RETRIES + 1))
  if (( RETRIES > 30 )); then
    warn "CMF API not reachable at ${CMF_URL} after 60s. Continuing anyway..."
    break
  fi
  sleep 2
done

# ════════════════════════════════════════════════════════════════════════════
# PHASE 9: Flink environment, catalog, database, compute pool
# ════════════════════════════════════════════════════════════════════════════
banner "Phase 9: Flink resources (environment, catalog, database, compute pool)"

if ! have confluent; then
  warn "confluent CLI not found. Skipping Flink resource creation."
  warn "You can create them manually after installing the CLI."
else
  # Environment
  log "Creating Flink environment 'training-env'..."
  confluent flink environment create training-env \
    --url "${CMF_URL}" \
    --kubernetes-namespace "$NAMESPACE" 2>/dev/null \
    || log "Environment 'training-env' may already exist."

  # Catalog
  log "Creating Flink catalog 'training-catalog'..."
  confluent flink catalog create "${LABS_SRC}/flink/catalog.json" \
    --url "${CMF_URL}" 2>/dev/null \
    || log "Catalog 'training-catalog' may already exist."

  # Database (REST API — CLI doesn't support this yet)
  log "Creating Flink database 'training-kafka' via REST API..."
  curl -sf -H "Content-Type: application/json" \
    -X POST "${CMF_URL}/cmf/api/v1/catalogs/kafka/training-catalog/databases" \
    -d @"${LABS_SRC}/flink/database.json" >/dev/null 2>&1 \
    || log "Database 'training-kafka' may already exist."

  # Compute pool
  log "Creating Flink compute pool 'training-compute-pool'..."
  confluent flink compute-pool create "${LABS_SRC}/flink/compute-pool.json" \
    --environment training-env \
    --url "${CMF_URL}" 2>/dev/null \
    || log "Compute pool 'training-compute-pool' may already exist."

  # Verify
  log "Verifying Flink resources..."
  echo
  echo "  Environments:"
  confluent flink environment list --url "${CMF_URL}" 2>/dev/null || true
  echo
  echo "  Catalogs:"
  confluent flink catalog list --url "${CMF_URL}" 2>/dev/null || true
  echo
  echo "  Compute pools:"
  confluent flink compute-pool list --environment training-env --url "${CMF_URL}" 2>/dev/null || true
  echo
fi

# ════════════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════════════
banner "Setup complete!"

cat <<EOF
  Kubernetes cluster : kind-${KIND_CLUSTER_NAME}
  Namespace          : ${NAMESPACE}

  Services (port-forwarded):
    Control Center   : http://localhost:9021
    CMF UI           : http://localhost:8080
    Schema Registry  : http://localhost:8081
    Kafka            : localhost:9094
    Flink REST       : http://localhost:8082
    S3proxy          : http://localhost:8000

  Flink resources:
    Environment      : training-env
    Catalog          : training-catalog
    Database         : training-kafka
    Compute pool     : training-compute-pool

  S3proxy credentials: admin / password
  Warehouse mount    : ~/warehouse-mount

  To open Flink SQL shell:
    confluent flink shell \\
      --compute-pool training-compute-pool \\
      --environment training-env \\
      --url ${CMF_URL}

  To teardown everything:
    $0 --teardown
EOF

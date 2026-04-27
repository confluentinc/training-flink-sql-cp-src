#!/usr/bin/env bash
# kind-cluster.sh — Spin up Kind + K8s Dashboard and manage login/proxy
# Usage:
#   ./kind-cluster.sh                 # create cluster, install dashboard, print token, start proxy
#   ./kind-cluster.sh --print-token   # print a fresh dashboard token
#   ./kind-cluster.sh --start-proxy   # (re)start kubectl proxy
#   ./kind-cluster.sh --stop-proxy    # stop kubectl proxy started by this script
#   ./kind-cluster.sh --delete        # delete the Kind cluster
# Options:
#   --name <cluster>  (default: kind)
#   --image <kindest/node:tag> (default: kindest/node:v1.31.0)
#   --dash-ver <vX.Y.Z> (default: v2.7.0)

set -euo pipefail

CLUSTER_NAME="kind"
KIND_IMAGE="kindest/node:v1.32.5"
DASH_VER="v2.7.0"
PIDFILE="${TMPDIR:-/tmp}/kubectl-proxy.pid"

# -------- helpers --------
have() { command -v "$1" >/dev/null 2>&1; }
die() { echo "ERROR: $*" >&2; exit 1; }

ctx() { echo "kind-${CLUSTER_NAME}"; }

kind_cluster_exists() {
  kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"
}

create_kind_cluster() {
  echo ">> Creating Kind cluster '${CLUSTER_NAME}' with image ${KIND_IMAGE} ..."
  kind create cluster --name "${CLUSTER_NAME}" --image "${KIND_IMAGE}"
}

install_dashboard() {
  local url="https://raw.githubusercontent.com/kubernetes/dashboard/${DASH_VER}/aio/deploy/recommended.yaml"
  echo ">> Installing Kubernetes Dashboard ${DASH_VER} ..."
  kubectl apply -f "${url}" --context "$(ctx)"
}

apply_admin_rbac() {
  echo ">> Applying admin ServiceAccount and ClusterRoleBinding for the dashboard ..."
  # SA (namespaced)
  kubectl --context "$(ctx)" -n kubernetes-dashboard apply -f - <<'EOF'
apiVersion: v1
kind: ServiceAccount
metadata:
  name: admin-user
  namespace: kubernetes-dashboard
EOF

  # ClusterRoleBinding (cluster-scoped)
  kubectl --context "$(ctx)" apply -f - <<'EOF'
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: admin-user-binding
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-admin
subjects:
  - kind: ServiceAccount
    name: admin-user
    namespace: kubernetes-dashboard
EOF
}

wait_for_dashboard() {
  echo ">> Waiting for dashboard to become available ..."
  kubectl --context "$(ctx)" -n kubernetes-dashboard \
    wait --for=condition=available --timeout=180s deployment/kubernetes-dashboard
}

print_token() {
  echo ">> Generating dashboard login token (24h) ..."
  # Preferred on K8s 1.24+
  if TOKEN=$(kubectl --context "$(ctx)" -n kubernetes-dashboard create token admin-user --duration=24h 2>/dev/null); then
    echo "${TOKEN}"
    return
  fi

  # Fallback (older clusters with SA token secrets)
  echo ">> 'kubectl create token' not available; trying secret fallback ..." >&2
  SECRET=$(kubectl --context "$(ctx)" -n kubernetes-dashboard get sa/admin-user -o jsonpath='{.secrets[0].name}')
  kubectl --context "$(ctx)" -n kubernetes-dashboard get secret "$SECRET" -o jsonpath='{.data.token}' | base64 --decode
  echo
}

start_proxy() {
  echo ">> Starting kubectl proxy (background) ..."
  stop_proxy || true
  kubectl proxy --context "$(ctx)" >/dev/null 2>&1 &
  echo $! > "${PIDFILE}"
  echo ">> Proxy PID: $(cat "${PIDFILE}")"
  echo ">> Dashboard URL:"
  echo "   http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#/login"
}

stop_proxy() {
  if [[ -f "${PIDFILE}" ]]; then
    local pid
    pid="$(cat "${PIDFILE}")"
    if ps -p "${pid}" >/dev/null 2>&1; then
      echo ">> Stopping kubectl proxy (PID ${pid}) ..."
      kill "${pid}" >/dev/null 2>&1 || true
    fi
    rm -f "${PIDFILE}"
    return 0
  fi
  # best-effort kill any leftover proxies on 8001
  if have lsof; then
    lsof -ti tcp:8001 2>/dev/null | xargs -r kill 2>/dev/null || true
  fi
  return 0
}

delete_cluster() {
  if kind_cluster_exists; then
    echo ">> Deleting Kind cluster '${CLUSTER_NAME}' ..."
    kind delete cluster --name "${CLUSTER_NAME}"
  else
    echo ">> Cluster '${CLUSTER_NAME}' does not exist; nothing to delete."
  fi
}

# -------- args --------
while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) CLUSTER_NAME="${2:-}"; shift 2 ;;
    --image) KIND_IMAGE="${2:-}"; shift 2 ;;
    --dash-ver) DASH_VER="${2:-}"; shift 2 ;;
    --delete) delete_cluster; exit 0 ;;
    --print-token) print_token; exit 0 ;;
    --start-proxy) start_proxy; exit 0 ;;
    --stop-proxy) stop_proxy; exit 0 ;;
    *) die "Unknown option: $1" ;;
  esac
done

# -------- checks --------
have kind || die "kind not found in PATH"
have kubectl || die "kubectl not found in PATH"

# -------- main --------
if ! kind_cluster_exists; then
  create_kind_cluster
else
  echo ">> Kind cluster '${CLUSTER_NAME}' already exists."
fi

install_dashboard
apply_admin_rbac
wait_for_dashboard

echo
TOKEN="$(print_token)"
echo
echo "============================================================"
echo " Dashboard is ready."
echo " Login URL:"
echo "   http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/#/login"
echo
echo " Your login token (copy-paste into the UI):"
echo "${TOKEN}"
echo "============================================================"
echo

start_proxy
echo ">> If you get logged out due to inactivity, you might see:"
echo "   'Error while proxying request: context canceled'"
echo "   Just run:  $(basename "$0") --start-proxy"

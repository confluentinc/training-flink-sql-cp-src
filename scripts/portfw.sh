#!/usr/bin/env bash
# portfw.sh — reset kubectl port-forward tunnels for Confluent demo
# Usage:
#   ./portfw.sh                # kill existing forwards on listed ports and recreate them
#   ./portfw.sh --stop         # kill only previously-started port-forward PIDs (from pidfile)
#   ./portfw.sh --namespace myns   # use a different namespace (default: confluent)

set -euo pipefail

NS="confluent"
PIDFILE="${TMPDIR:-/tmp}/confluent-portfw.pids"

# Local ports we want to ensure are free before (re)forwarding
PORTS=(8080 9094 8081 8082 9021)

# Forward specs: "<kubectl-target> <LOCAL:REMOTE>"
# Control Center: use service/controlcenter (matches metadata.name in confluent-platform.yaml)
# Pod controlcenter-0 may not exist if CFK uses different naming for Next Gen
FORWARDS=(
  "service/cmf-service 8080:80"
  "pod/kafka-0 9094:9094"
  "svc/schemaregistry 8081:8081"
  "service/flink-statement-rest 8082:8081"
  "service/controlcenter 9021:9021"
)

die() { echo "ERROR: $*" >&2; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

kill_port() {
  local port="$1"
  if have fuser; then
    sudo fuser -k "${port}/tcp" >/dev/null 2>&1 || true
    return
  fi
  if have lsof; then
    local pids
    pids="$(lsof -ti tcp:"${port}" 2>/dev/null || true)"
    [[ -n "${pids}" ]] && echo "${pids}" | xargs -r kill -9 || true
    return
  fi
  if have ss; then
    local pids
    pids="$(ss -lptn "sport = :${port}" 2>/dev/null | awk -F'[=,]' '/pid=/ {print $2}')"
    [[ -n "${pids}" ]] && echo "${pids}" | xargs -r kill -9 || true
    return
  fi
  echo "WARN: Could not free port ${port} (need fuser or lsof or ss)" >&2
}

kill_existing_ports() {
  echo ">> Killing any existing listeners on ports: ${PORTS[*]}"
  for p in "${PORTS[@]}"; do kill_port "$p"; done
}

start_forward() {
  local target="$1" mapping="$2"
  echo ">> Forwarding ${target}  ${mapping} (namespace ${NS})"
  kubectl -n "${NS}" port-forward "${target}" ${mapping} >/dev/null 2>&1 &
  echo $! >> "${PIDFILE}"
}

start_all_forwards() {
  : > "${PIDFILE}"
  for spec in "${FORWARDS[@]}"; do
    start_forward "$(awk '{print $1}' <<< "${spec}")" "$(awk '{print $2}' <<< "${spec}")"
    sleep 0.7
  done
  echo ">> Port-forwards started. PIDs saved to ${PIDFILE}"
}

stop_pidfile_forwards() {
  if [[ -f "${PIDFILE}" ]]; then
    echo ">> Stopping port-forwards from ${PIDFILE}"
    kill -9 $(cat "${PIDFILE}") 2>/dev/null || true
    rm -f "${PIDFILE}"
  else
    echo ">> No PID file found at ${PIDFILE}; nothing to stop."
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace|-n) NS="${2:-}"; shift 2 ;;
    --stop) stop_pidfile_forwards; exit 0 ;;
    *) die "Unknown argument: $1" ;;
  esac
done

have kubectl || die "kubectl not found in PATH"

kill_existing_ports
start_all_forwards

echo ">> Done."
echo "   - CMF UI     : http://localhost:8080"
echo "   - Kafka      : localhost:9094"
echo "   - Schema Reg : http://localhost:8081"
echo "   - Flink REST : http://localhost:8082"
echo "   - Control Ctr: http://localhost:9021"

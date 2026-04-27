#!/usr/bin/env bash
# setup-lab.sh — Quick health-check and recovery for lab environments
#
# Verifies that the infrastructure from Lab 1 is running and, for labs 3+,
# that the Flink resources from Lab 2 are configured. Restarts port forwarding
# if needed and prints a clear status summary.
#
# Usage:
#   ./setup-lab.sh              # check infrastructure only (Lab 2 prerequisites)
#   ./setup-lab.sh --lab 3      # also verify Flink env/catalog/db/compute pool (Lab 3+ prerequisites)
#   ./setup-lab.sh --fix        # attempt to fix issues (restart port forwarding, recreate missing resources)
#
# Prerequisites: kubectl, helm, curl

set -euo pipefail

NAMESPACE="confluent"
CMF_URL="http://localhost:8080"
KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-confluent-flink}"
LABS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

LAB_NUM=""
DO_FIX=false

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
PASS="${GREEN}✓${NC}"
FAIL="${RED}✗${NC}"
WARN="${YELLOW}!${NC}"

ERRORS=0

check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo -e "  ${PASS} ${label}"
    return 0
  else
    echo -e "  ${FAIL} ${label}"
    ERRORS=$((ERRORS + 1))
    return 1
  fi
}

warn_check() {
  local label="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo -e "  ${PASS} ${label}"
    return 0
  else
    echo -e "  ${WARN} ${label}"
    return 1
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lab)  LAB_NUM="${2:-}"; shift 2 ;;
    --fix)  DO_FIX=true; shift ;;
    -h|--help)
      sed -n '2,/^$/s/^# //p' "$0"
      exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

echo
echo -e "${CYAN}══════════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  Lab Environment Health Check${NC}"
if [[ -n "$LAB_NUM" ]]; then
  echo -e "${CYAN}  Checking prerequisites for Lab ${LAB_NUM}${NC}"
fi
echo -e "${CYAN}══════════════════════════════════════════════════════════${NC}"
echo

# ── Phase 1: Kubernetes cluster ─────────────────────────────────────────────
echo -e "${CYAN}Kubernetes Cluster${NC}"

check "Kind cluster '${KIND_CLUSTER_NAME}' exists" \
  kind get clusters 2>/dev/null

if ! check "kubectl can reach the cluster" \
  kubectl cluster-info --context "kind-${KIND_CLUSTER_NAME}"; then
  echo
  echo -e "${RED}Cannot reach the Kubernetes cluster.${NC}"
  echo -e "The Kind cluster may be stopped. Try: ${CYAN}docker start ${KIND_CLUSTER_NAME}-control-plane${NC}"
  echo -e "If that doesn't work, recreate it: ${CYAN}~/confluent-flink/scripts/setup-all.sh${NC}"
  exit 1
fi

echo

# ── Phase 2: Confluent Platform pods ────────────────────────────────────────
echo -e "${CYAN}Confluent Platform Pods${NC}"

pod_running() {
  local pattern="$1"
  kubectl get pods -n "$NAMESPACE" --no-headers 2>/dev/null \
    | grep -E "$pattern" \
    | grep -q Running
}

check "CFK Operator running" pod_running "confluent-operator"
check "KRaft Controller running" pod_running "kraftcontroller"
check "Kafka Broker running" pod_running "kafka-0"
check "Schema Registry running" pod_running "schemaregistry"
check "Control Center running" pod_running "controlcenter"
check "Flink Kubernetes Operator running" pod_running "flink-kubernetes-operator"
check "CMF running" pod_running "confluent-manager-for-apache-flink"

echo

# ── Phase 3: Port forwarding ───────────────────────────────────────────────
echo -e "${CYAN}Port Forwarding${NC}"

port_open() {
  local port="$1"
  if command -v lsof >/dev/null 2>&1; then
    lsof -ti "tcp:${port}" >/dev/null 2>&1
  elif command -v ss >/dev/null 2>&1; then
    ss -ltn "sport = :${port}" 2>/dev/null | grep -q "${port}"
  else
    curl -sf --max-time 2 "http://localhost:${port}/" >/dev/null 2>&1
  fi
}

PF_OK=true
warn_check "Control Center (port 9021)" port_open 9021 || PF_OK=false
warn_check "CMF (port 8080)" port_open 8080 || PF_OK=false
warn_check "Schema Registry (port 8081)" port_open 8081 || PF_OK=false
warn_check "Kafka (port 9094)" port_open 9094 || PF_OK=false

if [[ "$PF_OK" == false ]]; then
  echo -e "  ${WARN} Restarting port forwarding..."
  "${LABS_SRC}/scripts/portfw.sh" 2>/dev/null
  sleep 2
  echo -e "  ${PASS} Port forwarding restarted"
fi

echo

# ── Phase 4: Flink resources ───────────────────────────────────────────────
if [[ -n "$LAB_NUM" ]]; then
  echo -e "${CYAN}Flink Resources${NC}"

  CMF_REACHABLE=false
  if check "CMF API reachable" curl -sf --max-time 5 "${CMF_URL}/cmf/api/v1/environments"; then
    CMF_REACHABLE=true
  fi

  if [[ "$CMF_REACHABLE" == true ]]; then
    # Environment
    env_exists() {
      curl -sf "${CMF_URL}/cmf/api/v1/environments" 2>/dev/null | grep -q "training-env"
    }
    if ! check "Flink environment 'training-env'" env_exists; then
      if [[ "$DO_FIX" == true ]] && command -v confluent >/dev/null 2>&1; then
        echo -e "  ${WARN} Creating Flink environment 'training-env'..."
        if confluent flink environment create training-env \
          --url "${CMF_URL}" \
          --kubernetes-namespace "$NAMESPACE" 2>/dev/null; then
          echo -e "  ${PASS} Environment created"; ERRORS=$((ERRORS - 1))
        else
          echo -e "  ${FAIL} Failed to create environment"
        fi
      fi
    fi

    # Catalog
    catalog_exists() {
      confluent flink catalog list --url "${CMF_URL}" 2>/dev/null | grep -q "training-catalog"
    }
    if ! check "Flink catalog 'training-catalog'" catalog_exists; then
      if [[ "$DO_FIX" == true ]] && command -v confluent >/dev/null 2>&1; then
        echo -e "  ${WARN} Creating Flink catalog 'training-catalog'..."
        if confluent flink catalog create "${LABS_SRC}/flink/catalog.json" \
          --url "${CMF_URL}" 2>/dev/null; then
          echo -e "  ${PASS} Catalog created"; ERRORS=$((ERRORS - 1))
        else
          echo -e "  ${FAIL} Failed to create catalog"
        fi
      fi
    fi

    # Database
    db_exists() {
      curl -sf "${CMF_URL}/cmf/api/v1/catalogs/kafka/training-catalog/databases" 2>/dev/null | grep -q "training-kafka"
    }
    if ! check "Flink database 'training-kafka'" db_exists; then
      if [[ "$DO_FIX" == true ]]; then
        echo -e "  ${WARN} Creating Flink database 'training-kafka'..."
        if curl -sf -H "Content-Type: application/json" \
          -X POST "${CMF_URL}/cmf/api/v1/catalogs/kafka/training-catalog/databases" \
          -d @"${LABS_SRC}/flink/database.json" >/dev/null 2>&1; then
          echo -e "  ${PASS} Database created"; ERRORS=$((ERRORS - 1))
        else
          echo -e "  ${FAIL} Failed to create database"
        fi
      fi
    fi

    # Compute pool
    pool_exists() {
      confluent flink compute-pool list --environment training-env --url "${CMF_URL}" 2>/dev/null | grep -q "training-compute-pool"
    }
    if ! check "Flink compute pool 'training-compute-pool'" pool_exists; then
      if [[ "$DO_FIX" == true ]] && command -v confluent >/dev/null 2>&1; then
        echo -e "  ${WARN} Creating Flink compute pool 'training-compute-pool'..."
        if confluent flink compute-pool create "${LABS_SRC}/flink/compute-pool.json" \
          --environment training-env \
          --url "${CMF_URL}" 2>/dev/null; then
          echo -e "  ${PASS} Compute pool created"; ERRORS=$((ERRORS - 1))
        else
          echo -e "  ${FAIL} Failed to create compute pool"
        fi
      fi
    fi

    # Stale Flink jobs — after a restart, Flink's HA recovery can resurrect
    # jobs that were previously stopped via CMF. These zombie jobs consume
    # task slots and leave TaskManagers in Pending state.
    #
    # CMF creates a FlinkDeployment CR per statement. The Flink K8s Operator
    # watches these CRs and recreates Deployments/pods if we only delete those.
    # We must delete the FlinkDeployment CRs themselves (but NOT the compute
    # pool's FlinkDeployment).
    stale_flink_jobs() {
      kubectl get flinkdeployments -n "$NAMESPACE" --no-headers 2>/dev/null \
        | grep -E "(cli-|shoe-)" \
        | awk '{print $1}' || true
    }
    STALE_JOBS=$(stale_flink_jobs)
    if [[ -n "$STALE_JOBS" ]]; then
      STALE_COUNT=$(echo "$STALE_JOBS" | wc -l | tr -d ' ')
      echo -e "  ${WARN} Found ${STALE_COUNT} stale Flink job(s) recovered by HA — cleaning up..."
      while IFS= read -r job; do
        [[ -z "$job" ]] && continue
        kubectl delete flinkdeployment "$job" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1
      done <<< "$STALE_JOBS"
      # Clean up HA ConfigMaps (Flink stores HA metadata here)
      kubectl delete configmaps -n "$NAMESPACE" -l "configmap-type=high-availability" --ignore-not-found >/dev/null 2>&1
      # Delete orphaned CMF statements (only if CMF is reachable)
      if [[ "$CMF_REACHABLE" == true ]] && command -v confluent >/dev/null 2>&1; then
        confluent flink statement list \
          --environment training-env --url "${CMF_URL}" 2>/dev/null \
          | awk -F'|' 'NR>1 && $2 ~ /[a-zA-Z0-9]/ {gsub(/^[ \t]+|[ \t]+$/, "", $2); if ($2 != "" && $2 != "Name") print $2}' \
          | while IFS= read -r stmt; do
              [[ -z "$stmt" ]] && continue
              confluent flink statement stop "$stmt" \
                --environment training-env --url "${CMF_URL}" 2>/dev/null || true
              echo y | confluent flink statement delete "$stmt" \
                --environment training-env --url "${CMF_URL}" 2>/dev/null || true
            done
      fi
      echo -e "  ${PASS} Stale jobs and HA metadata cleaned up"
    else
      echo -e "  ${PASS} No stale Flink jobs"
    fi
  fi

  # ── The following checks use kubectl only (no CMF dependency) ───────────

  # S3proxy pod
  s3proxy_running() {
    kubectl get pods -n "$NAMESPACE" -l app=s3proxy --no-headers 2>/dev/null | grep -q Running
  }
  if ! check "S3proxy running" s3proxy_running; then
    if [[ "$DO_FIX" == true ]]; then
      echo -e "  ${WARN} Starting S3proxy..."
      if "${LABS_SRC}/scripts/setup-s3proxy-rclone.sh" --skip-rclone 2>/dev/null; then
        echo -e "  ${PASS} S3proxy started"; ERRORS=$((ERRORS - 1))
      else
        echo -e "  ${FAIL} Failed to start S3proxy"
      fi
    fi
  fi

  # S3proxy port forwarding (always auto-fix — not managed by portfw.sh)
  # Must come before bucket check since that uses curl localhost:8000
  if ! check "S3proxy port-forward (port 8000)" port_open 8000; then
    echo -e "  ${WARN} Starting S3proxy port-forward..."
    kubectl -n "$NAMESPACE" port-forward "service/s3proxy" 8000:8000 >/dev/null 2>&1 &
    sleep 2
    if port_open 8000; then
      echo -e "  ${PASS} S3proxy port-forward started"
      ERRORS=$((ERRORS - 1))
    else
      echo -e "  ${FAIL} Failed to start S3proxy port-forward"
    fi
  fi

  # S3proxy warehouse bucket — S3proxy uses persistent storage (PVC), so
  # the bucket survives pod/VM restarts. We only need to create it once.
  bucket_init_completed() {
    local succeeded
    succeeded=$(kubectl get job s3proxy-init -n "$NAMESPACE" \
      -o jsonpath='{.status.succeeded}' 2>/dev/null)
    [[ "$succeeded" == "1" ]]
  }
  if bucket_init_completed; then
    echo -e "  ${PASS} S3proxy warehouse bucket exists"
  else
    echo -e "  ${WARN} Creating warehouse bucket..."
    kubectl delete job s3proxy-init -n "$NAMESPACE" 2>/dev/null || true
    kubectl apply -f "${LABS_SRC}/k8s/s3proxy/s3proxy-init-job.yaml" -n "$NAMESPACE" 2>/dev/null
    if kubectl wait --for=condition=complete job/s3proxy-init -n "$NAMESPACE" --timeout=30s 2>/dev/null; then
      echo -e "  ${PASS} S3proxy warehouse bucket created"
    else
      echo -e "  ${FAIL} Failed to create warehouse bucket"
      ERRORS=$((ERRORS + 1))
    fi
  fi

  # Rclone warehouse mount — the FUSE mount dies on VM reboot.
  # Remount automatically if rclone is configured.
  if ! command -v rclone >/dev/null 2>&1; then
    echo -e "  ${WARN} rclone not installed — skipping warehouse mount"
  elif ! rclone listremotes 2>/dev/null | grep -q "^s3proxy:$"; then
    echo -e "  ${WARN} rclone 's3proxy' remote not configured — run setup-s3proxy-rclone.sh first"
  elif mountpoint -q "$HOME/warehouse-mount" 2>/dev/null; then
    echo -e "  ${PASS} Warehouse mount active (~\/warehouse-mount)"
  else
    fusermount -u "$HOME/warehouse-mount" 2>/dev/null || true
    mkdir -p "$HOME/warehouse-mount"
    MOUNT_ERR=$(rclone mount s3proxy:warehouse "$HOME/warehouse-mount" --daemon 2>&1) \
      && echo -e "  ${PASS} Warehouse mount restored (~\/warehouse-mount)" \
      || echo -e "  ${WARN} Could not mount warehouse: ${MOUNT_ERR}"
  fi

  echo

  echo
fi

# ── Summary ─────────────────────────────────────────────────────────────────
if (( ERRORS > 0 )); then
  echo -e "${RED}${ERRORS} check(s) failed.${NC} Please fix the issues above before starting the lab."
  if [[ "$DO_FIX" == false ]]; then
    echo -e "TIP: Re-run with ${CYAN}--fix${NC} to auto-repair missing resources."
  fi
  echo -e "If the infrastructure is completely missing, re-run Lab 01 or use: ~/confluent-flink/scripts/setup-all.sh"
  exit 1
else
  echo -e "${GREEN}All checks passed.${NC} Your environment is ready."
  echo
  echo "  Services:"
  echo "    Control Center : http://localhost:9021"
  echo "    CMF            : http://localhost:8080"
  echo "    Schema Registry: http://localhost:8081"
  echo "    Kafka          : localhost:9094"
  echo
fi

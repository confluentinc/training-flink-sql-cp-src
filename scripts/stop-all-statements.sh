#!/usr/bin/env bash
#
# stop-all-statements.sh -- Stop and clean up all Flink SQL statements
#
# Stops RUNNING statements via CMF, deletes PENDING/STOPPED/FAILED leftovers,
# and cleans up K8s-level zombie FlinkDeployments that Flink HA may have recovered.
#
# Usage: ./stop-all-statements.sh [--environment NAME] [--url URL] [--delete-all]
#

set -uo pipefail
# NOTE: set -e intentionally omitted — CMF may be unreachable after a restart
# and we must always reach the K8s-level cleanup (Step 4).

CMF_URL="${CMF_URL:-http://localhost:8080}"
FLINK_ENV="${FLINK_ENV:-training-env}"
NAMESPACE="${NAMESPACE:-confluent}"
DELETE_ALL=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --environment) FLINK_ENV="$2"; shift 2 ;;
    --url)         CMF_URL="$2"; shift 2 ;;
    --namespace)   NAMESPACE="$2"; shift 2 ;;
    --delete-all)  DELETE_ALL=true; shift ;;
    --help|-h)
      echo "Usage: $0 [--environment NAME] [--url URL] [--delete-all]"
      echo ""
      echo "Stops all running Flink SQL statements and cleans up stale resources."
      echo ""
      echo "Options:"
      echo "  --environment NAME   Flink environment name (default: training-env)"
      echo "  --url URL            CMF URL (default: http://localhost:8080)"
      echo "  --namespace NS       Kubernetes namespace (default: confluent)"
      echo "  --delete-all         Also delete COMPLETED/FAILED/STOPPED statements from CMF"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Helper: extract statement names from confluent CLI table output
extract_names() {
  awk -F'|' 'NR>1 && $2 ~ /[a-zA-Z0-9]/ {gsub(/^[ \t]+|[ \t]+$/, "", $2); if ($2 != "" && $2 != "Name") print $2}'
}

# ── Step 1: Stop RUNNING statements via CMF ─────────────────────────────────
echo "Checking for statements in environment '$FLINK_ENV'..."

RUNNING=$(confluent flink statement list \
  --status RUNNING \
  --environment "$FLINK_ENV" \
  --url "$CMF_URL" 2>/dev/null | extract_names || true)

COUNT=0
if [[ -n "$RUNNING" ]]; then
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    echo "  Stopping: $name"
    confluent flink statement stop "$name" \
      --environment "$FLINK_ENV" \
      --url "$CMF_URL" 2>/dev/null && echo "    Stopped." || echo "    Failed to stop."
    COUNT=$((COUNT + 1))
  done <<< "$RUNNING"
  echo "Stopped $COUNT statement(s)."
else
  echo "  No RUNNING statements found."
fi

# ── Step 2: Stop PENDING statements ─────────────────────────────────────────
PENDING=$(confluent flink statement list \
  --status PENDING \
  --environment "$FLINK_ENV" \
  --url "$CMF_URL" 2>/dev/null | extract_names || true)

if [[ -n "$PENDING" ]]; then
  echo "  Stopping PENDING statements..."
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    echo "    Stopping: $name"
    confluent flink statement stop "$name" \
      --environment "$FLINK_ENV" \
      --url "$CMF_URL" 2>/dev/null || true
  done <<< "$PENDING"
fi

# ── Step 3: Delete all statements if --delete-all ────────────────────────────
if [[ "$DELETE_ALL" == true ]]; then
  echo "  Deleting all statements from CMF..."
  ALL_STMTS=$(confluent flink statement list \
    --environment "$FLINK_ENV" \
    --url "$CMF_URL" 2>/dev/null | extract_names || true)

  if [[ -n "$ALL_STMTS" ]]; then
    while IFS= read -r name; do
      [[ -z "$name" ]] && continue
      echo "    Deleting: $name"
      echo y | confluent flink statement delete "$name" \
        --environment "$FLINK_ENV" \
        --url "$CMF_URL" 2>/dev/null || true
    done <<< "$ALL_STMTS"
  fi
fi

# ── Step 4: Clean up K8s-level zombie jobs (HA recovery) ─────────────────────
# CMF creates a FlinkDeployment CR per statement. The Flink K8s Operator
# watches these CRs and recreates Deployments/pods if we only delete those.
# We must delete the FlinkDeployment CRs (but NOT the compute pool's).
echo ""
echo "Checking for zombie Flink jobs at K8s level..."

ZOMBIES=$(kubectl get flinkdeployments -n "$NAMESPACE" --no-headers 2>/dev/null \
  | grep -E "(cli-|shoe-)" \
  | awk '{print $1}' || true)

if [[ -n "$ZOMBIES" ]]; then
  ZOMBIE_COUNT=$(echo "$ZOMBIES" | wc -l | tr -d ' ')
  echo "  Found $ZOMBIE_COUNT zombie FlinkDeployment(s) — cleaning up..."
  while IFS= read -r job; do
    [[ -z "$job" ]] && continue
    echo "    Deleting: $job"
    kubectl delete flinkdeployment "$job" -n "$NAMESPACE" --ignore-not-found >/dev/null 2>&1
  done <<< "$ZOMBIES"
  # Clean up HA ConfigMaps
  kubectl delete configmaps -n "$NAMESPACE" -l "configmap-type=high-availability" --ignore-not-found >/dev/null 2>&1
  echo "  Cleaned up zombie jobs and HA metadata."
else
  echo "  No zombie jobs found."
fi

echo ""
echo "Done."

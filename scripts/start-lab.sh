#!/usr/bin/env bash
#
# start-lab.sh -- Quick setup for a specific lab
#
# Runs the health check, creates required topics, and starts the appropriate
# producer in the background so students can focus on Flink SQL exercises.
#
# Usage:
#   ./start-lab.sh --lab 3    # Setup for Lab 3 (orders topic + producer)
#   ./start-lab.sh --lab 4    # Setup for Lab 4 (user-actions + clicks topics)
#   ./start-lab.sh --lab 5    # Setup for Lab 5 (shoe topics + shoe-producer --flink-setup)
#   ./start-lab.sh --lab 6    # Setup for Lab 6 (same as Lab 5)
#

set -euo pipefail

LABS_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAB_NUM=""
PIDS_FILE="/tmp/lab-producers.pids"

RED='\033[0;31m'; GREEN='\033[0;32m'; CYAN='\033[0;36m'; NC='\033[0m'

while [[ $# -gt 0 ]]; do
  case "$1" in
    --lab)  LAB_NUM="${2:-}"; shift 2 ;;
    --stop)
      if [[ -f "$PIDS_FILE" ]]; then
        echo "Stopping background producers..."
        while read -r pid; do
          kill "$pid" 2>/dev/null && echo "  Stopped PID $pid" || true
        done < "$PIDS_FILE"
        rm -f "$PIDS_FILE"
      else
        echo "No background producers to stop."
      fi
      exit 0
      ;;
    -h|--help)
      echo "Usage: $0 --lab N"
      echo ""
      echo "Quick setup for a specific lab. Runs health check, creates topics,"
      echo "and starts producers in the background."
      echo ""
      echo "Options:"
      echo "  --lab N    Lab number (3-6)"
      echo "  --stop     Stop any background producers started by this script"
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$LAB_NUM" ]]; then
  echo "Error: --lab N is required" >&2
  echo "Usage: $0 --lab N" >&2
  exit 1
fi

echo
echo -e "${CYAN}══════════════════════════════════════${NC}"
echo -e "${CYAN}  Starting Lab ${LAB_NUM} Environment${NC}"
echo -e "${CYAN}══════════════════════════════════════${NC}"
echo

# Step 1: Run health check with auto-fix
echo -e "${CYAN}Step 1: Running health check...${NC}"
"${LABS_SRC}/scripts/setup-lab.sh" --lab "$LAB_NUM" --fix || true
echo

# Step 2: Start appropriate producers
echo -e "${CYAN}Step 2: Starting producers...${NC}"

# Clean up any previous background producers
if [[ -f "$PIDS_FILE" ]]; then
  while read -r pid; do
    kill "$pid" 2>/dev/null || true
  done < "$PIDS_FILE"
  rm -f "$PIDS_FILE"
fi

case "$LAB_NUM" in
  3)
    echo "Starting orders producer in background..."
    nohup "${LABS_SRC}/producers/orders-producer/run-orders-producer.sh" > /tmp/orders-producer.log 2>&1 &
    echo $! >> "$PIDS_FILE"
    echo -e "${GREEN}  Orders producer started (PID $!, log: /tmp/orders-producer.log)${NC}"
    ;;
  4)
    echo "Lab 4 uses manual INSERT VALUES for watermark exercises."
    echo "No producer needed for the main exercises."
    echo "The clicks producer will be needed for the challenge section — start it manually when prompted:"
    echo "  ~/confluent-flink/producers/clicks-producer.sh"
    ;;
  5)
    echo "Starting shoe producer with --flink-setup in background..."
    nohup "${LABS_SRC}/producers/shoe-producer/run-shoe-producer.sh" --flink-setup > /tmp/shoe-producer.log 2>&1 &
    echo $! >> "$PIDS_FILE"
    echo -e "${GREEN}  Shoe producer started (PID $!, log: /tmp/shoe-producer.log)${NC}"
    echo "  Allow 1-2 minutes for data to flow before starting exercises."
    ;;
  6)
    echo "Starting shoe producer with --flink-setup in background..."
    nohup "${LABS_SRC}/producers/shoe-producer/run-shoe-producer.sh" --flink-setup > /tmp/shoe-producer.log 2>&1 &
    echo $! >> "$PIDS_FILE"
    echo -e "${GREEN}  Shoe producer started (PID $!, log: /tmp/shoe-producer.log)${NC}"
    echo "  Allow 1-2 minutes for data to flow before starting exercises."
    ;;
  *)
    echo "No producers needed for Lab ${LAB_NUM}."
    ;;
esac

echo
echo -e "${GREEN}Lab ${LAB_NUM} environment is ready.${NC}"
echo
echo "  Next steps:"
echo "    1. Open the Flink SQL shell:"
echo "       confluent flink shell --compute-pool training-compute-pool --environment training-env --url http://localhost:8080"
echo "    2. Set catalog and database:"
echo "       USE CATALOG training-catalog;"
echo "       USE training-kafka;"
echo
if [[ -f "$PIDS_FILE" ]]; then
  echo "  To stop background producers later:"
  echo "    ~/confluent-flink/scripts/start-lab.sh --stop"
  echo
fi

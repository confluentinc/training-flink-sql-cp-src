#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE="$SCRIPT_DIR/ShoeProducer.java"
CLASS_DIR="$SCRIPT_DIR/classes"

export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9094}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
export DELAY_MS="${DELAY_MS:-1000}"
export COUNT="${COUNT:-0}"

CMF_URL="${CMF_URL:-http://localhost:8080}"
FLINK_ENV="${FLINK_ENV:-training-env}"
COMPUTE_POOL="${COMPUTE_POOL:-training-compute-pool}"
CATALOG_NAME="${CATALOG_NAME:-training-catalog}"
DATABASE_NAME="${DATABASE_NAME:-training-kafka}"

DEBUG=false
FLINK_SETUP=false

for arg in "$@"; do
  case "$arg" in
    --flink-setup) FLINK_SETUP=true ;;
    --debug) DEBUG=true ;;
    --help|-h)
      echo "Usage: $0 [--flink-setup] [--debug]"
      echo ""
      echo "  --flink-setup   Set watermarks and create derived Flink tables before producing"
      echo "  --debug         Show detailed API responses on errors"
      echo ""
      echo "Environment Variables:"
      echo "  BOOTSTRAP_SERVERS    (default: localhost:9094)"
      echo "  SCHEMA_REGISTRY_URL  (default: http://localhost:8081)"
      echo "  DELAY_MS             Delay between iterations in ms (default: 1000)"
      echo "  COUNT                Number of iterations, 0 = infinite (default: 0)"
      echo "  CMF_URL              CMF REST API base URL (default: http://localhost:8080)"
      echo "  FLINK_ENV            Flink environment name (default: training-env)"
      echo "  COMPUTE_POOL         Flink compute pool name (default: training-compute-pool)"
      echo "  CATALOG_NAME         Flink catalog name (default: training-catalog)"
      echo "  DATABASE_NAME        Flink database name (default: training-kafka)"
      exit 0
      ;;
  esac
done

# ---------------------------------------------------------------------------
# CMF helper functions
# ---------------------------------------------------------------------------

flink_submit_statement() {
  local stmt_name="$1"
  local sql="$2"

  local payload
  payload=$(jq -n \
    --arg name "$stmt_name" \
    --arg sql "$sql" \
    --arg pool "$COMPUTE_POOL" \
    --arg catalog "$CATALOG_NAME" \
    --arg database "$DATABASE_NAME" \
    '{
      "apiVersion": "cmf.confluent.io/v1",
      "kind": "Statement",
      "metadata": { "name": $name },
      "spec": {
        "statement": $sql,
        "computePoolName": $pool,
        "properties": {
          "sql.current-catalog": $catalog,
          "sql.current-database": $database
        }
      }
    }')

  local response http_code body
  response=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    -d "$payload" \
    "$CMF_URL/cmf/api/v1/environments/$FLINK_ENV/statements")

  http_code=$(echo "$response" | tail -1)
  body=$(echo "$response" | sed '$d')

  if [ "$http_code" = "200" ]; then
    echo "    ✓ Statement '$stmt_name' submitted"
    return 0
  elif [ "$http_code" = "409" ]; then
    echo "    ✓ Statement '$stmt_name' already exists (skipped)"
    return 0
  else
    echo "    ✗ Failed to create statement '$stmt_name' (HTTP $http_code)"
    if [ "$DEBUG" = true ]; then
      echo "    Response: $body"
    fi
    return 1
  fi
}

flink_wait_statement_completed() {
  local stmt_name="$1"
  local max_wait=${2:-60}
  local elapsed=0

  while [ $elapsed -lt $max_wait ]; do
    local response
    response=$(curl -s "$CMF_URL/cmf/api/v1/environments/$FLINK_ENV/statements/$stmt_name")
    local phase
    phase=$(echo "$response" | jq -r '.status.phase // empty' 2>/dev/null)

    case "$phase" in
      COMPLETED)
        return 0
        ;;
      RUNNING)
        return 0
        ;;
      FAILED|STOPPED)
        local detail
        detail=$(echo "$response" | jq -r '.status.detail // "unknown error"' 2>/dev/null)
        echo "    ✗ Statement '$stmt_name' failed: $detail"
        return 1
        ;;
    esac

    sleep 3
    elapsed=$((elapsed + 3))
  done

  echo "    ⚠ Statement '$stmt_name' still pending after ${max_wait}s (may still succeed)"
  return 0
}

setup_flink_tables() {
  # Timestamp suffix to avoid name conflicts with previous runs
  local TS
  TS=$(date +%Y%m%d-%H%M%S)

  echo ""
  echo "=== Setting up Flink tables via CMF API ==="
  echo "    CMF URL: $CMF_URL"
  echo "    Environment: $FLINK_ENV"
  echo "    Compute Pool: $COMPUTE_POOL"
  echo "    Run ID: $TS"
  echo ""

  if ! curl -s -f "$CMF_URL/cmf/api/v1/environments/$FLINK_ENV" > /dev/null 2>&1; then
    echo "ERROR: Cannot reach CMF at $CMF_URL (environment: $FLINK_ENV)"
    echo "Ensure CMF is running and the Flink environment exists."
    exit 1
  fi

  echo "  1/6 Setting watermark on shoe-orders.ts..."
  flink_submit_statement "shoe-alter-orders-wm-${TS}" \
    "ALTER TABLE \`shoe-orders\` MODIFY WATERMARK FOR \`ts\` AS \`ts\`;"
  flink_wait_statement_completed "shoe-alter-orders-wm-${TS}"

  echo "  2/6 Setting watermark on shoe-clickstream.ts..."
  flink_submit_statement "shoe-alter-clicks-wm-${TS}" \
    "ALTER TABLE \`shoe-clickstream\` MODIFY WATERMARK FOR \`ts\` AS \`ts\`;"
  flink_wait_statement_completed "shoe-alter-clicks-wm-${TS}"

  echo "  3/6 Creating shoe_customers_keyed table..."
  flink_submit_statement "shoe-create-cust-keyed-${TS}" \
    "CREATE TABLE IF NOT EXISTS shoe_customers_keyed (customer_id STRING, first_name STRING, last_name STRING, email STRING, PRIMARY KEY (customer_id) NOT ENFORCED) DISTRIBUTED INTO 1 BUCKETS;"
  flink_wait_statement_completed "shoe-create-cust-keyed-${TS}"

  echo "  4/6 Creating shoe_products_keyed table..."
  flink_submit_statement "shoe-create-prod-keyed-${TS}" \
    "CREATE TABLE IF NOT EXISTS shoe_products_keyed (product_id STRING, brand STRING, \`model\` STRING, sale_price INT, rating DOUBLE, PRIMARY KEY (product_id) NOT ENFORCED) DISTRIBUTED INTO 1 BUCKETS;"
  flink_wait_statement_completed "shoe-create-prod-keyed-${TS}"

  echo "  5/6 Creating shoe_orders_enriched table..."
  flink_submit_statement "shoe-create-orders-enr-${TS}" \
    "CREATE TABLE IF NOT EXISTS shoe_orders_enriched (order_id INT, first_name STRING, last_name STRING, email STRING, brand STRING, \`model\` STRING, sale_price INT, rating DOUBLE) DISTRIBUTED INTO 1 BUCKETS WITH ('changelog.mode' = 'retract');"
  flink_wait_statement_completed "shoe-create-orders-enr-${TS}"

  echo "  6/6 Populating all tables (EXECUTE STATEMENT SET)..."
  flink_submit_statement "shoe-insert-all-${TS}" \
    "EXECUTE STATEMENT SET BEGIN INSERT INTO shoe_customers_keyed /*+ OPTIONS('properties.transaction.timeout.ms'='300000') */ SELECT id, first_name, last_name, email FROM \`shoe-customers\`; INSERT INTO shoe_products_keyed /*+ OPTIONS('properties.transaction.timeout.ms'='300000') */ SELECT id, brand, \`name\`, sale_price, rating FROM \`shoe-products\`; INSERT INTO shoe_orders_enriched /*+ OPTIONS('properties.transaction.timeout.ms'='300000') */ SELECT so.order_id, sc.first_name, sc.last_name, sc.email, sp.brand, sp.\`model\`, sp.sale_price, sp.rating FROM \`shoe-orders\` so INNER JOIN shoe_customers_keyed sc ON so.customer_id = sc.customer_id INNER JOIN shoe_products_keyed sp ON so.product_id = sp.product_id; END;"

  echo ""
  echo "=== Flink tables setup completed ==="
  echo "    Streaming INSERT INTO jobs are now running."
  echo "    Data will flow as the producer generates records."
  echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if [ "$FLINK_SETUP" = true ]; then
  setup_flink_tables
fi

CP_HOME="${CONFLUENT_HOME:-/usr}"
CLASSPATH=""
for dir in "$CP_HOME"/share/java/*/; do
  CLASSPATH="$CLASSPATH:${dir}*"
done
CLASSPATH="${CLASSPATH#:}"

if [ -z "$CLASSPATH" ]; then
  echo "ERROR: Could not find Confluent Platform JARs. Set CONFLUENT_HOME." >&2
  exit 1
fi

rm -rf "$CLASS_DIR"
mkdir -p "$CLASS_DIR"
echo "Compiling ShoeProducer..."
javac -proc:none -cp "$CLASSPATH" -d "$CLASS_DIR" "$SOURCE"

# Suppress verbose Kafka/SR config logging
LOG4J_CFG="$CLASS_DIR/log4j.properties"
cat > "$LOG4J_CFG" <<'LOGCFG'
log4j.rootLogger=WARN, stderr
log4j.appender.stderr=org.apache.log4j.ConsoleAppender
log4j.appender.stderr.target=System.err
log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
log4j.appender.stderr.layout.ConversionPattern=%m%n
log4j.logger.org.apache.kafka=WARN
log4j.logger.io.confluent=WARN
LOGCFG

echo "Starting shoe producer (delay=${DELAY_MS}ms, bootstrap=${BOOTSTRAP_SERVERS})..."
java -Dshoe.producer.dir="$SCRIPT_DIR" \
  -Dlog4j.configuration="file:$LOG4J_CFG" \
  -cp "$CLASS_DIR:$CLASSPATH" ShoeProducer 2> >(grep -v "^SLF4J:" >&2)

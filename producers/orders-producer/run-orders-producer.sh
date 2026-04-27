#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SOURCE="$SCRIPT_DIR/OrdersProducer.java"
CLASS_DIR="$SCRIPT_DIR/classes"

export BOOTSTRAP_SERVERS="${BOOTSTRAP_SERVERS:-localhost:9094}"
export SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
export DELAY_MS="${DELAY_MS:-500}"

# Parse flags
for arg in "$@"; do
  case "$arg" in
    --fast) export DELAY_MS=100 ;;
  esac
done

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

mkdir -p "$CLASS_DIR"
echo "Compiling OrdersProducer..."
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

echo "Starting orders producer (delay=${DELAY_MS}ms, bootstrap=${BOOTSTRAP_SERVERS})..."
java -Dlog4j.configuration="file:$LOG4J_CFG" -cp "$CLASS_DIR:$CLASSPATH" OrdersProducer 2> >(grep -v "^SLF4J:" >&2)

#!/bin/bash
set -euo pipefail

TOPIC="clicks"
BOOTSTRAP="localhost:9094"
SCHEMA_REGISTRY_URL="http://localhost:8081"
AVRO_PRODUCER_COMMAND="kafka-avro-console-producer"

SCHEMA=$(cat <<'JSON'
{
  "type": "record",
  "name": "clicks",
  "namespace": "io.confluent.training.avro",
  "fields": [
    { "name": "ip", "type": "string" },
    { "name": "userid", "type": "int" },
    { "name": "remote_user", "type": "string" },
    { "name": "time", "type": "string" },
    { "name": "_time", "type": "long" },
    { "name": "request", "type": "string" },
    { "name": "status", "type": "string" },
    { "name": "bytes", "type": "string" },
    { "name": "referrer", "type": "string" },
    { "name": "agent", "type": "string" }
  ]
}
JSON
)

command -v "$AVRO_PRODUCER_COMMAND" >/dev/null || { echo "ERROR: kafka-avro-console-producer not found"; exit 1; }

# IP address pools
IPS=(
  "111.152.45.45" "111.203.236.146" "111.168.57.122" "111.249.79.93" "111.168.57.122"
  "111.90.225.227" "111.173.165.103" "111.145.8.144" "111.245.174.248" "111.245.174.111"
  "222.152.45.45" "222.203.236.146" "222.168.57.122" "222.249.79.93" "222.168.57.122"
  "222.90.225.227" "222.173.165.103" "222.145.8.144" "222.245.174.248" "222.245.174.222"
  "122.152.45.245" "122.203.236.246" "122.168.57.222" "122.249.79.233" "122.168.57.222"
  "122.90.225.227" "122.173.165.203" "122.145.8.244" "122.245.174.248" "122.245.174.122"
  "233.152.245.45" "233.203.236.146" "233.168.257.122" "233.249.279.93" "233.168.257.122"
  "233.90.225.227" "233.173.215.103" "233.145.28.144" "233.245.174.248" "233.245.174.233"
)

# HTTP requests
REQUESTS=(
  "GET /index.html HTTP/1.1"
  "GET /site/user_status.html HTTP/1.1"
  "GET /site/login.html HTTP/1.1"
  "GET /site/user_status.html HTTP/1.1"
  "GET /images/track.png HTTP/1.1"
  "GET /images/logo-small.png HTTP/1.1"
)

# HTTP status codes
STATUS_CODES=("200" "302" "404" "405" "406" "407")

# Response bytes
BYTES_VALUES=("278" "1289" "2048" "4096" "4006" "4196" "14096")

# User agents
USER_AGENTS=(
  "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36"
)

# Function to pick random values
pick_ip() { IP=${IPS[$((RANDOM % ${#IPS[@]}))]}; }
pick_request() { REQUEST=${REQUESTS[$((RANDOM % ${#REQUESTS[@]}))]}; }
pick_status() { STATUS=${STATUS_CODES[$((RANDOM % ${#STATUS_CODES[@]}))]}; }
pick_bytes() { BYTES_VAL=${BYTES_VALUES[$((RANDOM % ${#BYTES_VALUES[@]}))]}; }
pick_agent() { AGENT=${USER_AGENTS[$((RANDOM % ${#USER_AGENTS[@]}))]}; }

# JSON escape function
json_escape() {
  echo "$1" | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/\\t/g; s/\n/\\n/g; s/\r/\\r/g'
}

# Generate userid based on IP hash (simple approach)
generate_userid() {
  local ip_hash=$(echo "$IP" | cksum | cut -d' ' -f1)
  USERIDS=(102 215 308 421 537 644 718 803 956 1047)
  USERID=${USERIDS[$((ip_hash % 10))]}
}

# Generate time formats
generate_time() {
  local epoch=$(date +%s)
  
  _TIME=$((epoch * 1000))  # Convert to milliseconds
  TIME=$(date -d "@$epoch" "+%d/%b/%Y:%H:%M:%S %z")
}

# State management for message ordering
MESSAGE_COUNT=0

while true; do
  # Generate all field values
  pick_ip
  generate_userid
  generate_time
  pick_request
  pick_status
  pick_bytes
  pick_agent
  
  # Fixed values
  REMOTE_USER="-"
  REFERRER="-"
  
  # Create message key (using userid for partitioning)
  KEY="$USERID"
  
  # Create AVRO message (single line JSON with proper escaping)
  VALUE="{\"ip\":\"$(json_escape "$IP")\",\"userid\":$USERID,\"remote_user\":\"$(json_escape "$REMOTE_USER")\",\"time\":\"$(json_escape "$TIME")\",\"_time\":$_TIME,\"request\":\"$(json_escape "$REQUEST")\",\"status\":\"$(json_escape "$STATUS")\",\"bytes\":\"$(json_escape "$BYTES_VAL")\",\"referrer\":\"$(json_escape "$REFERRER")\",\"agent\":\"$(json_escape "$AGENT")\"}"
  
  # Debug: Print the JSON (uncomment for debugging)
  # echo "DEBUG JSON: $VALUE" >&2

  # Set up quiet log4j config
  LOG_CFG="${LOG_CFG:-$(mktemp)}"
  if [ ! -s "$LOG_CFG" ]; then
    cat >"$LOG_CFG" <<'EOF'
log4j.rootLogger=ERROR, stderr
log4j.appender.stderr=org.apache.log4j.ConsoleAppender
log4j.appender.stderr.target=System.err
log4j.appender.stderr.layout=org.apache.log4j.PatternLayout
log4j.appender.stderr.layout.ConversionPattern=%m%n
log4j.logger.org.apache.kafka=ERROR
log4j.logger.io.confluent=ERROR
EOF
  fi

  ERRFILE=${ERRFILE:-$(mktemp)}
  trap 'rm -f "$ERRFILE"' EXIT
  : > "$ERRFILE"

  # Send message to Kafka
  if printf '%s|%s\n' "$KEY" "$VALUE" | \
     KAFKA_LOG4J_OPTS="-Dlog4j.configuration=file:$LOG_CFG" \
     KAFKA_OPTS="-Dlog4j.configuration=file:$LOG_CFG" \
     "$AVRO_PRODUCER_COMMAND" \
       --bootstrap-server "$BOOTSTRAP" \
       --topic "$TOPIC" \
       --property schema.registry.url="$SCHEMA_REGISTRY_URL" \
       --property value.schema="$SCHEMA" \
       --property value.schema.literal=true \
       --property parse.key=true \
       --property key.separator='|' \
       --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
       --producer-property acks=all \
       >"$ERRFILE" 2>&1
  then
    MESSAGE_COUNT=$((MESSAGE_COUNT + 1))
    echo "[$MESSAGE_COUNT] Sent click: IP=$IP, UserID=$USERID, Request='$REQUEST', Status=$STATUS"
  else
    echo "ERROR: send failed" >&2
    cat "$ERRFILE" >&2
    exit 1
  fi

  # ~2 messages per second
  sleep 0.5
done

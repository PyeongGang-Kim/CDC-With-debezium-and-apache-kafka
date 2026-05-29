#!/bin/sh
set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8084}

TARGET_DB_URL="jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)(FAILOVER=on)(ADDRESS=(PROTOCOL=TCP)(HOST=${TARGET_ORACLE_NODE1})(PORT=${TARGET_ORACLE_PORT:-1521}))(ADDRESS=(PROTOCOL=TCP)(HOST=${TARGET_ORACLE_NODE2})(PORT=${TARGET_ORACLE_PORT:-1521}))(CONNECT_DATA=(SERVICE_NAME=${TARGET_ORACLE_SERVICE_NAME})(SERVER=DEDICATED)))"

echo "=== Kafka Connect Sink 준비 대기 중... ==="
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "  대기 중..."
  sleep 5
done
echo "Kafka Connect Sink 준비 완료."

# ── 헬퍼: 커넥터 등록 ──────────────────────────────────────────────────────────
register_connector() {
  local NAME=$1
  local PAYLOAD=$2

  EXISTING=$(curl -sf "$CONNECT_URL/connectors/$NAME" 2>/dev/null || echo "")
  if [ -n "$EXISTING" ]; then
    echo "[$NAME] 기존 커넥터 삭제 후 재등록..."
    curl -sf -X DELETE "$CONNECT_URL/connectors/$NAME"
    sleep 2
  fi

  echo "[$NAME] 등록 중..."
  curl -sf -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD"
  echo ""
}

# ── 1. Upsert 커넥터 (키 있는 테이블) ─────────────────────────────────────────
if [ -n "${UPSERT_TOPICS}" ]; then
  register_connector "oracle-jdbc-sink-upsert" "{
    \"name\": \"oracle-jdbc-sink-upsert\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.jdbc.JdbcSinkConnector\",
      \"tasks.max\": \"4\",
      \"topics\": \"${UPSERT_TOPICS}\",
      \"connection.url\": \"${TARGET_DB_URL}\",
      \"connection.username\": \"${TARGET_ORACLE_USER}\",
      \"connection.password\": \"${TARGET_ORACLE_PASSWORD}\",
      \"insert.mode\": \"upsert\",
      \"primary.key.mode\": \"record_key\",
      \"delete.enabled\": \"true\",
      \"schema.evolution\": \"none\",
      \"transforms\": \"route\",
      \"transforms.route.type\": \"org.apache.kafka.connect.transforms.RegexRouter\",
      \"transforms.route.regex\": \"oracle\\\\.(.+)\",
      \"transforms.route.replacement\": \"\$1\",
      \"errors.log.enable\": \"true\",
      \"errors.log.include.messages\": \"true\",
      \"batch.size\": \"500\",
      \"max.retries\": \"5\",
      \"retry.backoff.ms\": \"3000\"
    }
  }"
else
  echo "[oracle-jdbc-sink-upsert] UPSERT_TOPICS 미설정, 건너뜀."
fi

# ── 2. Insert-only 커넥터 (PK 없는 테이블) ────────────────────────────────────
if [ -n "${INSERT_ONLY_TOPICS}" ]; then
  register_connector "oracle-jdbc-sink-insert-only" "{
    \"name\": \"oracle-jdbc-sink-insert-only\",
    \"config\": {
      \"connector.class\": \"io.debezium.connector.jdbc.JdbcSinkConnector\",
      \"tasks.max\": \"2\",
      \"topics\": \"${INSERT_ONLY_TOPICS}\",
      \"connection.url\": \"${TARGET_DB_URL}\",
      \"connection.username\": \"${TARGET_ORACLE_USER}\",
      \"connection.password\": \"${TARGET_ORACLE_PASSWORD}\",
      \"insert.mode\": \"insert\",
      \"primary.key.mode\": \"none\",
      \"delete.enabled\": \"false\",
      \"schema.evolution\": \"none\",
      \"transforms\": \"route\",
      \"transforms.route.type\": \"org.apache.kafka.connect.transforms.RegexRouter\",
      \"transforms.route.regex\": \"oracle\\\\.(.+)\",
      \"transforms.route.replacement\": \"\$1\",
      \"errors.log.enable\": \"true\",
      \"errors.log.include.messages\": \"true\",
      \"batch.size\": \"1000\",
      \"max.retries\": \"5\",
      \"retry.backoff.ms\": \"3000\"
    }
  }"
else
  echo "[oracle-jdbc-sink-insert-only] INSERT_ONLY_TOPICS 미설정, 건너뜀."
fi

echo ""
echo "=== 등록된 커넥터 상태 ==="
curl -sf "$CONNECT_URL/connectors" | tr ',' '\n'

#!/bin/sh
set -e

CONNECT_URL=${CONNECT_URL:-http://localhost:8083}
CDC_METHOD=${CDC_METHOD:-logminer}

echo "=== Kafka Connect 준비 대기 중... ==="
until curl -sf "$CONNECT_URL/connectors" > /dev/null; do
  echo "  대기 중..."
  sleep 5
done
echo "Kafka Connect 준비 완료."

EXISTING=$(curl -sf "$CONNECT_URL/connectors/oracle-rac-cdc-connector" 2>/dev/null || echo "")
if [ -n "$EXISTING" ]; then
  echo "기존 커넥터 발견. 삭제 후 재등록합니다..."
  curl -sf -X DELETE "$CONNECT_URL/connectors/oracle-rac-cdc-connector"
  sleep 3
fi

# ── 공통 JDBC URL (RAC LOAD_BALANCE + FAILOVER) ────────────────────────────
DB_URL="jdbc:oracle:thin:@(DESCRIPTION=(LOAD_BALANCE=on)(FAILOVER=on)(ADDRESS=(PROTOCOL=TCP)(HOST=${ORACLE_RAC_NODE1})(PORT=${ORACLE_PORT}))(ADDRESS=(PROTOCOL=TCP)(HOST=${ORACLE_RAC_NODE2})(PORT=${ORACLE_PORT}))(CONNECT_DATA=(SERVICE_NAME=${ORACLE_SERVICE_NAME})(SERVER=DEDICATED)))"

# ── 공통 커넥터 설정 ────────────────────────────────────────────────────────
COMMON_CONFIG="
      \"connector.class\": \"io.debezium.connector.oracle.OracleConnector\",
      \"tasks.max\": \"1\",
      \"database.hostname\": \"${ORACLE_SCAN_HOST}\",
      \"database.port\": \"${ORACLE_PORT}\",
      \"database.user\": \"${ORACLE_LOGMINER_USER}\",
      \"database.password\": \"${ORACLE_LOGMINER_PASSWORD}\",
      \"database.dbname\": \"${ORACLE_CDB_NAME}\",
      \"database.pdb.name\": \"${ORACLE_PDB_NAME}\",
      \"database.url\": \"${DB_URL}\",
      \"topic.prefix\": \"oracle\",
      \"schema.history.internal.kafka.bootstrap.servers\": \"${KAFKA_BOOTSTRAP_SERVERS}\",
      \"schema.history.internal.kafka.topic\": \"oracle.schema-history\",
      \"table.include.list\": \"${TABLE_INCLUDE_LIST}\",
      \"snapshot.include.collection.list\": \"${SNAPSHOT_INCLUDE_LIST}\",
      \"message.key.columns\": \"${MESSAGE_KEY_COLUMNS}\",
      \"signal.enabled.channels\": \"kafka\",
      \"signal.kafka.topic\": \"oracle.signals\",
      \"signal.kafka.bootstrap.servers\": \"${KAFKA_BOOTSTRAP_SERVERS}\",
      \"signal.kafka.group.id\": \"oracle-signal-group\",
      \"snapshot.mode\": \"initial\",
      \"snapshot.locking.mode\": \"none\",
      \"snapshot.fetch.size\": \"2000\",
      \"decimal.handling.mode\": \"string\",
      \"time.precision.mode\": \"connect\",
      \"binary.handling.mode\": \"base64\",
      \"heartbeat.interval.ms\": \"10000\",
      \"poll.interval.ms\": \"1000\",
      \"key.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"key.converter.schemas.enable\": \"true\",
      \"value.converter\": \"org.apache.kafka.connect.json.JsonConverter\",
      \"value.converter.schemas.enable\": \"true\",
      \"errors.log.enable\": \"true\",
      \"errors.log.include.messages\": \"true\",
      \"tombstones.on.delete\": \"true\",
      \"max.batch.size\": \"2048\",
      \"max.queue.size\": \"8192\""

if [ "$CDC_METHOD" = "xstream" ]; then
  echo "=== CDC 방식: XStream ==="

  curl -sf -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"oracle-rac-cdc-connector\",
      \"config\": {
        ${COMMON_CONFIG},
        \"database.connection.adapter\": \"xstream\",
        \"database.out.server.name\": \"${XSTREAM_OUT_SERVER_NAME}\"
      }
    }"

else
  echo "=== CDC 방식: LogMiner (기본값) ==="

  curl -sf -X POST "$CONNECT_URL/connectors" \
    -H "Content-Type: application/json" \
    -d "{
      \"name\": \"oracle-rac-cdc-connector\",
      \"config\": {
        ${COMMON_CONFIG},
        \"database.connection.adapter\": \"logminer\",
        \"log.mining.strategy\": \"online_catalog\",
        \"log.mining.batch.size.max\": \"20000\",
        \"log.mining.sleep.time.max.ms\": \"3000\",
        \"log.mining.transaction.retention.ms\": \"86400000\",
        \"rac.nodes\": \"${ORACLE_RAC_NODE1},${ORACLE_RAC_NODE2}\"
      }
    }"
fi

echo ""
echo "=== 소스 커넥터 등록 완료. 상태 확인 중... ==="
sleep 5
curl -sf "$CONNECT_URL/connectors/oracle-rac-cdc-connector/status"

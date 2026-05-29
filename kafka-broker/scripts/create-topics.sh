#!/bin/bash
set -e

KAFKA_CONTAINER="kafka-1"
BOOTSTRAP_SERVER="kafka-1:9092"
REPLICATION_FACTOR=2
PARTITIONS=3

echo "=== Kafka 토픽 생성 시작 ==="

echo "[1/4] oracle.schema-history 토픽 생성..."
docker exec $KAFKA_CONTAINER kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic oracle.schema-history \
  --partitions 1 \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=delete \
  --config retention.ms=-1

echo "[2/4] connect-offsets 토픽 생성..."
docker exec $KAFKA_CONTAINER kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic connect-offsets \
  --partitions 25 \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=compact

echo "[3/4] connect-configs 토픽 생성..."
docker exec $KAFKA_CONTAINER kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic connect-configs \
  --partitions 1 \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=compact

echo "[4/4] connect-status 토픽 생성..."
docker exec $KAFKA_CONTAINER kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic connect-status \
  --partitions 5 \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=compact

echo "[5/5] oracle.signals 토픽 생성 (ad-hoc 스냅샷 신호 채널)..."
docker exec $KAFKA_CONTAINER kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --create --if-not-exists \
  --topic oracle.signals \
  --partitions 1 \
  --replication-factor $REPLICATION_FACTOR \
  --config cleanup.policy=delete \
  --config retention.ms=86400000

echo ""
echo "=== 생성된 토픽 목록 ==="
docker exec $KAFKA_CONTAINER kafka-topics.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --list

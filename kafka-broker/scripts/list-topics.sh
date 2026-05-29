#!/bin/bash
BOOTSTRAP_SERVER="kafka-1:29092"

echo "=== 토픽 목록 ==="
docker exec kafka-1 kafka-topics \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --list

echo ""
echo "=== 토픽 상세 정보 ==="
docker exec kafka-1 kafka-topics \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --describe

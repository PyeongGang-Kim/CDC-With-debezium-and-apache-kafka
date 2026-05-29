#!/bin/bash
CONNECT_URL=${1:-http://localhost:8083}
CONNECTOR_NAME="oracle-rac-cdc-connector"

echo "=== 커넥터 상태 ==="
curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool

echo ""
echo "=== 전체 커넥터 목록 ==="
curl -sf "$CONNECT_URL/connectors"

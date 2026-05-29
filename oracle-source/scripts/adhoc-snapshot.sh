#!/bin/bash
# 실행 중인 커넥터에서 특정 테이블만 ad-hoc 스냅샷 트리거
# 사용법: ./adhoc-snapshot.sh <SCHEMA.TABLE1> [<SCHEMA.TABLE2> ...]
# 예시:   ./adhoc-snapshot.sh MYSCHEMA.ORDERS MYSCHEMA.CUSTOMERS

set -e

KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka-1}
BOOTSTRAP_SERVER=${BOOTSTRAP_SERVER:-kafka-1:9092}
SIGNAL_TOPIC="oracle.signals"

if [ $# -eq 0 ]; then
  echo "사용법: $0 <SCHEMA.TABLE> [<SCHEMA.TABLE> ...]"
  echo "예시:   $0 MYSCHEMA.ORDERS MYSCHEMA.CUSTOMERS"
  exit 1
fi

# 테이블 목록을 JSON 배열로 변환
COLLECTIONS=""
for TABLE in "$@"; do
  COLLECTIONS="${COLLECTIONS}\"${TABLE}\","
done
COLLECTIONS="[${COLLECTIONS%,}]"

SIGNAL_ID="adhoc-snapshot-$(date +%s)"
SIGNAL_KEY="${SIGNAL_ID}"
SIGNAL_VALUE="{\"type\":\"execute-snapshot\",\"data\":{\"data-collections\":${COLLECTIONS},\"type\":\"incremental\"}}"

echo "=== Ad-hoc 스냅샷 신호 전송 ==="
echo "대상 테이블: $*"
echo "Signal ID : $SIGNAL_ID"
echo ""

echo "${SIGNAL_KEY}:${SIGNAL_VALUE}" | docker exec -i $KAFKA_CONTAINER \
  kafka-console-producer.sh \
  --bootstrap-server $BOOTSTRAP_SERVER \
  --topic $SIGNAL_TOPIC \
  --property "parse.key=true" \
  --property "key.separator=:"

echo "스냅샷 신호 전송 완료. 커넥터 로그에서 진행 상황을 확인하세요."

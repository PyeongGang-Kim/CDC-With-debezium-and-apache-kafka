FROM debezium/connect:2.7

# Debezium Oracle 커넥터 플러그인 설치
# Oracle JDBC 드라이버(ojdbc11.jar)는 라이선스 제한으로 직접 포함 불가
# ./plugins/ 디렉터리에 미리 복사 후 빌드 필요
USER root

COPY plugins/ /kafka/connect/debezium-connector-oracle/

# Oracle Instant Client 경로 설정 (필요 시)
ENV LD_LIBRARY_PATH=/usr/lib/oracle/instantclient:$LD_LIBRARY_PATH

USER kafka

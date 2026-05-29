FROM debezium/connect:3.0.0.Final

# Debezium Oracle 커넥터 버전
# plugins/ 에 아래 파일을 배치 후 빌드:
#   debezium-connector-oracle-{DEBEZIUM_VERSION}-plugin.tar.gz
#   ojdbc11.jar
# 다운로드: https://repo1.maven.org/maven2/io/debezium/debezium-connector-oracle/
ARG DEBEZIUM_VERSION=3.0.0.Final

USER root

COPY plugins/ /tmp/debezium-plugins/
RUN tar -xzf "/tmp/debezium-plugins/debezium-connector-oracle-${DEBEZIUM_VERSION}-plugin.tar.gz" \
      -C /kafka/connect/ && \
    cp /tmp/debezium-plugins/ojdbc11.jar /kafka/connect/debezium-connector-oracle/ && \
    rm -rf /tmp/debezium-plugins

ENV LD_LIBRARY_PATH=/usr/lib/oracle/instantclient:$LD_LIBRARY_PATH

USER kafka

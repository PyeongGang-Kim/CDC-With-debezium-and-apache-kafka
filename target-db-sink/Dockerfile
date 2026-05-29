FROM debezium/connect:2.7

# Debezium JDBC Sink 커넥터 (Apache License 2.0, Maven Central)
# https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/
ARG DEBEZIUM_VERSION=2.7.0.Final

USER root

RUN mkdir -p /kafka/connect/debezium-connector-jdbc && \
    curl -fsSL \
      "https://repo1.maven.org/maven2/io/debezium/debezium-connector-jdbc/${DEBEZIUM_VERSION}/debezium-connector-jdbc-${DEBEZIUM_VERSION}-plugin.tar.gz" \
    | tar -xzf - -C /kafka/connect/

# Oracle JDBC 드라이버 - 라이선스 제한으로 자동 다운로드 불가
# 아래 URL에서 ojdbc11.jar를 내려받아 plugins/ 디렉터리에 배치 후 빌드하세요.
# https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html
COPY plugins/ojdbc11.jar /kafka/connect/debezium-connector-jdbc/

USER kafka

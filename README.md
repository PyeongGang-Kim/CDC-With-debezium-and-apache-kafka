# Oracle RAC → Kafka → Oracle CDC 파이프라인

Debezium을 사용하여 Oracle RAC 소스 DB의 변경 데이터를 캡처하고, Apache Kafka를 통해 타겟 Oracle DB에 실시간으로 반영하는 CDC(Change Data Capture) 파이프라인입니다.

## 아키텍처

```
  ┌─── oracle-source ─────────────────────────────────┐
  │                                                   │
  │  Oracle RAC (소스)          Kafka Connect          │
  │  ┌──────────────────┐      ┌───────────────────┐  │
  │  │ Node1 │  Node2   │      │ Debezium Oracle   │  │
  │  │  Redo │   Redo   ├─────►│ RAC Connector     │  │
  │  │  Log  │   Log    │      │ (LogMiner)        │  │
  │  └──────────────────┘      └─────────┬─────────┘  │
  └────────────────────────────────────  ┼ ───────────┘
                                         │ Produce (JSON + Schema)
                                         ▼
  ┌─── kafka-broker ──────────────────────────────────┐
  │                                                   │
  │  Apache Kafka 3.8.1  ·  KRaft (ZooKeeper 없음)    │
  │                                                   │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
  │  │ kafka-1  │  │ kafka-2  │  │ kafka-3  │        │
  │  │ broker   │  │ broker   │  │ broker   │        │
  │  │ +ctrl    │  │ +ctrl    │  │ +ctrl    │        │
  │  └──────────┘  └──────────┘  └──────────┘        │
  │                                                   │
  │  Topics: oracle.SCHEMA.TABLE_*                    │
  └────────────────────────────────────┬──────────────┘
                                       │ Consume (JSON + Schema)
                                       ▼
  ┌─── target-db-sink ────────────────────────────────┐
  │                                                   │
  │  Kafka Connect  +  Debezium JDBC Sink             │
  │                                                   │
  │  ┌─────────────────────────────────────────────┐  │
  │  │ upsert 커넥터    키 있는 테이블              │  │
  │  │  insert.mode: upsert  /  delete.enabled     │  │
  │  └─────────────────────────────────────────────┘  │
  │  ┌─────────────────────────────────────────────┐  │
  │  │ insert-only 커넥터   PK 없는 테이블          │  │
  │  │  insert.mode: insert  /  delete 무시         │  │
  │  └─────────────────────────────────────────────┘  │
  │                        │ Oracle JDBC (ojdbc11)     │
  │                        ▼                          │
  │  Oracle RAC (타겟)                                │
  │  ┌──────────────────────────────────────┐         │
  │  │  Node1            Node2              │         │
  │  └──────────────────────────────────────┘         │
  └───────────────────────────────────────────────────┘
```

## 디렉터리 구조

```
cdc/
├── kafka-broker/                   # Apache Kafka 클러스터
│   ├── .gitignore
│   ├── dev/                        # 개발용 (3노드 단일 호스트)
│   │   ├── docker-compose.yml
│   │   └── .env.example
│   ├── prod/                       # 운영용 (서버별 개별 배포)
│   │   ├── docker-compose.yml      # 단일 노드 템플릿
│   │   ├── .env.node1.example
│   │   ├── .env.node2.example
│   │   └── .env.node3.example
│   └── scripts/
│       ├── create-topics.sh        # 필수 토픽 생성
│       └── list-topics.sh
│
├── oracle-source/                  # Debezium Oracle RAC 소스 커넥터
│   ├── Dockerfile
│   ├── docker-compose.yml
│   ├── .env.example
│   ├── config/
│   │   └── oracle-connector.json   # 커넥터 설정
│   ├── plugins/                    # ojdbc11.jar 수동 배치 위치 (gitignore 권장)
│   └── scripts/
│       ├── oracle-setup.sql        # Oracle 사전 설정 (SYSDBA 실행)
│       ├── register-connector.sh   # 커넥터 등록
│       ├── check-connector-status.sh
│       └── adhoc-snapshot.sh       # 실행 중 특정 테이블 재스냅샷
│
└── target-db-sink/                 # Debezium JDBC Sink 커넥터
    ├── Dockerfile
    ├── docker-compose.yml
    ├── .env.example
    ├── config/
    │   ├── oracle-sink-upsert.json       # 키 있는 테이블용
    │   └── oracle-sink-insert-only.json  # PK 없는 테이블용
    ├── plugins/                    # ojdbc11.jar 수동 배치 위치
    └── scripts/
        └── register-sink-connector.sh    # 두 커넥터 일괄 등록
```

## 사전 요구사항

| 항목 | 버전 |
|------|------|
| Docker | 24.0 이상 |
| Docker Compose | v2.20 이상 |
| Oracle JDBC Driver | ojdbc11.jar ([다운로드](https://www.oracle.com/database/technologies/appdev/jdbc-downloads.html)) |
| Oracle 소스 DB | Oracle 12c 이상 (Archive Log 모드, Supplemental Logging 필요) |

## 설치 및 실행

### 1단계 — Oracle 소스 DB 사전 설정

Oracle 소스 DB에 SYSDBA로 접속하여 실행합니다.

```sql
-- oracle-source/scripts/oracle-setup.sql 참고
-- Archive Log 모드 활성화, Supplemental Logging 설정, Debezium 전용 계정 생성
@oracle-source/scripts/oracle-setup.sql
```

주요 작업:
- Archive Log 모드 활성화
- Supplemental Logging 활성화 (`PRIMARY KEY`, `UNIQUE INDEX`)
- Debezium LogMiner 전용 계정 생성 (`c##dbzuser`)
- LogMiner 관련 권한 부여

### 2단계 — Oracle JDBC 드라이버 배치

Oracle JDBC 드라이버는 라이선스 제한으로 자동 다운로드가 불가합니다.  
`ojdbc11.jar`를 각 `plugins/` 디렉터리에 직접 배치합니다.

```bash
cp /path/to/ojdbc11.jar oracle-source/plugins/
cp /path/to/ojdbc11.jar target-db-sink/plugins/
```

### 3단계 — kafka-broker 실행

#### 개발 환경 (단일 호스트에서 3노드 구동)

```bash
cd kafka-broker/dev

cp .env.example .env
# KAFKA_CLUSTER_ID 확인 (기본값 사용 가능, 변경 시 직접 생성)
# docker run --rm apache/kafka:3.8.1 kafka-storage.sh random-uuid

docker compose up -d

# 필수 토픽 생성 (Kafka 기동 완료 후)
bash ../scripts/create-topics.sh
```

Kafka UI: http://localhost:8080

#### 운영 환경 (서버 3대에 각각 배포)

각 서버에 `kafka-broker/prod/` 디렉토리를 복사한 뒤 서버별로 실행합니다.

```bash
cd kafka-broker/prod

# 서버1: .env.node1.example 복사 후 IP 수정
cp .env.node1.example .env
# NODE_HOST, QUORUM_VOTERS 의 IP를 실제 서버 IP로 변경

docker compose up -d
```

> 서버2, 서버3도 동일하게 해당 노드의 `.env.nodeN.example`을 `.env`로 복사 후 실행합니다.  
> 세 서버 모두 **동일한 `KAFKA_CLUSTER_ID`** 를 사용해야 합니다.

방화벽/보안그룹에서 서버 간 아래 포트를 허용해야 합니다.

| 포트 | 용도 |
|------|------|
| 9092 | 브로커 간 내부 통신 |
| 9093 | KRaft 컨트롤러 쿼럼 |

### 4단계 — oracle-source 실행

```bash
cd oracle-source

cp .env.example .env
# .env 파일에 Oracle RAC 연결 정보 입력

# 이미지 빌드 (ojdbc11.jar 배치 후)
docker compose build

docker compose up -d
# kafka-connect-source 기동 시 register-connector.sh 자동 실행
```

커넥터 상태 확인:

```bash
bash scripts/check-connector-status.sh
```

### 5단계 — target-db-sink 실행

```bash
cd target-db-sink

cp .env.example .env
# .env 파일에 타겟 Oracle RAC 연결 정보 및 토픽 목록 입력

docker compose build
docker compose up -d
# kafka-connect-sink 기동 시 register-sink-connector.sh 자동 실행
```

## 환경변수 설정

### oracle-source/.env

| 변수 | 설명 | 예시 |
|------|------|------|
| `ORACLE_SCAN_HOST` | Oracle RAC SCAN 주소 | `scan.example.com` |
| `ORACLE_SERVICE_NAME` | Oracle 서비스명 | `MYORCL` |
| `ORACLE_CDB_NAME` | CDB 이름 | `CDB1` |
| `ORACLE_PDB_NAME` | PDB 이름 (없으면 빈값) | `PDB1` |
| `ORACLE_RAC_NODE1` | RAC Node 1 IP | `192.168.1.101` |
| `ORACLE_RAC_NODE2` | RAC Node 2 IP | `192.168.1.102` |
| `ORACLE_LOGMINER_USER` | Debezium 전용 계정 | `c##dbzuser` |
| `TABLE_INCLUDE_LIST` | CDC 대상 테이블 전체 목록 | `SCHEMA.T1,SCHEMA.T2` |
| `SNAPSHOT_INCLUDE_LIST` | 초기 스냅샷 대상 테이블 | `SCHEMA.T1` |
| `MESSAGE_KEY_COLUMNS` | PK 없는 테이블의 키 컬럼 지정 | `SCHEMA.T2:COL1,COL2` |

### target-db-sink/.env

| 변수 | 설명 | 예시 |
|------|------|------|
| `TARGET_ORACLE_NODE1` | 타겟 RAC Node 1 IP | `192.168.2.101` |
| `TARGET_ORACLE_NODE2` | 타겟 RAC Node 2 IP | `192.168.2.102` |
| `TARGET_ORACLE_SERVICE_NAME` | 타겟 Oracle 서비스명 | `TGTORCL` |
| `TARGET_ORACLE_USER` | 타겟 DB 계정 | `cdc_target` |
| `UPSERT_TOPICS` | upsert 처리할 토픽 목록 | `oracle.SCHEMA.T1,oracle.SCHEMA.T2` |
| `INSERT_ONLY_TOPICS` | insert-only 처리할 토픽 목록 | `oracle.SCHEMA.T3` |

## 싱크 토픽 라우팅

### Kafka 토픽 네이밍 규칙

소스 커넥터는 Oracle 변경 이벤트를 아래 형식의 토픽으로 자동 발행합니다.

```
oracle.SCHEMA.TABLE
  │      │      └── 테이블명
  │      └───────── 스키마명
  └──────────────── database.server.name (소스 커넥터 설정값)
```

### UPSERT_TOPICS / INSERT_ONLY_TOPICS 동작 원리

두 환경변수는 각 싱크 커넥터가 **구독할 토픽 목록**을 지정합니다.  
`register-sink-connector.sh` 기동 시 이 값을 읽어 Kafka Connect REST API로 커넥터를 등록합니다.

```
target-db-sink/.env
  UPSERT_TOPICS=oracle.MYSCHEMA.TABLE1,oracle.MYSCHEMA.TABLE2
        │
        ▼
register-sink-connector.sh
  curl -X POST /connectors -d '{ "topics": "oracle.MYSCHEMA.TABLE1,oracle.MYSCHEMA.TABLE2", ... }'
        │
        ▼
oracle-jdbc-sink-upsert 커넥터
  oracle.MYSCHEMA.TABLE1, oracle.MYSCHEMA.TABLE2 두 토픽을 구독하여 처리
```

### 설정 예시 (TABLE1, TABLE2, TABLE3 중 TABLE1·TABLE2만 연계)

**oracle-source/.env**

```bash
# TABLE3 는 목록에서 제외 → Redo Log를 읽어도 무시, 토픽 미생성
TABLE_INCLUDE_LIST=MYSCHEMA.TABLE1,MYSCHEMA.TABLE2
MESSAGE_KEY_COLUMNS=MYSCHEMA.TABLE2:COL_A,COL_B   # TABLE2 PK 없는 경우 키 컬럼 지정
```

**target-db-sink/.env**

```bash
# TABLE1: PK 있음 → upsert 커넥터
UPSERT_TOPICS=oracle.MYSCHEMA.TABLE1,oracle.MYSCHEMA.TABLE2

# PK 없고 키도 지정 불가한 테이블이 있으면 여기에 추가
INSERT_ONLY_TOPICS=
```

**결과 흐름**

```
Oracle 소스
├── TABLE1 변경  →  oracle.MYSCHEMA.TABLE1  →  oracle-jdbc-sink-upsert
│                                               MERGE INTO MYSCHEMA.TABLE1
├── TABLE2 변경  →  oracle.MYSCHEMA.TABLE2  →  oracle-jdbc-sink-upsert
│                                               MERGE INTO MYSCHEMA.TABLE2
└── TABLE3 변경  →  (무시, 토픽 없음)
```

### 토픽명 → 타겟 테이블명 변환 (RegexRouter)

싱크 커넥터는 토픽명에서 `oracle.` 접두사를 제거하여 타겟 테이블명으로 사용합니다.

```
oracle.MYSCHEMA.TABLE1  →  MYSCHEMA.TABLE1
oracle.MYSCHEMA.TABLE2  →  MYSCHEMA.TABLE2
```

소스와 타겟의 스키마·테이블명이 동일한 구조를 전제합니다.

## 테이블별 스냅샷 제어

### 초기 스냅샷 선택

`SNAPSHOT_INCLUDE_LIST`에 포함된 테이블만 초기 스냅샷을 수행합니다.  
목록에 없는 테이블은 커넥터 시작 시점부터의 CDC 이벤트만 수신합니다.

```bash
# oracle-source/.env 예시
TABLE_INCLUDE_LIST=SCHEMA.ORDERS,SCHEMA.CUSTOMERS,SCHEMA.AUDIT_LOG
SNAPSHOT_INCLUDE_LIST=SCHEMA.ORDERS,SCHEMA.CUSTOMERS   # AUDIT_LOG 는 스냅샷 없이 CDC만
```

### 실행 중 특정 테이블 재스냅샷 (Ad-hoc)

커넥터를 재시작하지 않고 특정 테이블만 Incremental Snapshot을 트리거합니다.

```bash
cd oracle-source
bash scripts/adhoc-snapshot.sh SCHEMA.ORDERS
bash scripts/adhoc-snapshot.sh SCHEMA.ORDERS SCHEMA.CUSTOMERS   # 복수 테이블
```

## PK 없는 테이블 처리 전략

```
PK 있는 테이블
  → 자동으로 PK = Kafka Key 설정
  → UPSERT_TOPICS 에 포함
  → oracle-jdbc-sink-upsert 커넥터: MERGE INTO 처리

PK 없지만 유효 컬럼 조합이 있는 테이블
  → MESSAGE_KEY_COLUMNS 로 키 컬럼 직접 지정
  → UPSERT_TOPICS 에 포함
  → oracle-jdbc-sink-upsert 커넥터: MERGE INTO 처리

완전히 키를 특정할 수 없는 테이블 (순수 append-only)
  → INSERT_ONLY_TOPICS 에 포함
  → oracle-jdbc-sink-insert-only 커넥터: INSERT 처리
  → DELETE 이벤트 무시 (키 없이 대상 행 특정 불가)
```

## DML 이벤트 흐름

| Oracle DML | Debezium op | Kafka Value | 타겟 처리 |
|------------|-------------|-------------|-----------|
| `INSERT` | `c` | `payload.after` | `MERGE INTO` (upsert) |
| `UPDATE` | `u` | `payload.after` | `MERGE INTO` (upsert) |
| `DELETE` | `d` | tombstone (null) | `DELETE WHERE PK` |
| 스냅샷 읽기 | `r` | `payload.after` | `MERGE INTO` (upsert) |

## 포트 정보

### 개발 환경

| 서비스 | 포트 | 설명 |
|--------|------|------|
| kafka-1 | 19092 | 외부 접근용 |
| kafka-2 | 19093 | 외부 접근용 |
| kafka-3 | 19094 | 외부 접근용 |
| Kafka UI | 8080 | 브라우저 모니터링 |
| Kafka Connect (소스) | 8083 | REST API |
| Kafka Connect (싱크) | 8084 | REST API |

### 운영 환경

| 서비스 | 포트 | 설명 |
|--------|------|------|
| kafka (각 서버) | 9092 | 클라이언트 접근용 |
| kafka (각 서버) | 9093 | KRaft 컨트롤러 쿼럼 (서버 간 내부) |
| Kafka Connect (소스) | 8083 | REST API |
| Kafka Connect (싱크) | 8084 | REST API |

## 커넥터 REST API

```bash
# 커넥터 목록
curl http://localhost:8083/connectors

# 소스 커넥터 상태
curl http://localhost:8083/connectors/oracle-rac-cdc-connector/status

# 싱크 커넥터 상태
curl http://localhost:8084/connectors/oracle-jdbc-sink-upsert/status
curl http://localhost:8084/connectors/oracle-jdbc-sink-insert-only/status

# 커넥터 일시 중지 / 재개
curl -X PUT http://localhost:8083/connectors/oracle-rac-cdc-connector/pause
curl -X PUT http://localhost:8083/connectors/oracle-rac-cdc-connector/resume
```

## 주의사항

- **ojdbc11.jar**: Oracle 라이선스 정책으로 자동 배포 불가. `plugins/` 디렉터리에 직접 배치 필요.
- **Supplemental Logging**: 소스 Oracle에 반드시 활성화 필요. `oracle-setup.sql` 참고.
- **토픽 겹침 금지**: `UPSERT_TOPICS`와 `INSERT_ONLY_TOPICS`에 동일 토픽이 포함되면 이중 처리 발생.
- **schema.evolution: none**: 타겟 테이블 DDL 자동 변경을 차단. 소스 스키마 변경 시 타겟에 수동으로 DDL 적용 후 커넥터를 재시작해야 함.
- **CLUSTER_ID**: 동일 Kafka 클러스터의 모든 브로커가 동일한 값을 사용해야 함. 볼륨 초기화 시 재생성 필요.

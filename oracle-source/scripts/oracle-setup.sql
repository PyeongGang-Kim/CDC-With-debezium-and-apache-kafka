-- ============================================================
-- Oracle RAC Debezium CDC 설정 스크립트
-- SYSDBA 계정으로 실행
--
-- [섹션 구성]
--   1~10 : LogMiner / XStream 공통 설정
--   11~  : XStream 전용 설정 (CDC_METHOD=xstream 사용 시에만 실행)
-- ============================================================

-- 1. Archive Log 모드 활성화 (이미 활성화된 경우 생략)
-- SHUTDOWN IMMEDIATE;
-- STARTUP MOUNT;
-- ALTER DATABASE ARCHIVELOG;
-- ALTER DATABASE OPEN;

-- Archive Log 모드 확인
SELECT LOG_MODE FROM V$DATABASE;

-- 2. Supplemental Logging 활성화 (LogMiner 필수)
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY, UNIQUE INDEX) COLUMNS;

-- 테이블별 ALL COLUMN 로깅 (UPDATE 시 before 이미지 캡처 필수)
-- ALTER TABLE MYSCHEMA.ORDERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
-- ALTER TABLE MYSCHEMA.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- 3. Debezium 전용 CDB 공통 계정 생성
CREATE USER c##dbzuser IDENTIFIED BY "ChangeMe_DBZ_2024!"
  DEFAULT TABLESPACE USERS
  QUOTA UNLIMITED ON USERS
  CONTAINER = ALL;

-- 4. 기본 세션 권한
GRANT CREATE SESSION TO c##dbzuser CONTAINER = ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER = ALL;

-- 5. LogMiner 권한
GRANT LOGMINING TO c##dbzuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER = ALL;

-- 6. 딕셔너리 / 뷰 조회 권한
GRANT SELECT ANY DICTIONARY TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER = ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER = ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER = ALL;

-- 7. LogMiner 내부 뷰 권한
GRANT SELECT ON V_$DATABASE TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$TRANSACTION TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER = ALL;

-- 8. RAC 환경: 모든 노드의 redo log 접근 가능 여부 확인
SELECT INST_ID, GROUP#, MEMBERS, STATUS FROM GV$LOG ORDER BY INST_ID, GROUP#;

-- 9. 설정 확인
SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK,
       SUPPLEMENTAL_LOG_DATA_UI
FROM V$DATABASE;

-- 10. Supplemental Logging 테이블별 확인
SELECT OWNER, LOG_GROUP_NAME, TABLE_NAME, LOG_GROUP_TYPE
FROM DBA_LOG_GROUPS
WHERE OWNER = 'MYSCHEMA';


-- ============================================================
-- XStream 전용 설정 (CDC_METHOD=xstream 사용 시에만 실행)
-- Oracle GoldenGate 라이선스 필요
-- ============================================================

-- 11. XStream 관련 추가 권한 부여
-- GRANT DBA TO c##dbzuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_XSTREAM_AUTH TO c##dbzuser CONTAINER = ALL;
GRANT EXECUTE ON DBMS_XSTREAM_ADM TO c##dbzuser CONTAINER = ALL;
GRANT SELECT ON V_$XSTREAM_OUTBOUND_SERVER TO c##dbzuser CONTAINER = ALL;

-- XStream 관리자 권한 부여 (DBMS_XSTREAM_AUTH 실행)
-- BEGIN
--   DBMS_XSTREAM_AUTH.GRANT_ADMIN_PRIVILEGE(
--     grantee                 => 'c##dbzuser',
--     privilege_type          => 'CAPTURE',
--     grant_select_privileges => TRUE,
--     container               => 'ALL'
--   );
-- END;
-- /

-- 12. XStream Outbound Server 생성
-- 아래 이름(dbzxout)은 .env 의 XSTREAM_OUT_SERVER_NAME 과 일치해야 함
-- BEGIN
--   DBMS_XSTREAM_ADM.CREATE_OUTBOUND(
--     server_name     => 'dbzxout',
--     table_names     => DBMS_UTILITY.COMMA_TO_TABLE('MYSCHEMA.ORDERS,MYSCHEMA.CUSTOMERS'),
--     connect_user    => 'c##dbzuser'
--   );
-- END;
-- /

-- 13. XStream Outbound Server 상태 확인
-- SELECT SERVER_NAME, STATUS, CAPTURE_NAME FROM ALL_XSTREAM_OUTBOUND;

-- 14. XStream Outbound Server 삭제 (재생성 필요 시)
-- BEGIN
--   DBMS_XSTREAM_ADM.DROP_OUTBOUND(server_name => 'dbzxout');
-- END;
-- /

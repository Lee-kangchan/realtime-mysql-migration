# Realtime MySQL Migration (Debezium + Kafka + Spring Boot)

MySQL 변경 사항을 Debezium CDC로 Kafka에 발행하고, Spring Boot 앱이 이를 소비해 대상 DB에 반영하는 예제입니다.

## 구성 요소
- MySQL (binlog 기반 CDC)
- Kafka + Zookeeper
- Debezium Connect
- Spring Boot (Kotlin)

## 사전 준비
- Docker / Docker Compose
- JDK 17
- curl

## 로컬 실행 순서
1. JDK 17 사용 확인
   - 이 프로젝트는 Java 17 기준입니다.
   - 로컬 기본 JDK가 17이 아니면 `JAVA_HOME`을 Java 17로 지정합니다.
   ```bash
   java -version
   export JAVA_HOME=$(/usr/libexec/java_home -v 17)
   ```

2. 인프라 기동
   - `docker-compose.yml` 기준으로 Kafka, MySQL, Debezium Connect를 올립니다.
   ```bash
   docker-compose up -d
   ```

3. 컨테이너 상태 확인
   ```bash
   docker-compose ps
   curl http://localhost:8083/connectors
   ```

4. MySQL DB와 테이블 준비
   - 현재 Kafka 소비자는 `mysql-db-server.inventory.customers` 토픽을 구독합니다.
   - Debezium 소스 DB는 `inventory`, 애플리케이션이 쓰는 타깃 DB는 `test_db`로 준비합니다.
   - `inventory.customers`에 변경을 넣으면 Debezium이 Kafka로 발행하고, Spring Boot 앱이 `test_db.customers`에 반영합니다.
   ```sql
   CREATE DATABASE IF NOT EXISTS inventory;

   GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
   ON *.* TO 'debezium'@'%';
   FLUSH PRIVILEGES;

   CREATE TABLE IF NOT EXISTS inventory.customers (
     id INT PRIMARY KEY,
     first_name VARCHAR(100),
     last_name VARCHAR(100),
     email VARCHAR(200)
   );

   CREATE TABLE IF NOT EXISTS test_db.customers (
     id INT PRIMARY KEY,
     first_name VARCHAR(100),
     last_name VARCHAR(100),
     email VARCHAR(200)
   );
   ```
   - Docker MySQL 컨테이너에서 바로 실행하려면 아래처럼 접속합니다.
   ```bash
   docker exec -it mysql mysql -uroot -proot
   ```

5. Debezium 커넥터 등록
   - Connect REST API에 MySQL 커넥터를 등록합니다.
   - `topic.prefix`가 `mysql-db-server`이므로 `inventory.customers` 변경 이벤트는 `mysql-db-server.inventory.customers` 토픽으로 발행됩니다.
   ```bash
   curl -X POST http://localhost:8083/connectors \
     -H 'Content-Type: application/json' \
     -d '{
       "name": "mysql-source-connector",
       "config": {
         "connector.class": "io.debezium.connector.mysql.MySqlConnector",
         "database.hostname": "mysql",
         "database.port": "3306",
         "database.user": "debezium",
         "database.password": "dbz",
         "database.server.id": "184054",
         "topic.prefix": "mysql-db-server",
         "database.include.list": "inventory",
         "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
         "schema.history.internal.kafka.topic": "schema-changes.inventory"
       }
     }'
   ```

6. 커넥터 상태 확인
   ```bash
   curl http://localhost:8083/connectors/mysql-source-connector/status
   ```

7. Spring Boot 앱 실행
   ```bash
   ./gradlew bootRun
   ```
   - JDK 17을 명시해서 실행하려면 아래처럼 실행합니다.
   ```bash
   JAVA_HOME=$(/usr/libexec/java_home -v 17) ./gradlew bootRun
   ```

8. 변경 테스트
   - 소스 테이블인 `inventory.customers`에 데이터를 입력합니다.
   ```sql
   INSERT INTO inventory.customers (id, first_name, last_name, email)
   VALUES (1, 'Jane', 'Doe', 'jane@example.com');

   UPDATE inventory.customers
   SET email = 'jane.doe@example.com'
   WHERE id = 1;

   DELETE FROM inventory.customers
   WHERE id = 1;
   ```
   - 타깃 테이블인 `test_db.customers`에 반영됐는지 확인합니다.
   ```sql
   SELECT * FROM test_db.customers;
   ```

9. API와 대시보드 확인
   ```bash
   curl http://localhost:8080/snapshot/status
   curl http://localhost:8080/api/metrics
   curl http://localhost:8080/api/recovery/status
   ```
   - 브라우저에서 `http://localhost:8080/dashboard`를 열면 CDC 대시보드를 볼 수 있습니다.

10. 종료
   ```bash
   docker-compose down
   ```
   - MySQL 데이터를 포함해 볼륨까지 삭제하려면 아래 명령을 사용합니다.
   ```bash
   docker-compose down -v
   ```

## 빠른 실행 체크리스트
- `docker-compose ps`에서 `kafka`, `mysql`, `connect`가 실행 중인지 확인
- `curl http://localhost:8083/connectors`에서 커넥터가 등록됐는지 확인
- `curl http://localhost:8083/connectors/mysql-source-connector/status`에서 connector/task 상태가 `RUNNING`인지 확인
- Spring Boot 로그에서 `Received message:`가 출력되는지 확인
- `GET /api/metrics`의 `totalCount`가 증가하는지 확인
- `GET /api/recovery/status`의 `pendingFailures`가 0인지 확인

## 수동 테이블 생성 SQL
```sql
CREATE TABLE customers (
  id INT PRIMARY KEY,
  first_name VARCHAR(100),
  last_name VARCHAR(100),
  email VARCHAR(200)
);
```

## 애플리케이션 흐름
1. MySQL binlog → Debezium Connector
2. Kafka 토픽 `mysql-db-server.inventory.customers` 생성 및 이벤트 발행
3. Spring Boot의 `DebeziumKafkaConsumer`가 토픽을 구독
4. `MigrationService`가 `customers` 테이블에 upsert/delete 반영

## 스냅샷 진행 확인
- Debezium 스냅샷 이벤트(op=`r`)를 감지해 진행 상태를 집계합니다.
- 현재 상태 확인:
  - `GET /snapshot/status`
  - 예시 응답:
  ```json
  {
    "mode": "SNAPSHOT",
    "snapshotCount": 1234,
    "streamCount": 5678,
    "lastEventAt": "2026-02-07T10:15:30Z",
    "lastSnapshotState": "last"
  }
  ```

## CDC 대시보드
- 대시보드: `GET /dashboard`
- 지표 API: `GET /api/metrics`

## 실패 복구 절차
- Kafka 메시지 처리 중 JSON 파싱, 필수 필드 누락, DB 반영 오류가 발생하면 원본 CDC 메시지를 `cdc_failed_events` 테이블에 저장합니다.
- 실패 건은 애플리케이션이 처음 실패를 기록하거나 조회할 때 자동으로 생성되는 `cdc_failed_events` 테이블에 보관됩니다.
- Kafka offset은 record 단위로 처리되며, 실패 payload 저장까지 성공한 뒤 다음 메시지로 진행합니다.
- 실패 현황 확인:
  ```bash
  curl http://localhost:8080/api/recovery/status
  ```
- `storageAvailable=false`이면 MySQL 연결 또는 실패 저장소 접근이 불가능한 상태입니다.
- 최근 실패 목록 확인:
  ```bash
  curl "http://localhost:8080/api/recovery/failures?limit=20"
  ```
- 실패 원인을 수정한 뒤 단건 재처리:
  ```bash
  curl -X POST http://localhost:8080/api/recovery/failures/1/retry
  ```
- 재처리에 성공하면 해당 실패 건의 `resolved_at`이 기록되고 `pendingFailures`에서 제외됩니다.
- 재처리에 다시 실패하면 `retry_count`, `last_retry_at`, 마지막 에러 정보가 갱신됩니다.

## 대용량 처리 체크
- 대량 스냅샷 중에는 Kafka consumer lag를 함께 확인해야 합니다.
  ```bash
  docker exec -it kafka kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --describe \
    --group migration-consumer-group
  ```
- `CURRENT-OFFSET`과 `LOG-END-OFFSET` 차이가 계속 증가하면 Spring Boot 소비 속도가 Kafka 적재 속도를 따라가지 못하는 상태입니다.
- `GET /api/metrics`의 `lastLagMs`, `totalCount`, `errorCount`를 같이 확인합니다.
- `GET /api/recovery/status`의 `pendingFailures`가 증가하면 실패 원인 확인 후 `/api/recovery/failures/{id}/retry`로 재처리합니다.
- 대량 입력 중에는 전체 CDC payload 로그를 남기지 않고 실패 payload만 DB에 저장합니다.

## 주요 코드 위치
- Kafka 소비자: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/kotlin/com/example/realtimemysqlmigration/customer/DebeziumKafkaConsumer.kt`
- DB 반영 로직: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/kotlin/com/example/realtimemysqlmigration/service/MigrationService.kt`
- 실패 복구 API: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/kotlin/com/example/realtimemysqlmigration/controller/FailureRecoveryController.kt`
- 실패 이벤트 저장: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/kotlin/com/example/realtimemysqlmigration/service/FailedCdcEventService.kt`
- 설정: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/resources/application.yml`
- Docker: `/Users/kchan/IdeaProjects/realtime-mysql-migration/docker-compose.yml`

## 체크포인트
- Kafka 토픽 생성 여부: `mysql-db-server.inventory.customers`
- Connect 상태: `http://localhost:8083/connectors` 조회
- Spring Boot 로그에서 메시지 수신 확인

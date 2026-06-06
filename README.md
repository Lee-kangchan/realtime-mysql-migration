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

## 로컬 실행 순서
1. 인프라 기동
   - `docker-compose.yml` 기준으로 컨테이너를 올립니다.
   ```bash
   docker-compose up -d
   ```

2. MySQL 테이블 준비
   - 애플리케이션은 `customers` 테이블이 있다고 가정합니다.
   ```sql
   CREATE TABLE customers (
     id INT PRIMARY KEY,
     first_name VARCHAR(100),
     last_name VARCHAR(100),
     email VARCHAR(200)
   );
   ```

3. Debezium 커넥터 등록
   - `debezium.http` 파일을 참고하거나 아래 예시로 등록합니다.
   - `database.user` / `database.password`는 `docker-compose.yml`과 일치해야 합니다.
   - 예시 요청:
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
         "database.server.name": "mysql-db-server",
         "database.include.list": "inventory",
         "database.history.kafka.bootstrap.servers": "kafka:9092",
         "database.history.kafka.topic": "schema-changes.inventory"
       }
     }'
   ```

4. Spring Boot 앱 실행
   ```bash
   ./gradlew bootRun
   ```

5. 변경 테스트
   - MySQL에 `INSERT/UPDATE/DELETE` 수행
   - Kafka → Spring Boot → DB 반영 확인

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

## 주요 코드 위치
- Kafka 소비자: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/kotlin/com/example/realtimemysqlmigration/customer/DebeziumKafkaConsumer.kt`
- DB 반영 로직: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/kotlin/com/example/realtimemysqlmigration/service/MigrationService.kt`
- 설정: `/Users/kchan/IdeaProjects/realtime-mysql-migration/src/main/resources/application.yml`
- Docker: `/Users/kchan/IdeaProjects/realtime-mysql-migration/docker-compose.yml`

## 체크포인트
- Kafka 토픽 생성 여부: `mysql-db-server.inventory.customers`
- Connect 상태: `http://localhost:8083/connectors` 조회
- Spring Boot 로그에서 메시지 수신 확인

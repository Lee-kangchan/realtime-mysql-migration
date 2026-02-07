package com.example.realtimemysqlmigration.customer
import com.example.realtimemysqlmigration.service.MigrationService
import com.example.realtimemysqlmigration.service.SnapshotProgressService
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import org.slf4j.LoggerFactory

@Service
class DebeziumKafkaConsumer(
    private val migrationService: MigrationService,
    private val objectMapper: ObjectMapper,
    private val snapshotProgressService: SnapshotProgressService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["mysql-db-server.inventory.customers"], groupId = "migration-consumer-group")
    fun listen(message: String) {
        try {
            logger.info("Received message: $message")

            val jsonNode: JsonNode = objectMapper.readTree(message)
            val payload = jsonNode.get("payload")
            if (payload == null || payload.isNull) {
                logger.warn("Missing payload in message")
                return
            }

            val op = payload.get("op")?.asText()
            val source = payload.get("source")
            val snapshotState = source?.get("snapshot")?.asText()
            val sourceTsMs = source?.get("ts_ms")?.asLong()
            val db = source?.get("db")?.asText()
            val table = source?.get("table")?.asText()

            snapshotProgressService.recordEvent(
                op = op,
                snapshotState = snapshotState,
                sourceTsMs = sourceTsMs,
                db = db,
                table = table
            )

            when (op) {
                "c", "u" -> { // insert(c) or update(u)
                    val after = payload.get("after")
                    migrationService.upsertCustomer(after)
                }
                "d" -> { // delete(d)
                    val before = payload.get("before")
                    migrationService.deleteCustomer(before.get("id").asInt())
                }
                "r" -> { // snapshot read
                    val after = payload.get("after")
                    migrationService.upsertCustomer(after)
                }
                else -> logger.warn("Unsupported operation type: $op")
            }
        } catch (ex: Exception) {
            snapshotProgressService.recordError()
            logger.error("Failed to process message", ex)
        }
    }
}

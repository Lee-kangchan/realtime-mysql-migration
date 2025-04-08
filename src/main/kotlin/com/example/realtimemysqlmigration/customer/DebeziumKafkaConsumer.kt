package com.example.realtimemysqlmigration.customer
import com.example.realtimemysqlmigration.service.MigrationService
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service
import org.slf4j.LoggerFactory

@Service
class DebeziumKafkaConsumer(
    private val migrationService: MigrationService,
    private val objectMapper: ObjectMapper
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["mysql-db-server.inventory.customers"], groupId = "migration-consumer-group")
    fun listen(message: String) {
        logger.info("Received message: $message")

        val jsonNode: JsonNode = objectMapper.readTree(message)
        val payload = jsonNode.get("payload")
        val op = payload.get("op").asText()

        when (op) {
            "c", "u" -> { // insert(c) or update(u)
                val after = payload.get("after")
                migrationService.upsertCustomer(after)
            }
            "d" -> { // delete(d)
                val before = payload.get("before")
                migrationService.deleteCustomer(before.get("id").asInt())
            }
            else -> logger.warn("Unsupported operation type: $op")
        }
    }
}

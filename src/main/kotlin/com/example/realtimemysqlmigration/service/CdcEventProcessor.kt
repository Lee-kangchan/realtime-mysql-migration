package com.example.realtimemysqlmigration.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service

@Service
class CdcEventProcessor(
    private val migrationService: CustomerMigrationPort,
    private val objectMapper: ObjectMapper,
    private val snapshotProgressService: SnapshotProgressService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    fun process(message: String) {
        val root: JsonNode = objectMapper.readTree(message)
        val payload = root.get("payload")
        require(payload != null && !payload.isNull) { "Missing payload in CDC message" }

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
            "c", "u", "r" -> migrationService.upsertCustomer(requiredNode(payload, "after"))
            "d" -> migrationService.deleteCustomer(requiredNode(payload, "before").get("id").asInt())
            else -> logger.warn("Unsupported operation type: {}", op)
        }
    }

    private fun requiredNode(parent: JsonNode, fieldName: String): JsonNode {
        val node = parent.get(fieldName)
        require(node != null && !node.isNull) { "Missing '$fieldName' in CDC payload" }
        return node
    }
}

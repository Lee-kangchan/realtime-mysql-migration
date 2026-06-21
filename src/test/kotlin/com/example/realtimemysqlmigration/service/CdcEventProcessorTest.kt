package com.example.realtimemysqlmigration.service

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class CdcEventProcessorTest {
    private val objectMapper = jacksonObjectMapper()

    @Test
    fun `process upserts customer when create event arrives`() {
        // Given
        val migration = FakeCustomerMigrationPort()
        val processor = CdcEventProcessor(migration, objectMapper, SnapshotProgressService())
        val message = cdcMessage(
            op = "c",
            after = """{"id":1,"first_name":"Jane","last_name":"Doe","email":"jane@example.com"}"""
        )

        // When
        processor.process(message)

        // Then
        assertEquals(1, migration.upsertedIds.single())
    }

    @Test
    fun `process deletes customer when delete event arrives`() {
        // Given
        val migration = FakeCustomerMigrationPort()
        val processor = CdcEventProcessor(migration, objectMapper, SnapshotProgressService())
        val message = cdcMessage(
            op = "d",
            before = """{"id":7,"first_name":"Jane","last_name":"Doe","email":"jane@example.com"}"""
        )

        // When
        processor.process(message)

        // Then
        assertEquals(7, migration.deletedIds.single())
    }

    @Test
    fun `process fails when payload is missing`() {
        // Given
        val migration = FakeCustomerMigrationPort()
        val processor = CdcEventProcessor(migration, objectMapper, SnapshotProgressService())
        val message = "{}"

        // When / Then
        assertFailsWith<IllegalArgumentException> {
            processor.process(message)
        }
    }

    private fun cdcMessage(op: String, before: String = "null", after: String = "null"): String =
        """
        {
          "payload": {
            "before": $before,
            "after": $after,
            "source": {
              "db": "inventory",
              "table": "customers",
              "snapshot": "false",
              "ts_ms": 1710000000000
            },
            "op": "$op"
          }
        }
        """.trimIndent()
}

private class FakeCustomerMigrationPort : CustomerMigrationPort {
    val upsertedIds = mutableListOf<Int>()
    val deletedIds = mutableListOf<Int>()

    override fun upsertCustomer(data: JsonNode) {
        upsertedIds.add(data.get("id").asInt())
    }

    override fun deleteCustomer(id: Int) {
        deletedIds.add(id)
    }
}

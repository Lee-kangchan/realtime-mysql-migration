package com.example.realtimemysqlmigration.service
import com.fasterxml.jackson.databind.JsonNode
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Service

@Service
class MigrationService(private val jdbcTemplate: JdbcTemplate) {

    fun upsertCustomer(data: JsonNode) {
        val id = data.get("id").asInt()
        val firstName = data.get("first_name").asText()
        val lastName = data.get("last_name").asText()
        val email = data.get("email").asText()

        jdbcTemplate.update("""
            INSERT INTO customers (id, first_name, last_name, email)
            VALUES (?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
              first_name = VALUES(first_name),
              last_name = VALUES(last_name),
              email = VALUES(email)
        """.trimIndent(), id, firstName, lastName, email)
    }

    fun deleteCustomer(id: Int) {
        jdbcTemplate.update("DELETE FROM customers WHERE id = ?", id)
    }
}

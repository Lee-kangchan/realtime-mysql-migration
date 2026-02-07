package com.example.realtimemysqlmigration.service
import com.fasterxml.jackson.databind.JsonNode
import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.stereotype.Service

@Service
class MigrationService(private val dsl: DSLContext) {
    private val customers = DSL.table(DSL.name("customers"))
    private val idField = DSL.field(DSL.name("id"), Int::class.java)
    private val firstNameField = DSL.field(DSL.name("first_name"), String::class.java)
    private val lastNameField = DSL.field(DSL.name("last_name"), String::class.java)
    private val emailField = DSL.field(DSL.name("email"), String::class.java)

    fun upsertCustomer(data: JsonNode) {
        if (data == null || data.isNull) return
        val id = data.get("id").asInt()
        val firstName = data.get("first_name").asText()
        val lastName = data.get("last_name").asText()
        val email = data.get("email").asText()

        dsl.insertInto(customers)
            .columns(idField, firstNameField, lastNameField, emailField)
            .values(id, firstName, lastName, email)
            .onDuplicateKeyUpdate()
            .set(firstNameField, firstName)
            .set(lastNameField, lastName)
            .set(emailField, email)
            .execute()
    }

    fun deleteCustomer(id: Int) {
        dsl.deleteFrom(customers)
            .where(idField.eq(id))
            .execute()
    }
}

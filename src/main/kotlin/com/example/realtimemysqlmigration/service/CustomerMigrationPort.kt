package com.example.realtimemysqlmigration.service

import com.fasterxml.jackson.databind.JsonNode

interface CustomerMigrationPort {
    fun upsertCustomer(data: JsonNode)

    fun deleteCustomer(id: Int)
}

package com.example.realtimemysqlmigration.service

import org.jooq.DSLContext
import org.jooq.impl.DSL
import org.springframework.stereotype.Service
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicBoolean

@Service
class FailedCdcEventService(private val dsl: DSLContext) {
    private val initialized = AtomicBoolean(false)
    private val failures = DSL.table(DSL.name("cdc_failed_events"))
    private val idField = DSL.field(DSL.name("id"), Long::class.java)
    private val payloadField = DSL.field(DSL.name("payload"), String::class.java)
    private val errorClassField = DSL.field(DSL.name("error_class"), String::class.java)
    private val errorMessageField = DSL.field(DSL.name("error_message"), String::class.java)
    private val failedAtField = DSL.field(DSL.name("failed_at"), LocalDateTime::class.java)
    private val retryCountField = DSL.field(DSL.name("retry_count"), Int::class.java)
    private val lastRetryAtField = DSL.field(DSL.name("last_retry_at"), LocalDateTime::class.java)
    private val resolvedAtField = DSL.field(DSL.name("resolved_at"), LocalDateTime::class.java)

    fun recordFailure(input: FailedCdcEventInput): Long {
        ensureTable()
        val record = dsl.insertInto(failures)
            .columns(payloadField, errorClassField, errorMessageField)
            .values(input.payload, input.errorClass, input.errorMessage)
            .returningResult(idField)
            .fetchOne()

        return requireNotNull(record?.value1()) { "Failed to create CDC failure record" }
    }

    fun recordRetryFailure(id: Long, error: Throwable) {
        ensureTable()
        dsl.update(failures)
            .set(errorClassField, error.javaClass.name)
            .set(errorMessageField, error.message ?: error.javaClass.simpleName)
            .set(retryCountField, retryCountField.plus(1))
            .set(lastRetryAtField, DSL.currentLocalDateTime())
            .where(idField.eq(id))
            .execute()
    }

    fun markResolved(id: Long) {
        ensureTable()
        dsl.update(failures)
            .set(resolvedAtField, DSL.currentLocalDateTime())
            .set(lastRetryAtField, DSL.currentLocalDateTime())
            .set(retryCountField, retryCountField.plus(1))
            .where(idField.eq(id))
            .execute()
    }

    fun findPendingPayload(id: Long): String? {
        ensureTable()
        return dsl.select(payloadField)
            .from(failures)
            .where(idField.eq(id).and(resolvedAtField.isNull))
            .fetchOne(payloadField)
    }

    fun recentFailures(limit: Int): List<FailedCdcEvent> {
        ensureTable()
        return dsl.select(
            idField,
            payloadField,
            errorClassField,
            errorMessageField,
            failedAtField,
            retryCountField,
            lastRetryAtField,
            resolvedAtField
        )
            .from(failures)
            .orderBy(failedAtField.desc())
            .limit(limit.coerceIn(1, 100))
            .fetch { record ->
                FailedCdcEvent(
                    id = requireNotNull(record.get(idField)),
                    payload = requireNotNull(record.get(payloadField)),
                    errorClass = requireNotNull(record.get(errorClassField)),
                    errorMessage = requireNotNull(record.get(errorMessageField)),
                    failedAt = requireNotNull(record.get(failedAtField)).toString(),
                    retryCount = requireNotNull(record.get(retryCountField)),
                    lastRetryAt = record.get(lastRetryAtField)?.toString(),
                    resolvedAt = record.get(resolvedAtField)?.toString()
                )
            }
    }

    fun status(): FailureRecoveryStatus {
        ensureTable()
        val total = countWhere(null)
        val pending = countWhere(resolvedAtField.isNull)
        return FailureRecoveryStatus(
            totalFailures = total,
            pendingFailures = pending,
            resolvedFailures = total - pending,
            storageAvailable = true,
            storageError = null
        )
    }

    private fun countWhere(condition: org.jooq.Condition?): Long {
        val query = dsl.selectCount().from(failures)
        val record = if (condition == null) query.fetchOne() else query.where(condition).fetchOne()
        return requireNotNull(record?.value1()).toLong()
    }

    private fun ensureTable() {
        if (!initialized.compareAndSet(false, true)) return

        dsl.execute(
            """
            CREATE TABLE IF NOT EXISTS cdc_failed_events (
              id BIGINT AUTO_INCREMENT PRIMARY KEY,
              payload LONGTEXT NOT NULL,
              error_class VARCHAR(255) NOT NULL,
              error_message TEXT NOT NULL,
              failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
              retry_count INT NOT NULL DEFAULT 0,
              last_retry_at TIMESTAMP NULL,
              resolved_at TIMESTAMP NULL
            )
            """.trimIndent()
        )
    }
}

data class FailedCdcEventInput(
    val payload: String,
    val errorClass: String,
    val errorMessage: String
)

data class FailedCdcEvent(
    val id: Long,
    val payload: String,
    val errorClass: String,
    val errorMessage: String,
    val failedAt: String,
    val retryCount: Int,
    val lastRetryAt: String?,
    val resolvedAt: String?
)

data class FailureRecoveryStatus(
    val totalFailures: Long,
    val pendingFailures: Long,
    val resolvedFailures: Long,
    val storageAvailable: Boolean,
    val storageError: String?
)

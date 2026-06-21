package com.example.realtimemysqlmigration.controller

import com.example.realtimemysqlmigration.service.CdcEventProcessor
import com.example.realtimemysqlmigration.service.FailedCdcEvent
import com.example.realtimemysqlmigration.service.FailedCdcEventService
import com.example.realtimemysqlmigration.service.FailureRecoveryStatus
import org.springframework.dao.DataAccessException
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController

@RestController
class FailureRecoveryController(
    private val failedCdcEventService: FailedCdcEventService,
    private val cdcEventProcessor: CdcEventProcessor
) {

    @GetMapping("/api/recovery/status")
    fun status(): FailureRecoveryStatus = try {
        failedCdcEventService.status()
    } catch (ex: DataAccessException) {
        FailureRecoveryStatus(
            totalFailures = 0,
            pendingFailures = 0,
            resolvedFailures = 0,
            storageAvailable = false,
            storageError = ex.message
        )
    }

    @GetMapping("/api/recovery/failures")
    fun failures(@RequestParam(defaultValue = "50") limit: Int): List<FailedCdcEvent> =
        failedCdcEventService.recentFailures(limit)

    @PostMapping("/api/recovery/failures/{id}/retry")
    fun retry(@PathVariable id: Long): RecoveryRetryResult {
        val payload = failedCdcEventService.findPendingPayload(id)
            ?: throw FailureNotFoundException(id)

        return try {
            cdcEventProcessor.process(payload)
            failedCdcEventService.markResolved(id)
            RecoveryRetryResult(id = id, status = "RESOLVED", errorMessage = null)
        } catch (ex: Exception) {
            failedCdcEventService.recordRetryFailure(id, ex)
            RecoveryRetryResult(
                id = id,
                status = "FAILED",
                errorMessage = ex.message ?: ex.javaClass.simpleName
            )
        }
    }
}

data class RecoveryRetryResult(
    val id: Long,
    val status: String,
    val errorMessage: String?
)

@ResponseStatus(HttpStatus.NOT_FOUND)
class FailureNotFoundException(id: Long) : RuntimeException("Pending CDC failure not found: $id")

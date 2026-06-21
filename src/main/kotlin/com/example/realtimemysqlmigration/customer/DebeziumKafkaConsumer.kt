package com.example.realtimemysqlmigration.customer
import com.example.realtimemysqlmigration.service.CdcEventProcessor
import com.example.realtimemysqlmigration.service.FailedCdcEventInput
import com.example.realtimemysqlmigration.service.FailedCdcEventService
import com.example.realtimemysqlmigration.service.SnapshotProgressService
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class DebeziumKafkaConsumer(
    private val cdcEventProcessor: CdcEventProcessor,
    private val failedCdcEventService: FailedCdcEventService,
    private val snapshotProgressService: SnapshotProgressService
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @KafkaListener(topics = ["mysql-db-server.inventory.customers"], groupId = "migration-consumer-group")
    fun listen(message: String) {
        try {
            logger.debug("Received CDC message")
            cdcEventProcessor.process(message)
        } catch (ex: Exception) {
            snapshotProgressService.recordError()
            val failureId = failedCdcEventService.recordFailure(
                FailedCdcEventInput(
                    payload = message,
                    errorClass = ex.javaClass.name,
                    errorMessage = ex.message ?: ex.javaClass.simpleName
                )
            )
            logger.error("Failed to process CDC message. failureId={}", failureId, ex)
        }
    }
}

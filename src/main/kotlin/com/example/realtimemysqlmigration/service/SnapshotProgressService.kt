package com.example.realtimemysqlmigration.service

import org.springframework.stereotype.Service
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

@Service
class SnapshotProgressService {
    private val snapshotCount = AtomicLong(0)
    private val streamCount = AtomicLong(0)
    private val totalCount = AtomicLong(0)
    private val createCount = AtomicLong(0)
    private val updateCount = AtomicLong(0)
    private val deleteCount = AtomicLong(0)
    private val readCount = AtomicLong(0)
    private val unknownCount = AtomicLong(0)
    private val errorCount = AtomicLong(0)
    private val lastEventAt = AtomicReference<Instant?>(null)
    private val lastSnapshotState = AtomicReference<String?>(null)
    private val snapshotActive = AtomicReference<Boolean>(false)
    private val lastOp = AtomicReference<String?>(null)
    private val lastDb = AtomicReference<String?>(null)
    private val lastTable = AtomicReference<String?>(null)
    private val lastSourceTsMs = AtomicReference<Long?>(null)

    fun recordEvent(
        op: String?,
        snapshotState: String?,
        sourceTsMs: Long?,
        db: String?,
        table: String?
    ) {
        totalCount.incrementAndGet()
        lastOp.set(op)
        lastDb.set(db)
        lastTable.set(table)
        if (sourceTsMs != null) {
            lastSourceTsMs.set(sourceTsMs)
        }

        when (op) {
            "c" -> createCount.incrementAndGet()
            "u" -> updateCount.incrementAndGet()
            "d" -> deleteCount.incrementAndGet()
            "r" -> readCount.incrementAndGet()
            else -> unknownCount.incrementAndGet()
        }

        val isSnapshot = op == "r" ||
            snapshotState.equals("true", ignoreCase = true) ||
            snapshotState.equals("last", ignoreCase = true)

        if (isSnapshot) {
            snapshotActive.set(true)
            snapshotCount.incrementAndGet()
        } else {
            streamCount.incrementAndGet()
        }

        if (!snapshotState.isNullOrBlank()) {
            lastSnapshotState.set(snapshotState)
            if (snapshotState.equals("last", ignoreCase = true) ||
                snapshotState.equals("false", ignoreCase = true)
            ) {
                snapshotActive.set(false)
            }
        }

        lastEventAt.set(Instant.now())
    }

    fun recordError() {
        errorCount.incrementAndGet()
    }

    fun status(): SnapshotStatus {
        val active = snapshotActive.get()
        return SnapshotStatus(
            mode = if (active) "SNAPSHOT" else "STREAM",
            snapshotCount = snapshotCount.get(),
            streamCount = streamCount.get(),
            lastEventAt = lastEventAt.get()?.toString(),
            lastSnapshotState = lastSnapshotState.get()
        )
    }

    fun metrics(): CdcMetrics {
        val active = snapshotActive.get()
        val lastSource = lastSourceTsMs.get()
        val lagMs = if (lastSource != null) Instant.now().toEpochMilli() - lastSource else null
        return CdcMetrics(
            mode = if (active) "SNAPSHOT" else "STREAM",
            totalCount = totalCount.get(),
            snapshotCount = snapshotCount.get(),
            streamCount = streamCount.get(),
            createCount = createCount.get(),
            updateCount = updateCount.get(),
            deleteCount = deleteCount.get(),
            readCount = readCount.get(),
            unknownCount = unknownCount.get(),
            errorCount = errorCount.get(),
            lastEventAt = lastEventAt.get()?.toString(),
            lastSnapshotState = lastSnapshotState.get(),
            lastOp = lastOp.get(),
            lastDb = lastDb.get(),
            lastTable = lastTable.get(),
            lastSourceTsMs = lastSource,
            lastLagMs = lagMs
        )
    }
}

data class SnapshotStatus(
    val mode: String,
    val snapshotCount: Long,
    val streamCount: Long,
    val lastEventAt: String?,
    val lastSnapshotState: String?
)

data class CdcMetrics(
    val mode: String,
    val totalCount: Long,
    val snapshotCount: Long,
    val streamCount: Long,
    val createCount: Long,
    val updateCount: Long,
    val deleteCount: Long,
    val readCount: Long,
    val unknownCount: Long,
    val errorCount: Long,
    val lastEventAt: String?,
    val lastSnapshotState: String?,
    val lastOp: String?,
    val lastDb: String?,
    val lastTable: String?,
    val lastSourceTsMs: Long?,
    val lastLagMs: Long?
)

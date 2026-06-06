package com.example.realtimemysqlmigration.controller

import com.example.realtimemysqlmigration.service.CdcMetrics
import com.example.realtimemysqlmigration.service.SnapshotProgressService
import com.example.realtimemysqlmigration.service.SnapshotStatus
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RestController

@RestController
class SnapshotController(private val snapshotProgressService: SnapshotProgressService) {

    @GetMapping("/snapshot/status")
    fun status(): SnapshotStatus = snapshotProgressService.status()

    @GetMapping("/api/metrics")
    fun metrics(): CdcMetrics = snapshotProgressService.metrics()
}

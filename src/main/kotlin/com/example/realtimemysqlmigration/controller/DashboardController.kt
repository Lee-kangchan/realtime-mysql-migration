package com.example.realtimemysqlmigration.controller

import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping

@Controller
class DashboardController {

    @GetMapping("/dashboard")
    fun dashboard(): String = "forward:/dashboard.html"
}

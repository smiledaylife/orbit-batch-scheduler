package com.orbit.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Orbit 调度中心。
 *
 * <p>职责：任务 CRUD、Cron 触发、执行器注册/心跳、按路由策略 HTTP 派发到执行器。
 */
@SpringBootApplication
@EnableScheduling
public class OrbitAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrbitAdminApplication.class, args);
    }
}

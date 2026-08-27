package com.orbit.admin.config;

import com.orbit.admin.registry.ExecutorRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 定期清理心跳超时的执行器。
 */
@Component
public class AdminScheduleTasks {

    private final ExecutorRegistry registry;

    public AdminScheduleTasks(ExecutorRegistry registry) {
        this.registry = registry;
    }

    @Scheduled(fixedDelayString = "${orbit.admin.evict-interval-ms:30000}")
    public void evict() {
        registry.evictExpired();
    }
}

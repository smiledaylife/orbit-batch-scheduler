package com.orbit.admin.config;

import com.orbit.admin.registry.ExecutorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 调度中心内部后台定时任务组件。
 * 定期执行执行器注册表的清理维护，及时剔除心跳超时的离线节点，
 * 确保调度中心任务路由派发只流向健康的在线执行器。
 */
@Component
public class AdminScheduleTasks {

    private static final Logger log = LoggerFactory.getLogger(AdminScheduleTasks.class);

    private final ExecutorRegistry registry;

    /**
     * 构造方法，注入执行器内存注册表
     *
     * @param registry 执行器注册表
     */
    public AdminScheduleTasks(ExecutorRegistry registry) {
        this.registry = registry;
    }

    /**
     * 定期扫描并剔除失联超时的执行器节点。
     * 执行频率由配置项 {@code orbit.admin.evict-interval-ms} 指定，默认每 30 秒执行一次。
     */
    @Scheduled(fixedDelayString = "${orbit.admin.evict-interval-ms:30000}")
    public void evict() {
        try {
            int evicted = registry.evictExpired();
            if (evicted > 0) {
                log.debug("[orbit-admin] periodic eviction cleaned {} expired executor(s)", evicted);
            }
        } catch (Exception e) {
            log.error("[orbit-admin] failed to evict expired executors: {}", e.getMessage(), e);
        }
    }
}

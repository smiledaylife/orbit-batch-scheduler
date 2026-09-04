package com.orbit.admin.config;

import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 调度中心内部后台维护任务组件。
 * 核心职责：
 *
 *   - 剔除心跳超时的失联执行器，保证任务路由派发只流向健康的在线节点；
 *   - 清理超过保留期的调度日志，避免 {@code orbit_job_log} 无限增长；
 *   - 回收进程中断遗留的僵尸 RUNNING 日志，把它们收敛为 FAILED 终态。
 *
 * 两个任务共用 Spring 的任务调度线程池，池大小见 {@code spring.task.scheduling.pool.size}
 * （默认为 1，本项目在 application.yml 中调到 2，避免慢任务阻塞快任务）。
 */
@Component
public class AdminScheduleTasks {

    private static final Logger log = LoggerFactory.getLogger(AdminScheduleTasks.class);

    /**
     * 僵尸 RUNNING 日志的判定余量（毫秒）。
     * 在「最大派发预算」之外再多等 5 分钟，覆盖数据库写入、GC 停顿等抖动。
     */
    private static final long STALE_RUNNING_MARGIN_MS = 5 * 60 * 1000L;

    private static final long MILLIS_PER_DAY = 24 * 60 * 60 * 1000L;

    private final ExecutorRegistry registry;
    private final JobStore jobStore;
    private final AdminProperties properties;

    /**
     * 构造方法，注入后台任务所需协作组件
     *
     * @param registry   执行器注册表
     * @param jobStore   任务与日志存储
     * @param properties 调度中心配置
     */
    public AdminScheduleTasks(ExecutorRegistry registry, JobStore jobStore, AdminProperties properties) {
        this.registry = registry;
        this.jobStore = jobStore;
        this.properties = properties;
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

    /**
     * 定期维护调度日志：先回收僵尸 RUNNING 记录，再清理超过保留期的历史日志。
     * 执行频率由配置项 {@code orbit.admin.log-cleanup-interval-ms} 指定，默认每小时一次；
     * 首次执行延后一个周期，避免与启动时的任务装载争抢数据库连接。
     */
    @Scheduled(initialDelayString = "${orbit.admin.log-cleanup-interval-ms:3600000}",
            fixedDelayString = "${orbit.admin.log-cleanup-interval-ms:3600000}")
    public void cleanupLogs() {
        try {
            reclaimStaleRunningLogs();
        } catch (Exception e) {
            log.error("[orbit-admin] failed to reclaim stale RUNNING logs: {}", e.getMessage(), e);
        }
        try {
            purgeExpiredLogs();
        } catch (Exception e) {
            log.error("[orbit-admin] failed to purge expired job logs: {}", e.getMessage(), e);
        }
    }

    /**
     * 回收僵尸 RUNNING 日志。
     * 单次派发的最长耗时受 {@code orbit.admin.max-timeout-seconds} 约束
     * （failover 的多次尝试共享同一份预算，见 {@code JobService#dispatch}），
     * 因此超过「最大预算 + 余量」仍停在 RUNNING 的记录，必然是进程中断遗留的。
     */
    private void reclaimStaleRunningLogs() {
        int maxTimeoutSeconds = properties.getMaxTimeoutSeconds();
        if (maxTimeoutSeconds <= 0) {
            // 全局上限被关掉时单次派发耗时没有确定边界，跳过回收以免误伤仍在执行的任务
            log.debug("[orbit-admin] skip stale RUNNING reclaim: orbit.admin.max-timeout-seconds <= 0");
            return;
        }
        long boundMs = maxTimeoutSeconds * 1000L + STALE_RUNNING_MARGIN_MS;
        Date cutoff = new Date(System.currentTimeMillis() - boundMs);
        int reclaimed = jobStore.failStaleRunningLogs(cutoff,
                "reclaimed by cleanup task: still RUNNING after " + (boundMs / 1000)
                        + "s, admin most likely restarted or was killed mid-dispatch");
        if (reclaimed > 0) {
            log.warn("[orbit-admin] reclaimed {} stale RUNNING log(s) as FAILED (older than {}s)",
                    reclaimed, boundMs / 1000);
        }
    }

    /**
     * 按 {@code orbit.admin.log-retention-days} 删除过期调度日志。
     * 保留天数为 0 或负数时表示永久保留。
     */
    private void purgeExpiredLogs() {
        int retentionDays = properties.getLogRetentionDays();
        if (retentionDays <= 0) {
            log.debug("[orbit-admin] skip log purge: orbit.admin.log-retention-days <= 0");
            return;
        }
        Date cutoff = new Date(System.currentTimeMillis() - retentionDays * MILLIS_PER_DAY);
        int purged = jobStore.deleteLogsBefore(cutoff);
        if (purged > 0) {
            log.info("[orbit-admin] purged {} job log(s) older than {} day(s)", purged, retentionDays);
        }
    }
}

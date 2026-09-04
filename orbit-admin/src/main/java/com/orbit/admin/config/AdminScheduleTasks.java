package com.orbit.admin.config;

import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 调度中心内部后台定时任务组件，承担三类运维性清理：
 * <ol>
 *   <li><b>执行器注册表剔除</b>：周期剔除心跳超时的离线节点，保证路由只流向健康节点；</li>
 *   <li><b>僵尸 RUNNING 日志回收</b>：调度中心崩溃/重启会遗留无人收敛的 RUNNING 日志，
 *       周期性将其收敛为 FAILED 终态（阈值 = 单任务最大超时 + 5 分钟宽限，
 *       保证绝不误杀仍在正常执行中的记录）；</li>
 *   <li><b>日志保留期清理</b>：{@code orbit_job_log} 无限增长会拖垮分页查询与备份，
 *       周期性分批删除超过保留期（{@code orbit.admin.log-retention-days}，默认 30 天）的历史日志。</li>
 * </ol>
 * 所有任务均异常自愈：单轮失败只记日志，不影响下一轮。
 */
@Component
public class AdminScheduleTasks {

    private static final Logger log = LoggerFactory.getLogger(AdminScheduleTasks.class);

    /** 回收僵尸 RUNNING 日志前的额外宽限（毫秒）：在 maxTimeoutSeconds 之上再放宽 5 分钟 */
    private static final long REAP_EXTRA_GRACE_MS = 5 * 60 * 1000L;

    private final ExecutorRegistry registry;
    private final JobStore jobStore;
    private final AdminProperties properties;

    /**
     * 构造方法，注入执行器注册表、存储层与配置
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
     * 定期回收僵尸 RUNNING 日志（调度中心崩溃/重启遗留的无人收敛记录）。
     * 阈值 = {@code orbit.admin.max-timeout-seconds} + 5 分钟宽限：
     * 正常执行的记录其 RUNNING 时长不可能超过该值（派发 HTTP 读超时已被 max-timeout 封顶）。
     * 执行频率由 {@code orbit.admin.log-reap-interval-ms} 指定，默认每 60 秒一次。
     */
    @Scheduled(fixedDelayString = "${orbit.admin.log-reap-interval-ms:60000}")
    public void reapOrphanedRunningLogs() {
        try {
            long cutoffMs = properties.getMaxTimeoutSeconds() * 1000L + REAP_EXTRA_GRACE_MS;
            jobStore.reapOrphanedRunning(cutoffMs,
                    "orphaned running log: admin crashed or restarted mid-dispatch");
        } catch (Exception e) {
            log.error("[orbit-admin] failed to reap orphaned running logs: {}", e.getMessage(), e);
        }
    }

    /**
     * 定期清理超过保留期的历史执行日志（分批删除，避免大事务长锁）。
     * 保留天数由 {@code orbit.admin.log-retention-days} 控制（默认 30 天，0 = 关闭清理）。
     * 执行频率由 {@code orbit.admin.log-cleanup-interval-ms} 指定，默认每 1 小时一次。
     */
    @Scheduled(fixedDelayString = "${orbit.admin.log-cleanup-interval-ms:3600000}")
    public void cleanupExpiredLogs() {
        int retentionDays = properties.getLogRetentionDays();
        if (retentionDays <= 0) {
            return;
        }
        try {
            Date cutoff = new Date(System.currentTimeMillis() - retentionDays * 24L * 60L * 60L * 1000L);
            int deleted = jobStore.deleteLogsBefore(cutoff);
            if (deleted > 0) {
                log.info("[orbit-admin] log retention cleanup deleted {} log(s) older than {} days",
                        deleted, retentionDays);
            }
        } catch (Exception e) {
            log.error("[orbit-admin] failed to cleanup expired logs: {}", e.getMessage(), e);
        }
    }
}

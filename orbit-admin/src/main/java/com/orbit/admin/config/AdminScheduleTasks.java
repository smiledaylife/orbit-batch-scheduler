package com.orbit.admin.config;

import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.dispatch.OutstandingDispatches;
import com.orbit.admin.store.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 调度中心内部后台定时任务组件，承担三类运维性清理：
 *   1. 执行器注册表剔除：周期剔除心跳超时的离线节点，保证路由只流向健康节点；
 *   2. 僵尸 RUNNING 日志回收：调度中心崩溃/重启会遗留无人收敛的 RUNNING 日志，
 *       周期性将其收敛为 FAILED 终态（阈值 = 单任务最大超时 + 5 分钟宽限，
 *       保证绝不误杀仍在正常执行中的记录）；
 *   3. 日志保留期清理：{@code orbit_job_log} 无限增长会拖垮分页查询与备份，
 *       周期性分批删除超过保留期（{@code orbit.admin.log-retention-days}，默认 30 天）的历史日志。
 * 所有任务均异常自愈：单轮失败只记日志，不影响下一轮。
 */
@Component
public class AdminScheduleTasks {

    private static final Logger log = LoggerFactory.getLogger(AdminScheduleTasks.class);

    /** 回收僵尸 RUNNING 日志前的额外宽限（毫秒）：在阈值之上再放宽 5 分钟 */
    private static final long REAP_EXTRA_GRACE_MS = 5 * 60 * 1000L;

    private final ExecutorRegistry registry;
    private final JobStore jobStore;
    private final AdminProperties properties;
    private final OutstandingDispatches outstanding;

    /**
     * 构造方法，注入执行器注册表、存储层与配置
     *
     * @param registry    执行器注册表
     * @param jobStore    任务与日志存储
     * @param properties  调度中心配置
     * @param outstanding 在途执行登记簿
     */
    public AdminScheduleTasks(ExecutorRegistry registry, JobStore jobStore, AdminProperties properties,
                              OutstandingDispatches outstanding) {
        this.registry = registry;
        this.jobStore = jobStore;
        this.properties = properties;
        this.outstanding = outstanding;
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
            // 硬上界：任务 timeoutSeconds 在保存时已被 max-timeout-seconds 封顶，
            // 因此超过这个时长的一定是异常，无论执行器是否在线都收敛掉，
            // 保证不会有永久 RUNNING 的日志和永久被占用的串行守卫。
            long hardCapMs = properties.getMaxTimeoutSeconds() * 1000L + REAP_EXTRA_GRACE_MS;
            // 存活判定的宽限：心跳超时之上再放宽 5 分钟，
            // 避免执行器短暂网络抖动就被判死、把仍在正常运行的任务记成失败。
            long offlineMs = properties.getHeartbeatTimeoutSeconds() * 1000L + REAP_EXTRA_GRACE_MS;

            java.util.Set<String> live = new java.util.HashSet<String>();
            for (com.orbit.core.model.ExecutorNode node : registry.listAll()) {
                if (node.getAddress() != null) {
                    live.add(node.getAddress());
                }
            }

            java.util.List<String> reaped = jobStore.reapOrphanedRunning(hardCapMs, offlineMs, live,
                    "orphaned running log: execution exceeded max-timeout and no callback arrived",
                    "orphaned running log: executor went offline before calling back");
            // 日志已收敛到终态，必须同步释放串行守卫，
            // 否则这些任务会被登记簿一直判定为「上一轮在跑」而永久不再触发。
            for (String logId : reaped) {
                outstanding.release(logId);
            }
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

package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 在途执行登记簿。
 *
 * 单机模式保留 JVM 内存守卫；生产 cluster 模式使用 Redis Lease 作为集群级事实，
 * 避免多个 Admin 副本同时认为同一个 Job 可以执行。Lease 使用 Lua 保证占用、释放、
 * 续租的原子性，并以 logId 做所有权校验，避免旧回调释放新一轮执行的锁。
 */
@Component
public class OutstandingDispatches {

    private static final String KEY_PREFIX = "orbit:execution:lease:";
    private static final String LOG_PREFIX = "orbit:execution:lease-log:";

    private static final DefaultRedisScript<Long> ACQUIRE_SCRIPT = new DefaultRedisScript<Long>(
            "if redis.call('exists', KEYS[1]) == 0 then "
                    + "redis.call('psetex', KEYS[1], ARGV[1], ARGV[2]); "
                    + "redis.call('psetex', KEYS[2], ARGV[1], ARGV[3]); "
                    + "return 1; end; return 0;", Long.class);

    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<Long>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "redis.call('del', KEYS[1]); "
                    + "redis.call('del', KEYS[2]); "
                    + "return 1; end; return 0;", Long.class);

    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<Long>(
            "if redis.call('get', KEYS[1]) == ARGV[1] and redis.call('get', KEYS[2]) == ARGV[1] then "
                    + "redis.call('pexpire', KEYS[1], ARGV[2]); "
                    + "redis.call('pexpire', KEYS[2], ARGV[2]); "
                    + "return 1; end; return 0;", Long.class);

    private final ConcurrentHashMap<String, String> logIdByJob = new ConcurrentHashMap<String, String>();
    private final ConcurrentHashMap<String, String> jobByLogId = new ConcurrentHashMap<String, String>();
    private final AtomicLong skippedCount = new AtomicLong();
    private final AtomicLong leaseAcquireFailures = new AtomicLong();
    private final AtomicLong leaseRenewFailures = new AtomicLong();

    private final StringRedisTemplate redis;
    private final AdminProperties properties;

    public OutstandingDispatches(StringRedisTemplate redis, AdminProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /**
     * 尝试占用任务槽位。cluster 模式 Redis 成功才允许派发；Redis 不可用时快速失败，
     * 不降级为本地锁，否则 Redis 故障期间多个 Admin 副本可能发生重复执行。
     */
    public boolean tryAcquire(String jobName, String logId) {
        if (jobName == null) {
            return true;
        }
        if (logId == null || logId.trim().isEmpty()) {
            return false;
        }

        if (properties.isExecutionLeaseEnabled()) {
            try {
                Long result = redis.execute(ACQUIRE_SCRIPT,
                        java.util.Arrays.asList(jobKey(jobName), logKey(logId)),
                        String.valueOf(properties.getExecutionLeaseTtlMs()), logId, jobName);
                if (!Long.valueOf(1L).equals(result)) {
                    skippedCount.incrementAndGet();
                    return false;
                }
            } catch (RuntimeException e) {
                leaseAcquireFailures.incrementAndGet();
                throw new IllegalStateException("redis execution lease unavailable; dispatch refused", e);
            }
        } else if (logIdByJob.putIfAbsent(jobName, logId) != null) {
            skippedCount.incrementAndGet();
            return false;
        }

        logIdByJob.put(jobName, logId);
        jobByLogId.put(logId, jobName);
        return true;
    }

    /**
     * 按 logId 释放。Redis 模式不依赖当前 Admin JVM 是否持有本地映射，
     * 因此 callback 可以由任意 Admin 副本处理。
     */
    public boolean release(String logId) {
        if (logId == null || logId.trim().isEmpty()) {
            return false;
        }
        if (properties.isExecutionLeaseEnabled()) {
            try {
                String jobName = redis.opsForValue().get(logKey(logId));
                if (jobName == null) {
                    jobName = jobByLogId.get(logId);
                }
                if (jobName == null) {
                    return false;
                }
                Long result = redis.execute(RELEASE_SCRIPT,
                        java.util.Arrays.asList(jobKey(jobName), logKey(logId)), logId);
                removeLocal(jobName, logId);
                return Long.valueOf(1L).equals(result);
            } catch (RuntimeException e) {
                // 不能因为 Redis 瞬时异常阻断 callback 的 DB 状态收敛；Lease 自身会自然过期。
                logIdByJob.remove(jobByLogId.get(logId), logId);
                jobByLogId.remove(logId);
                return false;
            }
        }
        String jobName = jobByLogId.remove(logId);
        if (jobName == null) {
            return false;
        }
        return logIdByJob.remove(jobName, logId);
    }

    /**
     * 同步失败时按任务名释放本地/Redis Lease。
     */
    public void releaseByJob(String jobName) {
        if (jobName == null) {
            return;
        }
        String logId = logIdByJob.get(jobName);
        if (logId != null) {
            release(logId);
        }
    }

    /**
     * 周期续租。只续租本 JVM 成功取得的 Lease；回调释放后本地映射也会消失。
     */
    @Scheduled(fixedDelayString = "${orbit.admin.execution-lease-renew-interval-ms:30000}")
    public void renewLeases() {
        if (!properties.isExecutionLeaseEnabled() || logIdByJob.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : logIdByJob.entrySet()) {
            String jobName = entry.getKey();
            String logId = entry.getValue();
            try {
                Long renewed = redis.execute(RENEW_SCRIPT,
                        java.util.Arrays.asList(jobKey(jobName), logKey(logId)),
                        logId, String.valueOf(properties.getExecutionLeaseTtlMs()));
                if (!Long.valueOf(1L).equals(renewed)) {
                    leaseRenewFailures.incrementAndGet();
                    log.warn("[orbit-admin] execution lease renewal lost: job={}, logId={}", jobName, logId);
                    removeLocal(jobName, logId);
                }
            } catch (RuntimeException e) {
                leaseRenewFailures.incrementAndGet();
                // 不主动删除本地映射：Redis 短暂抖动恢复后下一轮仍有机会续租；
                // TTL 到期则下一次派发自然可以接管。
                log.warn("[orbit-admin] execution lease renewal failed: job={}, logId={}", jobName, logId);
            }
        }
    }

    public int outstanding() {
        return logIdByJob.size();
    }

    public long skipped() {
        return skippedCount.get();
    }

    public long leaseAcquireFailures() {
        return leaseAcquireFailures.get();
    }

    public long leaseRenewFailures() {
        return leaseRenewFailures.get();
    }

    private String jobKey(String jobName) {
        return KEY_PREFIX + jobName;
    }

    private String logKey(String logId) {
        return LOG_PREFIX + logId;
    }

    private void removeLocal(String jobName, String logId) {
        if (jobName != null) {
            logIdByJob.remove(jobName, logId);
        }
        jobByLogId.remove(logId, jobName);
    }
}

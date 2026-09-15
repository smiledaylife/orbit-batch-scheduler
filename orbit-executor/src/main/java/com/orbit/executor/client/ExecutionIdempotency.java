package com.orbit.executor.client;

import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 执行幂等保护（logId 去重）。
 *
 * 调度中心触发请求可能出现「Executor 已受理，但 HTTP 响应超时/连接被重置」的情况。
 * 调度中心随后带着同一个 logId 重试（或故障转移到其他节点）时，本组件确保同一个 logId
 * 在 TTL 内最多进入一次执行线程池。
 *
 * 两级实现（同一份公开契约，按部署形态自动选择）：
 *
 *   - 本类为 JVM 本地兜底实现：零外部依赖，任何部署形态下都默认生效，
 *       为「HTTP 超时后调度中心重试同节点」提供基线防重；代价是只保护本进程，
 *       多副本执行器下跨节点去重需要 Redis 模式；
 *   - {@link RedisExecutionIdempotency} 为跨副本 Redis 实现：幂等 key 以 Redis TTL 为准，
 *       classpath 存在 Redis 且容器中有 StringRedisTemplate 时由自动装配启用。
 *
 * 本地实现的 TTL 与容量边界：
 *
 *   - TTL 取 {@code min(配置 TTL, 300 秒)}：本地去重面向的是调度中心秒级~分钟级的
 *       重试窗口，不需要也不适合按天保留（无界增长反而拖累执行器内存）；
 *       需要完整 TTL 的跨副本语义请使用 Redis 模式；
 *   - 容量上限 {@value #LOCAL_MAX_ENTRIES} 条：达到后先清扫过期条目，
 *       仍超限则移除一半条目并告警（极端吞吐下保护降级为尽力而为）。
 */
public class ExecutionIdempotency {

    private static final Logger log = LoggerFactory.getLogger(ExecutionIdempotency.class);

    /** 本地实现的单条幂等记录最大保留时长（秒）；配置 TTL 超过该值时按该值截断 */
    static final long LOCAL_MAX_TTL_SECONDS = 300L;

    /** 触发过期清扫的容量阈值 */
    private static final int LOCAL_SWEEP_THRESHOLD = 4096;

    /** 本地幂等记录容量上限 */
    private static final int LOCAL_MAX_ENTRIES = 65536;

    /** 本地幂等记录：logId -> 过期时刻（epoch 毫秒） */
    private final ConcurrentHashMap<String, Long> localReservations = new ConcurrentHashMap<String, Long>();

    /** 容量超限告警只打一次，避免日志风暴 */
    private final AtomicBoolean capacityWarned = new AtomicBoolean(false);

    /** 执行器配置，提供幂等开关与 TTL */
    protected final ExecutorProperties properties;

    /**
     * @param properties 执行器配置，提供幂等开关与 TTL
     */
    public ExecutionIdempotency(ExecutorProperties properties) {
        this.properties = properties;
    }

    /**
     * 为 logId 预占执行资格。
     *
     * @param logId 一次调度执行的全局唯一日志 ID；空值直接拒绝
     * @return true=本次请求首次受理；false=同 logId 已被受理（重复触发，应返回已受理回执）
     */
    public boolean tryReserve(String logId) {
        if (!properties.isExecutionIdempotencyEnabled()) {
            return true;
        }
        if (logId == null || logId.trim().isEmpty()) {
            return false;
        }
        return reserveLocally(logId);
    }

    /**
     * 释放幂等占位（提交执行失败/饱和时调用，允许调度中心稍后重试）。
     *
     * @param logId 待释放的日志 ID，空值安全空操作
     */
    public void release(String logId) {
        if (!properties.isExecutionIdempotencyEnabled() || logId == null || logId.trim().isEmpty()) {
            return;
        }
        releaseLocally(logId);
    }

    /**
     * JVM 本地预占：putIfAbsent + 过期替换。
     * 语义与 Redis SET NX + TTL 对齐：同 logId 在 TTL 内只允许预占一次。
     */
    protected boolean reserveLocally(String logId) {
        long now = System.currentTimeMillis();
        long expiresAt = now + effectiveTtlMs();
        Long prev = localReservations.putIfAbsent(logId, expiresAt);
        if (prev == null) {
            maybeSweep(now);
            return true;
        }
        if (prev > now) {
            return false;
        }
        // 记录已过期：CAS 替换为新窗口；被其他并发请求抢先替换则视为重复
        return localReservations.replace(logId, prev, expiresAt);
    }

    /** JVM 本地释放：直接移除；键不存在（已被 TTL 逻辑清理）时静默 */
    protected void releaseLocally(String logId) {
        localReservations.remove(logId);
    }

    /** 本地模式生效 TTL（毫秒）：配置值超过本地上限时截断，见类注释 */
    protected long effectiveTtlMs() {
        long configured = properties.getExecutionIdempotencyTtlSeconds();
        long seconds = configured > LOCAL_MAX_TTL_SECONDS ? LOCAL_MAX_TTL_SECONDS : Math.max(1L, configured);
        return seconds * 1000L;
    }

    /**
     * 容量守护：达到清扫阈值时移除全部过期条目；
     * 清扫后仍达到硬上限（极端吞吐场景）则移除一半条目并告警一次。
     */
    private void maybeSweep(long now) {
        if (localReservations.size() < LOCAL_SWEEP_THRESHOLD) {
            return;
        }
        for (Map.Entry<String, Long> entry : localReservations.entrySet()) {
            if (entry.getValue() <= now) {
                localReservations.remove(entry.getKey(), entry.getValue());
            }
        }
        if (localReservations.size() < LOCAL_MAX_ENTRIES) {
            return;
        }
        int removed = 0;
        int target = localReservations.size() / 2;
        Iterator<Map.Entry<String, Long>> it = localReservations.entrySet().iterator();
        while (it.hasNext() && removed < target) {
            it.next();
            it.remove();
            removed++;
        }
        if (capacityWarned.compareAndSet(false, true)) {
            log.warn("[orbit-executor] local execution idempotency exceeded {} entries; "
                    + "evicted {} record(s). Enable Redis idempotency for high-throughput deployments",
                    LOCAL_MAX_ENTRIES, removed);
        }
    }

    /** 当前本地在册幂等记录数（诊断用） */
    public int localSize() {
        return localReservations.size();
    }
}

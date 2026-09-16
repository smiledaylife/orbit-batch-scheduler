package com.orbit.executor.client;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
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
 * 本地实现基于 Caffeine 单节点缓存，语义与 Redis SET NX + TTL 对齐：
 *
 *   - TTL 由 {@code expireAfterWrite} 统一治理，取 {@code min(配置 TTL, 300 秒)}：
 *       本地去重面向的是调度中心秒级~分钟级的重试窗口，不需要也不适合按天保留
 *       （内存驻留反而拖累执行器）；需要完整 TTL 的跨副本语义请使用 Redis 模式；
 *   - 容量上限 {@value #LOCAL_MAX_ENTRIES} 条，由 {@code maximumSize} 按
 *       Window TinyLFU 策略自动淘汰：极端吞吐下进场的冷 key 优先被挤出，
 *       淘汰即视为「保护降级为尽力而为」，发生首次容量淘汰时告警一次，
 *       提示高吞吐部署开启 Redis 幂等。
 */
public class ExecutionIdempotency {

    private static final Logger log = LoggerFactory.getLogger(ExecutionIdempotency.class);

    /** 本地实现的单条幂等记录最大保留时长（秒）；配置 TTL 超过该值时按该值截断 */
    static final long LOCAL_MAX_TTL_SECONDS = 300L;

    /** 本地幂等记录容量上限 */
    private static final int LOCAL_MAX_ENTRIES = 65536;

    /** 占位值：本缓存只关心 key 是否在 TTL 内存在 */
    private static final Boolean PRESENT = Boolean.TRUE;

    /**
     * 本地幂等记录缓存：logId -> 占位值。
     * TTL（写入后过期）与容量淘汰（maximumSize / Window TinyLFU）由 Caffeine 统一治理，
     * 无需手工记录过期时刻与周期性清扫。
     */
    private final Cache<String, Boolean> localReservations;

    /** 容量淘汰告警只打一次，避免日志风暴 */
    private final AtomicBoolean capacityWarned = new AtomicBoolean(false);

    /** 执行器配置，提供幂等开关与 TTL */
    protected final ExecutorProperties properties;

    /**
     * @param properties 执行器配置，提供幂等开关与 TTL
     */
    public ExecutionIdempotency(ExecutorProperties properties) {
        this.properties = properties;
        this.localReservations = buildLocalCache(effectiveTtlMs(), this::warnOnceOnEviction);
    }

    /**
     * 构建本地幂等缓存：maximumSize 提供容量淘汰（Window TinyLFU），
     * expireAfterWrite 提供 TTL；监听器的实参类型显式声明为 {@code RemovalListener<String, Boolean>}，
     * 与缓存键值类型一致，避免依赖 builder 泛型收窄的类型推断。
     */
    private static Cache<String, Boolean> buildLocalCache(long ttlMs,
                                                          RemovalListener<String, Boolean> listener) {
        return Caffeine.newBuilder()
                .maximumSize(LOCAL_MAX_ENTRIES)
                .expireAfterWrite(ttlMs, TimeUnit.MILLISECONDS)
                .removalListener(listener)
                .build();
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
     * JVM 本地预占：putIfAbsent 语义。
     * Caffeine 对已过期的条目按「不存在」处理，因此同一 map 视图天然等价于
     * 「putIfAbsent + 过期后可被重新预占」；语义与 Redis SET NX + TTL 对齐。
     */
    protected boolean reserveLocally(String logId) {
        return localReservations.asMap().putIfAbsent(logId, PRESENT) == null;
    }

    /** JVM 本地释放：直接失效；键不存在（已被 TTL 或容量淘汰回收）时静默 */
    protected void releaseLocally(String logId) {
        localReservations.asMap().remove(logId);
    }

    /** 本地模式生效 TTL（毫秒）：配置值超过本地上限时截断，见类注释 */
    protected long effectiveTtlMs() {
        long configured = properties.getExecutionIdempotencyTtlSeconds();
        long seconds = configured > LOCAL_MAX_TTL_SECONDS ? LOCAL_MAX_TTL_SECONDS : Math.max(1L, configured);
        return seconds * 1000L;
    }

    /** 容量淘汰时告警一次（RemovalCause.SIZE = maximumSize 溢出；TTL 过期与显式释放不告警） */
    private void warnOnceOnEviction(String logId, Boolean value, RemovalCause cause) {
        if (cause == RemovalCause.SIZE && capacityWarned.compareAndSet(false, true)) {
            log.warn("[orbit-executor] local execution idempotency exceeded {} entries and started evicting; "
                    + "enable Redis idempotency for high-throughput deployments", LOCAL_MAX_ENTRIES);
        }
    }

    /** 当前本地在册幂等记录数（诊断用，Caffeine 估算值） */
    public int localSize() {
        return (int) localReservations.estimatedSize();
    }
}

package com.orbit.executor.client;

import com.orbit.executor.config.ExecutorProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * 基于 Redis 的跨副本执行幂等实现。
 *
 * 幂等 key 以 logId 为粒度（SET NX + TTL），TTL 内同一 logId 在全部执行器副本上
 * 最多进入一次执行线程池；Redis 不可用时在开启该能力的生产配置下直接拒绝请求，
 * 不降级为 JVM 锁 —— JVM 锁只保护本进程，多副本执行器下形同虚设。
 *
 * 仅当 classpath 存在 Redis 且容器中注册了 StringRedisTemplate 时由自动装配创建；
 * 反之 {@link ExecutionIdempotency} 的 JVM 本地实现兜底。开关与 TTL 见
 * {@code orbit.executor.execution-idempotency-*}。
 */
public class RedisExecutionIdempotency extends ExecutionIdempotency {

    /** 幂等键前缀，完整键形如 {@code orbit:executor:execution:{logId}} */
    private static final String KEY_PREFIX = "orbit:executor:execution:";

    /**
     * 条件释放脚本：仅当键的值仍等于本 logId 时才删除。
     * 防御两种极端情况：旧请求误删新一轮的同名键、键已被 TTL 回收后误删他人键。
     * 使用实例字段而非 static：本类只在 Redis 就绪时被加载，脚本无需提前初始化。
     */
    private final DefaultRedisScript<Long> releaseScript = new DefaultRedisScript<Long>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]); end; return 0;",
            Long.class);

    /** Redis 客户端 */
    private final StringRedisTemplate redis;

    /**
     * @param redis      Redis 客户端
     * @param properties 执行器配置，提供幂等开关与 TTL
     */
    public RedisExecutionIdempotency(StringRedisTemplate redis, ExecutorProperties properties) {
        super(properties);
        this.redis = redis;
    }

    /**
     * Redis 预占：SET NX + TTL。
     *
     * @return true=本次请求首次受理；false=同 logId 已被任意副本受理（重复触发）
     * @throws IllegalStateException 开启幂等且 Redis 不可用时抛出，请求被拒绝而不是降级放行
     */
    @Override
    public boolean tryReserve(String logId) {
        if (!properties.isExecutionIdempotencyEnabled()) {
            return true;
        }
        if (logId == null || logId.trim().isEmpty()) {
            return false;
        }
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(
                    key(logId), logId, properties.getExecutionIdempotencyTtlSeconds(),
                    TimeUnit.SECONDS);
            return Boolean.TRUE.equals(acquired);
        } catch (RuntimeException e) {
            throw new IllegalStateException("redis execution idempotency unavailable; request refused", e);
        }
    }

    /**
     * Redis 释放（提交执行失败/饱和时调用，允许调度中心稍后重试）。
     * 只有 key 当前仍属于该 logId 时才删除，防止旧请求清理掉新一轮 key。
     */
    @Override
    public void release(String logId) {
        if (!properties.isExecutionIdempotencyEnabled() || logId == null || logId.trim().isEmpty()) {
            return;
        }
        try {
            redis.execute(releaseScript, Collections.singletonList(key(logId)), logId);
        } catch (RuntimeException ignored) {
            // TTL 会自然回收；不能因为 Redis 瞬时异常覆盖执行器原始结果。
        }
    }

    /** 拼装完整幂等键 */
    private String key(String logId) {
        return KEY_PREFIX + logId;
    }
}

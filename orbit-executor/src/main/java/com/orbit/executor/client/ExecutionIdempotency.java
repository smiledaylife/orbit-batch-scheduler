package com.orbit.executor.client;

import com.orbit.executor.config.ExecutorProperties;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * 基于 Redis 的执行幂等保护。
 *
 * 调度中心触发请求可能出现「Executor 已受理，但 HTTP 响应超时」的情况。
 * 调度中心随后重试同一个 logId 时，本组件确保同一个 logId 在 TTL 内最多进入一次执行线程池。
 * Redis 不可用时在开启该能力的生产配置下直接拒绝请求，不降级为 JVM 锁。
 */
@Component
public class ExecutionIdempotency {

    private static final String KEY_PREFIX = "orbit:executor:execution:";
    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<Long>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]); end; return 0;",
            Long.class);

    private final StringRedisTemplate redis;
    private final ExecutorProperties properties;

    public ExecutionIdempotency(StringRedisTemplate redis, ExecutorProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /**
     * 为 logId 预占执行资格。
     *
     * @return true=本次请求首次受理；false=同 logId 已被受理
     */
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
                    java.util.concurrent.TimeUnit.SECONDS);
            if (Boolean.TRUE.equals(acquired)) {
                return true;
            }
            return false;
        } catch (RuntimeException e) {
            throw new IllegalStateException("redis execution idempotency unavailable; request refused", e);
        }
    }

    /**
     * 只有 key 当前仍属于该 logId 时才删除，防止旧请求清理掉新一轮 key。
     */
    public void release(String logId) {
        if (!properties.isExecutionIdempotencyEnabled() || logId == null || logId.trim().isEmpty()) {
            return;
        }
        try {
            redis.execute(RELEASE_SCRIPT, Collections.singletonList(key(logId)), logId);
        } catch (RuntimeException ignored) {
            // TTL 会自然回收；不能因为 Redis 瞬时异常覆盖执行器原始结果。
        }
    }

    private String key(String logId) {
        return KEY_PREFIX + logId;
    }
}

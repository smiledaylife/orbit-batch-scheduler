package com.orbit.executor.client;

import com.orbit.executor.config.ExecutorProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ExecutionIdempotency} JVM 本地兜底实现的语义测试。
 *
 * 该实现是「Redis 不在 classpath / 容器无模板」时的降级路径，也是自动装配的默认兜底，
 * 必须与 Redis 实现保持同一份公开契约：
 *   - 开关关闭时全部放行；
 *   - 同一 logId 在 TTL 内只允许预占一次，释放后可再次预占；
 *   - TTL 过期后允许新一轮预占。
 */
class ExecutionIdempotencyTest {

    private static ExecutorProperties props(boolean enabled) {
        ExecutorProperties properties = new ExecutorProperties();
        properties.setExecutionIdempotencyEnabled(enabled);
        return properties;
    }

    @Test
    void disabledFlagAlwaysAllows() {
        ExecutionIdempotency idempotency = new ExecutionIdempotency(props(false));
        assertTrue(idempotency.tryReserve("log-1"));
        assertTrue(idempotency.tryReserve("log-1"));
        // 释放是安全空操作
        idempotency.release("log-1");
        assertEquals(0, idempotency.localSize());
    }

    @Test
    void blankLogIdIsRejectedWhenEnabled() {
        ExecutionIdempotency idempotency = new ExecutionIdempotency(props(true));
        assertFalse(idempotency.tryReserve(null));
        assertFalse(idempotency.tryReserve("   "));
        // 空值释放不抛异常
        idempotency.release(null);
    }

    @Test
    void sameLogIdReservesOnlyOnceWithinTtl() {
        ExecutionIdempotency idempotency = new ExecutionIdempotency(props(true));
        assertTrue(idempotency.tryReserve("log-a"));
        assertFalse(idempotency.tryReserve("log-a"));
        // 不同 logId 互不影响
        assertTrue(idempotency.tryReserve("log-b"));
        assertEquals(2, idempotency.localSize());
    }

    @Test
    void releaseAllowsSubsequentReserve() {
        ExecutionIdempotency idempotency = new ExecutionIdempotency(props(true));
        assertTrue(idempotency.tryReserve("log-c"));
        assertFalse(idempotency.tryReserve("log-c"));
        idempotency.release("log-c");
        assertTrue(idempotency.tryReserve("log-c"));
    }

    @Test
    void expiredReservationCanBeReacquired() throws InterruptedException {
        ExecutorProperties properties = props(true);
        // 本地实现的 TTL 上限为 300 秒，配置 1 秒即按 1 秒生效
        properties.setExecutionIdempotencyTtlSeconds(1L);
        ExecutionIdempotency idempotency = new ExecutionIdempotency(properties);

        assertTrue(idempotency.tryReserve("log-ttl"));
        assertFalse(idempotency.tryReserve("log-ttl"));
        Thread.sleep(1100L);
        assertTrue(idempotency.tryReserve("log-ttl"));
    }

    @Test
    void redisSubclassSharesContractSemantics() {
        // Redis 子类继承自基类：开关/空值检查走父类逻辑（不触 Redis，可在无 Redis 环境验证）
        ExecutorProperties properties = props(false);
        RedisExecutionIdempotency idempotency = new RedisExecutionIdempotency(null, properties);
        assertTrue(idempotency.tryReserve("log-x"));
        idempotency.release("log-x");
    }
}

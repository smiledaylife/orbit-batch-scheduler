package com.orbit.executor.client;

import com.orbit.core.model.TriggerResult;
import com.orbit.executor.config.ExecutorProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link DurableCallbackClient} 单元测试：写入 Stream 之后的近似裁剪。
 *
 * 关键回归点：XACK 只清 Pending Entries List、不删消息体，若写入侧不裁剪，
 * Stream 就是只增不减 —— 长跑必然把 Redis 内存吃光。
 */
class DurableCallbackClientTest {

    private static final String STREAM_KEY = "orbit:callback:stream";

    private DurableCallbackClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.shutdown(1);
        }
    }

    private static ExecutorProperties props() {
        ExecutorProperties p = new ExecutorProperties();
        p.setDurableCallbackEnabled(true);
        p.setCallbackStreamKey(STREAM_KEY);
        p.setCallbackRetryTimes(0);
        p.setCallbackRetryIntervalMs(20L);
        return p;
    }

    private static TriggerResult result(String logId) {
        return TriggerResult.ok(logId, 1L, "pod-orbit-executor-0", 10L, "done");
    }

    @SuppressWarnings("unchecked")
    private static StreamOperations<String, Object, Object> newOps() {
        StreamOperations<String, Object, Object> ops = mock(StreamOperations.class);
        when(ops.add(any())).thenReturn(RecordId.of("1-1"));
        return ops;
    }

    private DurableCallbackClient newClient(ExecutorProperties p, StringRedisTemplate redis) {
        client = new DurableCallbackClient(p, mock(AdminClient.class), redis);
        return client;
    }

    @Test
    void trimsStreamAfterWriting() {
        ExecutorProperties p = props();
        p.setCallbackStreamMaxLen(100000L);
        StreamOperations<String, Object, Object> ops = newOps();
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForStream()).thenReturn(ops);

        List<TriggerResult> batch = Collections.singletonList(result("log-trim"));
        assertTrue(newClient(p, redis).deliver(batch));

        verify(ops).add(any());
        verify(ops).trim(eq(STREAM_KEY), eq(100000L), eq(true));
        // stream 计数与 sent 计数都应入账（deliver 返回 true 即成功写入通道）
        assertEquals(1L, client.stats()[1]);
        assertEquals(1L, client.stats()[3]);
    }

    @Test
    void zeroUpperBoundDisablesTrimming() {
        ExecutorProperties p = props();
        p.setCallbackStreamMaxLen(0L);
        StreamOperations<String, Object, Object> ops = newOps();
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForStream()).thenReturn(ops);

        assertTrue(newClient(p, redis).deliver(Collections.singletonList(result("log-no-trim"))));

        verify(ops).add(any());
        verify(ops, never()).trim(anyString(), anyLong(), anyBoolean());
    }

    @Test
    void trimFailureMustNotBreakDeliveryOrFallBackToHttp() {
        ExecutorProperties p = props();
        p.setCallbackStreamMaxLen(500L);
        StreamOperations<String, Object, Object> ops = newOps();
        when(ops.trim(eq(STREAM_KEY), anyLong(), anyBoolean()))
                .thenThrow(new RuntimeException("TRIM boom"));
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForStream()).thenReturn(ops);
        AdminClient admin = mock(AdminClient.class);

        client = new DurableCallbackClient(p, admin, redis);
        // 消息已经写进 Stream，裁剪失败只是内存治理失败，不能降级成 HTTP 重发（会重复投递）
        assertTrue(client.deliver(Collections.singletonList(result("log-trim-fail"))));

        verify(admin, never()).post(anyString(), any());
        assertEquals(1L, client.stats()[1]);
        assertEquals(1L, client.stats()[3]);
    }
}

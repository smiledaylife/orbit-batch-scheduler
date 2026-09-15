package com.orbit.executor.client;

import com.orbit.core.model.TriggerResult;
import com.orbit.executor.config.ExecutorProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link CallbackClient} 单元测试：批量打包、失败重回队列、队列上限。
 */
class CallbackClientTest {

    private CallbackClient client;

    @AfterEach
    void tearDown() {
        if (client != null) {
            client.shutdown(1);
        }
    }

    /**
     * AdminClient 测试替身：记录每次 POST 的请求体，可控成功/失败，
     * 并支持把第一次调用挂住，用来制造「发送期间队列继续积压」的确定性场景。
     */
    private static class RecordingAdminClient extends AdminClient {
        final List<Object> bodies = new CopyOnWriteArrayList<Object>();
        volatile boolean succeed = true;
        volatile CountDownLatch blockFirstUntil;
        final CountDownLatch firstPostStarted = new CountDownLatch(1);

        RecordingAdminClient(ExecutorProperties properties) {
            super(properties);
        }

        @Override
        public boolean hasAdminAddress() {
            return true;
        }

        @Override
        public boolean post(String path, Object body) {
            if (bodies.isEmpty()) {
                firstPostStarted.countDown();
                CountDownLatch gate = blockFirstUntil;
                if (gate != null) {
                    try {
                        gate.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            bodies.add(body);
            return succeed;
        }
    }

    private static ExecutorProperties props() {
        ExecutorProperties p = new ExecutorProperties();
        p.setCallbackQueueCapacity(100);
        p.setCallbackRetryTimes(1);
        p.setCallbackRetryIntervalMs(20L);
        return p;
    }

    private static TriggerResult result(String logId) {
        return TriggerResult.ok(logId, 1L, "node-1", 10L, "done");
    }

    @SuppressWarnings("unchecked")
    private static int sizeOf(Object body) {
        return ((List<TriggerResult>) body).size();
    }

    @Test
    void batchesBackloggedResultsIntoOneRequest() throws Exception {
        RecordingAdminClient admin = new RecordingAdminClient(props());
        // 第一次 POST 挂住，让后续结果在队列里积压，从而确定性地形成批量
        admin.blockFirstUntil = new CountDownLatch(1);
        client = new CallbackClient(props(), admin);

        client.send(result("log-1"));
        assertTrue(admin.firstPostStarted.await(5, TimeUnit.SECONDS));
        client.send(result("log-2"));
        client.send(result("log-3"));
        admin.blockFirstUntil.countDown();

        long deadline = System.currentTimeMillis() + 5000L;
        while (admin.bodies.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }

        assertEquals(2, admin.bodies.size(), "expected 2 POSTs, got " + admin.bodies.size());
        assertEquals(1, sizeOf(admin.bodies.get(0)));
        // 第二批发出了积压的两条，说明 drainTo 批量生效
        assertEquals(2, sizeOf(admin.bodies.get(1)));
    }

    @Test
    void failedBatchIsRequeuedNotDropped() throws Exception {
        RecordingAdminClient admin = new RecordingAdminClient(props());
        admin.succeed = false;
        client = new CallbackClient(props(), admin);

        client.send(result("log-f"));

        // 重试耗尽后应重回队列（多次 POST 同一条），而不是一次就丢弃；
        // stats[2] 为重试退回计数，> 0 恰好证明结果被退回而非丢弃
        long deadline = System.currentTimeMillis() + 5000L;
        while (admin.bodies.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(admin.bodies.size() >= 2, "expected re-queued retries, got " + admin.bodies.size());
        assertTrue(client.stats()[2] >= 1L, "failed batch must be requeued (retry counter), never dropped");

        // 调度中心恢复后应能送达
        admin.succeed = true;
        deadline = System.currentTimeMillis() + 5000L;
        while (client.stats()[1] < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(1L, client.stats()[1], "callback should be delivered after admin recovers");
    }

    @Test
    void emptyBatchIsNeverSent() throws Exception {
        RecordingAdminClient admin = new RecordingAdminClient(props());
        client = new CallbackClient(props(), admin);

        Thread.sleep(300L);
        assertEquals(0, admin.bodies.size(), "idle sender must not POST empty batches");
    }

    @Test
    void shutdownStopsBackoffRetriesAndConvergesQuickly() throws Exception {
        RecordingAdminClient admin = new RecordingAdminClient(props());
        admin.succeed = false;
        ExecutorProperties p = props();
        // 宽退避配置：若停机后仍按完整重试节奏，本测试的耗时下限会被拉到秒级
        p.setCallbackRetryTimes(5);
        p.setCallbackRetryIntervalMs(500L);
        client = new CallbackClient(p, admin);

        client.send(result("log-shut"));
        // 等第一次发送失败完成（首次 POST 已记账）
        long deadline = System.currentTimeMillis() + 5000L;
        while (admin.bodies.size() < 1 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(admin.bodies.size() >= 1);

        // 停机：发送线程不得再按 retryTimes × interval 退避重试，
        // 每批只做一次尝试并退回队列，保证优雅停机在宽限期内收敛
        long t0 = System.currentTimeMillis();
        client.shutdown(1);
        long elapsed = System.currentTimeMillis() - t0;

        assertTrue(elapsed < 2500L,
                "shutdown must not wait for full backoff retries, took " + elapsed + "ms");
        // 结果仍留在队列中（stats[0] = 待发送数）而不是被丢弃，
        // 孤儿回收兜底之前仍有机会随下次启动补发
        long[] stats = client.stats();
        assertTrue(stats[0] >= 1L, "unsent callbacks must stay queued on shutdown, never dropped");
    }
}

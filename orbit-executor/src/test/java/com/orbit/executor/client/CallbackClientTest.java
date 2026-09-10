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

        // 重试耗尽后应重回队列（多次 POST 同一条），而不是一次就丢弃
        long deadline = System.currentTimeMillis() + 5000L;
        while (admin.bodies.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertTrue(admin.bodies.size() >= 2, "expected re-queued retries, got " + admin.bodies.size());
        assertEquals(0L, client.stats()[2], "nothing should be dropped while the queue has room");

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
}

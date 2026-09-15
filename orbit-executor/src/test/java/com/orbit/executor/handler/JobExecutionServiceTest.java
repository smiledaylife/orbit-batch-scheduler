package com.orbit.executor.handler;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.annotation.OrbitJob;
import com.orbit.executor.client.AdminClient;
import com.orbit.executor.client.CallbackClient;
import com.orbit.executor.config.ExecutorProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link JobExecutionService} 单元测试。
 *
 * 覆盖异步执行契约：受理即返回、结果经回传送达、超时按失败回传、
 * 饱和同步失败、worker-threads 兜底、业务异常包装。
 */
class JobExecutionServiceTest {

    private AnnotationConfigApplicationContext ctx;
    private JobExecutionService service;
    private CapturingCallback callback;

    /**
     * 回传客户端测试替身：只把结果收集起来，不发 HTTP。
     * 父类构造会起一条守护发送线程，但 send 被覆盖后队列为空，该线程只是空转。
     */
    private static class CapturingCallback extends CallbackClient {
        final List<TriggerResult> results = new CopyOnWriteArrayList<TriggerResult>();

        CapturingCallback(ExecutorProperties properties) {
            super(properties, new AdminClient(properties));
        }

        @Override
        public void send(TriggerResult result) {
            results.add(result);
        }
    }

    private void boot(int workerThreads, int queueCapacity) {
        boot(workerThreads, queueCapacity, new ExecutorProperties().getMaxJobWaitSeconds());
    }

    private void boot(int workerThreads, int queueCapacity, int maxJobWaitSeconds) {
        ExecutorProperties props = new ExecutorProperties();
        props.setWorkerThreads(workerThreads);
        props.setQueueCapacity(queueCapacity);
        props.setMaxJobWaitSeconds(maxJobWaitSeconds);
        callback = new CapturingCallback(props);
        service = new JobExecutionService(props, callback);
        ctx = new AnnotationConfigApplicationContext();
        ctx.register(PoolJobs.class, JobHandlerRegistry.class);
        ctx.refresh();
    }

    @AfterEach
    void tearDown() {
        // 先放开可能被卡住的任务，避免 destroy() 白等满优雅停机时间
        CountDownLatch release = PoolJobs.releaseBlocker;
        if (release != null) {
            release.countDown();
        }
        if (ctx != null) {
            ctx.close();
        }
        if (service != null) {
            service.destroy();
        }
    }

    private JobHandlerRegistry registry() {
        return ctx.getBean(JobHandlerRegistry.class);
    }

    private static TriggerRequest req(String handler, int timeoutSeconds) {
        TriggerRequest r = new TriggerRequest();
        r.setJobId(1L);
        r.setJobName("j");
        r.setHandler(handler);
        r.setLogId("log-" + handler);
        r.setTimeoutSeconds(timeoutSeconds);
        return r;
    }

    /** 轮询等待第 n 条回传到达（1 起） */
    private TriggerResult awaitCallback(int n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10000L;
        while (System.currentTimeMillis() < deadline) {
            if (callback.results.size() >= n) {
                return callback.results.get(n - 1);
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("callback #" + n + " not received within 10s, got " + callback.results.size());
    }

    @Test
    void submitReturnsAcceptedWithoutWaitingForJob() throws Exception {
        PoolJobs.blockerStarted = new CountDownLatch(1);
        PoolJobs.releaseBlocker = new CountDownLatch(1);
        boot(2, 8);

        long t0 = System.currentTimeMillis();
        TriggerResult result = service.submit(req("blocker", 60), registry(), "node-1");
        long elapsed = System.currentTimeMillis() - t0;

        // 任务还在跑，受理回执必须已经返回
        assertTrue(result.isAccepted());
        assertTrue(result.isSuccess());
        assertTrue(elapsed < 2000L, "submit should return immediately, took " + elapsed + "ms");
        assertEquals(0, callback.results.size());

        assertTrue(PoolJobs.blockerStarted.await(5, TimeUnit.SECONDS));
        PoolJobs.releaseBlocker.countDown();
    }

    @Test
    void successIsDeliveredByCallback() throws Exception {
        boot(2, 8);

        TriggerResult accepted = service.submit(req("quick", 10), registry(), "node-1");
        assertTrue(accepted.isAccepted());

        TriggerResult result = awaitCallback(1);
        assertFalse(result.isAccepted());
        assertTrue(result.isSuccess());
        assertEquals("ok", result.getMessage());
        assertEquals("log-quick", result.getLogId());
        assertEquals("node-1", result.getWorkerNode());
    }

    @Test
    void failureIsDeliveredByCallback() throws Exception {
        boot(2, 8);

        service.submit(req("boom", 10), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("boom!"), "got: " + result.getMessage());
    }

    @Test
    void jobDoesNotRunOnCallerThread() throws Exception {
        boot(2, 8);

        service.submit(req("threadName", 10), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        assertTrue(result.isSuccess());
        assertTrue(result.getMessage().startsWith("orbit-job-worker-"),
                "expected worker thread, got " + result.getMessage());
        assertNotEquals(Thread.currentThread().getName(), result.getMessage());
    }

    @Test
    void timeoutIsDeliveredAsFailure() throws Exception {
        boot(2, 8);

        long t0 = System.currentTimeMillis();
        service.submit(req("slow", 1), registry(), "node-1");
        TriggerResult result = awaitCallback(1);
        long elapsed = System.currentTimeMillis() - t0;

        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("timed out"), "got: " + result.getMessage());
        // slow handler 睡 8 秒，1 秒超时必须提前结束（含中断），不能等它自然跑完
        assertTrue(elapsed < 6000L, "timeout should cut the job short, took " + elapsed + "ms");
    }

    @Test
    void saturationReturnsSynchronousFailure() throws Exception {
        PoolJobs.blockerStarted = new CountDownLatch(1);
        PoolJobs.releaseBlocker = new CountDownLatch(1);
        // 1 个工作线程 + 不排队：第二个触发必然被拒绝
        boot(1, 0);

        TriggerResult first = service.submit(req("blocker", 60), registry(), "n");
        assertTrue(first.isAccepted());
        assertTrue(PoolJobs.blockerStarted.await(5, TimeUnit.SECONDS));

        TriggerResult second = service.submit(req("quick", 60), registry(), "n");
        assertFalse(second.isAccepted());
        assertFalse(second.isSuccess());
        assertTrue(second.getMessage().contains("saturated"), "got: " + second.getMessage());

        PoolJobs.releaseBlocker.countDown();
    }

    @Test
    void workerThreadsZeroFloorsToOneAndStillRunsJobs() throws Exception {
        // worker-threads=0 是误配：必须兜底为 1，而不是启动失败或任务永不执行
        boot(0, 8);

        service.submit(req("quick", 10), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        assertTrue(result.isSuccess());
        assertEquals("ok", result.getMessage());
        assertEquals(1, service.stats()[0]);
    }

    /**
     * {@code max-job-wait-seconds} 被误配成 0（且请求未带 timeoutSeconds）时，
     * 任务不能被瞬间判定为超时：等待时间有 1 秒下限，
     * 否则看门狗会立刻触发，表现为「所有任务都在 0ms 超时失败」。
     */
    @Test
    void zeroMaxJobWaitStillRunsJobs() throws Exception {
        boot(2, 8, 0);

        service.submit(req("quick", 0), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        assertTrue(result.isSuccess());
        assertEquals("ok", result.getMessage());
    }

    @Test
    void queueCapacityZeroBootsWithoutError() {
        // JDK 的 LinkedBlockingQueue 要求 capacity > 0；该场景必须改用 SynchronousQueue，
        // 否则 Bean 创建阶段就抛 IllegalArgumentException、业务应用直接启动失败。
        boot(1, 0);
        assertNotNull(service);
        assertEquals(1, service.stats()[0]);
    }

    // ==================== 失败重试（执行级，同 logId） ====================

    private static TriggerRequest reqWithRetry(String handler, int timeoutSeconds, int retryCount,
                                               int retryIntervalSeconds) {
        TriggerRequest r = req(handler, timeoutSeconds);
        r.setRetryCount(retryCount);
        r.setRetryIntervalSeconds(retryIntervalSeconds);
        return r;
    }

    /**
     * 执行级重试成功路径：首轮失败不回传，重试轮成功后一次性回传 SUCCESS，
     * 同一 logId 同一日志行；成功 message 标注重试轮次。
     */
    @Test
    void executionRetryRerunsAndSucceedsWithSameLogId() throws Exception {
        PoolJobs.flakyCount.set(0);
        boot(2, 8);

        service.submit(reqWithRetry("flaky", 10, 2, 0), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        // 整个重试链只有一条终局回传（中间失败不回传）
        assertEquals(1, callback.results.size(), "intermediate failures must not be delivered");
        assertTrue(result.isSuccess());
        assertEquals("log-flaky", result.getLogId());
        assertTrue(result.getMessage().contains("attempt 2/3"), "got: " + result.getMessage());
        // 重试轮次透传给业务上下文
        assertEquals(2, PoolJobs.lastAttempt.get());
    }

    /** 重试耗尽后回传失败，message 带轮次轨迹；同样只有一条终局回传。 */
    @Test
    void executionRetryExhaustsAndFailsWithAttemptTrail() throws Exception {
        boot(2, 8);

        service.submit(reqWithRetry("boom", 10, 1, 0), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        assertEquals(1, callback.results.size());
        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("boom!"), "got: " + result.getMessage());
        assertTrue(result.getMessage().contains("attempt 2/2"), "got: " + result.getMessage());
    }

    /** 超时同样计入重试范围：首轮超时后重试轮成功。 */
    @Test
    void timeoutTriggersRetryAndRetrySucceeds() throws Exception {
        PoolJobs.flakyCount.set(0);
        boot(2, 8);

        // 首轮故意慢到超时；重试轮 flakyCount 已经 >=1（首轮仍会执行 slowOnce 一次）
        service.submit(reqWithRetry("slowOnce", 1, 2, 0), registry(), "node-1");

        TriggerResult result = awaitCallback(1);
        assertEquals(1, callback.results.size());
        assertTrue(result.isSuccess(), "retry attempt should succeed, got: " + result.getMessage());
    }

    /**
     * 测试用 JobHandler 集合。
     */
    @Configuration
    static class PoolJobs {
        static volatile CountDownLatch blockerStarted;
        static volatile CountDownLatch releaseBlocker;

        /** flaky handler 的调用计数：首轮抛异常，后续轮成功 */
        static final java.util.concurrent.atomic.AtomicInteger flakyCount =
                new java.util.concurrent.atomic.AtomicInteger(0);

        /** 最近一次执行收到的业务轮次（经 JobContext.attempt） */
        static final java.util.concurrent.atomic.AtomicInteger lastAttempt =
                new java.util.concurrent.atomic.AtomicInteger(0);

        @Bean
        static Jobs jobs() {
            return new Jobs();
        }

        /** 测试用的 @OrbitJob 宿主 Bean：提供成功、失败、超时、带参等多种 handler */
        static class Jobs {
            @OrbitJob("quick")
            public String quick() {
                return "ok";
            }

            @OrbitJob("boom")
            public String boom() {
                throw new IllegalStateException("boom!");
            }

            @OrbitJob("slow")
            public String slow() throws InterruptedException {
                Thread.sleep(8000L);
                return "never";
            }

            @OrbitJob("blocker")
            public String blocker() throws InterruptedException {
                CountDownLatch started = PoolJobs.blockerStarted;
                if (started != null) {
                    started.countDown();
                }
                CountDownLatch release = PoolJobs.releaseBlocker;
                if (release != null) {
                    release.await();
                }
                return "released";
            }

            @OrbitJob("threadName")
            public String threadName() {
                return Thread.currentThread().getName();
            }

            @OrbitJob("flaky")
            public String flaky(com.orbit.executor.JobContext ctx) {
                lastAttempt.set(ctx.getAttempt());
                if (flakyCount.incrementAndGet() == 1) {
                    throw new IllegalStateException("first attempt always fails");
                }
                return "ok";
            }

            /** 首轮睡 3 秒（在 1 秒超时内必超时），后续轮立即成功 */
            @OrbitJob("slowOnce")
            public String slowOnce(com.orbit.executor.JobContext ctx) throws InterruptedException {
                if (ctx.getAttempt() == 1) {
                    Thread.sleep(3000L);
                }
                return "ok";
            }
        }
    }
}

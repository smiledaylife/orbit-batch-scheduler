package com.orbit.executor.handler;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.annotation.OrbitJob;
import com.orbit.executor.config.ExecutorProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link JobExecutionService} 单元测试：
 * 覆盖成功路径、超时强制中断（消除僵尸任务）、饱和快速失败、内联兼容模式与业务异常包装。
 */
class JobExecutionServiceTest {

    private AnnotationConfigApplicationContext ctx;
    private JobExecutionService service;

    private void boot(int workerThreads, int queueCapacity) {
        ExecutorProperties props = new ExecutorProperties();
        props.setWorkerThreads(workerThreads);
        props.setQueueCapacity(queueCapacity);
        service = new JobExecutionService(props);
        ctx = new AnnotationConfigApplicationContext();
        ctx.register(PoolJobs.class, JobHandlerRegistry.class);
        ctx.refresh();
    }

    @AfterEach
    void tearDown() {
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

    @Test
    void successReturnsResult() {
        boot(2, 8);
        TriggerResult result = service.execute(req("quick", 10), registry(), "node-1");
        assertTrue(result.isSuccess());
        assertEquals("ok", result.getMessage());
        assertEquals("node-1", result.getWorkerNode());
    }

    @Test
    void businessFailureIsWrapped() {
        boot(2, 8);
        TriggerResult result = service.execute(req("boom", 10), registry(), "node-1");
        assertFalse(result.isSuccess());
        // SDK 既有行为：业务异常被包装为 "handler 'X' failed: <cause message>"
        assertTrue(result.getMessage().contains("boom!"), result.getMessage());
    }

    @Test
    void timeoutEnforcementInterruptsSlowJob() {
        boot(1, 4);
        long start = System.currentTimeMillis();
        // slow 任务睡眠 8 秒；1 秒超时应被强制中断，快速返回失败
        TriggerResult result = service.execute(req("slow", 1), registry(), "node-1");
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("timed out"), result.getMessage());
        // 1 秒超时 + 容差，远小于 8 秒睡眠 —— 证明发生了强制中断而非等待自然结束
        assertTrue(elapsed < 4000, "timeout should fire quickly, elapsed=" + elapsed + "ms");
    }

    @Test
    void saturatedQueueFailsFast() throws Exception {
        boot(1, 1);
        JobHandlerRegistry reg = registry();

        // 第一把锁：让唯一的工作线程卡在 blocker 上
        CountDownLatch blockerStarted = new CountDownLatch(1);
        CountDownLatch releaseBlocker = new CountDownLatch(1);
        PoolJobs.blockerStarted = blockerStarted;
        PoolJobs.releaseBlocker = releaseBlocker;

        // 请求 1：占用工作线程
        AtomicReference<TriggerResult> first = new AtomicReference<TriggerResult>();
        Thread t1 = new Thread(() -> first.set(service.execute(req("blocker", 60), reg, "n")));
        t1.start();
        assertTrue(blockerStarted.await(5, TimeUnit.SECONDS), "blocker should start");

        // 请求 2：进入队列（容量 1）
        AtomicReference<TriggerResult> second = new AtomicReference<TriggerResult>();
        Thread t2 = new Thread(() -> second.set(service.execute(req("quick", 60), reg, "n")));
        t2.start();
        // 等待请求 2 完成入队（稍作等待保证顺序稳定）
        Thread.sleep(300);

        // 请求 3：队列已满，必须立即被拒绝而不是长时间阻塞
        long start = System.currentTimeMillis();
        TriggerResult third = service.execute(req("quick", 60), reg, "n");
        long elapsed = System.currentTimeMillis() - start;

        assertFalse(third.isSuccess());
        assertTrue(third.getMessage().contains("saturated"), third.getMessage());
        assertTrue(elapsed < 2000, "rejection should be immediate, elapsed=" + elapsed + "ms");

        // 释放锁，让前两个请求完成
        releaseBlocker.countDown();
        t1.join(5000);
        t2.join(5000);
        assertNotNull(first.get());
        assertNotNull(second.get());
        assertTrue(first.get().isSuccess());
        assertTrue(second.get().isSuccess());
    }

    @Test
    void inlineModeRunsOnCallerThread() {
        boot(0, 0);
        TriggerResult result = service.execute(req("threadName", 10), registry(), "node-1");
        assertTrue(result.isSuccess());
        // worker-threads=0 的旧版行为：任务在调用方（请求）线程内执行
        assertEquals(Thread.currentThread().getName(), result.getMessage());
    }

    /**
     * 测试用 JobHandler 集合。
     */
    @Configuration
    static class PoolJobs {
        static volatile CountDownLatch blockerStarted;
        static volatile CountDownLatch releaseBlocker;

        @Bean
        static Jobs jobs() {
            return new Jobs();
        }

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
        }
    }
}

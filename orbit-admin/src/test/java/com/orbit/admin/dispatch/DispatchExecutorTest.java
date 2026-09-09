package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.service.JobService;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.TriggerResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link DispatchExecutor} 单元测试。
 *
 * 覆盖触发通道的四项契约：
 *   - 触发跑在专职线程池上，不占用调用方（Quartz）线程；
 *   - 同名任务串行：受理成功后守卫一直持有到回传，其间到点被跳过；
 *   - 守卫释放：触发同步失败、或 dispatch 抛异常时立即释放，任务不会被永久锁死；
 *   - 背压：队列满时快速失败并写一条 FAILED 调度日志。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class DispatchExecutorTest {

    @Mock
    private JobService jobService;

    private DispatchExecutor executor;

    @AfterEach
    void tearDown() {
        if (executor != null) {
            executor.destroy();
        }
    }

    private static AdminProperties props(int threads, int queue, boolean serial) {
        AdminProperties p = new AdminProperties();
        p.setDispatchThreads(threads);
        p.setDispatchQueueCapacity(queue);
        p.setDispatchSerialPerJob(serial);
        return p;
    }

    private static JobInfo job(String name) {
        JobInfo j = new JobInfo();
        j.setId(1L);
        j.setJobName(name);
        j.setAppName("app");
        j.setHandler("h");
        return j;
    }

    /** 受理回执：日志保持 RUNNING，守卫继续持有 */
    private static TriggerResult accepted(String logId) {
        return TriggerResult.accepted(logId, 1L, "node-1", "accepted");
    }

    @Test
    void dispatchRunsOnPoolThreadNotCaller() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicReference<String> threadName = new AtomicReference<String>();
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            threadName.set(Thread.currentThread().getName());
            done.countDown();
            return accepted("log-1");
        });
        executor = new DispatchExecutor(jobService, props(2, 8, true), new OutstandingDispatches());

        executor.submit(job("jobA"));

        assertTrue(done.await(5, TimeUnit.SECONDS), "dispatch should have run");
        assertTrue(threadName.get().startsWith("orbit-dispatch-"),
                "expected pool thread, got " + threadName.get());
        assertNotEquals(Thread.currentThread().getName(), threadName.get());
    }

    @Test
    void serialPerJobSkipsWhilePreviousAwaitingCallback() throws Exception {
        final CountDownLatch first = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            first.countDown();
            return accepted("log-s1");
        });
        OutstandingDispatches outstanding = new OutstandingDispatches();
        executor = new DispatchExecutor(jobService, props(2, 8, true), outstanding);

        executor.submit(job("jobS"));
        assertTrue(first.await(5, TimeUnit.SECONDS));
        // 受理后守卫仍在持有（等回传），后续到点必须被跳过
        executor.submit(job("jobS"));
        executor.submit(job("jobS"));

        verify(jobService, times(1)).dispatch(any(JobInfo.class), any());
        assertEquals(1, outstanding.outstanding());
        assertEquals(2L, outstanding.skipped());
        assertEquals(2L, executor.metrics().get("dispatchSkipped"));
    }

    @Test
    void guardReleasedOnCallbackAllowsNextFire() throws Exception {
        final CountDownLatch first = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            first.countDown();
            return accepted("log-c1");
        });
        OutstandingDispatches outstanding = new OutstandingDispatches();
        executor = new DispatchExecutor(jobService, props(2, 8, true), outstanding);

        executor.submit(job("jobC"));
        assertTrue(first.await(5, TimeUnit.SECONDS));
        assertEquals(1, outstanding.outstanding());

        // 执行器回传 -> 释放守卫 -> 下一次到点可以正常触发
        assertTrue(outstanding.release("log-c1"));
        assertEquals(0, outstanding.outstanding());

        final CountDownLatch second = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            second.countDown();
            return accepted("log-c2");
        });
        executor.submit(job("jobC"));
        assertTrue(second.await(5, TimeUnit.SECONDS));
        verify(jobService, times(2)).dispatch(any(JobInfo.class), any());
    }

    @Test
    void guardReleasedWhenTriggerRejectedSynchronously() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            done.countDown();
            return TriggerResult.fail("log-f", 1L, "node-1", 0, "executor saturated");
        });
        OutstandingDispatches outstanding = new OutstandingDispatches();
        executor = new DispatchExecutor(jobService, props(2, 8, true), outstanding);

        executor.submit(job("jobF"));

        assertTrue(done.await(5, TimeUnit.SECONDS));
        // 触发同步失败不会有回传，守卫必须已经释放，否则任务永久锁死
        assertEquals(0, outstanding.outstanding());
        assertEquals(0L, outstanding.skipped());
    }

    @Test
    void guardReleasedAfterDispatchThrows() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            done.countDown();
            throw new IllegalStateException("db unavailable");
        });
        OutstandingDispatches outstanding = new OutstandingDispatches();
        executor = new DispatchExecutor(jobService, props(2, 8, true), outstanding);

        executor.submit(job("jobX"));

        assertTrue(done.await(5, TimeUnit.SECONDS));
        // insertLog 抛异常会逃出 dispatch 的 try：不兜住既会杀死工作线程，也会永久泄漏守卫
        assertEquals(0, outstanding.outstanding());
    }

    @Test
    void serialDisabledAllowsOverlap() throws Exception {
        final CountDownLatch both = new CountDownLatch(2);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            both.countDown();
            return accepted("log-o");
        });
        OutstandingDispatches outstanding = new OutstandingDispatches();
        executor = new DispatchExecutor(jobService, props(4, 8, false), outstanding);

        executor.submit(job("jobO"));
        executor.submit(job("jobO"));

        assertTrue(both.await(5, TimeUnit.SECONDS), "both fires should run when serial guard is off");
        verify(jobService, times(2)).dispatch(any(JobInfo.class), any());
        assertEquals(0, outstanding.outstanding());
    }

    @Test
    void differentJobsAreIndependent() throws Exception {
        final CountDownLatch both = new CountDownLatch(2);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            both.countDown();
            return accepted("log-" + ((JobInfo) inv.getArgument(0)).getJobName());
        });
        OutstandingDispatches outstanding = new OutstandingDispatches();
        executor = new DispatchExecutor(jobService, props(4, 8, true), outstanding);

        executor.submit(job("job1"));
        executor.submit(job("job2"));

        assertTrue(both.await(5, TimeUnit.SECONDS), "different jobs must not block each other");
        assertEquals(2, outstanding.outstanding());
    }

    @Test
    void saturationRecordsFailedLog() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
            return accepted("log-b");
        });
        // 1 个线程 + 不排队：第二个触发必然被拒绝
        executor = new DispatchExecutor(jobService, props(1, 0, false), new OutstandingDispatches());

        executor.submit(job("jobBusy"));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        executor.submit(job("jobBlocked"));

        verify(jobService, times(1)).recordRejectedDispatch(any(JobInfo.class), anyString());
        assertEquals(1L, executor.metrics().get("dispatchRejected"));
        release.countDown();
    }

    @Test
    void metricsExposePoolShape() {
        executor = new DispatchExecutor(jobService, props(3, 16, true), new OutstandingDispatches());
        Map<String, Object> m = executor.metrics();
        assertEquals(3, m.get("dispatchThreads"));
        assertEquals(16, m.get("dispatchQueueCapacity"));
        assertNotNull(m.get("dispatchActive"));
        assertNotNull(m.get("dispatchQueueSize"));
        assertNotNull(m.get("dispatchOutstanding"));
    }

    @Test
    void zeroThreadsBootsWithFloorOfOne() throws Exception {
        final CountDownLatch done = new CountDownLatch(1);
        final AtomicInteger calls = new AtomicInteger();
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            calls.incrementAndGet();
            done.countDown();
            return accepted("log-z");
        });
        // dispatch-threads=0 是误配，必须兜底为 1 而不是启动失败或永不执行
        executor = new DispatchExecutor(jobService, props(0, 4, true), new OutstandingDispatches());

        executor.submit(job("jobZ"));

        assertTrue(done.await(5, TimeUnit.SECONDS), "dispatch should still run with threads floored to 1");
        assertEquals(1, executor.metrics().get("dispatchThreads"));
        assertEquals(1, calls.get());
        verify(jobService, never()).recordRejectedDispatch(any(JobInfo.class), anyString());
    }

    @Test
    void rejectedDispatchFailureIsSwallowed() throws Exception {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            entered.countDown();
            release.await(5, TimeUnit.SECONDS);
            return accepted("log-rb");
        });
        // 落库同样不可用时，记录拒绝日志自身会抛异常：不能让它冒泡到 Quartz 线程
        org.mockito.Mockito.doThrow(new IllegalStateException("db down"))
                .when(jobService).recordRejectedDispatch(any(JobInfo.class), anyString());
        executor = new DispatchExecutor(jobService, props(1, 0, false), new OutstandingDispatches());

        executor.submit(job("jobBusy"));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        executor.submit(job("jobBlocked"));

        verify(jobService, atLeastOnce()).recordRejectedDispatch(any(JobInfo.class),
                org.mockito.ArgumentMatchers.contains("scheduler saturated"));
        release.countDown();
    }
}

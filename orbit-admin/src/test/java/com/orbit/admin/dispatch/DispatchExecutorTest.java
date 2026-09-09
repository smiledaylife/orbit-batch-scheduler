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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link DispatchExecutor} 单元测试（Mock JobService）。
 * 覆盖：派发在专职线程上执行、同名任务串行跳过、队列满快速失败并落 FAILED 日志、
 * 不同任务并发、串行守卫在派发抛异常后仍能释放。
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

    private AdminProperties props(int threads, int queueCapacity, boolean serialPerJob) {
        AdminProperties p = new AdminProperties();
        p.setDispatchThreads(threads);
        p.setDispatchQueueCapacity(queueCapacity);
        p.setDispatchSerialPerJob(serialPerJob);
        return p;
    }

    private static JobInfo job(String name) {
        JobInfo j = new JobInfo();
        j.setId(1L);
        j.setJobName(name);
        j.setAppName("app");
        j.setHandler("dailyReport");
        return j;
    }

    /** 让 dispatch 阻塞在门闩上，用于制造「上一轮尚未结束」与「工作线程被占满」两种场景 */
    private void blockDispatch(CountDownLatch started, CountDownLatch release) {
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            if (started != null) {
                started.countDown();
            }
            if (release != null) {
                release.await();
            }
            return TriggerResult.ok("log-1", 1L, "node", 1L, "ok");
        });
    }

    @Test
    void dispatchRunsOnDedicatedPoolThread() throws Exception {
        executor = new DispatchExecutor(jobService, props(2, 8, true));
        final AtomicReference<String> threadName = new AtomicReference<String>();
        CountDownLatch done = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            threadName.set(Thread.currentThread().getName());
            done.countDown();
            return TriggerResult.ok("log-1", 1L, "node", 1L, "ok");
        });

        executor.submit(job("jobA"));

        assertTrue(done.await(5, TimeUnit.SECONDS), "dispatch should run");
        assertTrue(threadName.get().startsWith("orbit-dispatch-"),
                "dispatch must not run on the Quartz thread: " + threadName.get());
    }

    /**
     * 同名任务串行：上一轮未结束时本次到点被跳过。
     * 跳过只累加计数、不写日志，避免高频 Cron 配慢 Handler 时刷爆日志表。
     */
    @Test
    void sameJobIsSkippedWhilePreviousRunInFlight() throws Exception {
        executor = new DispatchExecutor(jobService, props(2, 8, true));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        blockDispatch(started, release);

        executor.submit(job("jobA"));
        assertTrue(started.await(5, TimeUnit.SECONDS), "first dispatch should start");

        // 第一轮还在跑，第二次到点必须被跳过而不是并发执行
        executor.submit(job("jobA"));
        executor.submit(job("jobA"));
        Thread.sleep(200);

        verify(jobService, times(1)).dispatch(any(JobInfo.class), any());
        assertEquals(2L, executor.metrics().get("dispatchSkipped"));

        release.countDown();
    }

    /**
     * 关掉串行开关后，同名任务允许并发执行。
     */
    @Test
    void sameJobRunsConcurrentlyWhenSerialGuardDisabled() throws Exception {
        executor = new DispatchExecutor(jobService, props(4, 8, false));
        CountDownLatch bothStarted = new CountDownLatch(2);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            bothStarted.countDown();
            bothStarted.await(5, TimeUnit.SECONDS);
            return TriggerResult.ok("log-1", 1L, "node", 1L, "ok");
        });

        executor.submit(job("jobA"));
        executor.submit(job("jobA"));

        assertTrue(bothStarted.await(5, TimeUnit.SECONDS), "both runs should be in flight at once");
        assertEquals(0L, executor.metrics().get("dispatchSkipped"));
    }

    /**
     * 队列满时快速失败，并落一条 FAILED 调度日志 —— 否则「派发被拒绝」
     * 与「任务根本没被触发」在 /logs 上无法区分。
     */
    @Test
    void saturatedQueueFailsFastAndRecordsLog() throws Exception {
        // queue-capacity=0：唯一工作线程被占满后，新触发无处排队
        executor = new DispatchExecutor(jobService, props(1, 0, false));
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        blockDispatch(started, release);

        executor.submit(job("jobA"));
        assertTrue(started.await(5, TimeUnit.SECONDS), "worker should be busy");

        executor.submit(job("jobB"));

        assertEquals(1L, executor.metrics().get("dispatchRejected"));
        verify(jobService, times(1)).recordRejectedDispatch(any(JobInfo.class), anyString());

        release.countDown();
    }

    /**
     * 不同任务互不影响：A 在跑时 B 仍能派发。
     */
    @Test
    void differentJobsAreNotBlockedByEachOther() throws Exception {
        executor = new DispatchExecutor(jobService, props(2, 8, true));
        CountDownLatch aStarted = new CountDownLatch(1);
        CountDownLatch aRelease = new CountDownLatch(1);
        CountDownLatch bDone = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            JobInfo j = inv.getArgument(0);
            if ("jobA".equals(j.getJobName())) {
                aStarted.countDown();
                aRelease.await();
            } else {
                bDone.countDown();
            }
            return TriggerResult.ok("log-1", 1L, "node", 1L, "ok");
        });

        executor.submit(job("jobA"));
        assertTrue(aStarted.await(5, TimeUnit.SECONDS), "jobA should start");
        executor.submit(job("jobB"));

        assertTrue(bDone.await(5, TimeUnit.SECONDS), "jobB must not wait for jobA");
        verify(jobService, never()).recordRejectedDispatch(any(JobInfo.class), anyString());

        aRelease.countDown();
    }

    /**
     * 串行守卫必须放在 finally 里：dispatch 内部的 jobStore.insertLog 在它自己的 try 之外，
     * 数据库不可用时会抛出。此时若不清守卫，该任务会永久停在「在跑」状态、再也不被调度。
     */
    @Test
    void serialGuardReleasedEvenWhenDispatchThrows() throws Exception {
        executor = new DispatchExecutor(jobService, props(2, 8, true));
        CountDownLatch firstFailed = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            firstFailed.countDown();
            throw new IllegalStateException("db down");
        });

        executor.submit(job("jobA"));
        assertTrue(firstFailed.await(5, TimeUnit.SECONDS), "first dispatch should fail");
        // 等 finally 执行完
        Thread.sleep(200);

        CountDownLatch secondDone = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            secondDone.countDown();
            return TriggerResult.ok("log-1", 1L, "node", 1L, "ok");
        });
        executor.submit(job("jobA"));

        assertTrue(secondDone.await(5, TimeUnit.SECONDS),
                "job must be dispatchable again after a failed run (guard must be released)");
        assertEquals(0L, executor.metrics().get("dispatchSkipped"));
    }

    /**
     * 线程数被误配成 0 或负数时不能导致 Bean 创建失败，按 1 兜底。
     */
    @Test
    void zeroDispatchThreadsStillBoots() throws Exception {
        executor = new DispatchExecutor(jobService, props(0, 4, true));
        CountDownLatch done = new CountDownLatch(1);
        when(jobService.dispatch(any(JobInfo.class), any())).thenAnswer(inv -> {
            done.countDown();
            return TriggerResult.ok("log-1", 1L, "node", 1L, "ok");
        });

        executor.submit(job("jobA"));

        assertTrue(done.await(5, TimeUnit.SECONDS), "dispatch should still run with threads floored to 1");
        assertEquals(1, executor.metrics().get("dispatchThreads"));
    }
}

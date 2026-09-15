package com.orbit.admin.config;

import com.orbit.admin.alert.AlertDispatcher;
import com.orbit.admin.alert.JobAlertEvent;
import com.orbit.admin.alert.LoggingAlertHandler;
import com.orbit.admin.dispatch.OutstandingDispatches;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.JobLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link AdminScheduleTasks} 僵尸 RUNNING 日志回收的阈值保护测试。
 *
 * 重点回归：max-timeout-seconds / heartbeat-timeout-seconds 误配成 0 或负数时，
 * 回收阈值必须落到下限保护值，而不是把回收窗口缩到只剩宽限期、
 * 把仍在正常执行的长任务成批误判为僵尸。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class AdminScheduleTasksTest {

    @Mock
    private ExecutorRegistry registry;
    @Mock
    private JobStore jobStore;

    private AdminScheduleTasks tasks(AdminProperties properties) {
        return new AdminScheduleTasks(registry, jobStore, properties, new OutstandingDispatches(),
                new AlertDispatcher(new LoggingAlertHandler()));
    }

    @Test
    void reapUsesClampedThresholdsWhenPropertiesAreMisconfigured() {
        AdminProperties p = new AdminProperties();
        p.setMaxTimeoutSeconds(0);        // 误配
        p.setHeartbeatTimeoutSeconds(-5); // 误配
        when(registry.listAll()).thenReturn(java.util.Collections.emptyList());

        tasks(p).reapOrphanedRunningLogs();

        ArgumentCaptor<Long> hardCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> offline = ArgumentCaptor.forClass(Long.class);
        verify(jobStore).reapOrphanedRunning(hardCap.capture(), offline.capture(),
                anySet(), anyString(), anyString());

        // 硬上界 = max(1, 误配值) * 1000 + 5 分钟宽限
        assertEquals(1000L + 5 * 60 * 1000L, hardCap.getValue().longValue());
        // 存活判定 = max(5, 误配值) * 1000 + 5 分钟宽限（与 ExecutorRegistry 口径一致）
        assertEquals(5000L + 5 * 60 * 1000L, offline.getValue().longValue());
    }

    @Test
    void reapPassesThroughSaneThresholdsUnchanged() {
        AdminProperties p = new AdminProperties();
        p.setMaxTimeoutSeconds(3600);
        p.setHeartbeatTimeoutSeconds(90);
        when(registry.listAll()).thenReturn(java.util.Collections.emptyList());

        tasks(p).reapOrphanedRunningLogs();

        ArgumentCaptor<Long> hardCap = ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Long> offline = ArgumentCaptor.forClass(Long.class);
        verify(jobStore).reapOrphanedRunning(hardCap.capture(), offline.capture(),
                anySet(), anyString(), anyString());

        assertEquals(3600L * 1000 + 5 * 60 * 1000L, hardCap.getValue().longValue());
        assertEquals(90L * 1000 + 5 * 60 * 1000L, offline.getValue().longValue());
    }

    @Test
    void reapReleasesOutstandingSlotForEveryReapedLog() {
        AdminProperties p = new AdminProperties();
        when(registry.listAll()).thenReturn(java.util.Collections.emptyList());
        OutstandingDispatches outstanding = new OutstandingDispatches();
        outstanding.tryAcquire("jobA", "log-gone");
        AdminScheduleTasks tasks = new AdminScheduleTasks(registry, jobStore, p, outstanding,
                new AlertDispatcher(new LoggingAlertHandler()));
        JobLog gone = new JobLog();
        gone.setLogId("log-gone");
        gone.setJobName("jobA");
        when(jobStore.reapOrphanedRunning(anyLong(), anyLong(), anySet(), anyString(), anyString()))
                .thenReturn(java.util.Collections.singletonList(gone));

        tasks.reapOrphanedRunningLogs();

        // 日志收敛到终态后串行守卫必须同步释放，否则该任务永久不再触发
        assertEquals(0, outstanding.outstanding());
    }

    @Test
    void reapFiresExecutionLostAlertForEveryReapedLog() throws Exception {
        AdminProperties p = new AdminProperties();
        when(registry.listAll()).thenReturn(java.util.Collections.emptyList());

        // 用可观测的处理器记录事件：只有真正被回收的行才应告警（回收返回即生效行）；
        // 事件在分发线程到达，用并发安全集合保证可见性
        java.util.List<JobAlertEvent> events = new java.util.concurrent.CopyOnWriteArrayList<JobAlertEvent>();
        AlertDispatcher dispatcher = new AlertDispatcher(events::add);

        OutstandingDispatches outstanding = new OutstandingDispatches();
        AdminScheduleTasks tasks = new AdminScheduleTasks(registry, jobStore, p, outstanding, dispatcher);
        JobLog gone = new JobLog();
        gone.setLogId("log-gone");
        gone.setJobName("jobA");
        gone.setAppName("sample-app");
        gone.setHandler("demo");
        when(jobStore.reapOrphanedRunning(anyLong(), anyLong(), anySet(), anyString(), anyString()))
                .thenReturn(java.util.Collections.singletonList(gone));

        tasks.reapOrphanedRunningLogs();

        // 告警是异步投递的：轮询等待事件到达（上限 2 秒）
        long deadline = System.currentTimeMillis() + 2000L;
        while (events.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(1, events.size());
        assertEquals(JobAlertEvent.EXECUTION_LOST, events.get(0).eventType());
        assertEquals("jobA", events.get(0).jobName());
        assertEquals("sample-app", events.get(0).appName());
    }

    @Test
    void reapFailureIsSwallowedSoNextRoundStillRuns() {
        AdminProperties p = new AdminProperties();
        when(registry.listAll()).thenReturn(java.util.Collections.emptyList());
        when(jobStore.reapOrphanedRunning(anyLong(), anyLong(), anySet(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("db down"));

        // 不应向 @Scheduled 调度器外抛异常：单轮失败只记日志，下一轮照常执行
        tasks(p).reapOrphanedRunningLogs();
        assertTrue(true, "no exception should propagate");
    }
}

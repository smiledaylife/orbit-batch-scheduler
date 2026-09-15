package com.orbit.admin.service;

import com.orbit.admin.alert.AlertDispatcher;
import com.orbit.admin.alert.JobAlertEvent;
import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
import com.orbit.admin.dispatch.OutstandingDispatches;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.TriggerResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.JobPersistenceException;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link JobService} 派发与校验逻辑单元测试（Mock 依赖）。
 * 重点覆盖：
 *   - dispatch 单次查询：一次派发只允许调用一次 registry.listByApp；
 *   - 异步派发契约：受理回执下日志保持 RUNNING，终态只能由回传收敛；
 *   - failover：首选节点连接拒绝时立即摘除并切换下一个节点；
 *   - routeStrategy 合法性校验与规范化；
 *   - create 的唯一键竞态兜底（DataIntegrityViolationException → 友好 400 语义）；
 *   - Quartz 编排失败时 create / update 的数据库回滚（不留幽灵任务、不留新旧不一致）；
 *   - 超出数据库列宽的字段在入参校验阶段被拒绝（400 而非无信息的 500）；
 *   - timezone 非法值启动失败（fail-fast）。
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class JobServiceDispatchTest {

    @Mock
    private Scheduler scheduler;
    @Mock
    private JobStore jobStore;
    @Mock
    private ExecutorRegistry registry;
    @Mock
    private ExecutorClient executorClient;

    private AdminProperties properties;
    private OutstandingDispatches outstanding;
    private JobService jobService;

    /** 告警事件捕获器：验证失败告警的触发时机与内容 */
    private java.util.List<JobAlertEvent> alerts;

    @BeforeEach
    void setUp() {
        properties = new AdminProperties();
        properties.setTimezone("Asia/Shanghai");
        properties.setGroup("ORBIT");
        outstanding = new OutstandingDispatches();
        // 告警是异步的：用并发安全列表捕获，测试里轮询断言
        alerts = new java.util.concurrent.CopyOnWriteArrayList<JobAlertEvent>();
        jobService = new JobService(scheduler, jobStore, registry, executorClient, properties, outstanding,
                new AlertDispatcher(alerts::add));
    }

    private static JobInfo newJob(String name, String appName) {
        JobInfo job = new JobInfo();
        job.setId(1L);
        job.setJobName(name);
        job.setAppName(appName);
        job.setHandler("dailyReport");
        job.setCron(null); // 手动任务，避免走 Quartz 编排分支
        job.setTimeoutSeconds(60);
        job.setRouteStrategy("ROUND");
        job.setEnabled(true);
        return job;
    }

    private static ExecutorNode node(String address) {
        ExecutorNode n = new ExecutorNode();
        n.setAppName("app");
        n.setAddress(address);
        return n;
    }

    @Test
    void dispatchQueriesRegistryOnlyOnce() {
        JobInfo job = newJob("jobA", "app");
        List<ExecutorNode> candidates = java.util.Arrays.asList(node("http://10.0.0.1:8081"));
        when(registry.listByApp("app")).thenReturn(candidates);
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(candidates.get(0));
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any())).thenReturn(
                TriggerResult.accepted("log-1", 1L, "http://10.0.0.1:8081", "accepted"));

        TriggerResult result = jobService.dispatch(job, null);

        assertTrue(result.isSuccess());
        assertTrue(result.isAccepted());
        // 核心断言：单次派发只查一次候选列表
        verify(registry, times(1)).listByApp("app");
        // 受理回执不代表任务跑完：日志必须保持 RUNNING，一次 finishLog 都不能有
        verify(jobStore, never()).finishLog(any(), any(), any(), anyLong(), any());
    }

    @Test
    void dispatchFailsOverOnUnreachableNode() {
        JobInfo job = newJob("jobB", "app");
        ExecutorNode n1 = node("http://10.0.0.1:8081");
        ExecutorNode n2 = node("http://10.0.0.2:8081");
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(n1, n2));
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(n1);
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any())).thenReturn(
                TriggerResult.fail("log-2", 1L, "http://10.0.0.1:8081", 0, "Connection refused"));
        when(executorClient.trigger(eq("http://10.0.0.2:8081"), any())).thenReturn(
                TriggerResult.accepted("log-2", 1L, "http://10.0.0.2:8081", "accepted"));

        TriggerResult result = jobService.dispatch(job, null);

        assertTrue(result.isSuccess());
        assertTrue(result.isAccepted());
        // 不可达节点被立即摘除（不等心跳超时）
        verify(registry, times(1)).remove("app", "http://10.0.0.1:8081");
        // 第二个节点受理成功，日志同样保持 RUNNING 等回传
        verify(jobStore, never()).finishLog(any(), any(), any(), anyLong(), any());
    }

    /**
     * 模糊失败（connection reset / read timeout）：请求可能已被执行器受理。
     * 必须故障转移保证可用性，但绝不能据此摘除节点 —— 节点可能只是抖动，仍然健康。
     */
    @Test
    void dispatchFailsOverOnAmbiguousErrorWithoutEvicting() {
        JobInfo job = newJob("jobAmbiguous", "app");
        ExecutorNode n1 = node("http://10.0.0.1:8081");
        ExecutorNode n2 = node("http://10.0.0.2:8081");
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(n1, n2));
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(n1);
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any())).thenReturn(
                TriggerResult.fail("log-amb", 1L, "http://10.0.0.1:8081", 0, "Connection reset"));
        when(executorClient.trigger(eq("http://10.0.0.2:8081"), any())).thenReturn(
                TriggerResult.accepted("log-amb", 1L, "http://10.0.0.2:8081", "accepted"));

        TriggerResult result = jobService.dispatch(job, null);

        assertTrue(result.isSuccess());
        assertTrue(result.isAccepted());
        // 换了节点重试，但没有摘除任何节点
        verify(registry, never()).remove(anyString(), anyString());
        verify(executorClient, times(1)).trigger(eq("http://10.0.0.2:8081"), any());
    }

    /**
     * 手动触发必须接入同名任务串行守卫：上一轮在跑时拒绝本次触发并返回明确原因，
     * 而不是与在跑实例并行执行；同时不得影响守卫中已登记的上一轮槽位。
     */
    @Test
    void manualTriggerRejectsWhilePreviousRunInFlight() {
        JobInfo job = newJob("jobGuard", "app");
        when(jobStore.findJobByName("jobGuard")).thenReturn(Optional.of(job));
        // 预占用串行守卫（模拟上一轮执行尚未收敛）
        assertTrue(outstanding.tryAcquire("jobGuard", "prev-log"));

        TriggerResult result = jobService.triggerNow("jobGuard", null);

        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("manual trigger rejected"), result.getMessage());
        // 被拒的手动触发不得派发，也不能动上一轮的槽位
        verify(executorClient, never()).trigger(anyString(), any());
        assertEquals(1, outstanding.outstanding());
        outstanding.release("prev-log");
    }

    /**
     * 手动触发受理后槽位保持到回传收敛（与定时触发一致），同步失败则立即释放。
     */
    @Test
    void manualTriggerHoldsSlotUntilCallbackAndReleasesOnFailure() {
        JobInfo job = newJob("jobGuard2", "app");
        when(jobStore.findJobByName("jobGuard2")).thenReturn(Optional.of(job));
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(node("http://10.0.0.1:8081")));
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(null);
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any()))
                .thenReturn(TriggerResult.accepted("manual-log", 1L, "http://10.0.0.1:8081", "accepted"));

        TriggerResult accepted = jobService.triggerNow("jobGuard2", null);
        assertTrue(accepted.isAccepted());
        // 受理后槽位保持，防止守卫开启下的并行执行
        assertEquals(1, outstanding.outstanding());

        // 用实际下发的 logId 回传 -> 槽位随日志收敛一起释放
        org.mockito.ArgumentCaptor<com.orbit.core.model.TriggerRequest> reqCaptor =
                org.mockito.ArgumentCaptor.forClass(com.orbit.core.model.TriggerRequest.class);
        verify(executorClient).trigger(eq("http://10.0.0.1:8081"), reqCaptor.capture());
        String logId = reqCaptor.getValue().getLogId();
        when(jobStore.finishLogFromRunning(eq(logId), eq(true), any(), anyLong(), any())).thenReturn(true);
        jobService.handleCallback(TriggerResult.ok(logId, 1L, "http://10.0.0.1:8081", 5L, "done"));
        assertEquals(0, outstanding.outstanding());

        // 同步失败场景：槽位立刻释放，不会永久占用
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any()))
                .thenReturn(TriggerResult.fail("f", 1L, "http://10.0.0.1:8081", 0, "executor saturated"));
        TriggerResult rejected = jobService.triggerNow("jobGuard2", null);
        assertFalse(rejected.isAccepted());
        assertEquals(0, outstanding.outstanding());
    }

    @Test
    void dispatchMarksFailedWhenExecutorRejectsTrigger() {
        JobInfo job = newJob("jobRejected", "app");
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(node("http://10.0.0.1:8081")));
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(null);
        // 执行器同步失败（饱和、handler 不存在等）：accepted=false
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any())).thenReturn(
                TriggerResult.fail("log-r", 1L, "http://10.0.0.1:8081", 3L, "executor saturated"));

        TriggerResult result = jobService.dispatch(job, null);

        assertEquals(false, result.isSuccess());
        assertEquals(false, result.isAccepted());
        // 不会有回传到达，必须立刻收敛到 FAILED，不能留在 RUNNING 等孤儿回收
        verify(jobStore, times(1)).finishLog(anyString(), eq("FAILED"),
                eq("http://10.0.0.1:8081"), anyLong(), eq("executor saturated"));
    }

    @Test
    void handleCallbackConvergesRunningLog() {
        TriggerResult cb = TriggerResult.ok("log-cb", 1L, "http://10.0.0.9:8081", 4321L, "done");
        when(jobStore.finishLogFromRunning("log-cb", true, "http://10.0.0.9:8081", 4321L, "done"))
                .thenReturn(true);

        assertTrue(jobService.handleCallback(cb));
        verify(jobStore, times(1)).finishLogFromRunning("log-cb", true, "http://10.0.0.9:8081", 4321L, "done");
    }

    @Test
    void handleCallbackIsIdempotentOnDuplicate() {
        // 存储层匹配不到 RUNNING 行 -> 返回 false；重复回传不能被当成错误
        when(jobStore.finishLogFromRunning(anyString(), org.mockito.ArgumentMatchers.anyBoolean(),
                any(), anyLong(), any())).thenReturn(false);
        TriggerResult cb = TriggerResult.ok("log-dup", 1L, "n", 1L, "done");

        assertEquals(false, jobService.handleCallback(cb));
        assertEquals(false, jobService.handleCallback(cb));
        verify(jobStore, times(2)).finishLogFromRunning(anyString(),
                org.mockito.ArgumentMatchers.anyBoolean(), any(), anyLong(), any());
    }

    @Test
    void handleCallbacksProcessesWholeBatchAndCountsApplied() {
        // 批量里一条有效、一条重复（已被忽略），不能因为其中一条而整批失败
        when(jobStore.finishLogFromRunning(eq("log-b1"), org.mockito.ArgumentMatchers.anyBoolean(),
                any(), anyLong(), any())).thenReturn(true);
        when(jobStore.finishLogFromRunning(eq("log-b2"), org.mockito.ArgumentMatchers.anyBoolean(),
                any(), anyLong(), any())).thenReturn(false);

        int applied = jobService.handleCallbacks(java.util.Arrays.asList(
                TriggerResult.ok("log-b1", 1L, "n", 1L, "ok"),
                TriggerResult.ok("log-b2", 1L, "n", 1L, "ok")));

        assertEquals(1, applied);
        assertEquals(0, jobService.handleCallbacks(null));
        assertEquals(0, jobService.handleCallbacks(java.util.Collections.<TriggerResult>emptyList()));
    }

    @Test
    void handleCallbacksRejectsBatchAboveServerLimit() {
        // 服务端守门：超过单批上限直接 400（IllegalArgumentException），整批不处理。
        // 防止失控/恶意客户端用海量条目把回传端点变成内存与数据库压力源。
        java.util.List<TriggerResult> oversized =
                new java.util.ArrayList<TriggerResult>(JobService.MAX_CALLBACK_BATCH + 1);
        for (int i = 0; i <= JobService.MAX_CALLBACK_BATCH; i++) {
            oversized.add(TriggerResult.ok("log-" + i, 1L, "n", 1L, "ok"));
        }

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> jobService.handleCallbacks(oversized));
        assertTrue(ex.getMessage().contains("max " + JobService.MAX_CALLBACK_BATCH));
        verify(jobStore, never()).finishLogFromRunning(anyString(), org.mockito.ArgumentMatchers.anyBoolean(),
                any(), anyLong(), any());

        // 恰好在上限内的一批正常受理（不触发守门）
        when(jobStore.finishLogFromRunning(anyString(), org.mockito.ArgumentMatchers.anyBoolean(),
                any(), anyLong(), any())).thenReturn(true);
        java.util.List<TriggerResult> atLimit =
                new java.util.ArrayList<TriggerResult>(JobService.MAX_CALLBACK_BATCH);
        for (int i = 0; i < JobService.MAX_CALLBACK_BATCH; i++) {
            atLimit.add(TriggerResult.ok("ok-" + i, 1L, "n", 1L, "ok"));
        }
        assertEquals(JobService.MAX_CALLBACK_BATCH, jobService.handleCallbacks(atLimit));
    }

    @Test
    void handleCallbackWithoutLogIdIsIgnored() {
        assertEquals(false, jobService.handleCallback(null));
        assertEquals(false, jobService.handleCallback(TriggerResult.ok(null, 1L, "n", 0, "x")));
        verify(jobStore, never()).finishLogFromRunning(anyString(),
                org.mockito.ArgumentMatchers.anyBoolean(), any(), anyLong(), any());
    }

    @Test
    void dispatchNoOnlineExecutor() {
        JobInfo job = newJob("jobC", "app");
        when(registry.listByApp("app")).thenReturn(Collections.<ExecutorNode>emptyList());

        TriggerResult result = jobService.dispatch(job, null);

        assertNotNull(result);
        assertEquals(false, result.isSuccess());
        assertTrue(result.getMessage().contains("no online executor"));
        // 耗时为真实派发耗时（通常 0ms，偶尔 1ms），不能断言精确值
        verify(jobStore, times(1)).finishLog(anyString(), eq("FAILED"),
                eq((String) null), anyLong(), anyString());
        verify(executorClient, never()).trigger(anyString(), any());
    }

    @Test
    void createRejectsInvalidRouteStrategy() {
        JobInfo job = newJob("jobD", "app");
        job.setRouteStrategy("SHORTEST");

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> jobService.create(job));
        assertTrue(ex.getMessage().contains("ROUND/RANDOM/FIRST"));
    }

    @Test
    void createNormalizesRouteStrategyCase() {
        JobInfo job = newJob("jobE", "app");
        job.setRouteStrategy("random");
        when(jobStore.findJobByName("jobE")).thenReturn(Optional.<JobInfo>empty());
        when(jobStore.saveJob(any())).thenAnswer(inv -> inv.getArgument(0));

        JobInfo saved = jobService.create(job);
        assertEquals("RANDOM", saved.getRouteStrategy());
    }

    @Test
    void createTranslatesDuplicateKeyRace() {
        JobInfo job = newJob("jobF", "app");
        when(jobStore.findJobByName("jobF")).thenReturn(Optional.<JobInfo>empty());
        // 并发竞态：检查时不存在，插入时撞唯一键
        when(jobStore.saveJob(any())).thenThrow(
                new org.springframework.dao.DataIntegrityViolationException("uk_orbit_job_name"));

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> jobService.create(job));
        assertTrue(ex.getMessage().contains("job already exists"));
    }

    @Test
    void initFailsFastOnInvalidTimezone() {
        properties.setTimezone("Mars/Olympus");
        when(jobStore.findAllJobs()).thenReturn(Collections.<JobInfo>emptyList());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> jobService.init());
        assertTrue(ex.getMessage().contains("timezone"));
    }

    @Test
    void initAcceptsValidTimezoneAndLoadsJobs() {
        when(jobStore.findAllJobs()).thenReturn(Collections.<JobInfo>emptyList());
        jobService.init();
        verify(jobStore, times(1)).findAllJobs();
    }

    /**
     * Quartz 编排失败时必须回滚已落库的新任务行：
     * 否则会留下一条「任务列表里看得见、却永远不会触发」的幽灵任务，
     * 且每次重启 init() 都会重新装载它并再报一次同样的错。
     */
    @Test
    void createRollsBackWhenQuartzSchedulingFails() throws Exception {
        JobInfo job = newJob("jobG", "app");
        when(jobStore.findJobByName("jobG")).thenReturn(Optional.<JobInfo>empty());
        when(jobStore.saveJob(any())).thenAnswer(inv -> inv.getArgument(0));
        when(scheduler.checkExists(any(JobKey.class))).thenThrow(new JobPersistenceException("quartz down"));

        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> jobService.create(job));

        assertTrue(ex.getMessage().contains("schedule failed"), ex.getMessage());
        verify(jobStore, times(1)).deleteJob("jobG");
    }

    /**
     * 更新场景的对称保证：Quartz 没换上新计划时，数据库也不能留着新定义，
     * 否则会出现「接口说已保存、Quartz 仍按旧 cron 跑」的永久不一致。
     * 同时验证回滚写入的版本号已对齐库里的当前值（否则会被乐观锁判定为冲突）。
     */
    @Test
    void updateRollsBackWhenQuartzSchedulingFails() throws Exception {
        JobInfo existing = newJob("jobH", "app");
        existing.setDescription("original");
        when(jobStore.findJobByName("jobH")).thenReturn(Optional.of(existing));
        when(jobStore.saveJob(any())).thenAnswer(inv -> inv.getArgument(0));
        when(scheduler.checkExists(any(JobKey.class))).thenThrow(new JobPersistenceException("quartz down"));

        JobInfo input = newJob("jobH", "app");
        input.setDescription("updated");

        assertThrows(IllegalStateException.class, () -> jobService.update("jobH", input));

        ArgumentCaptor<JobInfo> captor = ArgumentCaptor.forClass(JobInfo.class);
        verify(jobStore, times(2)).saveJob(captor.capture());
        JobInfo rolledBack = captor.getAllValues().get(1);
        // 第二次写入的必须是「更新前」的定义
        assertEquals("original", rolledBack.getDescription());
        // 快照版本号 0 -> 回滚时对齐到库里的当前值 1，避免乐观锁空更新
        assertEquals(1, rolledBack.getVersion());
    }

    /**
     * 超长字段必须在入参校验阶段被拒绝（明确的 400），
     * 而不是等入库时抛 DataIntegrityViolationException、被压成无信息的 500。
     */
    @Test
    void createRejectsOversizedAppNameAndHandler() {
        JobInfo longApp = newJob("jobI", "app");
        longApp.setAppName(repeat('a', 65));   // app_name VARCHAR(64)
        IllegalArgumentException appEx = assertThrows(IllegalArgumentException.class,
                () -> jobService.create(longApp));
        assertTrue(appEx.getMessage().contains("appName too long"), appEx.getMessage());

        JobInfo longHandler = newJob("jobJ", "app");
        longHandler.setHandler(repeat('h', 129));   // handler VARCHAR(128)
        IllegalArgumentException handlerEx = assertThrows(IllegalArgumentException.class,
                () -> jobService.create(longHandler));
        assertTrue(handlerEx.getMessage().contains("handler too long"), handlerEx.getMessage());
    }

    private static String repeat(char c, int times) {
        StringBuilder sb = new StringBuilder(times);
        for (int i = 0; i < times; i++) {
            sb.append(c);
        }
        return sb.toString();
    }

    // ==================== 失败重试 / 告警扩展点 / 任务级串行开关 ====================

    /**
     * 触发级重试闭环：首次派发同步失败时安排重试（新 logId），重试到点后按
     * 库里最新任务定义重新走完整派发链路并成功。
     */
    @Test
    void triggerRetryRerunsDispatchAfterSyncFailure() throws Exception {
        JobInfo job = newJob("jobRetry", "app");
        job.setRetryCount(2);
        job.setRetryIntervalSeconds(0); // 立即重试，测试无等待
        when(jobStore.findJobByName("jobRetry")).thenReturn(Optional.of(job));
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(node("http://10.0.0.1:8081")));
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(null);
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any()))
                .thenReturn(TriggerResult.fail("f1", 1L, "http://10.0.0.1:8081", 0, "connect timed out"))
                .thenReturn(TriggerResult.accepted("ok", 1L, "http://10.0.0.1:8081", "accepted"));

        TriggerResult first = jobService.triggerNow("jobRetry", null);
        assertFalse(first.isAccepted());
        // 首次失败必须标注重试计划，且不告警（重试还有额度）
        assertTrue(first.getMessage().contains("trigger retry 2/3"), first.getMessage());
        assertTrue(alerts.isEmpty(), "no alert while retries remain");

        // 重试在定时器线程上异步执行：after() 最多等 5 秒，直到第二次派发发生
        verify(executorClient, org.mockito.Mockito.after(5000).times(2))
                .trigger(eq("http://10.0.0.1:8081"), any());
        assertTrue(alerts.isEmpty(), "retry succeeded, still no alert");
    }

    /** 重试额度用尽才告警：retryCount=0 时首次同步失败即终局，投递 TRIGGER_FAILED。 */
    @Test
    void triggerFiresAlertWhenRetriesExhausted() throws Exception {
        JobInfo job = newJob("jobNoRetry", "app");
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(node("http://10.0.0.1:8081")));
        when(registry.route(anyList(), eq("app"), anyString(), anyString())).thenReturn(null);
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any()))
                .thenReturn(TriggerResult.fail("f", 1L, "http://10.0.0.1:8081", 0, "executor saturated"));

        TriggerResult result = jobService.dispatch(job, null);
        assertFalse(result.isAccepted());
        assertFalse(result.getMessage().contains("trigger retry"), result.getMessage());

        long deadline = System.currentTimeMillis() + 2000L;
        while (alerts.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(1, alerts.size());
        JobAlertEvent event = alerts.get(0);
        assertEquals(JobAlertEvent.TRIGGER_FAILED, event.eventType());
        assertEquals("jobNoRetry", event.jobName());
        assertEquals("app", event.appName());
        assertEquals("http://10.0.0.1:8081", event.executorAddress());
    }

    /** 回传 FAILED（执行器侧执行级重试已耗尽）必须投递 EXECUTION_FAILED，且能反查补齐任务上下文。 */
    @Test
    void callbackFailureFiresExecutionFailedAlertWithFallbackContext() throws Exception {
        when(jobStore.finishLogFromRunning(eq("log-fail"), eq(false), any(), anyLong(), any()))
                .thenReturn(true);
        JobLog row = new JobLog();
        row.setLogId("log-fail");
        row.setJobName("jobCb");
        row.setAppName("app");
        row.setHandler("dailyReport");
        when(jobStore.findLogByLogId("log-fail")).thenReturn(Optional.of(row));

        // 回传不带任务上下文（模拟旧版执行器）：调度中心按 logId 反查补齐
        TriggerResult cb = TriggerResult.fail("log-fail", 1L, "http://n:8081", 10L, "boom");
        assertTrue(jobService.handleCallback(cb));

        long deadline = System.currentTimeMillis() + 2000L;
        while (alerts.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(1, alerts.size());
        JobAlertEvent event = alerts.get(0);
        assertEquals(JobAlertEvent.EXECUTION_FAILED, event.eventType());
        assertEquals("jobCb", event.jobName());
        assertEquals("app", event.appName());
        assertEquals("dailyReport", event.handler());
        assertEquals("boom", event.message());
    }

    /** 回传成功不得产生告警。 */
    @Test
    void callbackSuccessDoesNotAlert() throws Exception {
        when(jobStore.finishLogFromRunning(eq("log-ok"), eq(true), any(), anyLong(), any())).thenReturn(true);
        assertTrue(jobService.handleCallback(TriggerResult.ok("log-ok", 1L, "n", 1L, "OK")));
        Thread.sleep(100L);
        assertTrue(alerts.isEmpty());
    }

    /** 任务级串行开关覆盖：serialExecution=false 时，上一轮在跑也不拒绝手动触发。 */
    @Test
    void serialExecutionPerJobOverrideAllowsParallelRuns() {
        JobInfo job = newJob("jobParallel", "app");
        job.setSerialExecution(false); // 任务级允许并发，覆盖全局默认（串行）
        when(jobStore.findJobByName("jobParallel")).thenReturn(Optional.of(job));
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(node("http://10.0.0.1:8081")));
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any()))
                .thenReturn(TriggerResult.accepted("p-log", 1L, "http://10.0.0.1:8081", "accepted"));
        // 预占用串行守卫（模拟上一轮执行尚未收敛）
        assertTrue(outstanding.tryAcquire("jobParallel", "prev-log"));

        TriggerResult result = jobService.triggerNow("jobParallel", null);

        // 守卫被任务级关闭：不拒绝、正常派发受理
        assertTrue(result.isAccepted(), "per-job serial=false must bypass the guard, got: " + result.getMessage());
        verify(executorClient, org.mockito.Mockito.atLeastOnce()).trigger(anyString(), any());
        outstanding.release("prev-log");
    }

    /** 重试参数越界必须在创建/更新时以 400 拒绝，而不是静默入库。 */
    @Test
    void createRejectsInvalidRetrySettings() {
        JobInfo tooMany = newJob("jobR1", "app");
        tooMany.setRetryCount(JobService.MAX_RETRY_COUNT + 1);
        IllegalArgumentException countEx = assertThrows(IllegalArgumentException.class,
                () -> jobService.create(tooMany));
        assertTrue(countEx.getMessage().contains("retryCount"), countEx.getMessage());

        JobInfo negative = newJob("jobR2", "app");
        negative.setRetryCount(-1);
        assertThrows(IllegalArgumentException.class, () -> jobService.create(negative));

        JobInfo tooLongInterval = newJob("jobR3", "app");
        tooLongInterval.setRetryIntervalSeconds(JobService.MAX_RETRY_INTERVAL_SECONDS + 1);
        IllegalArgumentException intervalEx = assertThrows(IllegalArgumentException.class,
                () -> jobService.create(tooLongInterval));
        assertTrue(intervalEx.getMessage().contains("retryIntervalSeconds"), intervalEx.getMessage());
    }

    /** 一致性哈希路由：合法值（含小写）通过校验并规范化。 */
    @Test
    void createAcceptsConsistentHashRoute() {
        JobInfo job = newJob("jobHash", "app");
        job.setRouteStrategy("consistent_hash");
        when(jobStore.findJobByName("jobHash")).thenReturn(Optional.<JobInfo>empty());
        when(jobStore.saveJob(any())).thenAnswer(inv -> inv.getArgument(0));

        JobInfo saved = jobService.create(job);
        assertEquals("CONSISTENT_HASH", saved.getRouteStrategy());
    }
}

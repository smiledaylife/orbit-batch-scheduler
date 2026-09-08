package com.orbit.admin.service;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.JobInfo;
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
 * <ul>
 *   <li>dispatch 单次查询：一次派发只允许调用一次 registry.listByApp；</li>
 *   <li>failover：首选节点连接拒绝时立即摘除并切换下一个节点；</li>
 *   <li>routeStrategy 合法性校验与规范化；</li>
 *   <li>create 的唯一键竞态兜底（DataIntegrityViolationException → 友好 400 语义）；</li>
 *   <li>Quartz 编排失败时 create / update 的数据库回滚（不留幽灵任务、不留新旧不一致）；</li>
 *   <li>超出数据库列宽的字段在入参校验阶段被拒绝（400 而非无信息的 500）；</li>
 *   <li>timezone 非法值启动失败（fail-fast）。</li>
 * </ul>
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
    private JobService jobService;

    @BeforeEach
    void setUp() {
        properties = new AdminProperties();
        properties.setTimezone("Asia/Shanghai");
        properties.setGroup("ORBIT");
        jobService = new JobService(scheduler, jobStore, registry, executorClient, properties);
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
        when(registry.route(anyList(), eq("app"), anyString())).thenReturn(candidates.get(0));
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any())).thenReturn(
                TriggerResult.ok("log-1", 1L, "http://10.0.0.1:8081", 12L, "ok"));

        TriggerResult result = jobService.dispatch(job, null);

        assertTrue(result.isSuccess());
        // 核心断言：单次派发只查一次候选列表（优化前 route(appName,...) 内部会再查一次）
        verify(registry, times(1)).listByApp("app");
        // logId 由 dispatch 内部生成（UUID），用 anyString 匹配；其余参数精确匹配
        verify(jobStore, times(1)).finishLog(anyString(), eq("SUCCESS"),
                eq("http://10.0.0.1:8081"), eq(12L), eq("ok"));
    }

    @Test
    void dispatchFailsOverOnUnreachableNode() {
        JobInfo job = newJob("jobB", "app");
        ExecutorNode n1 = node("http://10.0.0.1:8081");
        ExecutorNode n2 = node("http://10.0.0.2:8081");
        when(registry.listByApp("app")).thenReturn(java.util.Arrays.asList(n1, n2));
        when(registry.route(anyList(), eq("app"), anyString())).thenReturn(n1);
        when(executorClient.trigger(eq("http://10.0.0.1:8081"), any())).thenReturn(
                TriggerResult.fail("log-2", 1L, "http://10.0.0.1:8081", 0, "Connection refused"));
        when(executorClient.trigger(eq("http://10.0.0.2:8081"), any())).thenReturn(
                TriggerResult.ok("log-2", 1L, "http://10.0.0.2:8081", 5L, "ok"));

        TriggerResult result = jobService.dispatch(job, null);

        assertTrue(result.isSuccess());
        // 不可达节点被立即摘除（不等心跳超时）
        verify(registry, times(1)).remove("app", "http://10.0.0.1:8081");
        // logId 由 dispatch 内部生成（UUID），用 anyString 匹配
        verify(jobStore, times(1)).finishLog(anyString(), eq("SUCCESS"),
                eq("http://10.0.0.2:8081"), anyLong(), anyString());
    }

    @Test
    void dispatchNoOnlineExecutor() {
        JobInfo job = newJob("jobC", "app");
        when(registry.listByApp("app")).thenReturn(Collections.<ExecutorNode>emptyList());

        TriggerResult result = jobService.dispatch(job, null);

        assertNotNull(result);
        assertEquals(false, result.isSuccess());
        assertTrue(result.getMessage().contains("no online executor"));
        verify(jobStore, times(1)).finishLog(anyString(), eq("FAILED"),
                eq((String) null), eq(0L), anyString());
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
}

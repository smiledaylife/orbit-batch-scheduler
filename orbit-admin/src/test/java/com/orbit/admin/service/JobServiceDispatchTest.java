package com.orbit.admin.service;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.quartz.Scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link JobService#dispatch} 派发逻辑单元测试（纯 Mockito，不启动 Spring 上下文）。
 * 覆盖：无在线执行器、节点不可达时摘除并转移、业务失败不转移、参数合并、派发预算共享。
 */
class JobServiceDispatchTest {

    private static final String APP = "demo-executor";

    private static final String ADDR_1 = "http://10.0.0.1:8081";

    private static final String ADDR_2 = "http://10.0.0.2:8081";

    private Scheduler scheduler;
    private JobStore jobStore;
    private ExecutorRegistry registry;
    private ExecutorClient executorClient;
    private AdminProperties properties;
    private JobService jobService;

    @BeforeEach
    void setUp() {
        scheduler = mock(Scheduler.class);
        jobStore = mock(JobStore.class);
        registry = mock(ExecutorRegistry.class);
        executorClient = mock(ExecutorClient.class);
        properties = new AdminProperties();
        jobService = new JobService(scheduler, jobStore, registry, executorClient, properties);
    }

    @Test
    void failFastWhenNoOnlineExecutor() {
        when(registry.listByApp(APP)).thenReturn(Collections.<ExecutorNode>emptyList());

        TriggerResult result = jobService.dispatch(job(120), null);

        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("no online executor"));
        // 没有可用节点时不应发起任何 HTTP 调用，也不该走路由
        verify(executorClient, never()).trigger(anyString(), any(TriggerRequest.class));
        verify(registry, never()).route(anyString(), anyString());
        // 日志必须从 RUNNING 收敛到终态
        verify(jobStore).finishLog(anyString(), eq("FAILED"), isNull(), eq(0L), anyString());
    }

    @Test
    void evictUnreachableNodeAndFailoverToNext() {
        ExecutorNode first = node(ADDR_1);
        ExecutorNode second = node(ADDR_2);
        when(registry.listByApp(APP)).thenReturn(new ArrayList<ExecutorNode>(Arrays.asList(first, second)));
        when(registry.route(APP, "ROUND")).thenReturn(first);
        when(executorClient.trigger(eq(ADDR_1), any(TriggerRequest.class)))
                .thenReturn(TriggerResult.fail("log", 1L, ADDR_1, 0L, "java.net.ConnectException: Connection refused"));
        when(executorClient.trigger(eq(ADDR_2), any(TriggerRequest.class)))
                .thenReturn(TriggerResult.ok("log", 1L, ADDR_2, 12L, "OK"));

        TriggerResult result = jobService.dispatch(job(120), null);

        assertTrue(result.isSuccess());
        // 不可达的旧 Pod 立刻摘除，不等心跳超时
        verify(registry).remove(APP, ADDR_1);
        verify(registry, never()).remove(APP, ADDR_2);
        // 耗时以执行器返回的为准
        verify(jobStore).finishLog(anyString(), eq("SUCCESS"), eq(ADDR_2), eq(12L), eq("OK"));
    }

    @Test
    void businessFailureDoesNotFailover() {
        ExecutorNode first = node(ADDR_1);
        ExecutorNode second = node(ADDR_2);
        when(registry.listByApp(APP)).thenReturn(new ArrayList<ExecutorNode>(Arrays.asList(first, second)));
        when(registry.route(APP, "ROUND")).thenReturn(first);
        when(executorClient.trigger(eq(ADDR_1), any(TriggerRequest.class)))
                .thenReturn(TriggerResult.fail("log", 1L, ADDR_1, 8L, "handler not found on this executor: dailyReport"));

        TriggerResult result = jobService.dispatch(job(120), null);

        assertFalse(result.isSuccess());
        // 执行器活着、只是业务失败：换节点重跑会造成重复执行
        verify(registry, never()).remove(anyString(), anyString());
        verify(executorClient, never()).trigger(eq(ADDR_2), any(TriggerRequest.class));
        verify(jobStore).finishLog(anyString(), eq("FAILED"), eq(ADDR_1), eq(8L), anyString());
    }

    @Test
    void lastUnreachableNodeIsNotEvicted() {
        ExecutorNode only = node(ADDR_1);
        when(registry.listByApp(APP)).thenReturn(new ArrayList<ExecutorNode>(Collections.singletonList(only)));
        when(registry.route(APP, "ROUND")).thenReturn(only);
        when(executorClient.trigger(eq(ADDR_1), any(TriggerRequest.class)))
                .thenReturn(TriggerResult.fail("log", 1L, ADDR_1, 0L, "connect timed out"));

        TriggerResult result = jobService.dispatch(job(120), null);

        assertFalse(result.isSuccess());
        // 只有一个候选时没有可转移的节点，摘除只会让下一次调度白白丢掉一次心跳窗口
        verify(registry, never()).remove(anyString(), anyString());
    }

    @Test
    void mergesJobParamsWithTriggerParams() {
        ExecutorNode first = node(ADDR_1);
        when(registry.listByApp(APP)).thenReturn(new ArrayList<ExecutorNode>(Collections.singletonList(first)));
        when(registry.route(APP, "ROUND")).thenReturn(first);
        when(executorClient.trigger(eq(ADDR_1), any(TriggerRequest.class)))
                .thenReturn(TriggerResult.ok("log", 1L, ADDR_1, 3L, "OK"));

        Map<String, Object> extra = new HashMap<String, Object>();
        extra.put("bizDate", "today");   // 覆盖任务静态参数
        extra.put("mode", "manual");     // 新增本次触发参数
        jobService.dispatch(job(120), extra);

        ArgumentCaptor<TriggerRequest> captor = ArgumentCaptor.forClass(TriggerRequest.class);
        verify(executorClient).trigger(eq(ADDR_1), captor.capture());
        TriggerRequest sent = captor.getValue();
        assertEquals("today", sent.getParams().get("bizDate"));
        assertEquals("manual", sent.getParams().get("mode"));
        assertEquals("static", sent.getParams().get("keep"));
        assertEquals("dailyReportJob", sent.getJobName());
        assertEquals("dailyReport", sent.getHandler());
        assertEquals(1L, sent.getJobId());
        assertTrue(sent.getLogId() != null && !sent.getLogId().isEmpty());
    }

    @Test
    void failoverAttemptsShareOneTimeoutBudget() {
        ExecutorNode first = node(ADDR_1);
        ExecutorNode second = node(ADDR_2);
        ExecutorNode third = node("http://10.0.0.3:8081");
        when(registry.listByApp(APP))
                .thenReturn(new ArrayList<ExecutorNode>(Arrays.asList(first, second, third)));
        when(registry.route(APP, "ROUND")).thenReturn(first);
        when(executorClient.trigger(anyString(), any(TriggerRequest.class)))
                .thenReturn(TriggerResult.fail("log", 1L, "x", 0L, "Connection refused"));

        jobService.dispatch(job(120), null);

        ArgumentCaptor<TriggerRequest> captor = ArgumentCaptor.forClass(TriggerRequest.class);
        verify(executorClient, times(3)).trigger(anyString(), captor.capture());
        List<TriggerRequest> attempts = captor.getAllValues();
        assertEquals(3, attempts.size());
        for (TriggerRequest attempt : attempts) {
            // 每次尝试的读超时都被压在任务预算之内，而不是各自再拿一份 120s
            assertTrue(attempt.getTimeoutSeconds() > 0);
            assertTrue(attempt.getTimeoutSeconds() <= 120,
                    "attempt timeout should not exceed job budget: " + attempt.getTimeoutSeconds());
        }
        // 前两个不可达节点被摘除，最后一个保留（无处可转移）
        verify(registry).remove(APP, ADDR_1);
        verify(registry).remove(APP, ADDR_2);
        verify(registry, never()).remove(APP, "http://10.0.0.3:8081");
        verify(jobStore).finishLog(anyString(), eq("FAILED"), anyString(), anyLong(), anyString());
    }

    // ---------------- 测试夹具 ----------------

    private static JobInfo job(int timeoutSeconds) {
        JobInfo job = new JobInfo();
        job.setId(1L);
        job.setJobName("dailyReportJob");
        job.setAppName(APP);
        job.setHandler("dailyReport");
        job.setRouteStrategy("ROUND");
        job.setTimeoutSeconds(timeoutSeconds);
        job.setEnabled(true);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("bizDate", "yesterday");
        params.put("keep", "static");
        job.setParams(params);
        return job;
    }

    private static ExecutorNode node(String address) {
        ExecutorNode node = new ExecutorNode();
        node.setAppName(APP);
        node.setAddress(address);
        node.setNodeId(address);
        node.setOnline(true);
        return node;
    }
}

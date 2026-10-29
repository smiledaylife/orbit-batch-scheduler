package com.orbit.executor.web;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.bootstrap.ExecutorBootstrap;
import com.orbit.executor.client.ExecutionIdempotency;
import com.orbit.executor.config.ExecutorProperties;
import com.orbit.executor.handler.JobExecutionService;
import com.orbit.executor.handler.JobHandlerRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link ExecutorController} 入口契约测试。
 *
 * 重点回归：幂等后端（Redis）不可用时必须返回结构化失败回执（accepted=false），
 * 不能抛 500 —— 否则调度中心会把本次触发留在 RUNNING 等回传，串行 Lease
 * 被持续续租，该任务在兜底到期前都无法再次触发。
 */
class ExecutorControllerTest {

    @Test
    void redisIdempotencyUnavailableReturnsStructuredRefusalInsteadOfThrowing() {
        JobHandlerRegistry registry = mock(JobHandlerRegistry.class);
        when(registry.has("demo")).thenReturn(true);
        ExecutorBootstrap bootstrap = mock(ExecutorBootstrap.class);
        when(bootstrap.getResolvedNodeId()).thenReturn("node-1");
        JobExecutionService executionService = mock(JobExecutionService.class);
        ExecutionIdempotency idempotency = mock(ExecutionIdempotency.class);
        when(idempotency.tryReserve(anyString()))
                .thenThrow(new IllegalStateException("redis execution idempotency unavailable; request refused"));

        ExecutorController controller = new ExecutorController(registry, new ExecutorProperties(),
                bootstrap, executionService, idempotency);

        TriggerRequest request = new TriggerRequest();
        request.setHandler("demo");
        request.setLogId("log-1");
        request.setJobId(7L);

        TriggerResult result = controller.run(request, null);

        assertFalse(result.isAccepted(), "request must not be accepted when idempotency backend is down");
        assertFalse(result.isSuccess());
        assertTrue(result.getMessage().contains("execution idempotency unavailable"),
                "admin classifies this message for failover / lease release: " + result.getMessage());
        // 确定没有进入执行线程池：调度中心可以安全地换节点或收敛失败
        verify(executionService, never()).submit(any(), any(), anyString());
    }
}

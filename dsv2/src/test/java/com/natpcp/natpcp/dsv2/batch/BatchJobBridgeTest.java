package com.natpcp.natpcp.dsv2.batch;

import com.natpcp.natpcp.dsv2.batch.BatchJobBridge;
import com.orbit.executor.JobContext;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.dao.CannotSerializeTransactionException;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * BatchJobBridge 单元测试：orbit JobContext -> Spring Batch Job 桥接。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("BatchJobBridge 测试")
class BatchJobBridgeTest {

    @Mock
    private JobLauncher jobLauncher;

    @Mock
    private Job job;

    @Mock
    private JobExecution jobExecution;

    private BatchJobBridge bridge;

    @BeforeEach
    void setUp() {
        bridge = new BatchJobBridge();
        // 手动注入（字段私有，用反射简化: 直接 set 访问失败时用反射）
        setField(bridge, "jobLauncher", jobLauncher);
    }

    private static void setField(Object target, String fieldName, Object value) {
        try {
            java.lang.reflect.Field f = target.getClass().getDeclaredField(fieldName);
            f.setAccessible(true);
            f.set(target, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @DisplayName("run: COMPLETED 返回 success，JobParameters 携带 handlerName/triggerJobId/runTime/jobParams")
    @Test
    void run_shouldReturnSuccess_whenCompleted() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("connectionJobId", "1001");
        JobContext context = new JobContext(99L, "jobName", "ConfigCheck", "log-1", params);
        when(jobLauncher.run(eq(job), any(JobParameters.class))).thenReturn(jobExecution);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.COMPLETED);
        when(jobExecution.getId()).thenReturn(1L);

        String result = bridge.run(job, "ConfigCheck", context);

        assertThat(result).isEqualTo("success");
        ArgumentCaptor<JobParameters> captor = ArgumentCaptor.forClass(JobParameters.class);
        verify(jobLauncher).run(eq(job), captor.capture());
        JobParameters jp = captor.getValue();
        assertThat(jp.getString("handlerName")).isEqualTo("ConfigCheck");
        assertThat(jp.getString("triggerJobId")).isEqualTo("99");
        assertThat(jp.getLong("runTime")).isNotNull();
        assertThat(JobParamsHelper.getParams(jp)).containsEntry("connectionJobId", "1001");
    }

    @DisplayName("run: 重载版本传入覆盖参数生效")
    @Test
    void run_overloadParams_shouldUseOverrideParams() throws Exception {
        Map<String, Object> overrideParams = new HashMap<>();
        overrideParams.put("custom", "overridden");
        JobContext context = new JobContext(5L, "jobName", "TableCheck", "log-2", new HashMap<>());
        when(jobLauncher.run(eq(job), any(JobParameters.class))).thenReturn(jobExecution);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.COMPLETED);
        when(jobExecution.getId()).thenReturn(2L);

        bridge.run(job, "TableCheck", context, overrideParams);

        ArgumentCaptor<JobParameters> captor = ArgumentCaptor.forClass(JobParameters.class);
        verify(jobLauncher).run(eq(job), captor.capture());
        assertThat(JobParamsHelper.getParams(captor.getValue())).containsEntry("custom", "overridden");
    }

    @DisplayName("run: FAILED 时抛出 RuntimeException 并携带状态信息")
    @Test
    void run_shouldThrow_whenNotCompleted() throws Exception {
        Map<String, Object> params = new HashMap<>();
        JobContext context = new JobContext(7L, "jobName", "Task", "log-3", params);
        when(jobLauncher.run(eq(job), any(JobParameters.class))).thenReturn(jobExecution);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.FAILED);
        when(jobExecution.getId()).thenReturn(3L);
        when(jobExecution.getExitStatus()).thenReturn(org.springframework.batch.core.ExitStatus.FAILED);

        assertThatThrownBy(() -> bridge.run(job, "Task", context))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("执行失败")
                .hasMessageContaining("executionId=3");
    }

    @DisplayName("run: jobLauncher 抛异常时向上传播")
    @Test
    void run_shouldPropagate_whenLauncherThrows() throws Exception {
        JobContext context = new JobContext(1L, "jobName", "Task", "log-4", new HashMap<>());
        when(jobLauncher.run(eq(job), any(JobParameters.class)))
                .thenThrow(new RuntimeException("launcher down"));

        assertThatThrownBy(() -> bridge.run(job, "Task", context))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("launcher down");
    }

    @DisplayName("run: PostgreSQL SSI 冲突首次重试成功，每次使用新的 runTime")
    @Test
    void run_shouldRetryOnce_whenSsiConflictThenSucceeds() throws Exception {
        Map<String, Object> params = new HashMap<>();
        params.put("k", "v");
        JobContext context = new JobContext(11L, "jobName", "SsiJob", "log-5", params);
        // 第一次：抛 CannotSerializeTransactionException（PG 可串行化冲突）；第二次：成功
        when(jobLauncher.run(eq(job), any(JobParameters.class)))
                .thenThrow(new CannotSerializeTransactionException("could not serialize access due to read/write dependencies among transactions"))
                .thenReturn(jobExecution);
        when(jobExecution.getStatus()).thenReturn(BatchStatus.COMPLETED);
        when(jobExecution.getId()).thenReturn(5L);

        String result = bridge.run(job, "SsiJob", context);

        assertThat(result).isEqualTo("success");
        // 共调用 2 次（首试 + 1 次重试），且两次 runTime 不同（保证 JobInstance 唯一）
        ArgumentCaptor<JobParameters> captor = ArgumentCaptor.forClass(JobParameters.class);
        verify(jobLauncher, org.mockito.Mockito.times(2)).run(eq(job), captor.capture());
        Long first = captor.getAllValues().get(0).getLong("runTime");
        Long second = captor.getAllValues().get(1).getLong("runTime");
        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(second).isNotEqualTo(first);
    }

    @DisplayName("run: PostgreSQL SSI 冲突连续多次仍失败则抛出")
    @Test
    void run_shouldThrow_whenSsiConflictPersists() throws Exception {
        JobContext context = new JobContext(12L, "jobName", "SsiJob", "log-6", new HashMap<>());
        when(jobLauncher.run(eq(job), any(JobParameters.class)))
                .thenThrow(new CannotSerializeTransactionException("could not serialize access"));

        assertThatThrownBy(() -> bridge.run(job, "SsiJob", context))
                .isInstanceOf(CannotSerializeTransactionException.class)
                .hasMessageContaining("could not serialize access");
        // 首试 + MAX_SSI_RETRY_ATTEMPTS-1 次重试（内部退避睡眠，总调用 3 次）
        verify(jobLauncher, org.mockito.Mockito.times(3)).run(eq(job), any(JobParameters.class));
    }
}
/*
 * AICode End
 */

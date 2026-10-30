package com.natpcp.natpcp.dsv2.jobs;

import com.natpcp.natpcp.dsv2.jobs.*;
import com.orbit.executor.JobContext;
import com.natpcp.natpcp.dsv2.batch.BatchJobBridge;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.Job;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * 六个桥接型 Job Handler 的统一测试：
 * MagtSchedulingJobs / UpdateRedisInfoJobs / TableCheckJob / ConfigCheckJob /
 * BatchMetadataCleanupJobHandler / PvcFileBackupJob / TaskSchedulingNoticeJobs。
 * 模式：@OrbitJob handler -> jobBridge.run(batchJob, handler, context)。
 */
@DisplayName("BatchBridge Jobs Test")
@ExtendWith(MockitoExtension.class)
class BatchBridgeJobsTest {

    @Mock
    private BatchJobBridge jobBridge;

    @Mock
    private Job job;

    private JobContext ctx;

    @BeforeEach
    void setUp() {
        Map<String, Object> params = new HashMap<>();
        params.put("k", "v");
        ctx = new JobContext(100L, "tableCheck", "TableCheck", "log-1", params);
    }

    private void stubBridge(String result) throws Exception {
        when(jobBridge.run(any(Job.class), any(String.class), any(JobContext.class)))
                .thenReturn(result);
    }

    // ---------- MagtSchedulingJobs ----------

    @Test
    @DisplayName("MagtSchedulingJobs.updateRedisFunctionUrls 桥接运行并返回结果")
    void magtUpdateRedisFunctionUrls_shouldBridge() throws Exception {
        MagtSchedulingJobs handler = new MagtSchedulingJobs();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "updateRedisFunctionUrlsJob", job);
        stubBridge("COMPLETED");

        String result = handler.updateRedisFunctionUrls(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    @Test
    @DisplayName("MagtSchedulingJobs.clearNotice 桥接运行并返回结果")
    void magtClearNotice_shouldBridge() throws Exception {
        MagtSchedulingJobs handler = new MagtSchedulingJobs();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "clearNoticeJob", job);
        stubBridge("COMPLETED");

        String result = handler.clearNotice(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    @Test
    @DisplayName("MagtSchedulingJobs: bridge 异常向上传播")
    void magt_shouldPropagate_whenBridgeFails() throws Exception {
        MagtSchedulingJobs handler = new MagtSchedulingJobs();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "updateRedisFunctionUrlsJob", job);
        when(jobBridge.run(any(Job.class), any(String.class), any(JobContext.class)))
                .thenThrow(new RuntimeException("batch failed"));

        assertThatThrownBy(() -> handler.updateRedisFunctionUrls(ctx))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("batch failed");
    }

    // ---------- UpdateRedisInfoJobs ----------

    @Test
    @DisplayName("UpdateRedisInfoJobs.updateRedisInfo 桥接运行")
    void updateRedisInfo_shouldBridge() throws Exception {
        UpdateRedisInfoJobs handler = new UpdateRedisInfoJobs();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "updateRedisInfoJob", job);
        stubBridge("COMPLETED");

        String result = handler.updateRedisInfo(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    // ---------- TableCheckJob ----------

    @Test
    @DisplayName("TableCheckJob.execute 桥接运行")
    void tableCheck_shouldBridge() throws Exception {
        TableCheckJob handler = new TableCheckJob();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "tableCheckJob", job);
        stubBridge("COMPLETED");

        String result = handler.execute(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    // ---------- ConfigCheckJob ----------

    @Test
    @DisplayName("ConfigCheckJob.execute 桥接运行")
    void configCheck_shouldBridge() throws Exception {
        ConfigCheckJob handler = new ConfigCheckJob();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "configCheckJob", job);
        stubBridge("COMPLETED");

        String result = handler.execute(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    // ---------- BatchMetadataCleanupJobHandler ----------

    @Test
    @DisplayName("BatchMetadataCleanupJobHandler.execute 桥接运行")
    void metadataCleanup_shouldBridge() throws Exception {
        BatchMetadataCleanupJobHandler handler = new BatchMetadataCleanupJobHandler();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "batchMetadataCleanupJob", job);
        stubBridge("COMPLETED");

        String result = handler.execute(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    // ---------- PvcFileBackupJob ----------

    @Test
    @DisplayName("PvcFileBackupJob.execute 桥接运行")
    void pvcFileBackup_shouldBridge() throws Exception {
        PvcFileBackupJob handler = new PvcFileBackupJob();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "pvcFileBackupJob", job);
        stubBridge("COMPLETED");

        String result = handler.execute(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }

    // ---------- TaskSchedulingNoticeJobs ----------

    @Test
    @DisplayName("TaskSchedulingNoticeJobs.versionNotice 桥接运行")
    void versionNotice_shouldBridge() throws Exception {
        TaskSchedulingNoticeJobs handler = new TaskSchedulingNoticeJobs();
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "jobBridge", jobBridge);
        org.springframework.test.util.ReflectionTestUtils.setField(handler, "versionNoticeJob", job);
        stubBridge("COMPLETED");

        String result = handler.versionNotice(ctx);

        assertEquals("COMPLETED", result);
        verify(jobBridge).run(job, "TableCheck", ctx);
    }
}
/*
 * AICode End
 */

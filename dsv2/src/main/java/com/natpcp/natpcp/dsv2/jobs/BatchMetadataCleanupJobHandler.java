/*
 * AICode Begin
 * BatchMetadataCleanupJobHandler：JobRepository 元数据清理任务（运维类）。
 *
 * 背景：versionNotice 为分钟级高频触发，每次执行会在 DSV2_BATCH_* 产生一组
 *       JobRepository 元数据记录（multiple表），长期运行会持续膨胀。
 * 本任务由调度中心按天/周 cron 触发，执行 batchMetadataCleanupJob
 * （ProductionJobsConfig），清理 Create_Time 早于保留期（默认 180 天）
 * 的历史 JobExecution/StepExecution/Context/Params 数据（按外键顺序删除）。
 */
package com.natpcp.natpcp.dsv2.jobs;

import com.orbit.executor.JobContext;
import com.orbit.executor.annotation.OrbitJob;
import com.natpcp.natpcp.dsv2.batch.BatchJobBridge;
import com.natpcp.natpcp.dsv2.batch.ProductionJobsConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * JobRepository 元数据清理任务（Spring Batch 桥接）。
 */
@Slf4j
@Component
public class BatchMetadataCleanupJobHandler {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_BATCH_METADATA_CLEANUP)
    private Job batchMetadataCleanupJob;

    /**
     * batchMetadataCleanup 任务入口（桥接 batchMetadataCleanupJob）。
     */
    @OrbitJob("batchMetadataCleanup")
    public String execute(JobContext jobContext) throws Exception {
        log.info("[{}] 元数据清理任务开始, jobId={}", jobContext.getHandler(), jobContext.getJobId());
        String result = jobBridge.run(batchMetadataCleanupJob, jobContext.getHandler(), jobContext);
        log.info("[{}] 元数据清理任务结束", jobContext.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

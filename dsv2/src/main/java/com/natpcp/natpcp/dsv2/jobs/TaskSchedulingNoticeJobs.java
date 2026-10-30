/*
 * AICode Begin
 * TaskSchedulingNoticeJobs：版本消息通知任务迁移执行器（Spring Batch 桥接）。
 *
 * 迁移自 plt-mgmt flow 域 TaskSchedulingNotice（原 @Component + @PostConstruct 自建
 * ScheduledThreadPoolExecutor 1 分钟周期跑 checkVersionAndSendNotice +
 * checkGatewayForwardVersionAndSendNotice）。
 *
 * 迁移要点：
 *   1. 去除线程池生命周期，改为 orbit @OrbitJob("versionNotice") 由调度中心按 cron 触发；
 *   2. 业务逻辑整体进入 Spring Batch versionNoticeJob（VersionNoticeTasklet），
 *      handler 仅做 orbit -> job 桥接；
 *   3. 注意：本任务为分钟级高频触发，每次执行会在 DSV2_BATCH_* 产生一组 JobRepository
 *      元数据记录，由 batchMetadataCleanupJob 定期清理（见生产接入说明）。
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
 * 版本消息通知任务（ds-V2 迁移版，Spring Batch 桥接）。
 */
@Slf4j
@Component
public class TaskSchedulingNoticeJobs {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_VERSION_NOTICE)
    private Job versionNoticeJob;

    /**
     * versionNotice 任务入口（桥接 Spring Batch versionNoticeJob）。
     */
    @OrbitJob("versionNotice")
    public String versionNotice(JobContext jobContext) throws Exception {
        log.info("[{}] 版本消息通知任务开始, jobId={}", jobContext.getHandler(), jobContext.getJobId());
        String result = jobBridge.run(versionNoticeJob, jobContext.getHandler(), jobContext);
        log.info("[{}] 版本消息通知任务结束", jobContext.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

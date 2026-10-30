/*
 * AICode Begin
 * MagtSchedulingJobs：auth 域定时任务迁移执行器（Spring Batch 版本）。
 *
 * 迁移自 plt-mgmt-auth MagtScheduling（@Scheduled 线程池方案），
 * 经 orbit-executor @OrbitJob handler 桥接 Spring Batch Job：
 *   - updateRedisFunctionUrls（原名 handler 保留，内部执行 updateRedisFunctionUrlsJob）
 *   - clearNotice（内部执行 clearNoticeJob）
 * 停用任务 clearUserAccess 不在迁移范围（保留注释说明）。
 *
 * 事务/重启/skip/retry 均由 Spring Batch 统一治理（见 ProductionJobsConfig）。
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
 * 管理端定时任务（ds-V2 迁移版，Spring Batch 桥接）。
 */
@Slf4j
@Component
public class MagtSchedulingJobs {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_UPDATE_REDIS_FUNCTION_URLS)
    private Job updateRedisFunctionUrlsJob;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_CLEAR_NOTICE)
    private Job clearNoticeJob;

    /**
     * 更新缓存中的 URL 信息（原 cron：0 16 * * * ?）。
     */
    @OrbitJob("updateRedisFunctionUrls")
    public String updateRedisFunctionUrls(JobContext context) throws Exception {
        log.info("[{}] 更新功能URL缓存任务开始, jobId={}", context.getHandler(), context.getJobId());
        String result = jobBridge.run(updateRedisFunctionUrlsJob, context.getHandler(), context);
        log.info("[{}] 更新功能URL缓存任务结束", context.getHandler());
        return result;
    }

    // 停用任务（原 cron 0 8 23 * * ?，删除 30 天前用户访问记录）不在本次迁移范围：
    // userAccessService.clearUserAccess(LocalDate.now().minusDays(14)) —— 用户要求仅迁移 7 个生效任务。

    /**
     * 删除消息通知表中 3 天以前的记录（原 cron：0 2 0 * * ?）。
     */
    @OrbitJob("clearNotice")
    public String clearNotice(JobContext context) throws Exception {
        log.info("[{}] 清理历史通知任务开始, jobId={}", context.getHandler(), context.getJobId());
        String result = jobBridge.run(clearNoticeJob, context.getHandler(), context);
        log.info("[{}] 清理历史通知任务结束", context.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

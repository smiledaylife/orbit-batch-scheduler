/*
 * AICode Begin
 * PvcFileBackupJob：pvc 文件备份任务迁移执行器（Spring Batch 版本）。
 *
 * 迁移自 plt-mgmt auth 域 soma 任务 PvcFileBackupJob（原 @SomaHandler("pvcFileBackup")），
 * 原 cron 每月 1 日 10:00；迁移版注册为 orbit @OrbitJob("pvcFileBackup")，
 * 业务逻辑整体进入 Spring Batch pvcFileBackupJob（ProductionJobsConfig），
 * 失败抛异常 -> Spring Batch Job FAILED，发监控（PvcFileBackupTasklet）。
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
 * pvc 文件定时备份和清理（ds-V2 迁移版，Spring Batch 桥接）。
 */
@Slf4j
@Component("pvcFileBackupJobHandler")
public class PvcFileBackupJob {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_PVC_FILE_BACKUP)
    private Job pvcFileBackupJob;

    /**
     * 每月 1 日 10:00 定时清理并备份 pvc 路径下 3 个月前文件（原样本 cron 由调度中心配置）。
     */
    @OrbitJob("pvcFileBackup")
    public String execute(JobContext context) throws Exception {
        log.info("[{}] 接收pvcFileBackup任务, jobId={}", context.getHandler(), context.getJobId());
        String result = jobBridge.run(pvcFileBackupJob, context.getHandler(), context);
        log.info("[{}] pvcFileBackup任务结束", context.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

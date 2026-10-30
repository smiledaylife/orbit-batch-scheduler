/*
 * AICode Begin
 * TableCheckJob：表检测任务迁移执行器（Spring Batch 版本）。
 *
 * 迁移自 plt-mgmt system 域 Soma 任务 TableCheckJob（原 @SomaHandler("TableCheck")）。
 *
 * 迁移要点：
 *   1. 业务逻辑整体进入 Spring Batch tableCheckJob（TableCheckTasklet），
 *      参数通过 JobParameters.jobParams 传入（JobParamsHelper），
 *      调度中心下发的 params（Map）由 BatchJobBridge 自动写入；
 *   2. 最新表名回写调度中心仍复用 AdminJobClient（TableCheckTasklet 内）；
 *   3. EncoderUtil/ConnectionInfo/notice 发送逻辑与原文保持一致。
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
 * 表检测任务（ds-V2 迁移版，Spring Batch 桥接）。
 */
@Slf4j
@Component("tableCheckJobHandler")
public class TableCheckJob {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_TABLE_CHECK)
    private Job tableCheckJob;

    /**
     * TableCheck 任务入口（桥接 Spring Batch tableCheckJob）。
     */
    @OrbitJob("TableCheck")
    public String execute(JobContext jobContext) throws Exception {
        log.info("[{}] 表检测参数：{}", jobContext.getHandler(), jobContext.getParams());
        String result = jobBridge.run(tableCheckJob, jobContext.getHandler(), jobContext);
        log.info("[{}] 表检测任务结束", jobContext.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

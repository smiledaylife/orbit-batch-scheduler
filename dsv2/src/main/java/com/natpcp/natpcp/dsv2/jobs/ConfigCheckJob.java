/*
 * AICode Begin
 * ConfigCheckJob：配置检测任务迁移执行器（Spring Batch 版本）。
 *
 * 迁移自 plt-mgmt system 域 Soma 任务 ConfigCheckJob（原 @SomaHandler("ConfigCheck")）。
 *
 * 迁移要点：
 *   1. 业务逻辑整体进入 Spring Batch configCheckJob（ConfigCheckTasklet），
 *      参数通过 JobParameters.jobParams 传入（JobParamsHelper）；
 *   2. 全部检测分支（LOG_LEVEL / JVM_MEMORY / JVM_THREAD / REDIS / POSTGRES / ROCKET_MQ）保持；
 *   3. 通知发送逻辑与原文保持一致。
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
 * 配置检测任务（ds-V2 迁移版，Spring Batch 桥接）。
 */
@Slf4j
@Component("configCheckJobHandler")
public class ConfigCheckJob {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_CONFIG_CHECK)
    private Job configCheckJob;

    /**
     * ConfigCheck 任务入口（桥接 Spring Batch configCheckJob）。
     */
    @OrbitJob("ConfigCheck")
    public String execute(JobContext jobContext) throws Exception {
        log.info("[{}] 配置检测参数：{}", jobContext.getHandler(), jobContext.getParams());
        String result = jobBridge.run(configCheckJob, jobContext.getHandler(), jobContext);
        log.info("[{}] 配置检测任务结束", jobContext.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

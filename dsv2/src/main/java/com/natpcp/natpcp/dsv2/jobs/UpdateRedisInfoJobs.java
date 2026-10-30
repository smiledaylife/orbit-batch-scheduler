/*
 * AICode Begin
 * UpdateRedisInfoJobs：Redis 数据刷新任务迁移执行器（Spring Batch 版本）。
 *
 * 迁移自 plt-mgmt flow 域 UpdateRedisInfoImpl（原 @PostConstruct 自建
 * ScheduledThreadPoolExecutor，周期 300s 运行 task()）。
 *
 * 迁移要点：
 *   1. 去除线程池热更新生命周期，改为 orbit @OrbitJob("updateRedisInfo") 由调度中心触发；
 *   2. 业务逻辑整体进入 Spring Batch updateRedisInfoJob（ProductionJobsConfig），
 *      4 步串联：forwardRule 增量 -> 规则表 chunk 同步（faultTolerant skip/retry）-> 配置版本 -> 大网关版本；
 *   3. handler 仅做 orbit -> job 桥接。
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
 * Redis 配置数据定时刷新任务（ds-V2 迁移版，Spring Batch 桥接）。
 */
@Slf4j
@Component
public class UpdateRedisInfoJobs {

    @Autowired
    private BatchJobBridge jobBridge;

    @Autowired
    @Qualifier(ProductionJobsConfig.JOB_UPDATE_REDIS_INFO)
    private Job updateRedisInfoJob;

    /**
     * updateRedisInfo 任务入口（桥接 Spring Batch updateRedisInfoJob）。
     */
    @OrbitJob("updateRedisInfo")
    public String updateRedisInfo(JobContext jobContext) throws Exception {
        log.info("[{}] 刷新redis缓存定时任务开始, jobId={}", jobContext.getHandler(), jobContext.getJobId());
        String result = jobBridge.run(updateRedisInfoJob, jobContext.getHandler(), jobContext);
        log.info("[{}] 刷新redis缓存定时任务结束", jobContext.getHandler());
        return result;
    }
}
/*
 * AICode End
 */

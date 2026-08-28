package com.orbit.admin.quartz;

import com.orbit.admin.service.JobService;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.JobInfo;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Quartz 统一调度触发任务类。
 * <p>所有注册到 Quartz 的定时任务均统一关联此 Job 实现类。
 * 当 Cron 触发时，本类根据任务名称从数据库拉取最新任务状态，并委托给 {@link JobService#dispatch}
 * 进行路由选择和远程 HTTP 派发。
 */
public class OrbitQuartzJob implements Job {

    /**
     * JobDataMap 中传递任务名称的键名常量
     */
    public static final String KEY_JOB_NAME = "jobName";

    private static final Logger log = LoggerFactory.getLogger(OrbitQuartzJob.class);

    @Autowired
    private JobService jobService;

    @Autowired
    private JobStore jobStore;

    /**
     * Quartz 定时触发核心入口。
     *
     * @param context Quartz 执行上下文（包含 Trigger、JobDetail 等信息）
     * @throws JobExecutionException Quartz 任务执行异常
     */
    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        // 1. 从 JobDataMap 中解析任务唯一名称
        String jobName = context.getMergedJobDataMap().getString(KEY_JOB_NAME);
        if (jobName == null || jobName.isEmpty()) {
            jobName = context.getJobDetail().getKey().getName();
        }

        try {
            // 2. 从持久化存储中获取任务最新状态，校验任务是否存在及是否处于启用状态
            JobInfo job = jobStore.findJobByName(jobName).orElse(null);
            if (job == null || !job.isEnabled()) {
                log.info("[orbit-admin] skip fire, job missing or disabled: {}", jobName);
                return;
            }

            // 3. 调度派发执行
            jobService.dispatch(job, null);
        } catch (Exception e) {
            // 捕获所有异常，避免异常抛出导致 Quartz 将任务标记为损坏或反复 misfire 重试
            log.error("[orbit-admin] quartz fire failed for {}", jobName, e);
        }
    }
}

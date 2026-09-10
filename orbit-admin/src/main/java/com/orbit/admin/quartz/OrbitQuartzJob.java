package com.orbit.admin.quartz;

import com.orbit.admin.dispatch.DispatchExecutor;
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
 * 所有注册到 Quartz 的定时任务都关联此 Job 实现类。
 * Cron 到点时，本类根据任务名称从数据库读取最新任务状态，然后交给
 * {@link DispatchExecutor} 异步派发，自身在微秒级内返回。
 *
 * 不在 Quartz 工作线程上直接派发，是因为派发是对执行器的同步阻塞 HTTP 调用：
 * 占满 Quartz 线程池会让其它任务的 Cron 到点也发不出去。
 * 「同一任务不并发执行」的保证同样由 {@link DispatchExecutor} 承担
 * （orbit.admin.dispatch-serial-per-job），因此这里不需要 @DisallowConcurrentExecution。
 */
public class OrbitQuartzJob implements Job {

    /**
     * JobDataMap 中传递任务名称的键名常量
     */
    public static final String KEY_JOB_NAME = "jobName";

    private static final Logger log = LoggerFactory.getLogger(OrbitQuartzJob.class);

    /** 触发线程池：Quartz 工作线程只做「读元数据 + 投递」，不等派发完成 */
    @Autowired
    private DispatchExecutor dispatchExecutor;

    /** 任务与日志存储，用于按名读取任务元数据 */
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

            // 3. 交给派发通道异步执行，立即释放 Quartz 工作线程
            dispatchExecutor.submit(job);
        } catch (Exception e) {
            // 捕获所有异常，避免异常抛出导致 Quartz 将任务标记为损坏或反复 misfire 重试
            log.error("[orbit-admin] quartz fire failed for {}", jobName, e);
        }
    }
}

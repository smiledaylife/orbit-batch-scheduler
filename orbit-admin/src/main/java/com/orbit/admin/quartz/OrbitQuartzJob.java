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
 * Quartz 统一 Job：触发后由 JobService 派发到在线执行器。
 */
public class OrbitQuartzJob implements Job {

    public static final String KEY_JOB_NAME = "jobName";

    private static final Logger log = LoggerFactory.getLogger(OrbitQuartzJob.class);

    @Autowired
    private JobService jobService;
    @Autowired
    private JobStore jobStore;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        String jobName = context.getMergedJobDataMap().getString(KEY_JOB_NAME);
        if (jobName == null || jobName.isEmpty()) {
            jobName = context.getJobDetail().getKey().getName();
        }
        try {
            JobInfo job = jobStore.findJobByName(jobName).orElse(null);
            if (job == null || !job.isEnabled()) {
                log.info("[orbit-admin] skip fire, job missing or disabled: {}", jobName);
                return;
            }
            jobService.dispatch(job, null);
        } catch (Exception e) {
            log.error("[orbit-admin] quartz fire failed for {}", jobName, e);
            // 不向 Quartz 抛，避免反复 misfire
        }
    }
}

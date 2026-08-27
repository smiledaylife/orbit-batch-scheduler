package com.orbit.admin.service;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
import com.orbit.admin.quartz.OrbitQuartzJob;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.PageResult;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;

/**
 * 调度中心核心：任务 CRUD + Quartz 联动 + 派发执行器。
 */
@Service
public class JobService {

    private static final Logger log = LoggerFactory.getLogger(JobService.class);

    private final Scheduler scheduler;
    private final JobStore jobStore;
    private final ExecutorRegistry registry;
    private final ExecutorClient executorClient;
    private final AdminProperties properties;

    public JobService(Scheduler scheduler, JobStore jobStore, ExecutorRegistry registry,
                      ExecutorClient executorClient, AdminProperties properties) {
        this.scheduler = scheduler;
        this.jobStore = jobStore;
        this.registry = registry;
        this.executorClient = executorClient;
        this.properties = properties;
    }

    @PostConstruct
    public void init() {
        try {
            for (JobInfo job : jobStore.findAllJobs()) {
                scheduleOrUpdate(job);
            }
            log.info("[orbit-admin] loaded {} job(s) into quartz", jobStore.countJobs());
        } catch (Exception e) {
            log.error("[orbit-admin] init schedule failed: {}", e.getMessage(), e);
        }
    }

    public JobInfo create(JobInfo input) {
        validate(input, true);
        if (jobStore.findJobByName(input.getJobName()).isPresent()) {
            throw new IllegalArgumentException("job already exists: " + input.getJobName());
        }
        JobInfo saved = jobStore.saveJob(input);
        applySchedule(saved);
        return saved;
    }

    public JobInfo update(String jobName, JobInfo input) {
        JobInfo existing = jobStore.findJobByName(jobName)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + jobName));
        validate(input, false);
        existing.setDescription(input.getDescription());
        existing.setAppName(input.getAppName());
        existing.setHandler(input.getHandler());
        existing.setCron(input.getCron());
        existing.setParams(input.getParams());
        existing.setTimeoutSeconds(input.getTimeoutSeconds());
        existing.setRouteStrategy(input.getRouteStrategy());
        existing.setEnabled(input.isEnabled());
        JobInfo saved = jobStore.saveJob(existing);
        applySchedule(saved);
        return saved;
    }

    public void delete(String jobName) {
        jobStore.findJobByName(jobName)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + jobName));
        jobStore.deleteJob(jobName);
        try {
            scheduler.deleteJob(jobKey(jobName));
        } catch (SchedulerException e) {
            throw new IllegalStateException("delete quartz job failed: " + e.getMessage(), e);
        }
    }

    public PageResult<JobInfo> page(String nameLike, int page, int size) {
        return jobStore.pageJobs(nameLike, page, size);
    }

    public JobInfo get(String jobName) {
        return jobStore.findJobByName(jobName)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + jobName));
    }

    public void pause(String jobName) {
        require(jobName);
        try {
            scheduler.pauseJob(jobKey(jobName));
        } catch (SchedulerException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public void resume(String jobName) {
        JobInfo job = require(jobName);
        try {
            if (!scheduler.checkExists(jobKey(jobName))) {
                scheduleOrUpdate(job);
            } else {
                scheduler.resumeJob(jobKey(jobName));
            }
        } catch (SchedulerException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /** 立即触发（异步走 Quartz，也可直接 dispatch） */
    public TriggerResult triggerNow(String jobName, Map<String, Object> params) {
        JobInfo job = require(jobName);
        return dispatch(job, params);
    }

    /**
     * Quartz / 手动触发统一派发入口。
     */
    public TriggerResult dispatch(JobInfo job, Map<String, Object> extraParams) {
        String logId = UUID.randomUUID().toString().replace("-", "");
        Date start = new Date();

        JobLog running = new JobLog();
        running.setLogId(logId);
        running.setJobId(job.getId());
        running.setJobName(job.getJobName());
        running.setAppName(job.getAppName());
        running.setHandler(job.getHandler());
        running.setStatus("RUNNING");
        running.setStartTime(start);
        jobStore.insertLog(running);

        ExecutorNode node = registry.route(job.getAppName(), job.getRouteStrategy());
        if (node == null) {
            String msg = "no online executor for appName=" + job.getAppName();
            jobStore.finishLog(logId, "FAILED", null, 0, msg);
            log.warn("[orbit-admin] {}", msg);
            return TriggerResult.fail(logId, job.getId() == null ? 0 : job.getId(), null, 0, msg);
        }

        TriggerRequest req = new TriggerRequest();
        req.setJobId(job.getId() == null ? 0 : job.getId());
        req.setJobName(job.getJobName());
        req.setHandler(job.getHandler());
        req.setLogId(logId);
        req.setTimeoutSeconds(job.getTimeoutSeconds());
        Map<String, Object> merged = new HashMap<String, Object>();
        if (job.getParams() != null) {
            merged.putAll(job.getParams());
        }
        if (extraParams != null) {
            merged.putAll(extraParams);
        }
        req.setParams(merged);

        TriggerResult result = executorClient.trigger(node.getAddress(), req);
        long cost = result.getCostMs() > 0 ? result.getCostMs() : (System.currentTimeMillis() - start.getTime());
        String status = result.isSuccess() ? "SUCCESS" : "FAILED";
        jobStore.finishLog(logId, status, node.getAddress(), cost, result.getMessage());
        log.info("[orbit-admin] job={} -> {} @ {} status={} {}ms",
                job.getJobName(), job.getHandler(), node.getAddress(), status, cost);
        return result;
    }

    public PageResult<JobLog> pageLogs(String jobName, int page, int size) {
        return jobStore.pageLogs(jobName, page, size);
    }

    public Map<String, Object> overview() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("jobCount", jobStore.countJobs());
        m.put("executorOnline", registry.onlineCount());
        try {
            m.put("scheduledCount", scheduler.getJobKeys(GroupMatcher.jobGroupEquals(properties.getGroup())).size());
            m.put("quartzClustered", scheduler.getMetaData().isJobStoreClustered());
        } catch (Exception e) {
            m.put("scheduledCount", -1);
            m.put("quartzClustered", false);
        }
        m.put("timezone", properties.getTimezone());
        return m;
    }

    public Map<String, Object> quartzInfo(String jobName) {
        Map<String, Object> info = new LinkedHashMap<String, Object>();
        try {
            JobKey key = jobKey(jobName);
            info.put("jobExists", scheduler.checkExists(key));
            Trigger t = scheduler.getTrigger(triggerKey(jobName));
            if (t == null) {
                info.put("triggerState", "NONE");
                info.put("nextFireTime", null);
            } else {
                info.put("triggerState", scheduler.getTriggerState(triggerKey(jobName)).name());
                Date next = t.getNextFireTime();
                info.put("nextFireTime", next == null ? null : next.getTime());
                if (t instanceof CronTrigger) {
                    info.put("cron", ((CronTrigger) t).getCronExpression());
                }
            }
        } catch (SchedulerException e) {
            info.put("error", e.getMessage());
        }
        return info;
    }

    // ---- quartz ----

    void scheduleOrUpdate(JobInfo job) throws SchedulerException {
        JobKey key = jobKey(job.getJobName());
        boolean cronOk = job.getCron() != null && CronExpression.isValidExpression(job.getCron());
        if (!job.isEnabled() || !cronOk) {
            if (scheduler.checkExists(key)) {
                scheduler.deleteJob(key);
            }
            return;
        }
        JobDetail detail = JobBuilder.newJob(OrbitQuartzJob.class)
                .withIdentity(key)
                .usingJobData(OrbitQuartzJob.KEY_JOB_NAME, job.getJobName())
                .storeDurably()
                .requestRecovery()
                .build();
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(job.getJobName()))
                .forJob(key)
                .withSchedule(CronScheduleBuilder.cronSchedule(job.getCron())
                        .withMisfireHandlingInstructionDoNothing()
                        .inTimeZone(TimeZone.getTimeZone(properties.getTimezone())))
                .build();
        if (!scheduler.checkExists(key)) {
            scheduler.scheduleJob(detail, trigger);
            log.info("[orbit-admin] scheduled {} cron={}", job.getJobName(), job.getCron());
        } else {
            scheduler.addJob(detail, true);
            scheduler.rescheduleJob(triggerKey(job.getJobName()), trigger);
            log.info("[orbit-admin] rescheduled {} cron={}", job.getJobName(), job.getCron());
        }
    }

    private void applySchedule(JobInfo job) {
        try {
            scheduleOrUpdate(job);
        } catch (SchedulerException e) {
            throw new IllegalStateException("schedule failed: " + e.getMessage(), e);
        }
    }

    private JobInfo require(String name) {
        return jobStore.findJobByName(name)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + name));
    }

    private void validate(JobInfo job, boolean create) {
        if (create) {
            if (job.getJobName() == null || !job.getJobName().matches("[A-Za-z0-9_\\-.]{1,64}")) {
                throw new IllegalArgumentException("jobName must match [A-Za-z0-9_-.]{1,64}");
            }
        }
        if (job.getAppName() == null || job.getAppName().trim().isEmpty()) {
            throw new IllegalArgumentException("appName required");
        }
        if (job.getHandler() == null || job.getHandler().trim().isEmpty()) {
            throw new IllegalArgumentException("handler required");
        }
        if (job.getCron() != null && !job.getCron().trim().isEmpty()
                && !CronExpression.isValidExpression(job.getCron())) {
            throw new IllegalArgumentException("invalid cron: " + job.getCron());
        }
        if (job.getRouteStrategy() == null || job.getRouteStrategy().trim().isEmpty()) {
            job.setRouteStrategy("ROUND");
        }
        if (job.getTimeoutSeconds() <= 0) {
            job.setTimeoutSeconds(300);
        }
    }

    private JobKey jobKey(String name) {
        return JobKey.jobKey(name, properties.getGroup());
    }

    private TriggerKey triggerKey(String name) {
        return TriggerKey.triggerKey(name, properties.getGroup());
    }
}

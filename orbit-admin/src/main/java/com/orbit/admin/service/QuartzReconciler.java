package com.orbit.admin.service;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.JobInfo;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** DB -> Quartz 最终一致性对账器；生产集群由 Redis 保证全局单实例对账。 */
@Component
@ConditionalOnProperty(prefix = "orbit.admin", name = "execution-lease-enabled", havingValue = "true")
public class QuartzReconciler {

    private static final Logger log = LoggerFactory.getLogger(QuartzReconciler.class);
    private static final String LOCK_KEY = "orbit:quartz:reconcile:lock";
    private static final String LOCK_VALUE = "1";
    private static final DefaultRedisScript<Long> RELEASE = new DefaultRedisScript<Long>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]); end; return 0;",
            Long.class);

    private final Scheduler scheduler;
    private final JobStore jobStore;
    private final JobService jobService;
    private final AdminProperties properties;
    private final StringRedisTemplate redis;

    public QuartzReconciler(Scheduler scheduler, JobStore jobStore, JobService jobService,
                            AdminProperties properties, StringRedisTemplate redis) {
        this.scheduler = scheduler;
        this.jobStore = jobStore;
        this.jobService = jobService;
        this.properties = properties;
        this.redis = redis;
    }

    @PostConstruct
    public void initialReconcile() {
        log.info("[orbit-admin] quartz reconciler initialized");
    }

    @Scheduled(fixedDelayString = "${orbit.admin.quartz-reconcile-interval-ms:60000}", initialDelayString = "${orbit.admin.quartz-reconcile-initial-delay-ms:15000}")
    public void reconcile() {
        if (!acquireLock()) {
            return;
        }
        try {
            List<JobInfo> jobs = jobStore.findAllJobs();
            Set<String> dbNames = new HashSet<String>();
            for (JobInfo job : jobs) {
                dbNames.add(job.getJobName());
            }

            int removed = 0;
            for (JobKey key : scheduler.getJobKeys(GroupMatcher.jobGroupEquals(properties.getGroup()))) {
                if (!dbNames.contains(key.getName())) {
                    try {
                        if (scheduler.deleteJob(key)) {
                            removed++;
                        }
                    } catch (SchedulerException e) {
                        log.error("[orbit-admin] failed to remove orphan quartz job {}", key, e);
                    }
                }
            }

            jobService.init();
            if (removed > 0) {
                log.warn("[orbit-admin] quartz reconciliation removed {} orphan job(s)", removed);
            }
        } catch (Exception e) {
            log.error("[orbit-admin] quartz reconciliation failed; will retry later", e);
        } finally {
            releaseLock();
        }
    }

    private boolean acquireLock() {
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(LOCK_KEY, LOCK_VALUE, 50L, TimeUnit.SECONDS);
            return Boolean.TRUE.equals(acquired);
        } catch (Exception e) {
            log.warn("[orbit-admin] redis unavailable; skip quartz reconciliation to avoid multi-node race");
            return false;
        }
    }

    private void releaseLock() {
        try {
            redis.execute(RELEASE, Arrays.asList(LOCK_KEY), LOCK_VALUE);
        } catch (Exception ignored) {
        }
    }
}

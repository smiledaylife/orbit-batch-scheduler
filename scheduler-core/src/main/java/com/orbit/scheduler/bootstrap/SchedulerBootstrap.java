package com.orbit.scheduler.bootstrap;

import com.orbit.scheduler.core.JobManager;
import com.orbit.scheduler.quartz.QuartzJobDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;

/**
 * 调度器启动引导：应用就绪后写入 Scheduler Context 兜底引用，并执行
 * 注解种子落库 + Quartz 触发器对账。
 *
 * @author orbit
 */
public class SchedulerBootstrap implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger log = LoggerFactory.getLogger(SchedulerBootstrap.class);

    private final JobManager jobManager;

    public SchedulerBootstrap(JobManager jobManager) {
        this.jobManager = jobManager;
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        try {
            event.getApplicationContext().getBean(org.quartz.Scheduler.class)
                    .getContext().put(QuartzJobDispatcher.CTX_KEY_JOB_MANAGER, jobManager);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] put JobManager into SchedulerContext failed: {}", e.getMessage());
        }
        log.info("[orbit-scheduler] bootstrap starting on node '{}' ...", jobManager.getNodeId());
        jobManager.synchronizeOnStartup();
        log.info("[orbit-scheduler] bootstrap completed");
    }
}

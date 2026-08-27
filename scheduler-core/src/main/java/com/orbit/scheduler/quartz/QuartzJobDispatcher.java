package com.orbit.scheduler.quartz;

import com.orbit.scheduler.core.JobManager;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Quartz 统一入口 Job：所有被调度任务共用此 JobDetail，
 * 由它按 taskName 路由到本地执行 / HTTP 远程派发。
 *
 * <p>依赖获取双通道：
 * <ol>
 *   <li>Spring Boot QuartzAutoConfiguration 的 SpringBeanJobFactory 自动注入（主）</li>
 *   <li>Scheduler Context 兜底（bootstrap 阶段写入，避免极端装配顺序问题）</li>
 * </ol>
 *
 * <p><b>优化</b>：requestId 生成逻辑收敛到 {@link JobManager#newRequestId()}，
 * 避免散落在各调用点重复 UUID 字符串构造，便于未来切换更轻量的 ID 算法。
 *
 * @author orbit
 */
@DisallowConcurrentExecution
public class QuartzJobDispatcher implements Job {

    private static final Logger log = LoggerFactory.getLogger(QuartzJobDispatcher.class);

    /** Scheduler Context 中 JobManager 的键 */
    public static final String CTX_KEY_JOB_MANAGER = "ORBIT_JOB_MANAGER";

    /** JobDataMap：任务名 */
    public static final String DATA_KEY_TASK_NAME = "taskName";
    /** JobDataMap：触发时临时参数 JSON（triggerJob 传入） */
    public static final String DATA_KEY_PARAMS_JSON = "paramsJson";

    @Autowired(required = false)
    private JobManager jobManager;

    @Override
    public void execute(JobExecutionContext context) {
        JobManager manager = this.jobManager;
        if (manager == null) {
            Object fallback = null;
            try {
                fallback = context.getScheduler().getContext().get(CTX_KEY_JOB_MANAGER);
            } catch (Exception ignore) {
                // ignore
            }
            if (fallback instanceof JobManager) {
                manager = (JobManager) fallback;
            }
        }
        if (manager == null) {
            log.error("[orbit-scheduler] JobManager is not available, cannot dispatch task. " +
                    "Please ensure scheduler-starter is on the classpath.");
            return;
        }

        String taskName = context.getMergedJobDataMap().getString(DATA_KEY_TASK_NAME);
        if (taskName == null || taskName.isEmpty()) {
            log.error("[orbit-scheduler] JobDataMap has no 'taskName', skip. JobKey={}", context.getJobDetail().getKey());
            return;
        }
        Map<String, Object> params = manager.parseParamsJson(context.getMergedJobDataMap().getString(DATA_KEY_PARAMS_JSON));
        long fireTime = context.getFireTime() == null ? System.currentTimeMillis() : context.getFireTime().getTime();
        // 构建 TaskContext 供本地执行；HTTP 派发时参数随请求透传
        try {
            manager.dispatch(taskName, params, JobManager.newRequestId(), fireTime);
        } catch (Exception e) {
            // dispatch 内部已兜底，这里防御日志框架级异常
            log.error("[orbit-scheduler] unexpected error dispatching task '{}'", taskName, e);
        }
    }
}

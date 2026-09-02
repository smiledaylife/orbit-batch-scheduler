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
 * 调度中心核心业务服务。
 * 核心职责：
 * 
 *   - 任务元数据生命周期管理（CRUD、校验、状态控制）；
 *   - Quartz 定时任务的动态编排、启动加载、Cron 动态刷新、暂停与恢复；
 *   - 任务统一派发（分发）：生成日志追踪链路 ID、按路由策略寻址、向执行器派发 HTTP 触发请求、记录执行日志与耗时。
 * 
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

    /**
     * 调度中心初始化方法。
     * 在系统启动时从数据库加载所有启用的定时任务并注册到 Quartz 调度器中。
     */
    @PostConstruct
    public void init() {
        List<JobInfo> jobs;
        try {
            jobs = jobStore.findAllJobs();
        } catch (Exception e) {
            // 读库失败意味着依赖不可用。原先这里把异常整体吞掉，会让调度中心
            // 以「零调度但一切正常」的姿态启动，属于静默故障 —— 改为让启动失败。
            throw new IllegalStateException("[orbit-admin] failed to load jobs on startup", e);
        }

        int loaded = 0;
        for (JobInfo job : jobs) {
            try {
                scheduleOrUpdate(job);
                loaded++;
            } catch (Exception e) {
                // 单个任务装载失败（例如历史遗留的非法 cron）不应阻断其余任务
                log.error("[orbit-admin] init schedule failed for job={}", job.getJobName(), e);
            }
        }
        log.info("[orbit-admin] loaded {}/{} job(s) into quartz", loaded, jobs.size());
    }

    /**
     * 创建新任务，并在满足条件时自动加入 Quartz 调度。
     *
     * @param input 任务元数据信息
     * @return 持久化后的任务对象
     */
    public JobInfo create(JobInfo input) {
        // 参数合法性校验
        validate(input, true);
        if (jobStore.findJobByName(input.getJobName()).isPresent()) {
            throw new IllegalArgumentException("job already exists: " + input.getJobName());
        }

        // 保存至数据库
        JobInfo saved = jobStore.saveJob(input);
        // 同步应用到 Quartz 调度器
        applySchedule(saved);
        return saved;
    }

    /**
     * 更新已有任务元数据，并同步热更新 Quartz 调度计划。
     *
     * @param jobName 任务名称
     * @param input   待更新的任务字段
     * @return 更新后的任务对象
     */
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

    /**
     * 删除任务，并同步从 Quartz 中移除定时计划。
     *
     * @param jobName 任务名称
     */
    public void delete(String jobName) {
        jobStore.findJobByName(jobName)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + jobName));
        // 以数据库为唯一事实来源：先删库，再清理 Quartz。
        // 原先 Quartz 清理失败会抛异常，但此时 DB 行已删除 —— 接口报错却已生效，
        // 调用方重试只会得到 "job not found"，两边状态对不上。
        // 改为 Quartz 失败仅记日志：DB 已无该任务，重启后 init() 也不会再装载它。
        jobStore.deleteJob(jobName);
        try {
            scheduler.deleteJob(jobKey(jobName));
        } catch (SchedulerException e) {
            log.error("[orbit-admin] job {} removed from db but quartz cleanup failed: {}",
                    jobName, e.getMessage(), e);
        }
    }

    /**
     * 分页查询任务列表。
     *
     * @param nameLike 名称模糊匹配
     * @param page     页码
     * @param size     每页记录数
     * @return 分页结果集
     */
    public PageResult<JobInfo> page(String nameLike, int page, int size) {
        return jobStore.pageJobs(nameLike, page, size);
    }

    /**
     * 根据任务名称查询单个任务详情。
     *
     * @param jobName 任务名称
     * @return 任务对象
     */
    public JobInfo get(String jobName) {
        return jobStore.findJobByName(jobName)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + jobName));
    }

    /**
     * 暂停任务的自动定时触发调度。
     * 同时将 enabled=false 持久化到数据库并从 Quartz 移除 Trigger，
     * 保证调度中心重启后暂停状态不丢失（Quartz 使用内存 JobStore，重启即清空）。
     *
     * @param jobName 任务名称
     */
    public void pause(String jobName) {
        JobInfo job = require(jobName);
        job.setEnabled(false);
        jobStore.saveJob(job);
        try {
            // 直接删除 Quartz 中的调度计划；重启时 init() 依据 enabled=false 也不会重新注册
            if (scheduler.checkExists(jobKey(jobName))) {
                scheduler.deleteJob(jobKey(jobName));
            }
        } catch (SchedulerException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 恢复任务的自动定时触发调度。
     * 同时将 enabled=true 持久化到数据库，并重新向 Quartz 注册 Cron 调度。
     *
     * @param jobName 任务名称
     */
    public void resume(String jobName) {
        JobInfo job = require(jobName);
        job.setEnabled(true);
        jobStore.saveJob(job);
        try {
            scheduleOrUpdate(job);
        } catch (SchedulerException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 手动立即触发一次任务执行。
     *
     * @param jobName 任务名称
     * @param params  本次单次触发的动态临时入参（可为空）
     * @return 执行响应结果
     */
    public TriggerResult triggerNow(String jobName, Map<String, Object> params) {
        JobInfo job = require(jobName);
        return dispatch(job, params);
    }

    /**
     * 调度中心统一派发执行逻辑（无论是 Quartz 定时触发还是手动触发，均走本方法）。
     * 
     *   - 生成全链路唯一追踪日志 ID，初始化 RUNNING 状态日志入库；
     *   - 从注册表中根据任务路由策略选取一个在线执行器节点；
     *   - 若无可用节点，更新日志为 FAILED 并终止；
     *   - 合并静态参数与动态参数，通过 HTTP 调用执行器端 /run 接口；
     *   - 计算本次调用耗时，根据执行结果更新日志状态为 SUCCESS 或 FAILED。
     * 
     * @param job         任务元数据
     * @param extraParams 单次触发传入的覆盖参数（可为空）
     * @return 任务执行结果
     */
    public TriggerResult dispatch(JobInfo job, Map<String, Object> extraParams) {
        // 1. 生成全局唯一日志 ID 与记录开始时间
        String logId = UUID.randomUUID().toString().replace("-", "");
        Date start = new Date();

        // 2. 插入初始运行中日志记录
        JobLog running = new JobLog();
        running.setLogId(logId);
        running.setJobId(job.getId());
        running.setJobName(job.getJobName());
        running.setAppName(job.getAppName());
        running.setHandler(job.getHandler());
        running.setStatus("RUNNING");
        running.setStartTime(start);
        jobStore.insertLog(running);

        // 3~6 全程包在 try/catch 中：无论路由寻址、参数合并还是落库环节抛异常，
        // 都必须把日志从 RUNNING 收敛到终态，否则会留下永久 RUNNING 的僵尸记录
        // （没有任何后台任务会回收它）。
        ExecutorNode node = null;
        try {
            // 3. 按照任务配置的路由策略寻址目标执行器实例
            node = registry.route(job.getAppName(), job.getRouteStrategy());
            if (node == null) {
                // 没有存活的执行器实例
                String msg = "no online executor for appName=" + job.getAppName();
                jobStore.finishLog(logId, "FAILED", null, 0, msg);
                log.warn("[orbit-admin] {}", msg);
                return TriggerResult.fail(logId, job.getId() == null ? 0 : job.getId(), null, 0, msg);
            }

            // 4. 构建触发请求并合并参数
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

            // 5. 向目标执行器发起 HTTP 调用触发任务执行
            TriggerResult result = executorClient.trigger(node.getAddress(), req);
            long cost = result.getCostMs() > 0 ? result.getCostMs() : (System.currentTimeMillis() - start.getTime());
            String status = result.isSuccess() ? "SUCCESS" : "FAILED";

            // 6. 更新执行日志最终结果
            jobStore.finishLog(logId, status, node.getAddress(), cost, result.getMessage());
            log.info("[orbit-admin] job={} -> {} @ {} status={} {}ms",
                    job.getJobName(), job.getHandler(), node.getAddress(), status, cost);
            return result;
        } catch (RuntimeException e) {
            String address = node == null ? null : node.getAddress();
            long cost = System.currentTimeMillis() - start.getTime();
            String msg = "dispatch failed: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            log.error("[orbit-admin] dispatch error for job={}", job.getJobName(), e);
            safeFinishLog(logId, address, cost, msg);
            return TriggerResult.fail(logId, job.getId() == null ? 0 : job.getId(), address, cost, msg);
        }
    }

    /**
     * 兜底写终态日志。自身异常只记录不外抛，避免在异常处理路径上二次抛出，
     * 把原始的派发异常覆盖掉。
     *
     * @param logId   日志追踪 ID
     * @param address 执行器地址（可为空）
     * @param cost    耗时毫秒
     * @param message 失败原因
     */
    private void safeFinishLog(String logId, String address, long cost, String message) {
        try {
            jobStore.finishLog(logId, "FAILED", address, cost, message);
        } catch (Exception ex) {
            log.error("[orbit-admin] failed to finalize log {}: {}", logId, ex.getMessage(), ex);
        }
    }

    /**
     * 分页查询调度执行日志。
     *
     * @param jobName 任务名称过滤（可为空）
     * @param page    页码
     * @param size    每页大小
     * @return 分页日志列表
     */
    public PageResult<JobLog> pageLogs(String jobName, int page, int size) {
        return jobStore.pageLogs(jobName, page, size);
    }

    /**
     * 查询调度中心系统总览数据（任务数、在线节点数、Quartz 集群状态等）。
     *
     * @return 统计指标字典
     */
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

    /**
     * 查询指定任务在 Quartz 内部的详细调度状态（下一次触发时间、触发状态等）。
     *
     * @param jobName 任务名称
     * @return Quartz 状态明细
     */
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

    // -------- Quartz 底层计划编排内部辅助方法 --------

    /**
     * 编排或更新任务在 Quartz 调度器中的 Trigger 与 JobDetail。
     *
     * @param job 任务元数据
     * @throws SchedulerException Quartz 调度器操作异常
     */
    void scheduleOrUpdate(JobInfo job) throws SchedulerException {
        JobKey key = jobKey(job.getJobName());
        boolean cronOk = job.getCron() != null && CronExpression.isValidExpression(job.getCron());

        // 若任务未启用或 Cron 表达式为空/无效，若 Quartz 中存在则直接删除移除调度
        if (!job.isEnabled() || !cronOk) {
            if (scheduler.checkExists(key)) {
                scheduler.deleteJob(key);
            }
            return;
        }

        // 构建 Quartz JobDetail
        JobDetail detail = JobBuilder.newJob(OrbitQuartzJob.class)
                .withIdentity(key)
                .usingJobData(OrbitQuartzJob.KEY_JOB_NAME, job.getJobName())
                .storeDurably()
                .requestRecovery()
                .build();

        // 构建 Cron 触发器并设置时区与错失策略（misfire do nothing）
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(job.getJobName()))
                .forJob(key)
                .withSchedule(CronScheduleBuilder.cronSchedule(job.getCron())
                        .withMisfireHandlingInstructionDoNothing()
                        .inTimeZone(TimeZone.getTimeZone(properties.getTimezone())))
                .build();

        // 动态注册或更新调度计划
        if (!scheduler.checkExists(key)) {
            scheduler.scheduleJob(detail, trigger);
            log.info("[orbit-admin] scheduled {} cron={}", job.getJobName(), job.getCron());
        } else {
            scheduler.addJob(detail, true);
            // rescheduleJob 在 Trigger 不存在时返回 null 且什么都不做。
            // 由于 JobDetail 是 storeDurably() 的，「Job 在、Trigger 不在」是可达状态，
            // 此时若不补救，任务会被静默搁置 —— 不报错，也永远不再触发。
            if (scheduler.rescheduleJob(triggerKey(job.getJobName()), trigger) == null) {
                scheduler.scheduleJob(trigger);
                log.info("[orbit-admin] trigger missing, re-created for {} cron={}",
                        job.getJobName(), job.getCron());
            } else {
                log.info("[orbit-admin] rescheduled {} cron={}", job.getJobName(), job.getCron());
            }
        }
    }

    /**
     * 包装执行 Quartz 调度更新
     *
     * @param job 任务元数据
     */
    private void applySchedule(JobInfo job) {
        try {
            scheduleOrUpdate(job);
        } catch (SchedulerException e) {
            throw new IllegalStateException("schedule failed: " + e.getMessage(), e);
        }
    }

    /**
     * 强校验任务是否存在
     *
     * @param name 任务名称
     * @return 任务对象
     */
    private JobInfo require(String name) {
        return jobStore.findJobByName(name)
                .orElseThrow(() -> new IllegalArgumentException("job not found: " + name));
    }

    /**
     * 任务字段业务校验与默认值回填
     *
     * @param job    待校验任务对象
     * @param create 是否为创建操作
     */
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
        // 路由策略默认 ROUND
        if (job.getRouteStrategy() == null || job.getRouteStrategy().trim().isEmpty()) {
            job.setRouteStrategy("ROUND");
        }
        // 超时时间兜底 300 秒
        if (job.getTimeoutSeconds() <= 0) {
            job.setTimeoutSeconds(300);
        }
        // 按全局上限封顶：派发是同步阻塞的，无上限的 timeoutSeconds 会让单次调用
        // 长时间占住 Tomcat 线程（手动触发）或 Quartz 工作线程（定时触发）。
        int maxTimeout = properties.getMaxTimeoutSeconds();
        if (maxTimeout > 0 && job.getTimeoutSeconds() > maxTimeout) {
            log.warn("[orbit-admin] job {} timeoutSeconds {} exceeds max {}, capped",
                    job.getJobName(), job.getTimeoutSeconds(), maxTimeout);
            job.setTimeoutSeconds(maxTimeout);
        }
    }

    private JobKey jobKey(String name) {
        return JobKey.jobKey(name, properties.getGroup());
    }

    private TriggerKey triggerKey(String name) {
        return TriggerKey.triggerKey(name, properties.getGroup());
    }
}

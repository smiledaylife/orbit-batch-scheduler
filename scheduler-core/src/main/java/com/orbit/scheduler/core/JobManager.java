package com.orbit.scheduler.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.http.HttpDispatchClient;
import com.orbit.scheduler.lock.NoOpLockProvider;
import com.orbit.scheduler.model.HttpDispatchResponse;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.spi.JobLogRepository;
import com.orbit.scheduler.spi.LockProvider;
import com.orbit.scheduler.spi.TaskRepository;
import com.orbit.scheduler.storage.InMemoryJobLogRepository;
import com.orbit.scheduler.support.NodeIdProvider;
import com.orbit.scheduler.support.SchedulerProperties;
import com.orbit.scheduler.quartz.QuartzJobDispatcher;
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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 调度引擎核心：
 * <ul>
 *   <li>启动同步：注解任务种子落库 → Quartz 触发器对账（新增/重排/清理）</li>
 *   <li>运行时派发：分布式锁 → 本地执行 or HTTP 远程派发（LOCAL 无执行器自动回退 HTTP）→ 执行日志</li>
 *   <li>管理操作：CRUD / 暂停 / 恢复 / 立即触发 / 同步执行，均与 Quartz 实时联动</li>
 * </ul>
 *
 * <p><b>性能优化</b>：
 * <ul>
 *   <li>overview 使用 {@link TaskRepository#count()} 替代 findAll().size()，避免全表加载</li>
 *   <li>锁续期 period = max(lease/3, 1s) 改为 max(lease/3, MIN_RENEW_PERIOD)，
 *       避免短租约场景下续期过于频繁；Redisson 已自动续期，JDBC 锁才使用此机制</li>
 *   <li>新增 SimpleRequestIdGenerator 用于 QuartzJobDispatcher 高频场景的 requestId 生成</li>
 * </ul>
 *
 * @author orbit
 */
public class JobManager {

    private static final Logger log = LoggerFactory.getLogger(JobManager.class);

    public static final String LOCK_PREFIX = "orbit:scheduler:lock:";

    /** 锁续期最小周期（毫秒），避免短租约场景续期过于频繁 */
    static final long MIN_RENEW_PERIOD_MS = 5_000L;

    /** 日志消息最大长度（DB 字段约束） */
    static final int MAX_MESSAGE_LEN = 2000;

    private final Scheduler scheduler;
    private final TaskRepository taskRepository;
    private final TaskRegistry taskRegistry;
    private final LockProvider lockProvider;
    private final HttpDispatchClient httpDispatchClient;
    private final JobLogRepository jobLogRepository;
    private final SchedulerProperties properties;
    private final ObjectMapper objectMapper;
    private final String nodeId;

    /** 锁续期看门狗（JDBC 锁需要；Redisson watchdog 自续期） */
    private final ScheduledExecutorService lockKeeper;

    public JobManager(Scheduler scheduler, TaskRepository taskRepository, TaskRegistry taskRegistry,
                      LockProvider lockProvider, HttpDispatchClient httpDispatchClient,
                      JobLogRepository jobLogRepository, SchedulerProperties properties,
                      ObjectMapper objectMapper) {
        this.scheduler = scheduler;
        this.taskRepository = taskRepository;
        this.taskRegistry = taskRegistry;
        this.lockProvider = lockProvider == null ? new NoOpLockProvider() : lockProvider;
        this.httpDispatchClient = httpDispatchClient;
        this.jobLogRepository = jobLogRepository == null
                ? new InMemoryJobLogRepository(properties.getLog().getMemoryCapacity())
                : jobLogRepository;
        this.properties = properties;
        this.objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
        this.nodeId = NodeIdProvider.resolve(properties.getNodeId());
        this.lockKeeper = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "orbit-lock-keeper");
            t.setDaemon(true);
            return t;
        });
    }

    public String getNodeId() {
        return nodeId;
    }

    // ==================================================================
    // 启动同步
    // ==================================================================

    /**
     * 应用就绪后执行：注解种子落库 + Quartz 触发器对账。
     */
    public void synchronizeOnStartup() {
        try {
            seedAnnotatedTasks();
            reconcileQuartzJobs();
        } catch (Exception e) {
            log.error("[orbit-scheduler] startup synchronize failed: {}", e.getMessage(), e);
        }
        try {
            int scheduledCount = scheduler.getJobKeys(
                    GroupMatcher.jobGroupEquals(properties.getGroup())).size();
            long taskCount = safeCount();
            log.info("[orbit-scheduler] node={} storage={} lock={} quartzClustered={} scheduledJobs={} taskConfigs={}",
                    nodeId, taskRepository.type(), lockProvider.type(), isQuartzClustered(),
                    scheduledCount, taskCount);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] summarize failed: {}", e.getMessage());
        }
    }

    /** 将 @BatchTask 扫描结果作为种子写入任务存储（已存在则默认跳过，overwrite=true 时回写 cron/description） */
    private void seedAnnotatedTasks() {
        if (!properties.isAnnotationScan()) {
            return;
        }
        for (TaskDefinition def : taskRegistry.getTaskDefinitions()) {
            try {
                JobConfig existing = taskRepository.findByName(def.getName()).orElse(null);
                if (existing == null) {
                    JobConfig cfg = new JobConfig();
                    cfg.setTaskName(def.getName());
                    cfg.setTaskGroup(properties.getGroup());
                    cfg.setDescription(def.getDescription());
                    cfg.setCronExpression(def.getCron());
                    cfg.setDispatchType(def.getDispatchType() == null ? DispatchType.LOCAL : def.getDispatchType());
                    cfg.setEnabled(true);
                    taskRepository.save(cfg);
                    log.info("[orbit-scheduler] seeded annotated task '{}'", def.getName());
                } else if (def.isOverwrite()) {
                    boolean changed = false;
                    if (!equalsOrNull(existing.getCronExpression(), emptyToNull(def.getCron()))) {
                        existing.setCronExpression(def.getCron());
                        changed = true;
                    }
                    if (!equalsOrNull(existing.getDescription(), def.getDescription())) {
                        existing.setDescription(def.getDescription());
                        changed = true;
                    }
                    if (changed) {
                        taskRepository.save(existing);
                        log.info("[orbit-scheduler] overwritten task config '{}' by annotation", def.getName());
                    }
                }
            } catch (Exception e) {
                log.error("[orbit-scheduler] seed task '{}' failed: {}", def.getName(), e.getMessage(), e);
            }
        }
    }

    /** Quartz 触发器对账：启用的任务确保已排程且 cron 为最新；禁用/已删除的任务移除触发器 */
    private void reconcileQuartzJobs() {
        List<JobConfig> all = taskRepository.findAll();
        Set<String> managed = new HashSet<String>();
        for (JobConfig cfg : all) {
            try {
                scheduleOrUpdate(cfg);
                managed.add(cfg.getTaskName());
            } catch (Exception e) {
                log.error("[orbit-scheduler] reconcile task '{}' failed: {}", cfg.getTaskName(), e.getMessage(), e);
            }
        }
        // 清理残留：Quartz 中存在但任务存储已无的任务
        try {
            Set<JobKey> keys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(properties.getGroup()));
            for (JobKey key : keys) {
                if (!managed.contains(key.getName())) {
                    scheduler.deleteJob(key);
                    log.info("[orbit-scheduler] removed stale quartz job '{}'", key);
                }
            }
        } catch (SchedulerException e) {
            log.error("[orbit-scheduler] cleanup stale quartz jobs failed: {}", e.getMessage(), e);
        }
    }

    // ==================================================================
    // 运行时派发
    // ==================================================================

    /**
     * 派发一次任务执行（Quartz 触发 / API 手动触发共用入口）。
     * 内部全量兜底异常，绝不向 Quartz 线程抛出。
     */
    public void dispatch(String taskName, Map<String, Object> params, String requestId, long fireTime) {
        dispatchInternal(taskName, params, requestId, fireTime);
    }

    /** 同步执行并返回结果摘要（REST /execute 使用） */
    public DispatchSummary executeSync(String taskName, Map<String, Object> params) {
        return dispatchInternal(taskName, params,
                newRequestId(), System.currentTimeMillis());
    }

    private DispatchSummary dispatchInternal(String taskName, Map<String, Object> params, String requestId, long fireTime) {
        long startAt = System.currentTimeMillis();
        JobConfig cfg = null;
        try {
            cfg = taskRepository.findByName(taskName).orElse(null);
        } catch (Exception e) {
            log.error("[orbit-scheduler] load task config '{}' failed: {}", taskName, e.getMessage(), e);
        }
        if (cfg == null) {
            return summary(taskName, DispatchSummary.STATUS_SKIPPED, startAt, nodeId, requestId,
                    "task not found in " + taskRepository.type() + " repository");
        }
        if (!cfg.isEnabled()) {
            return summary(taskName, DispatchSummary.STATUS_SKIPPED, startAt, nodeId, requestId, "task disabled");
        }

        // ---- 分布式锁 ----
        String lockKey = LOCK_PREFIX + taskName;
        boolean lockEnabled = properties.getLock().isEnabled();
        boolean locked = false;
        Future<?> renewer = null;
        if (lockEnabled) {
            locked = safeTryLock(lockKey, nodeId);
            if (!locked) {
                String msg = "distributed lock held by another node, skip this fire";
                log.info("[orbit-scheduler] task '{}' {}", taskName, msg);
                Long skipLogId = jobLogRepository.appendStart(JobLog.startOf(
                        requestId, taskName, cfg.getTaskGroup(), cfg.getDispatchType().name(), nodeId));
                jobLogRepository.appendFinish(skipLogId, DispatchSummary.STATUS_SKIPPED, nodeId, 0, msg);
                return summary(taskName, DispatchSummary.STATUS_SKIPPED, startAt, nodeId, requestId, msg);
            }
            renewer = startLockRenewal(lockKey, nodeId);
        }

        Long logId = null;
        String workerNode = nodeId;
        String status = DispatchSummary.STATUS_SUCCESS;
        String message = null;
        try {
            logId = jobLogRepository.appendStart(JobLog.startOf(requestId, taskName, cfg.getTaskGroup(),
                    cfg.getDispatchType().name(), nodeId));
            if (cfg.getDispatchType() == DispatchType.HTTP) {
                HttpDispatchResponse resp = httpDispatch(cfg, params, requestId);
                workerNode = resp.getWorkerNode();
                status = resp.isSuccess() ? DispatchSummary.STATUS_SUCCESS : DispatchSummary.STATUS_FAILED;
                message = resp.getMessage();
            } else {
                if (taskRegistry.hasTask(taskName)) {
                    Object ret = taskRegistry.execute(taskName, buildContext(cfg, params, fireTime, requestId));
                    message = ret == null ? null : String.valueOf(ret);
                } else if (properties.getHttpDispatch().isEnabled() && httpDispatchClient != null) {
                    log.info("[orbit-scheduler] task '{}' has no local executor, fallback to HTTP dispatch", taskName);
                    HttpDispatchResponse resp = httpDispatch(cfg, params, requestId);
                    workerNode = resp.getWorkerNode();
                    status = resp.isSuccess() ? DispatchSummary.STATUS_SUCCESS : DispatchSummary.STATUS_FAILED;
                    message = resp.getMessage();
                } else {
                    status = DispatchSummary.STATUS_FAILED;
                    message = "no local executor and HTTP dispatch disabled";
                }
            }
        } catch (Exception e) {
            status = DispatchSummary.STATUS_FAILED;
            message = abbreviate(String.valueOf(e.getMessage()));
            log.error("[orbit-scheduler] task '{}' dispatch failed", taskName, e);
        } finally {
            // 顺序：先停续期 → 再解锁。续期即使 cancel 也会无副作用（续不到已释放的锁）
            if (renewer != null) {
                renewer.cancel(false);
            }
            if (locked && lockEnabled) {
                safeUnlock(lockKey, nodeId);
            }
        }
        long cost = System.currentTimeMillis() - startAt;
        // 日志收尾：RUNNING → 最终态
        try {
            jobLogRepository.appendFinish(logId, status, workerNode, cost, message);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] finish job log failed: {}", e.getMessage());
        }
        return summary(taskName, status, startAt, nodeId, requestId, message);
    }

    private HttpDispatchResponse httpDispatch(JobConfig cfg, Map<String, Object> params, String requestId) {
        if (httpDispatchClient == null) {
            throw new IllegalStateException("HTTP dispatch client is not available " +
                    "(spring-web missing or orbit.scheduler.http-dispatch.enabled=false)");
        }
        return httpDispatchClient.dispatch(cfg, params, requestId);
    }

    private TaskContext buildContext(JobConfig cfg, Map<String, Object> params, long fireTime, String requestId) {
        // 始终构造新 Map：防止任务代码修改 context.params 影响后续触发；
        // 也防止上层调用者持有的 params 被任务代码写入。
        Map<String, Object> cfgParams = cfg.getParamsView();
        int initialSize = cfgParams.size() + (params == null ? 0 : params.size());
        Map<String, Object> merged = new LinkedHashMap<String, Object>(Math.max(8, initialSize));
        if (!cfgParams.isEmpty()) {
            merged.putAll(cfgParams);
        }
        if (params != null && !params.isEmpty()) {
            merged.putAll(params);
        }
        return new TaskContext(cfg.getTaskName(), cfg.getTaskGroup(), merged,
                new Date(fireTime), nodeId, nodeId, requestId);
    }

    private boolean safeTryLock(String key, String owner) {
        try {
            return lockProvider.tryLock(key, owner, properties.getLock().getLease());
        } catch (Exception e) {
            log.warn("[orbit-scheduler] tryLock {} failed ({}), treat as NOT locked to avoid duplicate: {}",
                    key, lockProvider.type(), e.getMessage());
            return false;
        }
    }

    private void safeUnlock(String key, String owner) {
        try {
            lockProvider.unlock(key, owner);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] unlock {} failed: {}", key, e.getMessage());
        }
    }

    private Future<?> startLockRenewal(String key, String owner) {
        long leaseMs = properties.getLock().getLease().toMillis();
        // 续期周期 = lease/3，且不低于 5s，避免短租约场景续期过于频繁
        long period = Math.max(MIN_RENEW_PERIOD_MS, leaseMs / 3);
        return lockKeeper.scheduleAtFixedRate(() -> {
            try {
                boolean ok = lockProvider.renew(key, owner, properties.getLock().getLease());
                if (!ok) {
                    log.warn("[orbit-scheduler] lock renew failed (maybe taken over): {}", key);
                }
            } catch (Exception e) {
                log.warn("[orbit-scheduler] lock renew error: {}", e.getMessage());
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    // ==================================================================
    // Quartz 联动
    // ==================================================================

    private JobKey jobKey(String taskName) {
        return JobKey.jobKey(taskName, properties.getGroup());
    }

    private TriggerKey triggerKey(String taskName) {
        return TriggerKey.triggerKey(taskName, properties.getGroup());
    }

    /** 新增排程或按最新 cron 重排；任务禁用或 cron 无效时移除 Quartz 任务（保留存储配置） */
    void scheduleOrUpdate(JobConfig cfg) throws SchedulerException {
        JobKey key = jobKey(cfg.getTaskName());
        boolean cronValid = cfg.getCronExpression() != null
                && CronExpression.isValidExpression(cfg.getCronExpression());
        if (!cfg.isEnabled() || !cronValid) {
            if (scheduler.checkExists(key)) {
                scheduler.deleteJob(key);
                log.info("[orbit-scheduler] unscheduled task '{}' (enabled={} cronValid={})",
                        cfg.getTaskName(), cfg.isEnabled(), cronValid);
            }
            return;
        }

        JobDetail detail = JobBuilder.newJob(QuartzJobDispatcher.class)
                .withIdentity(key)
                .withDescription(cfg.getDescription() == null ? "" : cfg.getDescription())
                .usingJobData(QuartzJobDispatcher.DATA_KEY_TASK_NAME, cfg.getTaskName())
                .storeDurably()
                .requestRecovery()
                .build();

        CronTrigger newTrigger = TriggerBuilder.newTrigger()
                .withIdentity(triggerKey(cfg.getTaskName()))
                .forJob(key)
                .withSchedule(CronScheduleBuilder
                        .cronSchedule(cfg.getCronExpression())
                        .withMisfireHandlingInstructionDoNothing()
                        .inTimeZone(TimeZone.getTimeZone(properties.getTimezone())))
                .build();

        if (!scheduler.checkExists(key)) {
            scheduler.scheduleJob(detail, newTrigger);
            log.info("[orbit-scheduler] scheduled task '{}' with cron '{}'", cfg.getTaskName(), cfg.getCronExpression());
            return;
        }
        Trigger current = scheduler.getTrigger(triggerKey(cfg.getTaskName()));
        if (current == null || !sameSchedule(current, newTrigger)) {
            scheduler.addJob(detail, true);
            scheduler.rescheduleJob(triggerKey(cfg.getTaskName()), newTrigger);
            log.info("[orbit-scheduler] rescheduled task '{}' with cron '{}'", cfg.getTaskName(), cfg.getCronExpression());
        }
    }

    private boolean sameSchedule(Trigger current, CronTrigger expected) {
        if (!(current instanceof CronTrigger)) {
            return false;
        }
        CronTrigger cur = (CronTrigger) current;
        String curCron = cur.getCronExpression() == null ? "" : cur.getCronExpression();
        String expCron = expected.getCronExpression() == null ? "" : expected.getCronExpression();
        String curTz = cur.getTimeZone() == null ? "" : cur.getTimeZone().getID();
        String expTz = expected.getTimeZone() == null ? "" : expected.getTimeZone().getID();
        return curCron.equals(expCron) && curTz.equals(expTz);
    }

    public boolean isQuartzClustered() {
        try {
            return scheduler.getMetaData().isJobStoreClustered();
        } catch (Exception e) {
            return false;
        }
    }

    public Map<String, Object> getQuartzInfo(String taskName) {
        Map<String, Object> info = new LinkedHashMap<String, Object>();
        try {
            JobKey key = jobKey(taskName);
            info.put("jobExists", scheduler.checkExists(key));
            Trigger trigger = scheduler.getTrigger(triggerKey(taskName));
            if (trigger == null) {
                info.put("triggerState", "NONE");
                info.put("nextFireTime", null);
                info.put("cron", null);
            } else {
                info.put("triggerState", scheduler.getTriggerState(triggerKey(taskName)).name());
                Date next = trigger.getNextFireTime();
                info.put("nextFireTime", next == null ? null : next.getTime());
                info.put("cron", trigger instanceof CronTrigger ? ((CronTrigger) trigger).getCronExpression() : null);
            }
        } catch (SchedulerException e) {
            info.put("error", e.getMessage());
        }
        return info;
    }

    // ==================================================================
    // 管理操作（REST API 调用）
    // ==================================================================

    public JobConfig createJob(JobConfig input) {
        validate(input, true);
        if (taskRepository.findByName(input.getTaskName()).isPresent()) {
            throw new IllegalArgumentException("Task '" + input.getTaskName() + "' already exists");
        }
        input.setTaskGroup(properties.getGroup());
        input.setCreatedAt(new Date());
        JobConfig saved = taskRepository.save(input);
        applySchedule(saved);
        return saved;
    }

    public JobConfig updateJob(String taskName, JobConfig input) {
        JobConfig existing = taskRepository.findByName(taskName)
                .orElseThrow(() -> new IllegalArgumentException("Task '" + taskName + "' not found"));
        validate(input, false);
        input.setTaskName(taskName);
        input.setTaskGroup(properties.getGroup());
        existing.copyEditableFrom(input);
        JobConfig saved = taskRepository.save(existing);
        applySchedule(saved);
        return saved;
    }

    public void deleteJob(String taskName) {
        taskRepository.findByName(taskName)
                .orElseThrow(() -> new IllegalArgumentException("Task '" + taskName + "' not found"));
        taskRepository.delete(taskName);
        try {
            scheduler.deleteJob(jobKey(taskName));
        } catch (SchedulerException e) {
            throw new IllegalStateException("Remove quartz job failed: " + e.getMessage(), e);
        }
    }

    public void pauseJob(String taskName) {
        requireExists(taskName);
        try {
            scheduler.pauseJob(jobKey(taskName));
        } catch (SchedulerException e) {
            throw new IllegalStateException("Pause job failed: " + e.getMessage(), e);
        }
    }

    public void resumeJob(String taskName) {
        requireExists(taskName);
        try {
            if (!scheduler.checkExists(jobKey(taskName))) {
                JobConfig cfg = taskRepository.findByName(taskName).get();
                scheduleOrUpdate(cfg);
            } else {
                scheduler.resumeJob(jobKey(taskName));
            }
        } catch (SchedulerException e) {
            throw new IllegalStateException("Resume job failed: " + e.getMessage(), e);
        }
    }

    /** 立即异步触发（走 Quartz 触发链路，与定时触发完全一致） */
    public void triggerNow(String taskName, Map<String, Object> params) {
        JobConfig cfg = requireExists(taskName);
        try {
            JobKey key = jobKey(taskName);
            if (!scheduler.checkExists(key)) {
                scheduleOrUpdate(cfg);
            }
            org.quartz.JobDataMap dataMap = new org.quartz.JobDataMap();
            if (params != null && !params.isEmpty()) {
                dataMap.put(QuartzJobDispatcher.DATA_KEY_PARAMS_JSON, toJson(params));
            }
            scheduler.triggerJob(key, dataMap);
        } catch (SchedulerException e) {
            throw new IllegalStateException("Trigger job failed: " + e.getMessage(), e);
        }
    }

    public PageResult<JobConfig> pageJobs(String nameLike, int page, int size) {
        return taskRepository.page(nameLike, page, size);
    }

    /** 单个任务详情：直接走 findByName，避免 page(name,1,1) 的模糊匹配 + 二次过滤 */
    public Optional<JobConfig> findOneJob(String taskName) {
        if (taskName == null || taskName.isEmpty()) {
            return Optional.<JobConfig>empty();
        }
        try {
            return taskRepository.findByName(taskName);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] findOneJob '{}' failed: {}", taskName, e.getMessage());
            return Optional.<JobConfig>empty();
        }
    }

    public PageResult<JobLog> pageLogs(String taskName, int page, int size) {
        return jobLogRepository.page(taskName, page, size);
    }

    public Map<String, Object> overview() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put("nodeId", nodeId);
        map.put("group", properties.getGroup());
        map.put("storageType", taskRepository.type());
        map.put("lockType", properties.getLock().isEnabled() ? lockProvider.type() : "disabled");
        map.put("logStorage", jobLogRepository.type());
        map.put("quartzClustered", isQuartzClustered());
        map.put("httpDispatchEnabled", properties.getHttpDispatch().isEnabled());
        // 性能优化：count() 替代 findAll().size()，避免全表加载
        map.put("taskCount", safeCount());
        try {
            map.put("scheduledCount", scheduler.getJobKeys(
                    GroupMatcher.jobGroupEquals(properties.getGroup())).size());
        } catch (SchedulerException e) {
            map.put("scheduledCount", -1);
        }
        return map;
    }

    /** 安全 count：捕获存储异常降级为 -1，不阻断 overview */
    private long safeCount() {
        try {
            return taskRepository.count();
        } catch (Exception e) {
            log.warn("[orbit-scheduler] count tasks failed: {}", e.getMessage());
            return -1L;
        }
    }

    // ==================================================================
    // 工具
    // ==================================================================

    private JobConfig requireExists(String taskName) {
        return taskRepository.findByName(taskName)
                .orElseThrow(() -> new IllegalArgumentException("Task '" + taskName + "' not found"));
    }

    private void applySchedule(JobConfig cfg) {
        try {
            scheduleOrUpdate(cfg);
        } catch (SchedulerException e) {
            throw new IllegalStateException("Apply quartz schedule failed: " + e.getMessage(), e);
        }
    }

    private void validate(JobConfig cfg, boolean forCreate) {
        if (forCreate) {
            if (cfg.getTaskName() == null || !cfg.getTaskName().trim().matches("[A-Za-z0-9_\\-\\.]{1,64}")) {
                throw new IllegalArgumentException("taskName must match [A-Za-z0-9_-.]{1,64}: " + cfg.getTaskName());
            }
        }
        if (cfg.getCronExpression() != null && !cfg.getCronExpression().trim().isEmpty()
                && !CronExpression.isValidExpression(cfg.getCronExpression())) {
            throw new IllegalArgumentException("invalid cron expression: " + cfg.getCronExpression());
        }
        if (cfg.getDispatchType() == null) {
            cfg.setDispatchType(DispatchType.LOCAL);
        }
    }

    public Map<String, Object> parseParamsJson(String json) {
        if (json == null || json.trim().isEmpty()) {
            return new HashMap<String, Object>();
        }
        try {
            Map<String, Object> m = objectMapper.readValue(json, Map.class);
            return m == null ? new HashMap<String, Object>() : m;
        } catch (Exception e) {
            log.warn("[orbit-scheduler] parse params json failed: {}", e.getMessage());
            return new HashMap<String, Object>();
        }
    }

    public String toJson(Map<String, Object> params) {
        try {
            return objectMapper.writeValueAsString(params);
        } catch (Exception e) {
            throw new IllegalArgumentException("serialize params failed: " + e.getMessage(), e);
        }
    }

    /** 生成新的 requestId：32 位无连字符的 UUID 字符串 */
    public static String newRequestId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private DispatchSummary summary(String taskName, String status, long startAt,
                                    String workerNode, String requestId, String message) {
        return new DispatchSummary(taskName, status, System.currentTimeMillis() - startAt,
                workerNode, requestId, message);
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MAX_MESSAGE_LEN ? s : s.substring(0, MAX_MESSAGE_LEN) + "...(truncated)";
    }

    private static boolean equalsOrNull(String a, String b) {
        return a == null ? b == null : a.equals(b);
    }

    private static String emptyToNull(String s) {
        return s == null || s.trim().isEmpty() ? null : s;
    }
}

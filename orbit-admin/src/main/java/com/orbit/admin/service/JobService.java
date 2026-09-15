package com.orbit.admin.service;

import com.orbit.admin.alert.AlertDispatcher;
import com.orbit.admin.alert.JobAlertEvent;
import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
import com.orbit.admin.dispatch.OutstandingDispatches;
import com.orbit.admin.quartz.OrbitQuartzJob;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.store.ColumnLimits;
import com.orbit.admin.store.JobStore;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.JobLogStatus;
import com.orbit.core.model.PageResult;
import com.orbit.core.model.RouteStrategy;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    /**
     * 单个回传请求允许携带的最大结果条数。执行器侧单批上限为 200 条，
     * 这里给出 10 倍余量做服务端守门：防止失控/恶意客户端一次投递海量条目，
     * 把回传端点变成内存与数据库的压力源。超限直接 400，整批不处理。
     */
    public static final int MAX_CALLBACK_BATCH = 2000;

    /** 任务级失败重试次数上限（不含首次执行）：防止误配出无意义的重试风暴 */
    public static final int MAX_RETRY_COUNT = 10;

    /** 任务级失败重试间隔上限（秒） */
    public static final int MAX_RETRY_INTERVAL_SECONDS = 3600;

    private final Scheduler scheduler;
    private final JobStore jobStore;
    private final ExecutorRegistry registry;
    private final ExecutorClient executorClient;
    private final AdminProperties properties;

    /** 在途执行登记簿：串行守卫的占用与释放，见 {@link OutstandingDispatches} */
    private final OutstandingDispatches outstanding;

    /** 告警异步分发器：触发最终失败 / 执行失败（回传 FAILED）时投递事件 */
    private final AlertDispatcher alertDispatcher;

    /**
     * 触发级重试定时器：单条守护线程负责所有任务的「同步派发失败后延迟重试」。
     * 只做时间触发，重试本身走完整派发链路（重新路由、重新写日志），不占用常驻线程。
     */
    private final ScheduledExecutorService triggerRetryPool;

    /**
     * @param scheduler      Quartz 调度器，任务的注册/暂停/恢复都作用在它上面
     * @param jobStore       任务与日志存储
     * @param registry       执行器注册表，派发时按策略选点
     * @param executorClient 执行器 HTTP 客户端
     * @param properties     调度中心配置，提供分组名与超时上限
     * @param outstanding    在途执行登记簿，同名任务串行守卫的依据
     * @param alertDispatcher 告警异步分发器
     */
    public JobService(Scheduler scheduler, JobStore jobStore, ExecutorRegistry registry,
                      ExecutorClient executorClient, AdminProperties properties,
                      OutstandingDispatches outstanding, AlertDispatcher alertDispatcher) {
        this.scheduler = scheduler;
        this.jobStore = jobStore;
        this.registry = registry;
        this.executorClient = executorClient;
        this.properties = properties;
        this.outstanding = outstanding;
        this.alertDispatcher = alertDispatcher;
        this.triggerRetryPool = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "orbit-admin-retry");
                t.setDaemon(true);
                return t;
            }
        });
    }

    /**
     * 容器销毁回调：关闭触发级重试定时器。
     * 直接 shutdownNow：未到期的重试直接放弃 —— 此类重试不落盘（见 README 已知限制），
     * 停机丢失是接受的语义，绝不延迟容器关闭。
     */
    @PreDestroy
    public void shutdownRetryPool() {
        triggerRetryPool.shutdownNow();
    }

    /**
     * 调度中心初始化方法。
     * 在系统启动时从数据库加载所有启用的定时任务并注册到 Quartz 调度器中。
     */
    @PostConstruct
    public void init() {
        // 时区合法性前置校验：TimeZone.getTimeZone 对非法 ID 静默回退 GMT，
        // 会导致所有 Cron 在错误时区触发且无任何告警 —— 在启动阶段直接失败更安全。
        String tz = properties.getTimezone();
        Set<String> available = new HashSet<String>(Arrays.asList(TimeZone.getAvailableIDs()));
        if (tz == null || !available.contains(tz)) {
            throw new IllegalStateException("invalid orbit.admin.timezone: " + tz
                    + " (must be a valid java.util.TimeZone id, e.g. Asia/Shanghai)");
        }

        List<JobInfo> jobs;
        try {
            jobs = jobStore.findAllJobs();
        } catch (Exception e) {
            // 读库失败意味着依赖不可用：让启动直接失败，
            // 避免调度中心以「零调度但一切正常」的姿态静默启动。
            throw new IllegalStateException("[orbit-admin] failed to load jobs on startup", e);
        }

        int[] counts = reloadSchedules(jobs);
        log.info("[orbit-admin] loaded {}/{} job(s) into quartz ({} already scheduled by peer node)",
                counts[0], jobs.size(), counts[1]);
    }

    /**
     * 把数据库中的全部任务重新装载进 Quartz（对账器周期调用入口）。
     *
     * 与 {@link #init()} 的差别：不做启动期校验，汇总结果只记 DEBUG —— 对账默认每分钟一次，
     * 每轮都打「loaded X/Y jobs」会成为纯噪音；单任务装载失败仍各自记 error，不会互相阻断。
     * 读库异常原样抛出，由对账器兜住后等待下一轮。
     */
    public void reloadSchedules() {
        reloadSchedules(jobStore.findAllJobs());
    }

    /**
     * 逐个任务装载进 Quartz：已存在的更新计划，多副本竞态（ObjectAlreadyExistsException）
     * 与单任务非法配置只记日志不阻断其余任务。
     *
     * @param jobs 数据库中的全部任务
     * @return [成功装载数, 已被对等副本装载而跳过数]
     */
    private int[] reloadSchedules(List<JobInfo> jobs) {
        int loaded = 0;
        int skipped = 0;
        for (JobInfo job : jobs) {
            try {
                scheduleOrUpdate(job);
                loaded++;
            } catch (ObjectAlreadyExistsException e) {
                // 集群（JDBC JobStore）模式下的正常竞态：多个副本同时启动时，
                // 两边都可能看到 checkExists(key)==false 并同时 scheduleJob，
                // 输的那个抛本异常 —— 任务其实已被另一个副本装好，不算失败。
                skipped++;
                log.debug("[orbit-admin] job {} already scheduled by another node", job.getJobName());
            } catch (Exception e) {
                // 单个任务装载失败（例如历史遗留的非法 cron）不应阻断其余任务
                log.error("[orbit-admin] init schedule failed for job={}", job.getJobName(), e);
            }
        }
        return new int[]{loaded, skipped};
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

        JobInfo saved;
        try {
            // 保存至数据库
            saved = jobStore.saveJob(input);
        } catch (DataIntegrityViolationException dup) {
            // check-then-act 竞态兜底：并发创建同名任务时，唯一约束保证只有一个胜出者，
            // 败者在此转换为与串行路径一致的友好错误（否则会以裸 500 暴露给调用方）。
            throw new IllegalArgumentException("job already exists: " + input.getJobName());
        }

        try {
            // 同步应用到 Quartz 调度器
            applySchedule(saved);
        } catch (RuntimeException scheduleFailed) {
            // DB 与 Quartz 必须同生共死：编排失败就回滚刚写入的行，
            // 否则会留下一条「任务列表里看得见、却永远不会触发」的幽灵任务，
            // 且每次重启 init() 都会重新装载它、再报一次同样的错。
            // 回滚之后，接口失败 == 什么都没发生。
            rollbackCreatedJob(saved);
            throw scheduleFailed;
        }
        return saved;
    }

    /**
     * 回滚「已落库但 Quartz 编排失败」的新建任务。
     * 回滚自身失败只记录不外抛，避免覆盖掉原始的调度失败原因。
     *
     * @param saved 已写入数据库的任务
     */
    private void rollbackCreatedJob(JobInfo saved) {
        try {
            jobStore.deleteJob(saved.getJobName());
            log.warn("[orbit-admin] job {} rolled back from db because quartz scheduling failed",
                    saved.getJobName());
        } catch (Exception rollbackFailed) {
            log.error("[orbit-admin] failed to roll back job {} after schedule failure",
                    saved.getJobName(), rollbackFailed);
        }
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

        // 更新前的快照：Quartz 编排失败时据此把库里的定义恢复原状。
        // 必须在覆盖字段之前取，否则快照拿到的就是新值。
        JobInfo snapshot = snapshotOf(existing);

        existing.setDescription(input.getDescription());
        existing.setAppName(input.getAppName());
        existing.setHandler(input.getHandler());
        existing.setCron(input.getCron());
        existing.setParams(input.getParams());
        existing.setTimeoutSeconds(input.getTimeoutSeconds());
        existing.setRouteStrategy(input.getRouteStrategy());
        existing.setRetryCount(input.getRetryCount());
        existing.setRetryIntervalSeconds(input.getRetryIntervalSeconds());
        existing.setSerialExecution(input.getSerialExecution());
        existing.setEnabled(input.isEnabled());

        JobInfo saved = jobStore.saveJob(existing);
        try {
            applySchedule(saved);
        } catch (RuntimeException scheduleFailed) {
            // 与 create 对称：Quartz 没换上新计划时，数据库也不能留着一份
            // 「接口说已保存、Quartz 却仍按旧 cron 跑」的新定义，否则两边永久不一致。
            rollbackUpdatedJob(snapshot);
            throw scheduleFailed;
        }
        return saved;
    }

    /**
     * 生成任务定义的快照（params 做防御性拷贝），用于 Quartz 编排失败时回滚。
     *
     * @param source 源任务
     * @return 与源任务字段一致、但相互独立的副本
     */
    private static JobInfo snapshotOf(JobInfo source) {
        JobInfo copy = new JobInfo();
        copy.setId(source.getId());
        copy.setJobName(source.getJobName());
        copy.setDescription(source.getDescription());
        copy.setAppName(source.getAppName());
        copy.setHandler(source.getHandler());
        copy.setCron(source.getCron());
        copy.setParams(source.getParams() == null ? null : new HashMap<String, Object>(source.getParams()));
        copy.setTimeoutSeconds(source.getTimeoutSeconds());
        copy.setRouteStrategy(source.getRouteStrategy());
        copy.setRetryCount(source.getRetryCount());
        copy.setRetryIntervalSeconds(source.getRetryIntervalSeconds());
        copy.setSerialExecution(source.getSerialExecution());
        copy.setEnabled(source.isEnabled());
        copy.setVersion(source.getVersion());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        return copy;
    }

    /**
     * 回滚一次失败的更新：把数据库行与 Quartz 计划一起恢复到更新前的状态。
     *
     * 版本号处理：第一次 {@code saveJob} 已经把库里的 version 从 N 抬到 N+1，
     * 而快照里仍是 N，直接回写会被乐观锁判定为并发冲突（影响 0 行 → 抛异常）。
     * 因此回滚前先把快照版本号对齐到库里当前的值。
     *
     * 回滚自身失败只记录不外抛，避免覆盖掉原始的调度失败原因。
     *
     * @param snapshot 更新前的任务定义
     */
    private void rollbackUpdatedJob(JobInfo snapshot) {
        try {
            snapshot.setVersion(snapshot.getVersion() + 1);
            jobStore.saveJob(snapshot);
            applySchedule(snapshot);
            log.warn("[orbit-admin] job {} rolled back to its previous definition "
                    + "because quartz scheduling failed", snapshot.getJobName());
        } catch (Exception rollbackFailed) {
            log.error("[orbit-admin] failed to roll back job {} after schedule failure",
                    snapshot.getJobName(), rollbackFailed);
        }
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
        // Quartz 清理失败仅记日志：DB 行已删除，此时抛异常等于「接口报错但改动已生效」，
        // 调用方重试只会得到 "job not found"，两边状态对不上。
        // DB 已无该任务，重启后 init() 也不会再装载它。
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
     * 与定时触发共用同名任务串行守卫（{@code orbit.admin.dispatch-serial-per-job=true} 时）：
     * 上一轮执行尚未收敛（结果未回传）时，手动触发被拒绝并返回明确原因，
     * 而不是与在跑实例并行执行；守卫关闭时行为与之前一致（总是派发）。
     *
     * 本方法仍是同步的：调用方需要立刻知道派发结果。
     *
     * @param jobName 任务名称
     * @param params  本次单次触发的动态临时入参（可为空）
     * @return 执行响应结果
     */
    public TriggerResult triggerNow(String jobName, Map<String, Object> params) {
        JobInfo job = require(jobName);
        // 任务级串行开关优先：null 表示跟随全局配置，true/false 为任务级覆盖
        boolean guard = isSerialExecution(job);
        // logId 预生成并预登记：与定时触发通道相同，消除「执行器毫秒级回传早于登记」的竞态。
        String logId = newLogId();
        if (guard && !outstanding.tryAcquire(jobName, logId)) {
            String msg = "manual trigger rejected: previous run of job '" + jobName
                    + "' is still awaiting callback (orbit.admin.dispatch-serial-per-job=true)";
            log.info("[orbit-admin] {}", msg);
            return TriggerResult.fail(logId, job.getId() == null ? 0 : job.getId(), null, 0, msg);
        }
        try {
            TriggerResult result = dispatch(job, params, logId);
            if (guard && (result == null || !result.isAccepted())) {
                // 同步失败（执行器不可达/饱和等）：日志已终态、不会有回传，槽位必须立刻释放。
                outstanding.release(logId);
            }
            return result;
        } catch (RuntimeException e) {
            if (guard) {
                outstanding.release(logId);
            }
            throw e;
        }
    }

    /**
     * 处理单条执行结果回传：把 RUNNING 日志收敛为终态，并释放在途登记簿。
     *
     * 收敛是条件更新（{@code WHERE log_id=? AND status='RUNNING'}），所以重复回传、
     * 回传与孤儿回收的竞态都不会覆盖已写入的结果，只是返回 false 表示本次未生效。
     * 无论是否生效都会释放在途登记 —— 日志已不在 RUNNING，串行守卫没有理由继续占用。
     *
     * @param result 执行结果，为 null 或缺 logId 时忽略
     * @return 是否真正把日志从 RUNNING 收敛为终态
     */
    public boolean handleCallback(TriggerResult result) {
        if (result == null || result.getLogId() == null || result.getLogId().trim().isEmpty()) {
            log.warn("[orbit-admin] callback without logId ignored");
            return false;
        }
        String logId = result.getLogId().trim();
        boolean applied;
        try {
            applied = jobStore.finishLogFromRunning(logId, result.isSuccess(), result.getWorkerNode(),
                    result.getCostMs(), result.getMessage());
        } finally {
            outstanding.release(logId);
        }
        if (applied) {
            log.info("[orbit-admin] callback logId={} job={} success={} {}ms",
                    logId, result.getJobId(), result.isSuccess(), result.getCostMs());
            // 执行最终失败（执行器侧的重试已在该次执行链内耗尽，回传即终局）：告警
            if (!result.isSuccess()) {
                fireExecutionFailedAlert(result);
            }
        } else {
            log.info("[orbit-admin] callback for logId={} ignored: log is no longer RUNNING "
                    + "(duplicate callback or already reaped)", logId);
        }
        return applied;
    }

    /**
     * 投递「执行失败」告警事件。
     *
     * 执行器回传会回填 jobName / appName / handler（新版 SDK）；
     * 字段缺失时按 logId 反查日志行补齐，保证告警事件总能定位到任务。
     * 反查失败不阻断：字段留空也照常投递，总比静默吞掉一条失败告警强。
     *
     * @param result 执行器回传的失败结果
     */
    private void fireExecutionFailedAlert(TriggerResult result) {
        String jobName = blankToNull(result.getJobName());
        String appName = result.getAppName();
        String handler = result.getHandler();
        if (jobName == null || appName == null || handler == null) {
            Optional<JobLog> found = jobStore.findLogByLogId(result.getLogId());
            if (found.isPresent()) {
                JobLog l = found.get();
                if (jobName == null) {
                    jobName = l.getJobName();
                }
                if (appName == null) {
                    appName = l.getAppName();
                }
                if (handler == null) {
                    handler = l.getHandler();
                }
            }
        }
        alertDispatcher.fire(new JobAlertEvent(JobAlertEvent.EXECUTION_FAILED, jobName, appName, handler,
                result.getLogId(), result.getWorkerNode(), result.getCostMs(), result.getMessage(), new Date()));
    }

    /** 空白字符串转 null（告警上下文补齐时的辅助判断） */
    private static String blankToNull(String s) {
        return s == null || s.trim().isEmpty() ? null : s.trim();
    }

    /**
     * 为被拒绝的派发补一条终态日志，并投递触发失败告警。
     *
     * 触发线程池饱和时任务根本没进队列，不会有执行器回传；不补这条日志，
     * 这次调度在日志里就完全没有痕迹，只能从 {@code /overview} 的 dispatchRejected 计数间接看到。
     * 自身异常只记录不外抛。
     *
     * 饱和场景刻意不做触发级重试（重试会进一步放大过载），只靠告警与
     * dispatchRejected 指标提示运维扩容；任务自身到期后由下一轮 Cron 自然接续。
     *
     * @param job    被拒绝的任务
     * @param reason 拒绝原因，写入日志 message
     */
    public void recordRejectedDispatch(JobInfo job, String reason) {
        Date now = new Date();
        JobLog rejected = new JobLog();
        rejected.setLogId(newLogId());
        rejected.setJobId(job.getId());
        rejected.setJobName(job.getJobName());
        rejected.setAppName(job.getAppName());
        rejected.setHandler(job.getHandler());
        rejected.setStatus(JobLogStatus.FAILED);
        rejected.setMessage(reason);
        rejected.setCostMs(0);
        rejected.setStartTime(now);
        rejected.setEndTime(now);
        jobStore.insertLog(rejected);
        alertDispatcher.fire(new JobAlertEvent(JobAlertEvent.TRIGGER_FAILED, job.getJobName(),
                job.getAppName(), job.getHandler(), rejected.getLogId(), null, 0, reason, now));
    }

    /**
     * 处理执行器批量回传的一批执行结果，把对应的 RUNNING 日志逐条收敛到终态。
     *
     * 执行器会把积压的结果打包成一个请求发送，因此这里是逐条处理、逐条幂等，
     * 单条被忽略（重复回传或已被回收）不影响同批其余结果。
     *
     * 服务端守门：单批条数不得超过 {@link #MAX_CALLBACK_BATCH}，超限抛出
     * {@link IllegalArgumentException}（由 Controller 转为 400）。回传的幂等收敛
     * 按 logId 逐条进行，分批重发是安全的。
     *
     * @param results 执行器回传的一批最终结果
     * @return 其中真正完成 RUNNING -> 终态 转换的条数
     */
    public int handleCallbacks(List<TriggerResult> results) {
        if (results == null || results.isEmpty()) {
            return 0;
        }
        if (results.size() > MAX_CALLBACK_BATCH) {
            throw new IllegalArgumentException("callback batch too large: " + results.size()
                    + " (max " + MAX_CALLBACK_BATCH + "); split the batch and retry");
        }
        int applied = 0;
        for (TriggerResult result : results) {
            if (handleCallback(result)) {
                applied++;
            }
        }
        if (results.size() > 1) {
            log.info("[orbit-admin] callback batch of {} item(s), {} applied", results.size(), applied);
        }
        return applied;
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
        return dispatch(job, extraParams, null, 1);
    }

    /**
     * 派发执行逻辑（带外部指定 logId 的版本，执行轮次固定为首次）。
     *
     * logId 允许由调用方预先生成：定时触发通道（DispatchExecutor）需要在派发前
     * 把 logId 预登记进串行守卫，以消除「执行器毫秒级回传早于登记」的竞态；
     * 手动触发则传 null，由本方法自行生成。
     *
     * @param job         任务元数据
     * @param extraParams 单次触发传入的覆盖参数（可为空）
     * @param logId       外部指定的调度日志 ID（可为空，为空时自动生成）
     * @return 受理回执或同步失败结果
     */
    public TriggerResult dispatch(JobInfo job, Map<String, Object> extraParams, String logId) {
        return dispatch(job, extraParams, logId, 1);
    }

    /**
     * 派发执行逻辑（完整版本：外部指定 logId + 执行轮次）。
     *
     * 执行轮次（attempt）服务于触发级失败重试：
     * attempt 为本次尝试的序号（1 = 首次），总尝试上限 = retryCount + 1。
     * 同步派发失败时，未达上限则按 retryIntervalSeconds 延迟安排下一次尝试
     * （新 logId、新日志行，失败原因标注「已安排重试」），达到上限则投递
     * {@code TRIGGER_FAILED} 告警 —— 执行级重试（同一 logId 内重跑）由执行器负责，
     * 两层重试互不叠加：只有执行器受理失败（含完全派发不出去）才走触发级重试。
     *
     * @param job         任务元数据
     * @param extraParams 单次触发传入的覆盖参数（可为空）
     * @param logId       外部指定的调度日志 ID（可为空，为空时自动生成）
     * @param attempt     执行轮次（1 起始；触发级重试传入递增后的轮次）
     * @return 受理回执或同步失败结果
     */
    public TriggerResult dispatch(JobInfo job, Map<String, Object> extraParams, String logId, int attempt) {
        // 1. 生成全局唯一日志 ID 与记录开始时间
        if (logId == null || logId.trim().isEmpty()) {
            logId = newLogId();
        }
        Date start = new Date();

        // 2. 插入初始运行中日志记录；重试轮次在 message 里标注，收敛后可见
        JobLog running = new JobLog();
        running.setLogId(logId);
        running.setJobId(job.getId());
        running.setJobName(job.getJobName());
        running.setAppName(job.getAppName());
        running.setHandler(job.getHandler());
        running.setStatus(JobLogStatus.RUNNING);
        if (attempt > 1) {
            running.setMessage("trigger retry attempt " + attempt + "/"
                    + (job.getRetryCount() + 1));
        }
        running.setStartTime(start);
        jobStore.insertLog(running);

        // 3~6 全程包在 try/catch 中：无论路由寻址、参数合并还是落库环节抛异常，
        // 都必须把日志从 RUNNING 收敛到终态，否则会留下永久 RUNNING 的僵尸记录
        // （没有任何后台任务会回收它）。
        ExecutorNode node = null;
        try {
            List<ExecutorNode> candidates = registry.listByApp(job.getAppName());
            if (candidates.isEmpty()) {
                String msg = "no online executor for appName=" + job.getAppName();
                return failTrigger(job, extraParams, logId, attempt, null, start, msg);
            }

            // 先按路由策略选起点，再对剩余节点做故障转移：
            // Pod 重建后 IP/Pod 名都会变，旧地址在心跳超时前仍在表里；
            // 连不上就立刻摘除并换下一个（对齐 XXL-JOB FAILOVER）。
            // 直接在已查出的 candidates 上选点，避免一次派发查两遍库/缓存。
            ExecutorNode preferred = registry.route(candidates, job.getAppName(),
                    job.getRouteStrategy(), job.getJobName());
            if (preferred != null) {
                candidates = rotateToFront(candidates, preferred.getAddress());
            }

            TriggerRequest req = new TriggerRequest();
            req.setJobId(job.getId() == null ? 0 : job.getId());
            req.setJobName(job.getJobName());
            req.setAppName(job.getAppName());
            req.setHandler(job.getHandler());
            req.setLogId(logId);
            req.setTimeoutSeconds(job.getTimeoutSeconds());
            // 失败重试参数随触发下发：执行级重试（同 logId 内重跑）由执行器执行
            req.setRetryCount(job.getRetryCount());
            req.setRetryIntervalSeconds(job.getRetryIntervalSeconds());
            Map<String, Object> merged = new HashMap<String, Object>();
            if (job.getParams() != null) {
                merged.putAll(job.getParams());
            }
            if (extraParams != null) {
                merged.putAll(extraParams);
            }
            req.setParams(merged);

            TriggerResult result = null;
            for (int i = 0; i < candidates.size(); i++) {
                node = candidates.get(i);
                result = executorClient.trigger(node.getAddress(), req);
                if (result.isSuccess()) {
                    break;
                }
                boolean last = i == candidates.size() - 1;
                if (last) {
                    break;
                }
                String failMsg = result.getMessage();
                if (looksUnreachable(failMsg)) {
                    // 明确的连接级失败：节点大概率已不存在（Pod 重建后旧 IP 未过期），
                    // 立刻摘除并换下一个（对齐 XXL-JOB FAILOVER），不等心跳超时。
                    registry.remove(job.getAppName(), node.getAddress());
                    log.warn("[orbit-admin] executor unreachable, evict and failover: {} @ {} ({})",
                            job.getAppName(), node.getAddress(), failMsg);
                    continue;
                }
                if (looksAmbiguous(failMsg)) {
                    // 模糊失败（connection reset / read timeout）：请求可能已被执行器受理。
                    // 换下一个节点重试（可用性优先），但绝不摘除本节点 —— 节点可能仍然健康，
                    // 是否下线交由心跳/超时剔除判断；同 logId 的执行器本地幂等会拦住同节点重复入池。
                    log.warn("[orbit-admin] trigger outcome unknown, failover without evicting: {} @ {} ({})",
                            job.getAppName(), node.getAddress(), failMsg);
                    continue;
                }
                // 业务性拒绝（饱和、handler 不存在等）：执行器明确答复，换节点也不会成功，直接终止。
                break;
            }

            String address = node == null ? null : node.getAddress();

            // 受理回执：执行器已入队、任务开始异步执行。日志必须保持 RUNNING，
            // 等执行器回传 /orbit/admin/callback 时再由 handleCallback 收敛到终态。
            // 此处若误判为终态，长任务会在真正跑完前就被记成 SUCCESS/FAILED。
            if (result.isAccepted()) {
                // 立刻记录承接节点：孤儿回收靠它判断执行器是否还活着
                jobStore.markDispatched(logId, address);
                log.info("[orbit-admin] job={} -> {} @ {} accepted, awaiting callback (logId={}, attempt={})",
                        job.getJobName(), job.getHandler(), address, logId, attempt);
                return result;
            }

            // 触发同步失败（执行器不可达、执行器饱和、路由失败等）：日志立刻收敛到 FAILED，
            // 不会有回传到达，因此不能留在 RUNNING 等孤儿回收；
            // 是否重试/告警由 failTrigger 统一裁决。
            return failTrigger(job, extraParams, logId, attempt, address, start, result.getMessage());
        } catch (RuntimeException e) {
            String address = node == null ? null : node.getAddress();
            long cost = System.currentTimeMillis() - start.getTime();
            String msg = "dispatch failed: " + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            log.error("[orbit-admin] dispatch error for job={}", job.getJobName(), e);
            return failTrigger(job, extraParams, logId, attempt, address, start, msg);
        }
    }

    /**
     * 触发同步失败的统一收尾：把日志收敛为 FAILED，并按任务的重试配置裁决
     * 「安排重试」还是「告警终局」。
     *
     * 两种出口：
     *
     *   - attempt 未达上限（attempt < retryCount + 1）：安排触发级重试
     *     （新 logId 新日志行），失败日志 message 标注重试计划，返回值 message 同步标注；
     *   - 已达上限：投递 {@code TRIGGER_FAILED} 告警（重试链路终局）。
     *
     * @param job        任务元数据
     * @param extraParams 原始覆盖参数（重试时透传）
     * @param logId      本次失败尝试的日志 ID
     * @param attempt    本次尝试轮次（1 起始）
     * @param address    派发目标地址（可为空）
     * @param start      本次尝试开始时间
     * @param message    失败原因
     * @return 携带终局说明的失败结果
     */
    private TriggerResult failTrigger(JobInfo job, Map<String, Object> extraParams, String logId, int attempt,
                                      String address, Date start, String message) {
        int maxAttempts = Math.max(1, job.getRetryCount() + 1);
        long cost = System.currentTimeMillis() - start.getTime();
        String finalMessage = message;

        if (attempt < maxAttempts) {
            long interval = Math.max(0, job.getRetryIntervalSeconds());
            finalMessage = message + " (trigger retry " + (attempt + 1) + "/" + maxAttempts
                    + " scheduled in " + interval + "s)";
            scheduleTriggerRetry(job, extraParams, attempt + 1, maxAttempts, interval, logId);
        } else {
            if (attempt > 1) {
                finalMessage = message + " (trigger failed after " + attempt + "/" + maxAttempts + " attempt(s))";
            }
            alertDispatcher.fire(new JobAlertEvent(JobAlertEvent.TRIGGER_FAILED, job.getJobName(),
                    job.getAppName(), job.getHandler(), logId, address, cost, finalMessage, new Date()));
        }

        jobStore.finishLog(logId, JobLogStatus.FAILED, address, cost, finalMessage);
        log.warn("[orbit-admin] job={} trigger failed (attempt {}/{}): {}",
                job.getJobName(), attempt, maxAttempts, finalMessage);
        return TriggerResult.fail(logId, job.getId() == null ? 0 : job.getId(), address, cost, finalMessage);
    }

    /**
     * 安排一次触发级重试（在重试定时器线程上延迟执行）。
     *
     * 重试到点后走 {@link #executeTriggerRetry}：重新读任务、重新走完整派发链路，
     * 因此 cron/手动触发失败与重试失败共用同一套路由、日志与告警逻辑。
     * 定时器已关闭（停机）时静默放弃：未到期的重试不落盘（见 README 已知限制）。
     *
     * @param job          失败时的任务快照
     * @param extraParams  原始覆盖参数
     * @param nextAttempt  下一次尝试轮次
     * @param maxAttempts  总尝试上限
     * @param intervalSec  延迟秒数
     * @param failedLogId  本次失败尝试的 logId（重试执行前会先幂等释放它占用的串行槽位）
     */
    private void scheduleTriggerRetry(JobInfo job, Map<String, Object> extraParams,
                                      int nextAttempt, int maxAttempts, long intervalSec, String failedLogId) {
        try {
            triggerRetryPool.schedule(
                    () -> executeTriggerRetry(job, extraParams, nextAttempt, maxAttempts, failedLogId),
                    intervalSec, TimeUnit.SECONDS);
        } catch (RejectedExecutionException shutdown) {
            log.warn("[orbit-admin] trigger retry for job {} abandoned: scheduler is shutting down",
                    job.getJobName());
        }
    }

    /**
     * 执行一次触发级重试（重试定时器线程调用）。
     *
     * 重试前重新读取任务最新定义：等待间隔内任务可能被删除、停用或修改了重试参数，
     * 以库里最新状态为准。串行守卫同样生效：若上一轮已在途（例如模糊失败后
     * 执行器其实受理了、结果尚未回传），本次重试主动让位 —— 宁可少跑一次
     * 也不违反「同名任务不并发」的契约。
     *
     * 关键时序：失败尝试自己的槽位必须先释放 —— 派发失败后调度中心在
     * dispatch 返回之后才释放槽位，而重试可能在释放之前就到点了
     * （interval=0 时尤甚）；若不先释放，重试会被守卫误判为「上一轮在跑」而永久丢失。
     * 先幂等释放失败尝试的 logId 再取新槽位，从根上消除这个窗口；
     * 若期间确实有更新的执行占住了槽位（不同 logId），本次释放不伤及它，重试照旧让位。
     *
     * @param staleJob    失败时的任务快照（仅用于取任务名）
     * @param extraParams 原始覆盖参数
     * @param attempt     本次重试轮次
     * @param maxAttempts 总尝试上限（仅用于日志展示；重试后再失败由 dispatch 按最新定义继续裁决）
     * @param failedLogId 失败尝试的日志 ID（先释放其槽位）
     */
    private void executeTriggerRetry(JobInfo staleJob, Map<String, Object> extraParams,
                                     int attempt, int maxAttempts, String failedLogId) {
        String jobName = staleJob.getJobName();
        try {
            JobInfo current = jobStore.findJobByName(jobName).orElse(null);
            if (current == null || !current.isEnabled()) {
                log.info("[orbit-admin] trigger retry aborted, job missing or disabled: {} (attempt {}/{})",
                        jobName, attempt, maxAttempts);
                return;
            }
            // 先释放失败尝试的槽位（幂等：已释放时无操作，绝不误伤其它轮次的槽位）
            outstanding.release(failedLogId);
            boolean guard = isSerialExecution(current);
            String logId = newLogId();
            if (guard && !outstanding.tryAcquire(jobName, logId)) {
                log.warn("[orbit-admin] trigger retry skipped, a newer run of {} is still in flight "
                        + "(attempt {}/{})", jobName, attempt, maxAttempts);
                return;
            }
            try {
                TriggerResult r = dispatch(current, extraParams, logId, attempt);
                if (guard && (r == null || !r.isAccepted())) {
                    outstanding.release(logId);
                }
            } catch (RuntimeException e) {
                if (guard) {
                    outstanding.release(logId);
                }
                log.error("[orbit-admin] trigger retry dispatch error for job {}", jobName, e);
            }
        } catch (Exception e) {
            // 重试自身失败只记日志：不能让异常杀死重试线程
            log.error("[orbit-admin] trigger retry failed unexpectedly for job {}", jobName, e);
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
            jobStore.finishLog(logId, JobLogStatus.FAILED, address, cost, message);
        } catch (Exception ex) {
            log.error("[orbit-admin] failed to finalize log {}: {}", logId, ex.getMessage(), ex);
        }
    }

    /**
     * 分页查询调度执行日志（支持任务名、状态、时间范围过滤）。
     *
     * @param jobName 任务名称过滤（可为空）
     * @param status  状态过滤（RUNNING / SUCCESS / FAILED，可为空，非法值拋 400）
     * @param from    起始时间过滤（start_time >= from，可为空）
     * @param to      截止时间过滤（start_time <= to，可为空）
     * @param page    页码
     * @param size    每页大小
     * @return 分页日志列表
     */
    public PageResult<JobLog> pageLogs(String jobName, String status, Date from, Date to, int page, int size) {
        // 状态过滤是失败排查（/logs?status=FAILED）的主入口：拼错的状态直接拒绝，
        // 而不是静默返回空列表让调用方误以为「没有失败记录」
        String normalizedStatus = null;
        if (status != null && !status.trim().isEmpty()) {
            String s = status.trim().toUpperCase(Locale.ROOT);
            switch (s) {
                case JobLogStatus.RUNNING, JobLogStatus.SUCCESS, JobLogStatus.FAILED -> normalizedStatus = s;
                default -> throw new IllegalArgumentException(
                        "status must be one of RUNNING/SUCCESS/FAILED: " + status);
            }
        }
        return jobStore.pageLogs(jobName, normalizedStatus, from, to, page, size);
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
     * 把首选地址旋到列表头部，其余顺序不变，便于 ROUND 后再 failover。
     */
    private static List<ExecutorNode> rotateToFront(List<ExecutorNode> list, String address) {
        if (address == null || list.size() <= 1) {
            return list;
        }
        int idx = -1;
        for (int i = 0; i < list.size(); i++) {
            if (address.equals(list.get(i).getAddress())) {
                idx = i;
                break;
            }
        }
        if (idx <= 0) {
            return list;
        }
        List<ExecutorNode> rotated = new ArrayList<ExecutorNode>(list.size());
        rotated.addAll(list.subList(idx, list.size()));
        rotated.addAll(list.subList(0, idx));
        return rotated;
    }

    /**
     * 生成全局唯一调度日志 ID（32 位无连字符 UUID）。
     */
    private static String newLogId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 判断派发失败是否像「对端已不存在」（连接拒绝 / 连接超时 / 无路由 / 域名不可解析），
     * 此时应立刻摘除旧 Pod IP，而不是等心跳超时。
     */
    private static boolean looksUnreachable(String message) {
        if (message == null) {
            return false;
        }
        String m = message.toLowerCase();
        return m.contains("connection refused")
                || m.contains("connect timed out")
                || m.contains("connectexception")
                || m.contains("no route to host")
                || m.contains("network is unreachable")
                || m.contains("failed to connect")
                || m.contains("unknownhost");
    }

    /**
     * 判断派发失败是否属于「结果不明」：请求可能已经被执行器受理（连接被重置、读超时）。
     * 这类失败允许故障转移到下一个节点，但绝不能据此摘除节点 —— 节点可能仍然健康，
     * 只是一次网络抖动；是否下线交由心跳超时剔除判断。
     */
    private static boolean looksAmbiguous(String message) {
        if (message == null) {
            return false;
        }
        String m = message.toLowerCase();
        boolean genericTimeout = m.contains("timeout") && !m.contains("connect timed out");
        return m.contains("connection reset")
                || m.contains("read timed out")
                || genericTimeout;
    }

    /**
     * 按名取任务，不存在时抛 {@link IllegalArgumentException}（由 Controller 的异常处理器转成 404/400）。
     *
     * @param name 任务名
     * @return 任务元数据
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
        // 列宽前置校验：超长值在此以 400 拒绝，而不是等入库失败后只剩一句 internal error
        ColumnLimits.requireMaxLength("appName", job.getAppName(), ColumnLimits.JOB_APP_NAME);
        ColumnLimits.requireMaxLength("handler", job.getHandler(), ColumnLimits.JOB_HANDLER);
        if (job.getCron() != null && !job.getCron().trim().isEmpty()
                && !CronExpression.isValidExpression(job.getCron())) {
            throw new IllegalArgumentException("invalid cron: " + job.getCron());
        }
        ColumnLimits.requireMaxLength("cron", job.getCron(), ColumnLimits.JOB_CRON_EXPR);
        // 路由策略：空值回填 ROUND；非空时必须属于合法集合（拼错的策略在此直接拒绝，
        // 而不是静默按 ROUND 处理、只能事后翻日志才发现），统一规范化为大写存储。
        // JDK 21 箭头 switch：合法值多标签命中后直接回填，default 一律拒绝。
        if (job.getRouteStrategy() == null || job.getRouteStrategy().trim().isEmpty()) {
            job.setRouteStrategy(RouteStrategy.ROUND);
        } else {
            // Locale.ROOT：默认 toUpperCase 在土耳其语等 locale 下会把 "first" 变成 "FİRST"，
            // 导致合法策略被误拒
            String s = job.getRouteStrategy().trim().toUpperCase(Locale.ROOT);
            switch (s) {
                case RouteStrategy.ROUND, RouteStrategy.RANDOM, RouteStrategy.FIRST,
                        RouteStrategy.CONSISTENT_HASH -> job.setRouteStrategy(s);
                default -> throw new IllegalArgumentException(
                        "routeStrategy must be one of ROUND/RANDOM/FIRST/CONSISTENT_HASH: "
                                + job.getRouteStrategy());
            }
        }
        // 失败重试参数校验：retryCount [0,10]、retryIntervalSeconds [0,3600]。
        // 上下限拒绝而不是静默钳位：重试风暴与超长重试窗口都是需要运维知情的配置错误。
        if (job.getRetryCount() < 0 || job.getRetryCount() > MAX_RETRY_COUNT) {
            throw new IllegalArgumentException("retryCount must be within [0," + MAX_RETRY_COUNT + "]");
        }
        if (job.getRetryIntervalSeconds() < 0 || job.getRetryIntervalSeconds() > MAX_RETRY_INTERVAL_SECONDS) {
            throw new IllegalArgumentException("retryIntervalSeconds must be within [0,"
                    + MAX_RETRY_INTERVAL_SECONDS + "]");
        }
        // 超时时间兜底 300 秒
        if (job.getTimeoutSeconds() <= 0) {
            job.setTimeoutSeconds(300);
        }
        // 按全局上限封顶：该值随触发下发给执行器做超时强制，也是孤儿回收硬上界的基准，
        // 无上限会让单个任务的超时口径脱离调度中心的回收阈值。
        // 长时间占住 Tomcat 线程（手动触发）或 Quartz 工作线程（定时触发）。
        int maxTimeout = properties.getMaxTimeoutSeconds();
        if (maxTimeout > 0 && job.getTimeoutSeconds() > maxTimeout) {
            log.warn("[orbit-admin] job {} timeoutSeconds {} exceeds max {}, capped",
                    job.getJobName(), job.getTimeoutSeconds(), maxTimeout);
            job.setTimeoutSeconds(maxTimeout);
        }
    }

    /**
     * 任务的生效串行开关：任务级显式配置优先，未配置（null）时跟随全局默认。
     *
     * @param job 任务元数据
     * @return true 表示同名任务串行（上一轮未收敛则跳过后续触发）
     */
    private boolean isSerialExecution(JobInfo job) {
        Boolean perJob = job.getSerialExecution();
        return perJob != null ? perJob : properties.isDispatchSerialPerJob();
    }

    /**
     * 构造 Quartz 的 JobKey：任务名 + 配置的分组名。
     *
     * @param name 任务名
     * @return JobKey
     */
    private JobKey jobKey(String name) {
        return JobKey.jobKey(name, properties.getGroup());
    }

    /**
     * 构造 Quartz 的 TriggerKey：与 JobKey 同名同组，一个任务对应一个触发器。
     *
     * @param name 任务名
     * @return TriggerKey
     */
    private TriggerKey triggerKey(String name) {
        return TriggerKey.triggerKey(name, properties.getGroup());
    }
}

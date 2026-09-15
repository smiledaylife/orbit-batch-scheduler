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

/**
 * DB -> Quartz 最终一致性对账器。
 *
 * <p>数据库是任务定义的事实来源，Quartz 是运行时调度状态。本组件周期性检查 Quartz 中
 * 是否存在数据库已删除的孤儿 Job，并重新执行 {@link JobService#init()} 修复数据库任务
 * 未正确注册或配置变更后的调度状态。</p>
 *
 * <p>Admin 多副本部署时使用 Redis 分布式锁确保同一时刻只有一个实例执行对账。Redis 不可用
 * 时直接跳过本轮，而不是退化为本地锁，避免多个实例同时修改 Quartz。</p>
 */
@Component
@ConditionalOnProperty(prefix = "orbit.admin", name = "execution-lease-enabled", havingValue = "true")
public class QuartzReconciler {

    private static final Logger log = LoggerFactory.getLogger(QuartzReconciler.class);

    /** Quartz 对账全局锁；TTL 略短于默认对账周期，避免实例异常退出后长期阻塞。 */
    private static final String LOCK_KEY = "orbit:quartz:reconcile:lock";
    private static final String LOCK_VALUE = "1";

    /** 只允许锁持有者释放锁，避免旧实例误删新实例刚获得的锁。 */
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

    /** 应用启动时记录对账器已经初始化；实际首次对账由定时任务按配置延迟执行。 */
    @PostConstruct
    public void initialReconcile() {
        log.info("[orbit-admin] quartz reconciler initialized");
    }

    /**
     * 周期执行 DB 与 Quartz 对账。
     * 对账失败不会影响主调度线程，下一周期会自动再次尝试。
     */
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

            // 删除 Quartz 中已经不存在于 DB 的任务，防止历史任务定义继续触发。
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

            // 复用统一初始化逻辑，把 DB 中存在但 Quartz 缺失/过期的任务重新注册。
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

    /** 尝试获得全局对账锁；Redis 故障时拒绝执行本轮对账。 */
    private boolean acquireLock() {
        try {
            Boolean acquired = redis.opsForValue().setIfAbsent(LOCK_KEY, LOCK_VALUE, 50L, TimeUnit.SECONDS);
            return Boolean.TRUE.equals(acquired);
        } catch (Exception e) {
            log.warn("[orbit-admin] redis unavailable; skip quartz reconciliation to avoid multi-node race");
            return false;
        }
    }

    /** 使用 Lua 按锁值校验后释放全局锁。 */
    private void releaseLock() {
        try {
            redis.execute(RELEASE, Arrays.asList(LOCK_KEY), LOCK_VALUE);
        } catch (Exception ignored) {
            // 锁有 TTL，即使主动释放失败也会自动过期。
        }
    }
}

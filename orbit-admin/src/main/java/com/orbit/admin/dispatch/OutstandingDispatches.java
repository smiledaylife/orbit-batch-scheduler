package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 在途执行登记簿：同名任务串行守卫的占用与释放。
 *
 * 登记发生在派发之前（而非拿到受理回执之后）：执行器可能毫秒级跑完任务并回传，
 * 若回传先于登记到达，释放动作会查不到映射而空过，随后登记把槽位永久占用，
 * 该任务从此不再被触发。预登记让「回传可见」先于「回传可能发生」，从根上消除竞态窗口。
 *
 * 两种实现模式（由 orbit.admin.execution-lease-enabled 切换）：
 *
 *   - 单机模式（默认）：JVM 内存映射（{@code ConcurrentHashMap}）作守卫，零外部依赖；
 *   - 集群模式：Redis Lease 作为跨副本事实来源，避免多个 Admin 副本同时认为同一个
 *       Job 可以执行。Lua 脚本保证占用、释放、续租的原子性，并以 logId 做所有权校验，
 *       防止旧一轮的迟到回调误释放新一轮执行的 Lease。Redis 故障时快速失败拒绝派发，
 *       绝不降级为本地锁 —— 否则故障期间多副本会重复执行同一任务。
 *
 * 容器装配固定走 {@link #OutstandingDispatches(ObjectProvider, AdminProperties)}
 * （{@code @Autowired} 消除多构造器歧义：orbit-admin 依赖 Redis starter，
 * StringRedisTemplate 默认存在；Lease 关闭时该模板仅闲置不建连）。
 */
@Component
public class OutstandingDispatches {

    private static final Logger log = LoggerFactory.getLogger(OutstandingDispatches.class);

    /** Redis Lease 键前缀：job -> logId 占用标记 */
    private static final String KEY_PREFIX = "orbit:execution:lease:";

    /** Redis Lease 键前缀：logId -> job 反向映射，供任意副本按回传 logId 定位任务 */
    private static final String LOG_PREFIX = "orbit:execution:lease-log:";

    /**
     * 原子占用：仅当 job 键不存在时写入 job->logId 与 logId->job 两条映射并设置 TTL。
     * ARGV 依次为 lease TTL 毫秒、logId、jobName。
     *
     * JDK 文本块：Lua 脚本按原始缩进呈现，比字符串拼接更易读、易比对 Redis 侧脚本。
     */
    private static final DefaultRedisScript<Long> ACQUIRE_SCRIPT = new DefaultRedisScript<Long>("""
            if redis.call('exists', KEYS[1]) == 0 then
                redis.call('psetex', KEYS[1], ARGV[1], ARGV[2]);
                redis.call('psetex', KEYS[2], ARGV[1], ARGV[3]);
                return 1;
            end;
            return 0;
            """, Long.class);

    /**
     * 原子释放：仅当 job 键的值仍等于本次回传的 logId 时删除两条映射。
     * 所有权校验保证旧一轮的迟到回调不会误删新一轮已占用的 Lease。
     */
    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<Long>("""
            if redis.call('get', KEYS[1]) == ARGV[1] then
                redis.call('del', KEYS[1]);
                redis.call('del', KEYS[2]);
                return 1;
            end;
            return 0;
            """, Long.class);

    /**
     * 原子续租：job 与 logId 两条映射都仍属于本 logId 时，重置两者 TTL。
     * 长任务靠周期续租维持占用，避免执行期间 Lease 先行过期导致重复派发。
     */
    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<Long>("""
            if redis.call('get', KEYS[1]) == ARGV[1] and redis.call('get', KEYS[2]) == ARGV[1] then
                redis.call('pexpire', KEYS[1], ARGV[2]);
                redis.call('pexpire', KEYS[2], ARGV[2]);
                return 1;
            end;
            return 0;
            """, Long.class);

    /** 在途登记正向映射：jobName -> 占用中的 logId */
    private final ConcurrentHashMap<String, String> logIdByJob = new ConcurrentHashMap<String, String>();

    /** 在途登记反向映射：logId -> jobName，回调按 logId 释放时定位任务 */
    private final ConcurrentHashMap<String, String> jobByLogId = new ConcurrentHashMap<String, String>();

    /** 因「上一轮未收敛」而被跳过的触发次数，经 /overview 的 dispatchSkipped 暴露 */
    private final AtomicLong skippedCount = new AtomicLong();

    /** Redis Lease 获取失败（含 Redis 不可用）累计次数 */
    private final AtomicLong leaseAcquireFailures = new AtomicLong();

    /** Redis Lease 续租失败累计次数 */
    private final AtomicLong leaseRenewFailures = new AtomicLong();

    /** Redis 客户端；仅集群 Lease 模式使用，单机便捷构造下为 null */
    private final StringRedisTemplate redis;

    /** 调度中心配置，提供 Lease 开关与 TTL/续租周期 */
    private final AdminProperties properties;

    /**
     * 创建单机模式登记簿：纯 JVM 内存守卫，不访问 Redis。
     *
     * 供测试与手动装配使用；容器装配固定走
     * {@link #OutstandingDispatches(ObjectProvider, AdminProperties)}。
     */
    public OutstandingDispatches() {
        // 显式指向私有规范构造器：null 无类型实参会同时匹配
        // (StringRedisTemplate, AdminProperties) 与 (ObjectProvider, AdminProperties) 两个构造器，
        // 必须强转消除重载歧义。
        this((StringRedisTemplate) null, new AdminProperties());
    }

    /**
     * 容器装配入口：注入配置与（可能存在的）Redis 客户端。
     *
     * {@code @Autowired} 显式选定本构造器：类上同时保留了无参便捷构造器，
     * 不标注时 Spring 对多构造器组件会回退到无参构造器，execution-lease-*
     * 配置与 Redis 客户端将永远注入不进来，Lease 模式静默失效。
     * 开启 Lease 但容器没有 StringRedisTemplate 时启动即失败（配置错误应尽早暴露）。
     *
     * @param redis      Redis 客户端提供者，集群 Lease 模式必需
     * @param properties 调度中心配置，提供 execution-lease-* 参数
     */
    @Autowired
    public OutstandingDispatches(ObjectProvider<StringRedisTemplate> redis, AdminProperties properties) {
        this(resolveRedis(redis, properties), properties);
        log.info("[orbit-admin] serial dispatch guard mode: {}",
                properties.isExecutionLeaseEnabled() ? "redis lease (cross-replica)" : "local jvm (single replica)");
    }

    /** 规范构造器：字段赋值的唯一入口 */
    private OutstandingDispatches(StringRedisTemplate redis, AdminProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /** 解析 Redis 客户端：Lease 开启时必须存在，否则启动失败并给出可操作的错误信息 */
    private static StringRedisTemplate resolveRedis(ObjectProvider<StringRedisTemplate> redis,
                                                    AdminProperties properties) {
        StringRedisTemplate template = redis.getIfAvailable();
        if (properties.isExecutionLeaseEnabled() && template == null) {
            throw new IllegalStateException(
                    "orbit.admin.execution-lease-enabled=true requires a StringRedisTemplate bean; "
                            + "check spring.data.redis.* configuration");
        }
        return template;
    }

    /**
     * 尝试占用任务槽位（预登记本次派发的 logId）。
     *
     * cluster 模式下 Redis 占用成功才允许派发；Redis 不可用时快速失败，
     * 不降级为本地锁，否则 Redis 故障期间多个 Admin 副本可能发生重复执行。
     *
     * @param jobName 任务名，null 视为不启用守卫
     * @param logId   本次派发预先生成的日志 ID
     * @return true=占用成功可以派发；false=上一轮尚未收敛，本次触发应跳过
     */
    public boolean tryAcquire(String jobName, String logId) {
        if (jobName == null) {
            return true;
        }
        if (logId == null || logId.trim().isEmpty()) {
            return false;
        }

        if (properties.isExecutionLeaseEnabled()) {
            try {
                Long result = redis.execute(ACQUIRE_SCRIPT,
                        Arrays.asList(jobKey(jobName), logKey(logId)),
                        String.valueOf(properties.getExecutionLeaseTtlMs()), logId, jobName);
                if (!Long.valueOf(1L).equals(result)) {
                    skippedCount.incrementAndGet();
                    return false;
                }
            } catch (RuntimeException e) {
                leaseAcquireFailures.incrementAndGet();
                // 快速失败并向上抛：宁可不派发，也不能在 Redis 故障期间放开跨副本互斥
                throw new IllegalStateException("redis execution lease unavailable; dispatch refused", e);
            }
        } else if (logIdByJob.putIfAbsent(jobName, logId) != null) {
            skippedCount.incrementAndGet();
            return false;
        }

        // 同步本地映射：集群模式用于续租任务清单与 Redis 故障时的兜底清理
        logIdByJob.put(jobName, logId);
        jobByLogId.put(logId, jobName);
        return true;
    }

    /**
     * 按 logId 释放在途槽位（回调收敛终态、同步失败、孤儿回收三条路径都会调用）。
     * Redis 模式不依赖当前 Admin JVM 是否持有本地映射，
     * 因此 callback 可以由任意 Admin 副本处理。
     *
     * @param logId 待释放的日志 ID
     * @return 是否真正释放了一个占用中的槽位
     */
    public boolean release(String logId) {
        if (logId == null || logId.trim().isEmpty()) {
            return false;
        }
        if (properties.isExecutionLeaseEnabled()) {
            try {
                String jobName = redis.opsForValue().get(logKey(logId));
                if (jobName == null) {
                    jobName = jobByLogId.get(logId);
                }
                if (jobName == null) {
                    return false;
                }
                Long result = redis.execute(RELEASE_SCRIPT,
                        Arrays.asList(jobKey(jobName), logKey(logId)), logId);
                removeLocal(jobName, logId);
                return Long.valueOf(1L).equals(result);
            } catch (RuntimeException e) {
                // 不能因为 Redis 瞬时异常阻断 callback 的 DB 状态收敛；Lease 自身会自然过期。
                // 本地兜底清理只清本 JVM 持有的映射；映射缺失时 remove 的 key 为 null 会抛 NPE，需先判空。
                String localJob = jobByLogId.get(logId);
                if (localJob != null) {
                    logIdByJob.remove(localJob, logId);
                }
                jobByLogId.remove(logId);
                return false;
            }
        }
        String jobName = jobByLogId.remove(logId);
        if (jobName == null) {
            return false;
        }
        return logIdByJob.remove(jobName, logId);
    }

    /**
     * 按任务名释放（触发同步失败、派发异常时使用）：定位该任务当前占用的 logId 后走统一释放。
     *
     * @param jobName 任务名，null 安全空操作
     */
    public void releaseByJob(String jobName) {
        if (jobName == null) {
            return;
        }
        String logId = logIdByJob.get(jobName);
        if (logId != null) {
            release(logId);
        }
    }

    /**
     * 周期续租（频率 {@code orbit.admin.execution-lease-renew-interval-ms}，必须小于 TTL 的三分之一）。
     * 只续租本 JVM 成功取得的 Lease；回调释放后本地映射消失，自然不再续。
     */
    @Scheduled(fixedDelayString = "${orbit.admin.execution-lease-renew-interval-ms:30000}")
    public void renewLeases() {
        if (!properties.isExecutionLeaseEnabled() || logIdByJob.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : logIdByJob.entrySet()) {
            String jobName = entry.getKey();
            String logId = entry.getValue();
            try {
                Long renewed = redis.execute(RENEW_SCRIPT,
                        Arrays.asList(jobKey(jobName), logKey(logId)),
                        logId, String.valueOf(properties.getExecutionLeaseTtlMs()));
                if (!Long.valueOf(1L).equals(renewed)) {
                    leaseRenewFailures.incrementAndGet();
                    log.warn("[orbit-admin] execution lease renewal lost: job={}, logId={}", jobName, logId);
                    removeLocal(jobName, logId);
                }
            } catch (RuntimeException e) {
                leaseRenewFailures.incrementAndGet();
                // 不主动删除本地映射：Redis 短暂抖动恢复后下一轮仍有机会续租；
                // TTL 到期则下一次派发自然可以接管。
                log.warn("[orbit-admin] execution lease renewal failed: job={}, logId={}", jobName, logId);
            }
        }
    }

    /** 当前占用中的槽位数（即等待回传的任务数） */
    public int outstanding() {
        return logIdByJob.size();
    }

    /** 累计跳过的触发次数 */
    public long skipped() {
        return skippedCount.get();
    }

    /** 累计 Lease 获取失败次数 */
    public long leaseAcquireFailures() {
        return leaseAcquireFailures.get();
    }

    /** 累计 Lease 续租失败次数 */
    public long leaseRenewFailures() {
        return leaseRenewFailures.get();
    }

    /** Redis Lease 正向键：orbit:execution:lease:{jobName} */
    private String jobKey(String jobName) {
        return KEY_PREFIX + jobName;
    }

    /** Redis Lease 反向键：orbit:execution:lease-log:{logId} */
    private String logKey(String logId) {
        return LOG_PREFIX + logId;
    }

    /** 清理本 JVM 的双向映射；remove(key, value) 形式避免误删新一轮已登记的映射 */
    private void removeLocal(String jobName, String logId) {
        if (jobName != null) {
            logIdByJob.remove(jobName, logId);
        }
        jobByLogId.remove(logId, jobName);
    }
}

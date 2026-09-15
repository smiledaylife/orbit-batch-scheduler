package com.orbit.admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * 调度中心配置属性。
 *
 * <p>统一承载注册中心、派发、超时、集群执行 Lease、Redis Stream callback 和 Quartz
 * 对账等运行参数。cluster profile 下会执行更严格的生产配置校验。</p>
 */
@Component
@ConfigurationProperties(prefix = "orbit.admin")
public class AdminProperties {

    /** Admin 与 Executor 之间的共享访问令牌；生产环境必须通过 Secret 注入。 */
    private String accessToken = "";
    /** Executor 心跳超过该时间未更新时，视为节点失活。 */
    private int heartbeatTimeoutSeconds = 90;
    /** Admin 访问 Executor 的 HTTP 连接超时时间。 */
    private int connectTimeoutMs = 3000;
    /** Quartz 与业务任务使用的统一任务分组。 */
    private String group = "ORBIT";
    /** 单个任务允许的最大执行超时时间，防止异常配置长期占用资源。 */
    private int maxTimeoutSeconds = 3600;
    /** Executor 地址白名单；cluster 模式必须显式配置，避免注册任意目标地址。 */
    private String executorAddressAllowPattern = "";
    /** Cron 表达式解析和 Quartz Trigger 使用的时区。 */
    private String timezone = "Asia/Shanghai";
    /** Executor 注册信息本地缓存 TTL。 */
    private long registryCacheTtlMs = 3000;
    /** 执行日志保留天数，供定时清理任务使用。 */
    private int logRetentionDays = 30;
    /** Admin 派发线程池大小。 */
    private int dispatchThreads = 64;
    /** 派发线程池等待队列容量；0 表示不缓存等待任务。 */
    private int dispatchQueueCapacity = 256;
    /** 同一 job 是否在 Admin 层面保证同一时刻最多一个执行实例。 */
    private boolean dispatchSerialPerJob = true;
    /** Admin 等待 Executor 接受触发请求的 HTTP 超时时间。 */
    private int triggerTimeoutSeconds = 10;

    /** Redis 集群级执行 Lease。单机开发模式默认关闭，cluster profile 开启。 */
    private boolean executionLeaseEnabled = false;
    /** Lease 持有时间；应明显大于 Redis/网络瞬时抖动。 */
    private long executionLeaseTtlMs = 120000L;
    /** Lease 续租周期，要求小于 TTL 的三分之一以留出故障恢复窗口。 */
    private long executionLeaseRenewIntervalMs = 30000L;

    /** Redis Stream callback 持久化。生产 cluster 模式开启。 */
    private boolean durableCallbackEnabled = false;
    /** Executor 写入、Admin 消费的 Redis Stream key。 */
    private String callbackStreamKey = "orbit:callback:stream";
    /** Admin Redis Stream Consumer Group 名称。 */
    private String callbackStreamGroup = "orbit-admin";
    /** DB 与 Quartz 状态对账周期。 */
    private long quartzReconcileIntervalMs = 60000L;
    /** 首次 Quartz 对账延迟，给应用启动和 Quartz 初始化预留时间。 */
    private long quartzReconcileInitialDelayMs = 15000L;

    /** Spring 当前启用的 profile，用于识别 cluster 生产模式。 */
    @Value("${spring.profiles.active:}")
    private String activeProfiles;

    /**
     * 应用启动阶段执行配置校验。
     * 配置错误应尽早失败，而不是等到真实任务触发时才暴露。
     */
    @PostConstruct
    public void validate() {
        if (heartbeatTimeoutSeconds < 5) {
            throw new IllegalStateException("orbit.admin.heartbeat-timeout-seconds must be >= 5");
        }
        if (maxTimeoutSeconds < 1) {
            throw new IllegalStateException("orbit.admin.max-timeout-seconds must be >= 1");
        }
        if (connectTimeoutMs < 1 || triggerTimeoutSeconds < 1) {
            throw new IllegalStateException("orbit.admin HTTP timeout settings must be > 0");
        }
        if (dispatchThreads < 1 || dispatchQueueCapacity < 0) {
            throw new IllegalStateException("orbit.admin dispatch thread/queue settings are invalid");
        }
        if (executionLeaseEnabled) {
            if (executionLeaseTtlMs < 5000L) {
                throw new IllegalStateException("orbit.admin.execution-lease-ttl-ms must be >= 5000");
            }
            if (executionLeaseRenewIntervalMs < 1000L || executionLeaseRenewIntervalMs * 3L >= executionLeaseTtlMs) {
                throw new IllegalStateException(
                        "orbit.admin.execution-lease-renew-interval-ms must be >= 1000 and less than one third of lease TTL");
            }
        }
        if (durableCallbackEnabled) {
            if (callbackStreamKey == null || callbackStreamKey.trim().isEmpty()) {
                throw new IllegalStateException("orbit.admin.callback-stream-key must not be empty");
            }
            if (callbackStreamGroup == null || callbackStreamGroup.trim().isEmpty()) {
                throw new IllegalStateException("orbit.admin.callback-stream-group must not be empty");
            }
        }
        if (quartzReconcileIntervalMs < 10000L || quartzReconcileInitialDelayMs < 0L) {
            throw new IllegalStateException("orbit.admin quartz reconciliation interval settings are invalid");
        }
        if (isClusterProfile()) {
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new IllegalStateException("orbit.admin.access-token is required in cluster profile");
            }
            if (executorAddressAllowPattern == null || executorAddressAllowPattern.trim().isEmpty()) {
                throw new IllegalStateException(
                        "orbit.admin.executor-address-allow-pattern is required in cluster profile");
            }
            if (!executionLeaseEnabled) {
                throw new IllegalStateException("orbit.admin.execution-lease-enabled must be true in cluster profile");
            }
            if (!durableCallbackEnabled) {
                throw new IllegalStateException("orbit.admin.durable-callback-enabled must be true in cluster profile");
            }
        }
    }

    /** 判断当前 Spring profile 是否包含 cluster。 */
    private boolean isClusterProfile() {
        if (activeProfiles == null) {
            return false;
        }
        for (String profile : activeProfiles.split(",")) {
            if ("cluster".equalsIgnoreCase(profile.trim())) {
                return true;
            }
        }
        return false;
    }

    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
    public int getHeartbeatTimeoutSeconds() { return heartbeatTimeoutSeconds; }
    public void setHeartbeatTimeoutSeconds(int value) { this.heartbeatTimeoutSeconds = value; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int value) { this.connectTimeoutMs = value; }
    public String getGroup() { return group; }
    public void setGroup(String value) { this.group = value; }
    public String getTimezone() { return timezone; }
    public void setTimezone(String value) { this.timezone = value; }
    public int getMaxTimeoutSeconds() { return maxTimeoutSeconds; }
    public void setMaxTimeoutSeconds(int value) { this.maxTimeoutSeconds = value; }
    public String getExecutorAddressAllowPattern() { return executorAddressAllowPattern; }
    public void setExecutorAddressAllowPattern(String value) { this.executorAddressAllowPattern = value; }
    public long getRegistryCacheTtlMs() { return registryCacheTtlMs; }
    public void setRegistryCacheTtlMs(long value) { this.registryCacheTtlMs = value; }
    public int getLogRetentionDays() { return logRetentionDays; }
    public void setLogRetentionDays(int value) { this.logRetentionDays = value; }
    public int getDispatchThreads() { return dispatchThreads; }
    public void setDispatchThreads(int value) { this.dispatchThreads = value; }
    public int getDispatchQueueCapacity() { return dispatchQueueCapacity; }
    public void setDispatchQueueCapacity(int value) { this.dispatchQueueCapacity = value; }
    public boolean isDispatchSerialPerJob() { return dispatchSerialPerJob; }
    public void setDispatchSerialPerJob(boolean value) { this.dispatchSerialPerJob = value; }
    public int getTriggerTimeoutSeconds() { return triggerTimeoutSeconds; }
    public void setTriggerTimeoutSeconds(int value) { this.triggerTimeoutSeconds = value; }
    public boolean isExecutionLeaseEnabled() { return executionLeaseEnabled; }
    public void setExecutionLeaseEnabled(boolean value) { this.executionLeaseEnabled = value; }
    public long getExecutionLeaseTtlMs() { return executionLeaseTtlMs; }
    public void setExecutionLeaseTtlMs(long value) { this.executionLeaseTtlMs = value; }
    public long getExecutionLeaseRenewIntervalMs() { return executionLeaseRenewIntervalMs; }
    public void setExecutionLeaseRenewIntervalMs(long value) { this.executionLeaseRenewIntervalMs = value; }
    public boolean isDurableCallbackEnabled() { return durableCallbackEnabled; }
    public void setDurableCallbackEnabled(boolean value) { this.durableCallbackEnabled = value; }
    public String getCallbackStreamKey() { return callbackStreamKey; }
    public void setCallbackStreamKey(String value) { this.callbackStreamKey = value; }
    public String getCallbackStreamGroup() { return callbackStreamGroup; }
    public void setCallbackStreamGroup(String value) { this.callbackStreamGroup = value; }
    public long getQuartzReconcileIntervalMs() { return quartzReconcileIntervalMs; }
    public void setQuartzReconcileIntervalMs(long value) { this.quartzReconcileIntervalMs = value; }
    public long getQuartzReconcileInitialDelayMs() { return quartzReconcileInitialDelayMs; }
    public void setQuartzReconcileInitialDelayMs(long value) { this.quartzReconcileInitialDelayMs = value; }
}

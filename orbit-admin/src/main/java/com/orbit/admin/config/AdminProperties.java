package com.orbit.admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 调度中心配置属性类（对应配置前缀：{@code orbit.admin.*}）。
 * 涵盖调度中心鉴权令牌、执行器心跳超时阈值、派发超时控制以及 Quartz 分组和时区配置。
 */
@Component
@ConfigurationProperties(prefix = "orbit.admin")
public class AdminProperties {

    /**
     * 安全访问令牌（Token）。
     * 用于调度中心与执行器之间的鉴权认证：执行器注册/心跳，以及调度中心向执行器派发任务时进行双向校验。
     * 为空时跳过鉴权。
     */
    private String accessToken = "";

    /**
     * 执行器心跳超时剔除时间（秒），默认为 90 秒。
     * 调度中心后台任务将周期性扫描注册表，若节点的最近心跳时间距当前时间超过该阈值，
     * 则判定该执行器实例失联并从内存注册表中剔除。
     */
    private int heartbeatTimeoutSeconds = 90;

    /**
     * 调度中心向执行器发起 HTTP 调用时的建立连接超时时间（毫秒），默认为 3000ms（3秒）。
     */
    private int connectTimeoutMs = 3000;

    /**
     * Quartz 调度框架内部的任务分组名称，默认为 "ORBIT"。
     */
    private String group = "ORBIT";

    /**
     * 单个任务允许的最大执行超时（秒），默认 3600。
     * 该上限同时用于两处：
     *   - 任务元数据校验时对 {@code timeoutSeconds} 做封顶；
     *   - 派发调用时限制 HTTP readTimeout，避免长时间占住 Tomcat / Quartz 工作线程，
     *       并防止 {@code timeoutSeconds * 1000} 的 int 溢出。
     */
    private int maxTimeoutSeconds = 3600;

    /**
     * 执行器注册地址白名单（Java 正则，需匹配整个 address），默认为空表示不启用。
     * 未启用时仍会做基础校验：必须是 http/https、必须有 host、禁止链路本地地址（169.254.0.0/16）。
     * 生产环境建议显式配置，以杜绝执行器注册接口被用于 SSRF。
     */
    private String executorAddressAllowPattern = "";

    /**
     * 定时任务 Cron 表达式计算所依据的时区标识，默认为 "Asia/Shanghai"。
     */
    private String timezone = "Asia/Shanghai";

    /**
     * 执行器注册表本地缓存 TTL（毫秒），默认 3000。
     *
     * 调度热路径（Quartz 每次触发、手动触发、API 查询）经本缓存提供，
     * 高频调度下注册表查询 QPS 比逐次直查数据库低 1~2 个数量级。
     * 缓存要点：
     *   - TTL 远小于心跳超时阈值（默认 90s），陈旧度上界可控；
     *       （XXL-JOB 调度中心为纯内存注册表 + 30 秒 DB 拉取，本实现 3 秒 TTL 远比其新鲜）；
     *   - 本进程内的 register / remove / evict 写操作会立即失效缓存；
     *   - 多副本部署时其他副本的写入经 TTL 自然传播，最大延迟即 TTL；
     *   - 命中过期节点的派发由 failover（不可达即摘除换节点）兜底。
     * 设为 0 表示关闭缓存，恢复每次直查数据库。
     */
    private long registryCacheTtlMs = 3000;

    /**
     * 调度执行日志保留天数，默认 30 天（0 表示关闭自动清理）。
     *
     * {@code orbit_job_log} 无限增长会拖垮查询与备份；后台任务周期性
     * 删除 start_time 早于保留期的日志（分批删除，避免大事务锁表）。
     */
    private int logRetentionDays = 30;

    /**
     * 触发线程池大小，默认 64。
     * 触发是对执行器的短 HTTP 调用（读超时见 trigger-timeout-seconds，与任务耗时无关），
     * 线程只在触发往返期间被占用，因此可以远大于 {@code org.quartz.threadPool.threadCount}，
     * 两者相互独立。
     */
    private int dispatchThreads = 64;

    /**
     * 定时派发排队队列容量，默认 256；0 表示不排队。
     * 队列满时新触发快速失败，并写入一条 FAILED 调度日志（reason = scheduler saturated）。
     */
    private int dispatchQueueCapacity = 256;

    /**
     * 同一任务串行执行开关，默认 true。
     * 开启时，上一轮**执行**尚未结束的任务在本次 Cron 到点会被跳过：只累加计数、不写日志，
     * 避免高频 Cron 配慢 Handler 时刷爆日志表；计数经 /orbit/admin/overview 的
     * dispatchSkipped 暴露。
     *
     * 判定口径是「上一轮的执行结果是否已回传」：派发时占用守卫，
     * 收到执行器回传或孤儿回收把日志收敛到终态时释放。
     *
     * 该守卫是进程内的，只保证单副本内不重叠；
     * 跨副本不重叠依赖 Quartz 集群的行锁（同一 trigger 只被一个副本触发）。
     */
    private boolean dispatchSerialPerJob = true;

    /**
     * 触发请求的 HTTP 读超时（秒），默认 10。
     *
     * 触发是异步契约：执行器入队后立即回执，因此该超时只需覆盖「网络往返 + 入队」，
     * 与任务真实耗时无关，不必也不应该跟着 max-timeout-seconds 放大。
     * 任务真实耗时的上限由 max-timeout-seconds（传给执行器做超时强制）约束。
     */
    private int triggerTimeoutSeconds = 10;

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public int getHeartbeatTimeoutSeconds() {
        return heartbeatTimeoutSeconds;
    }

    public void setHeartbeatTimeoutSeconds(int heartbeatTimeoutSeconds) {
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public int getMaxTimeoutSeconds() {
        return maxTimeoutSeconds;
    }

    public void setMaxTimeoutSeconds(int maxTimeoutSeconds) {
        this.maxTimeoutSeconds = maxTimeoutSeconds;
    }

    public String getExecutorAddressAllowPattern() {
        return executorAddressAllowPattern;
    }

    public void setExecutorAddressAllowPattern(String executorAddressAllowPattern) {
        this.executorAddressAllowPattern = executorAddressAllowPattern;
    }

    public long getRegistryCacheTtlMs() {
        return registryCacheTtlMs;
    }

    public void setRegistryCacheTtlMs(long registryCacheTtlMs) {
        this.registryCacheTtlMs = registryCacheTtlMs;
    }

    public int getLogRetentionDays() {
        return logRetentionDays;
    }

    public void setLogRetentionDays(int logRetentionDays) {
        this.logRetentionDays = logRetentionDays;
    }

    public int getDispatchThreads() {
        return dispatchThreads;
    }

    public void setDispatchThreads(int dispatchThreads) {
        this.dispatchThreads = dispatchThreads;
    }

    public int getDispatchQueueCapacity() {
        return dispatchQueueCapacity;
    }

    public void setDispatchQueueCapacity(int dispatchQueueCapacity) {
        this.dispatchQueueCapacity = dispatchQueueCapacity;
    }

    public boolean isDispatchSerialPerJob() {
        return dispatchSerialPerJob;
    }

    public void setDispatchSerialPerJob(boolean dispatchSerialPerJob) {
        this.dispatchSerialPerJob = dispatchSerialPerJob;
    }

    public int getTriggerTimeoutSeconds() {
        return triggerTimeoutSeconds;
    }

    public void setTriggerTimeoutSeconds(int triggerTimeoutSeconds) {
        this.triggerTimeoutSeconds = triggerTimeoutSeconds;
    }
}

package com.orbit.executor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 执行器核心配置属性（对应前缀：{@code orbit.executor.*}）。
 *
 * 配置覆盖 Admin 地址、注册/心跳、工作线程池、任务等待、callback 重试以及 Redis
 * 执行幂等和持久化 callback。生产多副本场景建议通过环境变量或配置中心注入。
 */
@ConfigurationProperties(prefix = "orbit.executor")
public class ExecutorProperties {

    /** 是否启用 Orbit Executor 自动装配。 */
    private boolean enabled = true;
    /** Executor 注册到 Admin 时使用的应用名称，任务 appName 与之严格对应。 */
    private String appName = "orbit-executor";
    /** Admin 地址列表，多个地址以英文逗号分隔（容灾多活，逐个尝试，任一成功即算成功）。 */
    private String adminAddresses = "http://127.0.0.1:8080";
    /** Executor 对外暴露的地址；为空时按「显式配置 > POD_IP > 本机 IP > 127.0.0.1」推导。 */
    private String address = "";
    /** Executor HTTP 服务端口；0 表示自动感知，继承应用自身的 server.port。 */
    private int port = 0;
    /** 与 Admin 通信使用的访问令牌；生产环境必须通过 Secret 注入。 */
    private String accessToken = "";
    /** Executor 向 Admin 发送心跳的周期（毫秒），保底不低于 5000。 */
    private long heartbeatIntervalMs = 20000;
    /** Executor 节点唯一标识；为空时取 POD_NAME / 主机名。 */
    private String nodeId = "";
    /** 任务工作线程数：单节点并发上限，同时作为超时强制中断的依据；下限 1。 */
    private int workerThreads = 8;
    /** 工作线程池等待队列容量，限制本地任务堆积；满则快速失败（executor saturated）。 */
    private int queueCapacity = 256;
    /**
     * 任务工作线程是否改用 JDK 21 虚拟线程。
     * 并发上限、排队与拒绝语义完全不变（worker-threads 仍是单节点并发上限，
     * 超时看门狗 cancel(true) 对虚拟线程同样生效）；仅线程实现不同。
     * 适合 HTTP/DB 等 IO 密集型任务（虚拟线程让等待不占平台线程）；
     * 业务代码大量使用 synchronized 阻塞时，JDK 21 下会钉住载体线程（JEP 491 于 JDK 24 才解除），
     * 此时建议保持默认的平台线程。默认关闭。
     */
    private boolean workerVirtualThreads = false;
    /** 触发请求未携带 timeoutSeconds 时的兜底等待/执行窗口（秒），实际等待有 1 秒下限。 */
    private int maxJobWaitSeconds = 86400;
    /** callback 失败后的额外重试次数（不含首次）；重试耗尽整批退回队列。 */
    private int callbackRetryTimes = 3;
    /** callback 重试之间的退避间隔（毫秒）。 */
    private long callbackRetryIntervalMs = 2000;
    /**
     * 兼容保留配置；callback 实际使用无界队列避免因容量限制主动丢结果。
     * @deprecated 无界队列策略下该配置不再生效，仅为老配置文件兼容保留；见 {@link com.orbit.executor.client.CallbackClient}。
     */
    @Deprecated
    private int callbackQueueCapacity = 1000;

    /** Redis 执行幂等保护开关：开启后同 logId 在 TTL 内最多进入一次执行线程池。 */
    private boolean executionIdempotencyEnabled = false;
    /** Redis 幂等 key 保留时间（秒）；应覆盖最大任务耗时及 callback 重试窗口。 */
    private long executionIdempotencyTtlSeconds = 86400;
    /** 是否启用 Redis Stream 持久化 callback；cluster 生产模式建议开启。 */
    private boolean durableCallbackEnabled = false;
    /** Executor 写入、Admin 消费的 Redis Stream key，两端必须一致。 */
    private String callbackStreamKey = "orbit:callback:stream";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }
    public String getAdminAddresses() { return adminAddresses; }
    public void setAdminAddresses(String adminAddresses) { this.adminAddresses = adminAddresses; }
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }
    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
    public long getHeartbeatIntervalMs() { return heartbeatIntervalMs; }
    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) { this.heartbeatIntervalMs = heartbeatIntervalMs; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public int getWorkerThreads() { return workerThreads; }
    public void setWorkerThreads(int workerThreads) { this.workerThreads = workerThreads; }
    public int getQueueCapacity() { return queueCapacity; }
    public void setQueueCapacity(int queueCapacity) { this.queueCapacity = queueCapacity; }
    public boolean isWorkerVirtualThreads() { return workerVirtualThreads; }
    public void setWorkerVirtualThreads(boolean value) { this.workerVirtualThreads = value; }
    public int getMaxJobWaitSeconds() { return maxJobWaitSeconds; }
    public void setMaxJobWaitSeconds(int maxJobWaitSeconds) { this.maxJobWaitSeconds = maxJobWaitSeconds; }
    public int getCallbackRetryTimes() { return callbackRetryTimes; }
    public void setCallbackRetryTimes(int callbackRetryTimes) { this.callbackRetryTimes = callbackRetryTimes; }
    public long getCallbackRetryIntervalMs() { return callbackRetryIntervalMs; }
    public void setCallbackRetryIntervalMs(long callbackRetryIntervalMs) { this.callbackRetryIntervalMs = callbackRetryIntervalMs; }
    /** @deprecated 兼容保留，不再生效 */
    @Deprecated
    public int getCallbackQueueCapacity() { return callbackQueueCapacity; }
    /** @deprecated 兼容保留，不再生效 */
    @Deprecated
    public void setCallbackQueueCapacity(int callbackQueueCapacity) { this.callbackQueueCapacity = callbackQueueCapacity; }
    public boolean isExecutionIdempotencyEnabled() { return executionIdempotencyEnabled; }
    public void setExecutionIdempotencyEnabled(boolean value) { this.executionIdempotencyEnabled = value; }
    public long getExecutionIdempotencyTtlSeconds() { return executionIdempotencyTtlSeconds; }
    public void setExecutionIdempotencyTtlSeconds(long value) { this.executionIdempotencyTtlSeconds = value; }
    public boolean isDurableCallbackEnabled() { return durableCallbackEnabled; }
    public void setDurableCallbackEnabled(boolean value) { this.durableCallbackEnabled = value; }
    public String getCallbackStreamKey() { return callbackStreamKey; }
    public void setCallbackStreamKey(String value) { this.callbackStreamKey = value; }
}

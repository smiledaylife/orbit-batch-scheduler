package com.orbit.executor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 执行器核心配置属性（对应前缀：{@code orbit.executor.*}）。
 *
 * <p>配置覆盖 Admin 地址、注册/心跳、工作线程池、任务等待、callback 重试以及 Redis
 * 执行幂等和持久化 callback。生产多副本场景建议通过环境变量或配置中心注入。</p>
 */
@ConfigurationProperties(prefix = "orbit.executor")
public class ExecutorProperties {

    /** 是否启用 Orbit Executor 自动装配。 */
    private boolean enabled = true;
    /** Executor 注册到 Admin 时使用的应用名称。 */
    private String appName = "orbit-executor";
    /** Admin 地址列表，多个地址以配置约定的分隔符组织。 */
    private String adminAddresses = "http://127.0.0.1:8080";
    /** Executor 对外暴露的地址；为空时由运行环境/Bootstrap 自动推导。 */
    private String address = "";
    /** Executor HTTP 服务端口；0 表示使用应用容器的默认端口配置。 */
    private int port = 0;
    /** 与 Admin 通信使用的访问令牌；生产环境必须通过 Secret 注入。 */
    private String accessToken = "";
    /** Executor 向 Admin 发送心跳的周期。 */
    private long heartbeatIntervalMs = 20000;
    /** Executor 节点唯一标识；K8S 场景通常使用 Pod 名称。 */
    private String nodeId = "";
    /** 执行任务的工作线程数。 */
    private int workerThreads = 8;
    /** 工作线程池等待队列容量，用于限制本地任务堆积。 */
    private int queueCapacity = 256;
    /** 单个任务最长允许等待/执行的时间窗口，单位秒。 */
    private int maxJobWaitSeconds = 86400;
    /** callback 失败后的额外重试次数。 */
    private int callbackRetryTimes = 3;
    /** callback 重试之间的等待时间，单位毫秒。 */
    private long callbackRetryIntervalMs = 2000;
    /** 保留兼容配置；callback 实际使用无界队列避免因容量限制主动丢结果。 */
    private int callbackQueueCapacity = 1000;

    /** Redis 幂等保护。生产多副本建议开启，使用 logId 防止 HTTP 超时重试造成重复执行。 */
    private boolean executionIdempotencyEnabled = false;
    /** Redis 幂等 key 保留时间；应覆盖最大任务耗时及 callback 重试窗口。 */
    private long executionIdempotencyTtlSeconds = 86400;
    /** 使用 Redis Stream 持久化执行结果；cluster 生产模式建议开启。 */
    private boolean durableCallbackEnabled = false;
    /** Executor 写入、Admin 消费的 Redis Stream key。 */
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
    public int getMaxJobWaitSeconds() { return maxJobWaitSeconds; }
    public void setMaxJobWaitSeconds(int maxJobWaitSeconds) { this.maxJobWaitSeconds = maxJobWaitSeconds; }
    public int getCallbackRetryTimes() { return callbackRetryTimes; }
    public void setCallbackRetryTimes(int callbackRetryTimes) { this.callbackRetryTimes = callbackRetryTimes; }
    public long getCallbackRetryIntervalMs() { return callbackRetryIntervalMs; }
    public void setCallbackRetryIntervalMs(long callbackRetryIntervalMs) { this.callbackRetryIntervalMs = callbackRetryIntervalMs; }
    public int getCallbackQueueCapacity() { return callbackQueueCapacity; }
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

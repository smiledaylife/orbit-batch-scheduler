package com.orbit.executor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** 执行器核心配置属性（对应前缀：{@code orbit.executor.*}）。 */
@ConfigurationProperties(prefix = "orbit.executor")
public class ExecutorProperties {
    private boolean enabled = true;
    private String appName = "orbit-executor";
    private String adminAddresses = "http://127.0.0.1:8080";
    private String address = "";
    private int port = 0;
    private String accessToken = "";
    private long heartbeatIntervalMs = 20000;
    private String nodeId = "";
    private int workerThreads = 8;
    private int queueCapacity = 256;
    private int maxJobWaitSeconds = 86400;
    private int callbackRetryTimes = 3;
    private long callbackRetryIntervalMs = 2000;
    private int callbackQueueCapacity = 1000;

    /** Redis 幂等保护。生产多副本建议开启，使用 logId 防止 HTTP 超时重试造成重复执行。 */
    private boolean executionIdempotencyEnabled = false;
    /** Redis 幂等 key 保留时间；应覆盖最大任务耗时及 callback 重试窗口。 */
    private long executionIdempotencyTtlSeconds = 86400;

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
}

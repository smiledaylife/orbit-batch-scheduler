package com.orbit.executor;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 执行器配置 orbit.executor.*
 */
@ConfigurationProperties(prefix = "orbit.executor")
public class ExecutorProperties {

    /** 是否启用执行器 */
    private boolean enabled = true;
    /** 执行器应用名（调度中心任务 appName 与此匹配） */
    private String appName = "orbit-executor";
    /** 调度中心地址，如 http://orbit-admin:8080 */
    private String adminAddresses = "http://127.0.0.1:8080";
    /** 本执行器对外可访问地址；空则自动探测 */
    private String address = "";
    /** 本机监听端口（用于拼接 address） */
    private int port = 8081;
    /** 与调度中心 access-token 一致 */
    private String accessToken = "";
    /** 心跳间隔毫秒 */
    private long heartbeatIntervalMs = 20000;
    /** 节点 ID；空则取主机名 */
    private String nodeId = "";

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
}

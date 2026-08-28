package com.orbit.executor;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 执行器核心配置属性（对应前缀：{@code orbit.executor.*}）。
 * <p>提供执行器的应用名称、调度中心地址、心跳频率、安全凭证等核心参数配置。
 */
@ConfigurationProperties(prefix = "orbit.executor")
public class ExecutorProperties {

    /**
     * 是否启用执行器组件（默认为 true）。
     * <p>设为 false 时将跳过执行器的自动注册和心跳逻辑。
     */
    private boolean enabled = true;

    /**
     * 执行器所属应用名称（例如：order-service）。
     * <p>调度中心配置的任务 appName 将与此名称严格匹配以完成任务路由。
     */
    private String appName = "orbit-executor";

    /**
     * 调度中心集群地址列表，多个地址使用英文逗号分隔。
     * <p>示例：{@code http://orbit-admin:8080} 或 {@code http://admin1:8080,http://admin2:8080}。
     */
    private String adminAddresses = "http://127.0.0.1:8080";

    /**
     * 执行器对外暴露的完整访问地址（例如：{@code http://192.168.1.50:8081} 或 K8s Service 域名）。
     * <p>若留空（默认），系统将根据环境自动探测：优先使用 K8s 环境变量 {@code POD_IP}，其次探测本地网卡 IP，
     * 端口自动继承应用的 {@code server.port}。
     */
    private String address = "";

    /**
     * 执行器通信端口。
     * <p><b>默认值为 0，表示无需显式配置</b>：系统将自动继承应用自身的 Web 端口（即 {@code server.port}
     * 或内嵌 Tomcat/Undertow 实际监听的端口）。
     * <p>仅当存在 Docker 容器端口映射（例如容器内监听 8080，外部宿主机映射为 18080）等特殊网络场景时，
     * 才需要显式指定此项以覆盖自动探测的端口。
     */
    private int port = 0;

    /**
     * 安全访问令牌（Token）。
     * <p>若配置，执行器在向调度中心发送注册/心跳，以及调度中心调用执行器触发任务时，将进行双向令牌校验。
     * 为空则跳过安全校验。
     */
    private String accessToken = "";

    /**
     * 执行器向调度中心发送心跳的间隔周期（毫秒），默认为 20000（20秒）。
     * <p>框架底层强制保底不低于 5000 毫秒（5秒）。
     */
    private long heartbeatIntervalMs = 20000;

    /**
     * 执行器节点唯一标识符（nodeId）。
     * <p>若留空（默认），系统将优先读取环境变量 {@code POD_NAME}，其次读取本地主机名（hostname）。
     */
    private String nodeId = "";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAdminAddresses() {
        return adminAddresses;
    }

    public void setAdminAddresses(String adminAddresses) {
        this.adminAddresses = adminAddresses;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public void setHeartbeatIntervalMs(long heartbeatIntervalMs) {
        this.heartbeatIntervalMs = heartbeatIntervalMs;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }
}

package com.orbit.executor.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 执行器核心配置属性（对应前缀：{@code orbit.executor.*}）。
 * 提供执行器的应用名称、调度中心地址、心跳频率、安全凭证等核心参数配置。
 */
@ConfigurationProperties(prefix = "orbit.executor")
public class ExecutorProperties {

    /**
     * 是否启用执行器组件（默认为 true）。
     * 设为 false 时将跳过执行器的自动注册和心跳逻辑。
     */
    private boolean enabled = true;

    /**
     * 执行器所属应用名称（例如：order-service）。
     * 调度中心配置的任务 appName 将与此名称严格匹配以完成任务路由。
     */
    private String appName = "orbit-executor";

    /**
     * 调度中心集群地址列表，多个地址使用英文逗号分隔。
     * 示例：{@code http://orbit-admin:8080} 或 {@code http://admin1:8080,http://admin2:8080}。
     */
    private String adminAddresses = "http://127.0.0.1:8080";

    /**
     * 执行器对外暴露的完整访问地址（例如：{@code http://192.168.1.50:8081} 或 K8s Service 域名）。
     * 若留空（默认），系统将根据环境自动探测：优先使用 K8s 环境变量 {@code POD_IP}，其次探测本地网卡 IP，
     * 端口自动继承应用的 {@code server.port}。
     */
    private String address = "";

    /**
     * 执行器通信端口。
     * 默认值为 0，表示无需显式配置：系统将自动继承应用自身的 Web 端口（即 {@code server.port}
     * 或内嵌 Tomcat/Undertow 实际监听的端口）。
     * 仅当存在 Docker 容器端口映射（例如容器内监听 8080，外部宿主机映射为 18080）等特殊网络场景时，
     * 才需要显式指定此项以覆盖自动探测的端口。
     */
    private int port = 0;

    /**
     * 安全访问令牌（Token）。
     * 若配置，执行器在向调度中心发送注册/心跳，以及调度中心调用执行器触发任务时，将进行双向令牌校验。
     * 为空则跳过安全校验。
     */
    private String accessToken = "";

    /**
     * 执行器向调度中心发送心跳的间隔周期（毫秒），默认为 20000（20秒）。
     * 框架底层强制保底不低于 5000 毫秒（5秒）。
     */
    private long heartbeatIntervalMs = 20000;

    /**
     * 执行器节点唯一标识符（nodeId）。
     * 若留空（默认），系统将优先读取环境变量 {@code POD_NAME}，其次读取本地主机名（hostname）。
     */
    private String nodeId = "";

    /**
     * 任务执行工作线程数（默认 8）。
     * <p>
     * 引入有界工作线程池后的收益：
     * <ul>
     *   <li>限制单节点并发的任务执行数，防止瞬时触发风暴打爆业务应用；</li>
     *   <li>超出线程数的触发进入队列排队，队列满则立即返回「executor saturated」失败
     *       （调度中心可据此观测并扩容副本）；</li>
     *   <li>任务在独立线程执行后，可按任务 {@code timeoutSeconds} 进行<b>超时强制中断</b>，
     *       解决「调度中心 HTTP 读超时放弃后，执行器任务永久僵尸运行」的问题；</li>
     *   <li>任务线程独立命名（orbit-job-worker-N），便于线程 dump 定位。</li>
     * </ul>
     * 设为 0 表示退回旧版行为：任务直接在 Web 容器请求线程内执行，无超时强制。
     */
    private int workerThreads = 8;

    /**
     * 任务排队队列容量（默认 256）。仅当 {@code worker-threads > 0} 时生效。
     * 队列满后新触发立即失败返回，不会再占用请求线程等待。
     */
    private int queueCapacity = 256;

    /**
     * 传入 timeoutSeconds 非法（&lt;=0）时，执行器侧兜底的最大等待秒数（24 小时）。
     * 正常情况下调度中心总会下发正的超时值，此项仅为防御性兜底。
     */
    private int maxJobWaitSeconds = 86400;

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

    public int getWorkerThreads() {
        return workerThreads;
    }

    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public int getMaxJobWaitSeconds() {
        return maxJobWaitSeconds;
    }

    public void setMaxJobWaitSeconds(int maxJobWaitSeconds) {
        this.maxJobWaitSeconds = maxJobWaitSeconds;
    }
}

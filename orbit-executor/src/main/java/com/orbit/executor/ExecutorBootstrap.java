package com.orbit.executor;

import com.orbit.core.model.RegistryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.Environment;

import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 执行器生命周期管理与心跳自注册启动器。
 * <p>核心职责：
 * <ul>
 *   <li>在 Spring 容器启动完成并且内嵌 Web 容器就绪后，解析当前执行器的可访问地址与节点标识；</li>
 *   <li>向调度中心发送首次上线注册请求，并开启定时线程池周期性上报心跳维持在线状态；</li>
 *   <li>在 Spring 容器关闭（优雅停机）时，注销心跳定时任务并向调度中心主动发起下线通知；</li>
 *   <li>自动感知应用的实际监听端口（{@code server.port}），无需用户显式配置 {@code orbit.executor.port}。</li>
 * </ul>
 */
public class ExecutorBootstrap implements SmartLifecycle, EnvironmentAware, ApplicationListener<WebServerInitializedEvent> {

    private static final Logger log = LoggerFactory.getLogger(ExecutorBootstrap.class);

    /**
     * 执行器配置属性
     */
    private final ExecutorProperties properties;

    /**
     * 本地 JobHandler 扫描注册表
     */
    private final JobHandlerRegistry handlerRegistry;

    /**
     * 与调度中心通信的 HTTP 客户端
     */
    private final AdminClient adminClient;

    /**
     * 标记当前启动器是否处于运行状态的原子布尔值
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 周期性上报心跳的单线程定时任务调度器
     */
    private ScheduledExecutorService scheduler;

    /**
     * 运行时解析得到的执行器基础 HTTP 地址（例如：http://192.168.1.10:8081）
     */
    private String resolvedAddress;

    /**
     * 运行时解析得到的节点标识（例如 Pod 名或主机名）
     */
    private String resolvedNodeId;

    /**
     * Spring 运行环境，用于自动读取 server.port
     */
    private Environment environment;

    /**
     * 内嵌 Web 容器（如 Tomcat/Undertow）运行期实际监听的端口缓存
     */
    private volatile Integer serverPort;

    /**
     * 构造方法，注入核心依赖组件
     *
     * @param properties      执行器配置
     * @param handlerRegistry JobHandler 注册表
     * @param adminClient     调度中心客户端
     */
    public ExecutorBootstrap(ExecutorProperties properties, JobHandlerRegistry handlerRegistry,
                             AdminClient adminClient) {
        this.properties = properties;
        this.handlerRegistry = handlerRegistry;
        this.adminClient = adminClient;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    /**
     * 监听 Spring Boot 内嵌 Web 容器就绪事件，获取 Web 服务器实际绑定的端口
     *
     * @param event Web 容器初始化完成事件
     */
    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        if (event != null && event.getWebServer() != null) {
            this.serverPort = event.getWebServer().getPort();
            log.debug("[orbit-executor] detected web server initialized on port: {}", this.serverPort);
        }
    }

    /**
     * Spring 容器刷新完成后启动执行器组件
     */
    @Override
    public void start() {
        // 若配置禁用执行器，或已经处于运行中，则直接返回
        if (!properties.isEnabled() || !running.compareAndSet(false, true)) {
            return;
        }

        // 解析执行器对外通信地址和节点 ID
        resolvedAddress = resolveAddress();
        resolvedNodeId = resolveNodeId();

        log.info("[orbit-executor] starting appName={} address={} admin={}",
                properties.getAppName(), resolvedAddress, properties.getAdminAddresses());

        // 启动时立即向调度中心同步执行一次心跳注册
        heartbeatOnce();

        // 创建专职的单线程守护线程池，用于执行周期性心跳任务
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "orbit-executor-heartbeat");
            t.setDaemon(true);
            return t;
        });

        // 心跳周期保底不得低于 5 秒（5000 毫秒）
        long interval = Math.max(5000L, properties.getHeartbeatIntervalMs());

        // 按照固定频率定期发送心跳请求
        scheduler.scheduleAtFixedRate(this::heartbeatOnce, interval, interval, TimeUnit.MILLISECONDS);
    }

    /**
     * Spring 容器停止时触发，执行优雅停机与主动下线注销
     */
    @Override
    public void stop() {
        // CAS 保证停机逻辑仅执行一次
        if (!running.compareAndSet(true, false)) {
            return;
        }

        // 立即关闭心跳调度线程池，避免继续发送心跳
        if (scheduler != null) {
            scheduler.shutdownNow();
        }

        // 向调度中心发送主动注销下线请求
        try {
            RegistryRequest req = buildRequest();
            adminClient.remove(req);
            log.info("[orbit-executor] unregistered from admin");
        } catch (Exception e) {
            log.warn("[orbit-executor] unregister failed: {}", e.getMessage());
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * 返回最大优先级 Phase，确保在 Spring 内嵌 Web 容器完全启动并开始监听端口后才启动执行器
     *
     * @return 生命周期阶段数值
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    /**
     * 单次心跳上报逻辑
     */
    private void heartbeatOnce() {
        try {
            boolean ok = adminClient.registry(buildRequest());
            if (ok) {
                log.debug("[orbit-executor] heartbeat ok handlers={}", handlerRegistry.listNames().size());
            }
        } catch (Exception e) {
            log.warn("[orbit-executor] heartbeat error: {}", e.getMessage());
        }
    }

    /**
     * 组装注册与心跳请求数据体
     *
     * @return 注册请求实体
     */
    private RegistryRequest buildRequest() {
        RegistryRequest req = new RegistryRequest();
        req.setAppName(properties.getAppName());
        req.setAddress(resolvedAddress);
        req.setNodeId(resolvedNodeId);
        req.setHandlers(handlerRegistry.listNames());
        return req;
    }

    /**
     * 解析执行器对外暴露的访问基地址（Base URL）
     * <p>解析优先级规则：
     * <ol>
     *   <li>显式配置：若显式配置了 {@code orbit.executor.address}（如 K8s Service 域名），直接使用该地址；</li>
     *   <li>云原生环境：若环境变量中存在 {@code POD_IP}，拼接为 {@code http://${POD_IP}:${port}}；</li>
     *   <li>本地网卡探测：通过 {@link InetAddress#getLocalHost()} 获取本机有效 IP 并拼接端口；</li>
     *   <li>兜底方案：使用 {@code http://127.0.0.1:${port}}。</li>
     * </ol>
     *
     * @return 执行器对外访问基地址
     */
    private String resolveAddress() {
        // 1. 若显式指定了 address，去除末尾斜杠后直接返回
        if (properties.getAddress() != null && !properties.getAddress().trim().isEmpty()) {
            String a = properties.getAddress().trim();
            return a.endsWith("/") ? a.substring(0, a.length() - 1) : a;
        }

        // 解析通信端口（优先从容器/环境中推断，无需强制配置）
        int port = resolvePort();

        // 2. K8s 云原生环境：优先获取 Downward API 注入的 POD_IP
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isEmpty()) {
            return "http://" + podIp + ":" + port;
        }

        // 3. 探测本机局域网 IP
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            // 若获取到的是回环地址，则尝试取主机名
            if ("127.0.0.1".equals(host) || "0:0:0:0:0:0:0:1".equals(host)) {
                host = InetAddress.getLocalHost().getHostName();
            }
            return "http://" + host + ":" + port;
        } catch (Exception e) {
            // 4. 异常兜底采用本地回环地址
            return "http://127.0.0.1:" + port;
        }
    }

    /**
     * 解析执行器自身通信端口。
     * <p>默认直接继承业务应用的 {@code server.port}，无需显式配置。
     * <p>解析优先级规则：
     * <ol>
     *   <li>显式配置覆盖：若配置了 {@code orbit.executor.port} 且大于 0，优先采用该端口（适用于 Docker 宿主机端口映射场景）；</li>
     *   <li>Web 容器运行期端口：若内嵌 Web 容器已就绪（捕获到 WebServerInitializedEvent），获取容器实际监听的本地端口（完美兼容 server.port=0 随机端口）；</li>
     *   <li>Spring 环境配置：从 Spring Environment 中读取 {@code server.port} 配置项；</li>
     *   <li>默认兜底：若均无法获取，则兜底采用 8080（Spring Boot 官方默认 Web 端口）。</li>
     * </ol>
     *
     * @return 执行器通信端口
     */
    private int resolvePort() {
        // 1. 若显式配置了 orbit.executor.port 且大于 0，以显式配置为准（支持外部端口映射场景）
        if (properties.getPort() > 0) {
            return properties.getPort();
        }

        // 2. 若捕获到了内嵌 WebServer 初始化事件，直接获取 Web 容器实际监听的本地端口
        if (serverPort != null && serverPort > 0) {
            return serverPort;
        }

        // 3. 尝试从 Spring 环境配置中读取 server.port
        if (environment != null) {
            Integer envPort = environment.getProperty("server.port", Integer.class);
            if (envPort != null && envPort > 0) {
                return envPort;
            }
        }

        // 4. 默认采用 Spring Boot 默认端口 8080
        return 8080;
    }

    /**
     * 解析执行器节点唯一标识符（Node ID）
     * <p>解析优先级规则：
     * <ol>
     *   <li>显式配置：若配置了 {@code orbit.executor.node-id}，直接使用；</li>
     *   <li>云原生环境：读取环境变量 {@code POD_NAME}；</li>
     *   <li>本地主机名：通过 {@link InetAddress#getLocalHost()} 获取 HostName；</li>
     *   <li>兜底方案：生成带时间戳的临时标识 {@code executor-${timestamp}}。</li>
     * </ol>
     *
     * @return 节点唯一标识
     */
    private String resolveNodeId() {
        // 1. 显式配置优先
        if (properties.getNodeId() != null && !properties.getNodeId().trim().isEmpty()) {
            return properties.getNodeId().trim();
        }

        // 2. K8s Pod 名称
        String pod = System.getenv("POD_NAME");
        if (pod != null && !pod.isEmpty()) {
            return pod;
        }

        // 3. 本地主机名
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            // 4. 兜底带时间戳标识
            return "executor-" + System.currentTimeMillis();
        }
    }

    /**
     * 获取解析后的执行器通信基地址
     *
     * @return 通信基地址
     */
    public String getResolvedAddress() {
        return resolvedAddress;
    }

    /**
     * 获取解析后的执行器节点标识
     *
     * @return 节点标识
     */
    public String getResolvedNodeId() {
        return resolvedNodeId;
    }
}

package com.orbit.scheduler.support;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 调度框架配置项（前缀 orbit.scheduler）。
 *
 * <pre>
 * orbit:
 *   scheduler:
 *     enabled: true
 *     group: ORBIT
 *     node-id: ${POD_NAME:}
 *     timezone: Asia/Shanghai
 *     database:
 *       dialect: auto        # auto(自动探测) | postgresql | gaussdb(opengauss)
 *     storage:
 *       type: auto          # auto | database | memory
 *     lock:
 *       enabled: true
 *       type: auto          # auto | redis | database | none
 *       lease: 10m
 *       redis:
 *         address: redis:6379
 *     http-dispatch:
 *       enabled: true
 *       service-name: orbit-scheduler
 *       discovery-mode: service-dns  # service-dns | headless-dns | static
 *       port: 8080
 *       path: /api/scheduler/execute
 *       connect-timeout: 3s
 *       read-timeout: 300s
 *       secret: ""
 *       static-endpoints: []
 *     log:
 *       storage: auto       # auto | database | memory
 *       memory-capacity: 1000
 * </pre>
 *
 * @author orbit
 */
@ConfigurationProperties(prefix = "orbit.scheduler")
public class SchedulerProperties {

    /** 是否启用调度框架 */
    private boolean enabled = true;

    /** Quartz 任务分组（框架管理的所有 Job 均注册在该组下） */
    private String group = "ORBIT";

    /** 节点 ID：默认取 POD_NAME 环境变量，其次主机名，最后随机串 */
    private String nodeId = "";

    /** Cron 触发器时区 */
    private String timezone = "Asia/Shanghai";

    /** 是否扫描 @BatchTask 注解并自动注册 */
    private boolean annotationScan = true;

    /** 是否暴露 REST 管理 API（Web 环境下生效） */
    private boolean apiEnabled = true;

    private final Database database = new Database();
    private final Storage storage = new Storage();
    private final Lock lock = new Lock();
    private final HttpDispatch httpDispatch = new HttpDispatch();
    private final Log log = new Log();

    public static class Database {
        /**
         * 数据库方言：auto(按 URL/元数据/sql_compatibility 三级自动探测) /
         * postgresql / gaussdb(含 openGauss)。
         * 决定 Quartz driverDelegateClass 自动注入与主键生成策略（GaussDB 走
         * SEQUENCE 预取），用户 yaml 显式配置优先。
         */
        private String dialect = "auto";

        public String getDialect() { return dialect; }

        public void setDialect(String dialect) { this.dialect = dialect; }
    }

    public static class Storage {
        /** 存储类型：auto(有数据源则 database，否则 memory) / database / memory */
        private String type = "auto";

        public String getType() { return type; }

        public void setType(String type) { this.type = type; }
    }

    public static class Lock {
        /** 是否启用任务级分布式锁 */
        private boolean enabled = true;
        /** 锁实现：auto(优先 Redis，其次数据库，最后无锁) / redis / database / none */
        private String type = "auto";
        /** 租约时长（须大于最长任务执行时长；执行期间自动续期） */
        private Duration lease = Duration.ofMinutes(10);
        private final Redis redis = new Redis();

        public static class Redis {
            /** Redis 地址，格式 host:port；留空且上下文无 RedissonClient 时不启用 Redis 锁 */
            private String address = "";
            private String password = "";
            private int database = 0;

            public String getAddress() { return address; }

            public void setAddress(String address) { this.address = address; }

            public String getPassword() { return password; }

            public void setPassword(String password) { this.password = password; }

            public int getDatabase() { return database; }

            public void setDatabase(int database) { this.database = database; }
        }

        public boolean isEnabled() { return enabled; }

        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getType() { return type; }

        public void setType(String type) { this.type = type; }

        public Duration getLease() { return lease; }

        public void setLease(Duration lease) { this.lease = lease; }

        public Redis getRedis() { return redis; }
    }

    public static class HttpDispatch {
        /** 是否启用 HTTP 远程派发（LOCAL 任务本节点无执行器时也会尝试回退到 HTTP 派发） */
        private boolean enabled = true;
        /** Kubernetes Service 名称（默认使用普通 ClusterIP Service） */
        private String serviceName = "";
        /**
         * 服务发现模式：
         * service-dns：解析普通 Service DNS，K8s 负责 Pod 负载均衡（默认）；
         * headless-dns：解析 Headless Service，客户端直接获取 Pod IP；
         * static：使用 static-endpoints。
         */
        private String discoveryMode = "service-dns";
        /** 应用容器端口 */
        private int port = 8080;
        /** 远端执行端点路径 */
        private String path = "/api/scheduler/execute";
        private Duration connectTimeout = Duration.ofSeconds(3);
        private Duration readTimeout = Duration.ofMinutes(5);
        /** 派发令牌（写入 X-Scheduler-Token 头；为空则不校验） */
        private String secret = "";
        /** 默认任务超时秒数 */
        private int defaultTimeoutSeconds = 300;
        /** 静态端点列表（discovery-mode=static 时使用），如 http://10.0.0.1:8080 */
        private List<String> staticEndpoints = new ArrayList<String>();

        public boolean isEnabled() { return enabled; }

        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        public String getServiceName() { return serviceName; }

        public void setServiceName(String serviceName) { this.serviceName = serviceName; }

        public String getDiscoveryMode() { return discoveryMode; }

        public void setDiscoveryMode(String discoveryMode) { this.discoveryMode = discoveryMode; }

        public int getPort() { return port; }

        public void setPort(int port) { this.port = port; }

        public String getPath() { return path; }

        public void setPath(String path) { this.path = path; }

        public Duration getConnectTimeout() { return connectTimeout; }

        public void setConnectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; }

        public Duration getReadTimeout() { return readTimeout; }

        public void setReadTimeout(Duration readTimeout) { this.readTimeout = readTimeout; }

        public String getSecret() { return secret; }

        public void setSecret(String secret) { this.secret = secret; }

        public int getDefaultTimeoutSeconds() { return defaultTimeoutSeconds; }

        public void setDefaultTimeoutSeconds(int defaultTimeoutSeconds) { this.defaultTimeoutSeconds = defaultTimeoutSeconds; }

        public List<String> getStaticEndpoints() { return staticEndpoints; }

        public void setStaticEndpoints(List<String> staticEndpoints) { this.staticEndpoints = staticEndpoints; }
    }

    public static class Log {
        /** 日志存储：auto(有数据源则 database，否则 memory) / database / memory */
        private String storage = "auto";
        /** 内存日志环形队列容量 */
        private int memoryCapacity = 1000;

        public String getStorage() { return storage; }

        public void setStorage(String storage) { this.storage = storage; }

        public int getMemoryCapacity() { return memoryCapacity; }

        public void setMemoryCapacity(int memoryCapacity) { this.memoryCapacity = memoryCapacity; }
    }

    public boolean isEnabled() { return enabled; }

    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public String getGroup() { return group; }

    public void setGroup(String group) { this.group = group; }

    public String getNodeId() { return nodeId; }

    public void setNodeId(String nodeId) { this.nodeId = nodeId; }

    public String getTimezone() { return timezone; }

    public void setTimezone(String timezone) { this.timezone = timezone; }

    public boolean isAnnotationScan() { return annotationScan; }

    public void setAnnotationScan(boolean annotationScan) { this.annotationScan = annotationScan; }

    public boolean isApiEnabled() { return apiEnabled; }

    public void setApiEnabled(boolean apiEnabled) { this.apiEnabled = apiEnabled; }

    public Database getDatabase() { return database; }

    public Storage getStorage() { return storage; }

    public Lock getLock() { return lock; }

    public HttpDispatch getHttpDispatch() { return httpDispatch; }

    public Log getLog() { return log; }
}

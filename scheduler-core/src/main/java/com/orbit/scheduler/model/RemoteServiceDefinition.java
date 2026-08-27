package com.orbit.scheduler.model;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 远程业务服务定义：调度框架编排外部微服务时的目标描述。
 *
 * <p>可通过 {@code orbit.scheduler.remote-services.<name>.*} 配置，
 * 也可在任务级直接指定完整 URL（{@code httpPath} 以 {@code http://} 开头时）。
 *
 * @author orbit
 */
public class RemoteServiceDefinition {

    /** 服务逻辑名（配置 key / JobConfig.httpServiceName） */
    private String name;

    /**
     * 服务发现名（K8s Service DNS / 域名）；与 {@link #baseUrl} 二选一。
     * 为空时回退到 name。
     */
    private String serviceName = "";

    /** 固定 Base URL，如 http://order-service:8080 或 http://10.0.0.5:8080；优先于 serviceName */
    private String baseUrl = "";

    /** 服务端口（serviceName 解析时使用） */
    private int port = 8080;

    /** 路径前缀，拼接到任务 httpPath 之前，如 /api/v1 */
    private String pathPrefix = "";

    /** 默认 HTTP 方法（任务级可覆盖） */
    private String defaultMethod = "POST";

    /** 连接超时 */
    private Duration connectTimeout = Duration.ofSeconds(3);

    /** 读超时（任务级 timeoutSeconds 优先） */
    private Duration readTimeout = Duration.ofMinutes(5);

    /** 派发令牌（写入 Authorization 或自定义头） */
    private String secret = "";

    /** 令牌头名称，默认 X-Scheduler-Token；可改为 Authorization */
    private String secretHeader = "X-Scheduler-Token";

    /** 固定请求头 */
    private Map<String, String> headers = new LinkedHashMap<String, String>();

    /**
     * 静态端点列表（非空时优先使用，跳过 DNS）。
     * 适用于本地联调 / 传统虚机。
     */
    private List<String> staticEndpoints = new ArrayList<String>();

    /**
     * 发现模式：service-dns / headless-dns / static / base-url。
     * 空则自动：有 staticEndpoints → static；有 baseUrl → base-url；否则 service-dns。
     */
    private String discoveryMode = "";

    /** 成功 HTTP 状态码下限（含），默认 200 */
    private int successStatusMin = 200;

    /** 成功 HTTP 状态码上限（含），默认 299 */
    private int successStatusMax = 299;

    public String getName() { return name; }

    public void setName(String name) { this.name = name; }

    public String getServiceName() { return serviceName; }

    public void setServiceName(String serviceName) { this.serviceName = serviceName; }

    public String getBaseUrl() { return baseUrl; }

    public void setBaseUrl(String baseUrl) { this.baseUrl = baseUrl; }

    public int getPort() { return port; }

    public void setPort(int port) { this.port = port; }

    public String getPathPrefix() { return pathPrefix; }

    public void setPathPrefix(String pathPrefix) { this.pathPrefix = pathPrefix; }

    public String getDefaultMethod() { return defaultMethod; }

    public void setDefaultMethod(String defaultMethod) { this.defaultMethod = defaultMethod; }

    public Duration getConnectTimeout() { return connectTimeout; }

    public void setConnectTimeout(Duration connectTimeout) { this.connectTimeout = connectTimeout; }

    public Duration getReadTimeout() { return readTimeout; }

    public void setReadTimeout(Duration readTimeout) { this.readTimeout = readTimeout; }

    public String getSecret() { return secret; }

    public void setSecret(String secret) { this.secret = secret; }

    public String getSecretHeader() { return secretHeader; }

    public void setSecretHeader(String secretHeader) { this.secretHeader = secretHeader; }

    public Map<String, String> getHeaders() { return headers; }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers == null ? new LinkedHashMap<String, String>() : headers;
    }

    public List<String> getStaticEndpoints() { return staticEndpoints; }

    public void setStaticEndpoints(List<String> staticEndpoints) {
        this.staticEndpoints = staticEndpoints == null ? new ArrayList<String>() : staticEndpoints;
    }

    public String getDiscoveryMode() { return discoveryMode; }

    public void setDiscoveryMode(String discoveryMode) { this.discoveryMode = discoveryMode; }

    public int getSuccessStatusMin() { return successStatusMin; }

    public void setSuccessStatusMin(int successStatusMin) { this.successStatusMin = successStatusMin; }

    public int getSuccessStatusMax() { return successStatusMax; }

    public void setSuccessStatusMax(int successStatusMax) { this.successStatusMax = successStatusMax; }

    /** 解析实际用于 DNS 的主机名 */
    public String resolveHost() {
        if (serviceName != null && !serviceName.trim().isEmpty()) {
            return serviceName.trim();
        }
        return name == null ? "" : name.trim();
    }

    /** 是否配置了可用目标（baseUrl / static / serviceName / name） */
    public boolean hasTarget() {
        if (baseUrl != null && !baseUrl.trim().isEmpty()) {
            return true;
        }
        if (staticEndpoints != null && !staticEndpoints.isEmpty()) {
            return true;
        }
        return resolveHost().length() > 0;
    }
}

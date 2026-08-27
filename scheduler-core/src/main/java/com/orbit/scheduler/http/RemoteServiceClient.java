package com.orbit.scheduler.http;

import com.orbit.scheduler.model.HttpDispatchResponse;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.RemoteServiceDefinition;
import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.model.WorkflowDefinition;
import com.orbit.scheduler.discovery.RemoteServiceRegistry;
import com.orbit.scheduler.support.NodeIdProvider;
import com.orbit.scheduler.support.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.ClassUtils;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 远程业务服务调用客户端：将批量任务派发到<strong>不强制接入本框架</strong>的外部微服务。
 *
 * <p>与 {@link HttpDispatchClient} 的区别：
 * <ul>
 *   <li>HttpDispatchClient：目标必须是同框架节点，走统一 {@code /api/scheduler/execute} 协议</li>
 *   <li>RemoteServiceClient：目标是任意业务 HTTP 接口，请求体为任务 params（或自定义），
 *       按 HTTP 状态码判定成功</li>
 * </ul>
 *
 * @author orbit
 */
public class RemoteServiceClient {

    private static final Logger log = LoggerFactory.getLogger(RemoteServiceClient.class);

    private static final boolean HTTP_CLIENT_PRESENT = ClassUtils.isPresent(
            "org.apache.http.impl.client.CloseableHttpClient",
            RemoteServiceClient.class.getClassLoader());

    private static final int DEFAULT_MAX_CONN_PER_ROUTE = 16;
    private static final int DEFAULT_MAX_CONN_TOTAL = 64;

    private final SchedulerProperties properties;
    private final RemoteServiceRegistry registry;
    private final String nodeId;
    private final AtomicInteger cursor = new AtomicInteger(0);
    private final ConcurrentHashMap<Long, RestTemplate> templatesByTimeout =
            new ConcurrentHashMap<Long, RestTemplate>();

    public RemoteServiceClient(SchedulerProperties properties, RemoteServiceRegistry registry) {
        this.properties = properties;
        this.registry = registry;
        this.nodeId = NodeIdProvider.resolve(properties.getNodeId());
    }

    /** 兼容旧构造（ObjectMapper 参数已不再使用，请求体由 RestTemplate 序列化） */
    public RemoteServiceClient(SchedulerProperties properties, RemoteServiceRegistry registry,
                               Object objectMapperIgnored) {
        this(properties, registry);
    }

    public String getNodeId() {
        return nodeId;
    }

    public RemoteServiceRegistry getRegistry() {
        return registry;
    }

    /**
     * 按 JobConfig 调用远程业务服务。
     *
     * @param cfg       任务配置（httpServiceName / httpPath / httpMethod / timeoutSeconds）
     * @param params    请求参数（作为 JSON body 或 query，取决于 method）
     * @param requestId 追踪 ID
     */
    public HttpDispatchResponse invoke(JobConfig cfg, Map<String, Object> params, String requestId) {
        String serviceName = cfg.getHttpServiceName();
        String path = cfg.getHttpPath();
        String method = cfg.getHttpMethod();
        int timeout = (cfg.getTimeoutSeconds() != null && cfg.getTimeoutSeconds() > 0)
                ? cfg.getTimeoutSeconds()
                : properties.getHttpDispatch().getDefaultTimeoutSeconds();
        Map<String, String> extraHeaders = Collections.emptyMap();
        return invoke(serviceName, path, method, params, extraHeaders, timeout, requestId, null);
    }

    /**
     * 按工作流步骤调用远程业务服务。
     */
    public HttpDispatchResponse invokeStep(WorkflowDefinition.Step step, Map<String, Object> params,
                                           int defaultTimeout, String requestId) {
        int timeout = (step.getTimeoutSeconds() != null && step.getTimeoutSeconds() > 0)
                ? step.getTimeoutSeconds() : defaultTimeout;
        return invoke(step.getService(), step.getPath(), step.getMethod(),
                params, step.getHeaders(), timeout, requestId, null);
    }

    /**
     * 通用远程调用。
     *
     * @param serviceName  远程服务注册名，或完整 base URL
     * @param path         接口路径（可含 query；若以 http 开头则忽略 serviceName）
     * @param method       HTTP 方法，默认 POST
     * @param params       请求体/查询参数
     * @param extraHeaders 额外请求头
     * @param timeoutSeconds 读超时秒数
     * @param requestId    追踪 ID
     * @param overrideDef  可选，覆盖注册表中的服务定义（一次性）
     */
    public HttpDispatchResponse invoke(String serviceName, String path, String method,
                                       Map<String, Object> params, Map<String, String> extraHeaders,
                                       int timeoutSeconds, String requestId,
                                       RemoteServiceDefinition overrideDef) {
        long start = System.currentTimeMillis();
        String httpMethod = (method == null || method.trim().isEmpty()) ? "POST" : method.trim().toUpperCase();
        String resolvedPath = path;

        // path 本身是完整 URL：直接调用，不再走服务发现
        if (resolvedPath != null && (resolvedPath.startsWith("http://") || resolvedPath.startsWith("https://"))) {
            int pathIdx = indexOfPath(resolvedPath);
            String base = pathIdx >= resolvedPath.length()
                    ? resolvedPath : resolvedPath.substring(0, pathIdx);
            String onlyPath = pathIdx >= resolvedPath.length() ? "" : resolvedPath.substring(pathIdx);
            return doSingle(new ServiceEndpoint(trimSlash(base)), onlyPath, httpMethod, params, extraHeaders,
                    timeoutSeconds, requestId, resolveDef(serviceName, overrideDef), start);
        }

        RemoteServiceDefinition def = resolveDef(serviceName, overrideDef);
        if (def != null && def.getDefaultMethod() != null && (method == null || method.trim().isEmpty())) {
            httpMethod = def.getDefaultMethod().trim().toUpperCase();
        }

        int defaultPort = properties.getHttpDispatch().getPort();
        List<ServiceEndpoint> endpoints;
        if (def != null) {
            endpoints = registry.resolveEndpoints(def.getName(), def.getPort() > 0 ? def.getPort() : defaultPort);
            // 拼接 pathPrefix
            String prefix = def.getPathPrefix() == null ? "" : def.getPathPrefix().trim();
            if (!prefix.isEmpty()) {
                if (!prefix.startsWith("/")) {
                    prefix = "/" + prefix;
                }
                if (prefix.endsWith("/")) {
                    prefix = prefix.substring(0, prefix.length() - 1);
                }
                String p = resolvedPath == null ? "" : resolvedPath.trim();
                if (!p.isEmpty() && !p.startsWith("/")) {
                    p = "/" + p;
                }
                resolvedPath = prefix + p;
            }
        } else if (serviceName != null && !serviceName.trim().isEmpty()) {
            endpoints = registry.resolveEndpoints(serviceName.trim(), defaultPort);
        } else {
            throw new IllegalStateException("REMOTE dispatch requires httpServiceName or absolute httpPath");
        }

        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalStateException("no endpoint resolved for remote service '"
                    + serviceName + "' (register it under orbit.scheduler.remote-services)");
        }

        // 单端点或 baseUrl/static 单入口：直接调用
        if (endpoints.size() == 1) {
            return doSingle(endpoints.get(0), resolvedPath, httpMethod, params, extraHeaders,
                    timeoutSeconds, requestId, def, start);
        }

        // 多端点：轮询 + 连接失败故障转移
        int n = endpoints.size();
        int begin = Math.floorMod(cursor.getAndIncrement(), n);
        Exception lastEx = null;
        HttpDispatchResponse lastResp = null;
        for (int i = 0; i < n; i++) {
            ServiceEndpoint ep = endpoints.get((begin + i) % n);
            try {
                HttpDispatchResponse resp = doSingle(ep, resolvedPath, httpMethod, params, extraHeaders,
                        timeoutSeconds, requestId, def, start);
                if (resp.isSuccess()) {
                    return resp;
                }
                // 业务失败不跨端点重试
                return resp;
            } catch (ResourceAccessException e) {
                lastEx = e;
                log.warn("[orbit-scheduler] remote invoke {} {} failed (transport): {}",
                        httpMethod, ep, e.getMessage());
            } catch (Exception e) {
                throw new IllegalStateException("remote invoke failed on " + ep + ": " + e.getMessage(), e);
            }
        }
        if (lastResp != null) {
            return lastResp;
        }
        throw new IllegalStateException("remote invoke failed on all " + n + " endpoint(s), service='"
                + serviceName + "'", lastEx);
    }

    private HttpDispatchResponse doSingle(ServiceEndpoint endpoint, String path, String method,
                                          Map<String, Object> params, Map<String, String> extraHeaders,
                                          int timeoutSeconds, String requestId,
                                          RemoteServiceDefinition def, long startAt) {
        String url = buildUrl(endpoint.getUrl(), path);
        // GET/DELETE：params 拼到 query
        if ("GET".equals(method) || "DELETE".equals(method) || "HEAD".equals(method)) {
            url = appendQuery(url, params);
        }

        RestTemplate restTemplate = obtainRestTemplate(timeoutSeconds);
        HttpHeaders headers = buildHeaders(def, extraHeaders, requestId);

        HttpEntity<?> entity;
        if ("GET".equals(method) || "DELETE".equals(method) || "HEAD".equals(method)) {
            entity = new HttpEntity<Void>(headers);
        } else {
            Map<String, Object> body = params == null
                    ? new LinkedHashMap<String, Object>()
                    : new LinkedHashMap<String, Object>(params);
            // 注入追踪字段，方便下游关联
            if (!body.containsKey("_requestId") && requestId != null) {
                body.put("_requestId", requestId);
            }
            if (!body.containsKey("_dispatchNode")) {
                body.put("_dispatchNode", nodeId);
            }
            entity = new HttpEntity<Map<String, Object>>(body, headers);
        }

        try {
            ResponseEntity<String> response = restTemplate.exchange(
                    url, HttpMethod.valueOf(method), entity, String.class);
            long cost = System.currentTimeMillis() - startAt;
            int status = response.getStatusCodeValue();
            boolean ok = isSuccessStatus(status, def);
            String body = response.getBody();
            String message = abbreviate(body == null ? ("HTTP " + status) : body);
            String worker = endpoint.getUrl();
            log.info("[orbit-scheduler] remote {} {} -> {} ({}ms) requestId={}",
                    method, url, status, cost, requestId);
            if (ok) {
                return HttpDispatchResponse.success(requestId, worker, cost, message);
            }
            return HttpDispatchResponse.failure(requestId, worker, "HTTP " + status + ": " + message);
        } catch (RestClientResponseException e) {
            long cost = System.currentTimeMillis() - startAt;
            String body = e.getResponseBodyAsString();
            String message = "HTTP " + e.getRawStatusCode() + ": " + abbreviate(body);
            log.warn("[orbit-scheduler] remote {} {} failed: {}", method, url, message);
            return HttpDispatchResponse.failure(requestId, endpoint.getUrl(), message);
        }
    }

    private HttpHeaders buildHeaders(RemoteServiceDefinition def, Map<String, String> extra,
                                     String requestId) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        headers.set("X-Request-Id", requestId == null ? "" : requestId);
        headers.set("X-Scheduler-Node", nodeId);

        if (def != null) {
            if (def.getHeaders() != null) {
                for (Map.Entry<String, String> e : def.getHeaders().entrySet()) {
                    if (e.getKey() != null && e.getValue() != null) {
                        headers.set(e.getKey(), e.getValue());
                    }
                }
            }
            String secret = def.getSecret();
            if (secret != null && !secret.isEmpty()) {
                String headerName = def.getSecretHeader() == null || def.getSecretHeader().isEmpty()
                        ? "X-Scheduler-Token" : def.getSecretHeader();
                headers.set(headerName, secret);
            }
        } else {
            // 回退全局 http-dispatch.secret
            String secret = properties.getHttpDispatch().getSecret();
            if (secret != null && !secret.isEmpty()) {
                headers.set(HttpDispatchClient.TOKEN_HEADER, secret);
            }
        }

        if (extra != null) {
            for (Map.Entry<String, String> e : extra.entrySet()) {
                if (e.getKey() != null && e.getValue() != null) {
                    headers.set(e.getKey(), e.getValue());
                }
            }
        }
        return headers;
    }

    private boolean isSuccessStatus(int status, RemoteServiceDefinition def) {
        int min = def == null ? 200 : def.getSuccessStatusMin();
        int max = def == null ? 299 : def.getSuccessStatusMax();
        return status >= min && status <= max;
    }

    private RemoteServiceDefinition resolveDef(String serviceName, RemoteServiceDefinition override) {
        if (override != null) {
            return override;
        }
        if (serviceName == null || serviceName.trim().isEmpty()) {
            return null;
        }
        return registry.get(serviceName.trim());
    }

    private RestTemplate obtainRestTemplate(int timeoutSeconds) {
        long key = (long) Math.max(1, timeoutSeconds);
        RestTemplate existing = templatesByTimeout.get(key);
        if (existing != null) {
            return existing;
        }
        return templatesByTimeout.computeIfAbsent(key, k -> buildRestTemplate(k.intValue()));
    }

    private RestTemplate buildRestTemplate(int readTimeoutSeconds) {
        int connectMs = (int) properties.getHttpDispatch().getConnectTimeout().toMillis();
        int readMs = readTimeoutSeconds * 1000;
        return new RestTemplate(buildRequestFactory(connectMs, readMs));
    }

    private ClientHttpRequestFactory buildRequestFactory(int connectMs, int readMs) {
        if (HTTP_CLIENT_PRESENT) {
            try {
                return buildHttpComponentsRequestFactory(connectMs, readMs);
            } catch (Exception e) {
                log.warn("[orbit-scheduler] build HttpComponents factory failed, fallback Simple: {}",
                        e.getMessage());
            }
        }
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectMs);
        factory.setReadTimeout(readMs);
        return factory;
    }

    private ClientHttpRequestFactory buildHttpComponentsRequestFactory(int connectMs, int readMs)
            throws Exception {
        ClassLoader cl = RemoteServiceClient.class.getClassLoader();
        Class<?> poolingMgrClass = ClassUtils.forName(
                "org.apache.http.impl.conn.PoolingHttpClientConnectionManager", cl);
        Object poolingMgr = poolingMgrClass.getDeclaredConstructor().newInstance();
        poolingMgrClass.getMethod("setDefaultMaxPerRoute", int.class)
                .invoke(poolingMgr, DEFAULT_MAX_CONN_PER_ROUTE);
        poolingMgrClass.getMethod("setMaxTotal", int.class).invoke(poolingMgr, DEFAULT_MAX_CONN_TOTAL);

        Class<?> httpClientBuilderClass = ClassUtils.forName(
                "org.apache.http.impl.client.HttpClientBuilder", cl);
        Class<?> connMgrClass = ClassUtils.forName(
                "org.apache.http.conn.HttpClientConnectionManager", cl);
        Object builder = httpClientBuilderClass.getMethod("create").invoke(null);
        httpClientBuilderClass.getMethod("setConnectionManager", connMgrClass).invoke(builder, poolingMgr);
        httpClientBuilderClass.getMethod("disableConnectionState").invoke(builder);
        Object httpClient = httpClientBuilderClass.getMethod("build").invoke(builder);

        Class<?> factoryClass = ClassUtils.forName(
                "org.springframework.http.client.HttpComponentsClientHttpRequestFactory", cl);
        Object factory = factoryClass.getDeclaredConstructor().newInstance();
        Class<?> httpClientClass = ClassUtils.forName("org.apache.http.client.HttpClient", cl);
        factoryClass.getMethod("setHttpClient", httpClientClass).invoke(factory, httpClient);
        factoryClass.getMethod("setConnectTimeout", int.class).invoke(factory, connectMs);
        factoryClass.getMethod("setReadTimeout", int.class).invoke(factory, readMs);
        return (ClientHttpRequestFactory) factory;
    }

    static String buildUrl(String base, String path) {
        String b = trimSlash(base == null ? "" : base);
        if (path == null || path.trim().isEmpty()) {
            return b;
        }
        String p = path.trim();
        if (p.startsWith("http://") || p.startsWith("https://")) {
            return p;
        }
        if (!p.startsWith("/")) {
            p = "/" + p;
        }
        return b + p;
    }

    private static String appendQuery(String url, Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return url;
        }
        StringBuilder sb = new StringBuilder(url);
        boolean first = !url.contains("?");
        for (Map.Entry<String, Object> e : params.entrySet()) {
            if (e.getKey() == null || e.getKey().startsWith("_")) {
                continue;
            }
            sb.append(first ? '?' : '&');
            first = false;
            sb.append(encode(e.getKey())).append('=')
                    .append(encode(e.getValue() == null ? "" : String.valueOf(e.getValue())));
        }
        return sb.toString();
    }

    private static String encode(String s) {
        try {
            return java.net.URLEncoder.encode(s, "UTF-8");
        } catch (Exception e) {
            return s;
        }
    }

    private static int indexOfPath(String absoluteUrl) {
        int scheme = absoluteUrl.indexOf("://");
        int start = scheme >= 0 ? scheme + 3 : 0;
        int slash = absoluteUrl.indexOf('/', start);
        return slash >= 0 ? slash : absoluteUrl.length();
    }

    private static String trimSlash(String s) {
        if (s == null) {
            return "";
        }
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        String t = s.trim();
        return t.length() <= 2000 ? t : t.substring(0, 2000) + "...(truncated)";
    }

    /** 列出已注册远程服务（管理 API 展示） */
    public Map<String, Object> overview() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        Map<String, RemoteServiceDefinition> all = registry.listAll();
        map.put("count", all.size());
        List<Map<String, Object>> items = new java.util.ArrayList<Map<String, Object>>();
        for (Map.Entry<String, RemoteServiceDefinition> e : all.entrySet()) {
            RemoteServiceDefinition d = e.getValue();
            Map<String, Object> item = new LinkedHashMap<String, Object>();
            item.put("name", d.getName());
            item.put("serviceName", d.resolveHost());
            item.put("baseUrl", d.getBaseUrl());
            item.put("port", d.getPort());
            item.put("pathPrefix", d.getPathPrefix());
            item.put("defaultMethod", d.getDefaultMethod());
            try {
                item.put("endpoints", registry.resolveEndpoints(d.getName(), d.getPort()));
            } catch (Exception ex) {
                item.put("endpoints", Collections.emptyList());
                item.put("resolveError", ex.getMessage());
            }
            items.add(item);
        }
        map.put("services", items);
        return map;
    }
}

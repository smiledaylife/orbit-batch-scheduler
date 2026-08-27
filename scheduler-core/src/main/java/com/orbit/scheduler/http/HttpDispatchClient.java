package com.orbit.scheduler.http;

import com.orbit.scheduler.model.HttpDispatchRequest;
import com.orbit.scheduler.model.HttpDispatchResponse;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.spi.ServiceEndpointResolver;
import com.orbit.scheduler.support.NodeIdProvider;
import com.orbit.scheduler.support.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.util.ClassUtils;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HTTP 远程派发客户端：将任务派发到配置的服务端点。
 *
 * <p>路由策略由 {@link ServiceEndpointResolver} 决定：普通 Kubernetes Service 由平台负责
 * Pod 负载均衡；Headless/Static 模式由客户端进行端点选择和有限故障转移。超时按任务级
 * timeoutSeconds 控制（读超时）。
 *
 * <p><b>性能优化</b>：按 (connectTimeout, readTimeout) 缓存 ClientHttpRequestFactory /
 * RestTemplate 实例，避免每次请求重建工厂与拦截器链；当 classpath 存在 Apache HttpClient
 * 时自动采用 {@link HttpComponentsClientHttpRequestFactory}（支持连接池），否则降级为
 * SimpleClientHttpRequestFactory（仍为单连接，但实例复用，避免反射与初始化开销）。
 *
 * @author orbit
 */
public class HttpDispatchClient {

    private static final Logger log = LoggerFactory.getLogger(HttpDispatchClient.class);

    public static final String TOKEN_HEADER = "X-Scheduler-Token";

    /** 是否使用 Apache HttpClient 连接池（classpath 探测一次即缓存） */
    private static final boolean HTTP_CLIENT_PRESENT = ClassUtils.isPresent(
            "org.apache.http.impl.client.CloseableHttpClient",
            HttpDispatchClient.class.getClassLoader());

    /** 默认连接池上限（仅在使用 Apache HttpClient 时生效） */
    private static final int DEFAULT_MAX_CONN_PER_ROUTE = 16;
    private static final int DEFAULT_MAX_CONN_TOTAL = 64;

    private final SchedulerProperties properties;
    private final ServiceEndpointResolver endpointResolver;
    private final String nodeId;
    private final AtomicInteger cursor = new AtomicInteger(0);

    /**
     * 按 (connectMs, readMs) 缓存的 RestTemplate。RestTemplate 本身线程安全，
     * 仅 ClientHttpRequestFactory 的超时配置不同则需要单独实例。
     */
    private final ConcurrentHashMap<Long, RestTemplate> templatesByReadTimeout =
            new ConcurrentHashMap<Long, RestTemplate>();

    /** 共享 header 模板（仅 token 在 dispatch 时 setIfAbsent，避免每次构造） */
    private final boolean hasSecret;
    private final String secret;

    public HttpDispatchClient(SchedulerProperties properties, ServiceEndpointResolver endpointResolver) {
        this.properties = properties;
        this.endpointResolver = endpointResolver;
        this.nodeId = NodeIdProvider.resolve(properties.getNodeId());
        String s = properties.getHttpDispatch().getSecret();
        this.hasSecret = s != null && !s.isEmpty();
        this.secret = s;
    }

    public String getNodeId() {
        return nodeId;
    }

    /**
     * 派发任务到远端执行节点。
     *
     * <p>普通 Kubernetes Service 模式下，Service 本身负责 Pod 级负载均衡和故障摘除，
     * 客户端只发送一次请求，不再做应用层端点轮询。这样可以避免"Service 已经负载均衡，
     * Scheduler 又二次负载均衡"的语义重复。</p>
     *
     * <p>Headless/Static 模式下才启用客户端端点选择。只有"任务不存在"或网络连接失败时
     * 才尝试下一端点；远端已经开始执行后返回业务失败时不重试，避免任务被重复执行。</p>
     *
     * @throws IllegalStateException 无可用端点或全部端点失败
     */
    public HttpDispatchResponse dispatch(JobConfig cfg, Map<String, Object> params, String requestId) {
        List<ServiceEndpoint> endpoints = endpointResolver.resolve(cfg.getHttpServiceName());
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalStateException("no endpoint resolved for HTTP dispatch of task '" +
                    cfg.getTaskName() + "' (service=" + cfg.getHttpServiceName() +
                    ", check orbit.scheduler.http-dispatch.service-name/static-endpoints)");
        }

        // 普通 ClusterIP Service：Kubernetes 负载负载均衡，不做客户端轮询。
        if (endpointResolver.isLoadBalanced()) {
            ServiceEndpoint endpoint = endpoints.get(0);
            try {
                return post(endpoint, cfg, params, requestId);
            } catch (Exception e) {
                log.warn("[orbit-scheduler] dispatch via load-balanced service {} failed: {}",
                        endpoint, e.getMessage());
                throw new IllegalStateException("HTTP dispatch via load-balanced service failed, task='"
                        + cfg.getTaskName() + "'", e);
            }
        }

        // Headless / Static：多个真实端点由客户端显式选择。
        int n = endpoints.size();
        int start = Math.floorMod(cursor.getAndIncrement(), n);
        HttpDispatchResponse lastResponse = null;
        Exception lastException = null;

        for (int i = 0; i < n; i++) {
            ServiceEndpoint endpoint = endpoints.get((start + i) % n);
            try {
                HttpDispatchResponse resp = post(endpoint, cfg, params, requestId);
                if (resp != null && resp.isSuccess()) {
                    return resp;
                }

                // 只有明确表示"该节点没有这个执行器"才切换下一个节点。
                // 普通业务失败不重试，避免远端已经执行后因返回失败导致重复执行。
                if (resp != null && resp.isTaskMissing()) {
                    lastResponse = resp;
                    log.warn("[orbit-scheduler] task '{}' is missing on {}, try next endpoint",
                            cfg.getTaskName(), endpoint);
                    continue;
                }

                return resp;
            } catch (ResourceAccessException e) {
                lastException = e;
                log.warn("[orbit-scheduler] dispatch to {} failed with transport error: {}",
                        endpoint, e.getMessage());
            } catch (Exception e) {
                // HTTP 4xx/5xx 或其他异常不判定为"节点不可用"，不跨节点重试。
                throw new IllegalStateException("HTTP dispatch failed on endpoint " + endpoint
                        + ", task='" + cfg.getTaskName() + "'", e);
            }
        }

        if (lastResponse != null) {
            return lastResponse;
        }
        throw new IllegalStateException("HTTP dispatch failed on all " + n + " endpoint(s), task='"
                + cfg.getTaskName() + "'", lastException);
    }

    private HttpDispatchResponse post(ServiceEndpoint endpoint, JobConfig cfg,
                                      Map<String, Object> params, String requestId) {
        int timeoutSeconds = (cfg.getTimeoutSeconds() != null && cfg.getTimeoutSeconds() > 0)
                ? cfg.getTimeoutSeconds()
                : properties.getHttpDispatch().getDefaultTimeoutSeconds();

        RestTemplate restTemplate = obtainRestTemplate(timeoutSeconds);

        HttpDispatchRequest body = new HttpDispatchRequest(
                requestId, cfg.getTaskName(), params, System.currentTimeMillis(),
                nodeId, timeoutSeconds);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (hasSecret) {
            headers.set(TOKEN_HEADER, secret);
        }

        String url = buildUrl(endpoint, pathOf(cfg));
        ResponseEntity<HttpDispatchResponse> entity = restTemplate.postForEntity(
                url, new HttpEntity<HttpDispatchRequest>(body, headers), HttpDispatchResponse.class);
        return entity.getBody();
    }

    /**
     * 按读超时（秒）获取缓存的 RestTemplate。
     *
     * <p>读超时由 JobConfig.timeoutSeconds 决定，连接超时使用全局配置；同一超时
     * 配置复用同一 RestTemplate，避免重复构造 ClientHttpRequestFactory、MessageConverter
     * 链等开销。当 classpath 存在 Apache HttpClient 时使用连接池工厂，否则降级
     * SimpleClientHttpRequestFactory（仅实例复用，无连接池）。
     */
    private RestTemplate obtainRestTemplate(int timeoutSeconds) {
        long key = (long) timeoutSeconds; // read 超时秒
        RestTemplate existing = templatesByReadTimeout.get(key);
        if (existing != null) {
            return existing;
        }
        // computeIfAbsent 会在 absent 时原子地构造，避免并发重复构造
        return templatesByReadTimeout.computeIfAbsent(key, k -> buildRestTemplate(k.intValue()));
    }

    private RestTemplate buildRestTemplate(int readTimeoutSeconds) {
        ClientHttpRequestFactory factory = buildRequestFactory(
                (int) properties.getHttpDispatch().getConnectTimeout().toMillis(),
                readTimeoutSeconds * 1000);
        return new RestTemplate(factory);
    }

    private ClientHttpRequestFactory buildRequestFactory(int connectMs, int readMs) {
        if (HTTP_CLIENT_PRESENT) {
            try {
                return buildHttpComponentsRequestFactory(connectMs, readMs);
            } catch (Exception e) {
                log.warn("[orbit-scheduler] build HttpComponents request factory failed, " +
                        "fallback to SimpleClientHttpRequestFactory: {}", e.getMessage());
            }
        }
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(connectMs);
        factory.setReadTimeout(readMs);
        return factory;
    }

    /** 反射构建 HttpComponentsClientHttpRequestFactory，避免在 scheduler-core 中硬依赖 httpclient */
    private ClientHttpRequestFactory buildHttpComponentsRequestFactory(int connectMs, int readMs) throws Exception {
        ClassLoader cl = HttpDispatchClient.class.getClassLoader();
        // 构造连接池
        Class<?> poolingMgrClass = ClassUtils.forName(
                "org.apache.http.impl.conn.PoolingHttpClientConnectionManager", cl);
        Object poolingMgr = poolingMgrClass.getDeclaredConstructor().newInstance();
        poolingMgrClass.getMethod("setDefaultMaxPerRoute", int.class).invoke(poolingMgr, DEFAULT_MAX_CONN_PER_ROUTE);
        poolingMgrClass.getMethod("setMaxTotal", int.class).invoke(poolingMgr, DEFAULT_MAX_CONN_TOTAL);

        // 构造 client
        Class<?> httpClientBuilderClass = ClassUtils.forName(
                "org.apache.http.impl.client.HttpClientBuilder", cl);
        Class<?> connMgrClass = ClassUtils.forName(
                "org.apache.http.conn.HttpClientConnectionManager", cl);
        Object builder = httpClientBuilderClass.getMethod("create").invoke(null);
        httpClientBuilderClass.getMethod("setConnectionManager", connMgrClass).invoke(builder, poolingMgr);
        httpClientBuilderClass.getMethod("disableConnectionState").invoke(builder);
        Object httpClient = httpClientBuilderClass.getMethod("build").invoke(builder);

        // 包装为 Spring 的 RequestFactory（纯反射构造，避免编译期引用 HttpComponentsClientHttpRequestFactory
        // —— 它的 setHttpClient 方法签名引用了 org.apache.http.client.HttpClient，
        // 直接 import 会导致 scheduler-core 编译期需要 httpclient 类）
        Class<?> factoryClass = ClassUtils.forName(
                "org.springframework.http.client.HttpComponentsClientHttpRequestFactory", cl);
        Object factory = factoryClass.getDeclaredConstructor().newInstance();
        Class<?> httpClientClass = ClassUtils.forName("org.apache.http.client.HttpClient", cl);
        java.lang.reflect.Method setHttpClient = factoryClass.getMethod("setHttpClient", httpClientClass);
        setHttpClient.invoke(factory, httpClient);
        factoryClass.getMethod("setConnectTimeout", int.class).invoke(factory, connectMs);
        factoryClass.getMethod("setReadTimeout", int.class).invoke(factory, readMs);
        return (ClientHttpRequestFactory) factory;
    }

    private String pathOf(JobConfig cfg) {
        String path = (cfg.getHttpPath() == null || cfg.getHttpPath().trim().isEmpty())
                ? properties.getHttpDispatch().getPath()
                : cfg.getHttpPath().trim();
        return path.startsWith("/") ? path : "/" + path;
    }

    static String buildUrl(ServiceEndpoint endpoint, String path) {
        String base = endpoint.getUrl();
        if (base.endsWith("/")) {
            base = base.substring(0, base.length() - 1);
        }
        return base + path;
    }

    /** 列出当前可路由端点（供管理 API / 健康检查展示） */
    public List<ServiceEndpoint> listEndpoints(String serviceName) {
        return endpointResolver.resolve(serviceName);
    }
}

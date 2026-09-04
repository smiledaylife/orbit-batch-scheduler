package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 调度中心向执行器派发任务的 HTTP 通信客户端。
 * 核心职责：
 * 
 *   - 负责调用执行器暴露的 {@code POST /orbit/executor/run} 接口；
 *   - 动态适配不同任务指定的读取超时时间（ReadTimeout）；
 *   - 携带鉴权安全令牌（{@code X-Orbit-Token}）；
 *   - 捕获网络连通性异常、超时异常，并优雅封装为失败的 {@link TriggerResult}。
 * 
 */
@Component
public class ExecutorClient {

    private static final Logger log = LoggerFactory.getLogger(ExecutorClient.class);

    /**
     * 安全令牌 Header 字段名称
     */
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    private final AdminProperties properties;

    /**
     * 预构建的 JSON + 鉴权 Header（accessToken 在运行期不可变，构造时一次性构建）
     */
    private final HttpHeaders jsonHeaders;

    /**
     * RestTemplate 缓存，按 readTimeout 复用。
     * 原先每次派发都 new 一个 RestTemplate + SimpleClientHttpRequestFactory（底层 HttpURLConnection、
     * 无连接池），高频调度下是持续的无谓分配。key 的取值受任务 timeoutSeconds 上限约束，规模可控。
     */
    private final ConcurrentHashMap<Integer, RestTemplate> restTemplates =
            new ConcurrentHashMap<Integer, RestTemplate>();

    public ExecutorClient(AdminProperties properties) {
        this.properties = properties;
        this.jsonHeaders = buildJsonHeaders(properties.getAccessToken());
    }

    /**
     * 向目标执行器发起任务触发请求。
     *
     * @param executorBaseUrl 目标执行器通信基地址（例如：http://10.0.0.1:8081）
     * @param request         任务触发入参（含任务 ID、参数、日志 ID、超时等）
     * @return 执行器返回的执行结果；若请求失败或超时则返回包含错误原因的失败结果
     */
    public TriggerResult trigger(String executorBaseUrl, TriggerRequest request) {
        // 1. 确定本次 HTTP 调用的读取超时时间：优先使用任务自身配置的 timeoutSeconds（受全局上限封顶），
        //    兜底使用全局 readTimeoutMs
        int readTimeout = resolveReadTimeoutMs(request.getTimeoutSeconds());

        // 2. 按超时时间复用 RestTemplate（RestTemplate 配置完成后是线程安全的）
        RestTemplate rest = restTemplateFor(readTimeout);

        // 3. 设置鉴权令牌（若配置）
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            request.setAccessToken(properties.getAccessToken());
        }

        // 4. 拼接执行器触发端点 URL
        String url = trimSlash(executorBaseUrl) + "/orbit/executor/run";
        try {
            TriggerResult result = rest.postForObject(url, new HttpEntity<TriggerRequest>(request, jsonHeaders),
                    TriggerResult.class);
            // 处理空响应异常场景
            if (result == null) {
                return TriggerResult.fail(request.getLogId(), request.getJobId(), executorBaseUrl, 0,
                        "empty response from executor");
            }
            // 补充执行节点标识
            if (result.getWorkerNode() == null) {
                result.setWorkerNode(executorBaseUrl);
            }
            return result;
        } catch (Exception e) {
            // 捕获 ConnectTimeout、ReadTimeout、404、500 等网络或远端服务异常
            log.warn("[orbit-admin] trigger {} failed: {}", url, e.getMessage());
            return TriggerResult.fail(request.getLogId(), request.getJobId(), executorBaseUrl, 0,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    /**
     * 解析本次调用的 readTimeout（毫秒）。
     * <p>
     * 两点修正：
     *   - 用 {@code long} 做乘法：原先 {@code timeoutSeconds * 1000} 是 int 运算，
     *       timeoutSeconds 超过 2147483 时会溢出为负数，而负的 readTimeout 在
     *       {@code HttpURLConnection} 中等同于「无限等待」；
     *   - 按 {@code orbit.admin.max-timeout-seconds} 封顶，避免单次派发长时间占住线程。
     *
     * @param timeoutSeconds 任务配置的超时秒数，&lt;=0 表示使用全局默认
     * @return 读取超时毫秒数
     */
    private int resolveReadTimeoutMs(int timeoutSeconds) {
        if (timeoutSeconds <= 0) {
            return properties.getReadTimeoutMs();
        }
        long seconds = timeoutSeconds;
        int max = properties.getMaxTimeoutSeconds();
        if (max > 0 && seconds > max) {
            seconds = max;
        }
        long ms = seconds * 1000L;
        if (ms > Integer.MAX_VALUE) {
            ms = Integer.MAX_VALUE;
        }
        return (int) ms;
    }

    /**
     * 取得（或创建）指定 readTimeout 对应的 RestTemplate。
     * 使用 computeIfAbsent 原子复用：并发首次派发同一超时档位时不再重复构建。
     *
     * @param readTimeoutMs 读取超时毫秒数
     * @return 可复用的 RestTemplate
     */
    private RestTemplate restTemplateFor(int readTimeoutMs) {
        return restTemplates.computeIfAbsent(readTimeoutMs,
                k -> buildRest(properties.getConnectTimeoutMs(), k));
    }

    /**
     * 预构建 JSON + 鉴权 Header。
     */
    private static HttpHeaders buildJsonHeaders(String accessToken) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (accessToken != null && !accessToken.isEmpty()) {
            headers.set(TOKEN_HEADER, accessToken);
        }
        return headers;
    }

    /**
     * 辅助工厂方法：根据指定的连接超时和读取超时构建 RestTemplate
     *
     * @param connectMs 连接超时毫秒数
     * @param readMs    读取超时毫秒数
     * @return RestTemplate 实例
     */
    private static RestTemplate buildRest(int connectMs, int readMs) {
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(connectMs);
        f.setReadTimeout(readMs);
        return new RestTemplate(f);
    }

    /**
     * 规范化 URL 地址，去除末尾可能多余的斜杠
     *
     * @param s 原始 URL
     * @return 规范化后的 URL
     */
    private static String trimSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }
}

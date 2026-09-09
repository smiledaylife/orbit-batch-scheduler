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

/**
 * 调度中心向执行器派发任务的 HTTP 通信客户端。
 * 核心职责：
 *
 *   - 负责调用执行器暴露的 {@code POST /orbit/executor/run} 接口；
 *   - 使用固定的短读超时（orbit.admin.trigger-timeout-seconds）：触发是「受理即返回」的，
 *       超时只需覆盖网络往返与入队，与任务真实耗时无关；
 *   - 携带鉴权安全令牌（{@code X-Orbit-Token}）；
 *   - 捕获网络连通性异常、超时异常，并优雅封装为失败的 {@link TriggerResult}。
 *
 * 返回值语义：{@code accepted=true} 表示执行器已受理并入队（任务尚未跑完），
 * 此时调度中心必须把日志留在 RUNNING；{@code accepted=false} 表示触发本身失败，日志应立刻判失败。
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
     * 触发调用共用的 RestTemplate。读超时对所有任务一致，因此一个实例即可复用
     * （RestTemplate 配置完成后是线程安全的），不必再按超时档位缓存多份。
     */
    private final RestTemplate restTemplate;

    public ExecutorClient(AdminProperties properties) {
        this.properties = properties;
        this.jsonHeaders = buildJsonHeaders(properties.getAccessToken());
        this.restTemplate = buildRest(properties.getConnectTimeoutMs(), triggerReadTimeoutMs(properties));
    }

    /**
     * 向目标执行器发起任务触发请求。
     *
     * @param executorBaseUrl 目标执行器通信基地址（例如：http://10.0.0.1:8081）
     * @param request         任务触发入参（含任务 ID、参数、日志 ID、超时等）
     * @return 受理回执（accepted=true）；若请求失败或超时则返回包含错误原因的失败结果（accepted=false）
     */
    public TriggerResult trigger(String executorBaseUrl, TriggerRequest request) {
        // 1. 设置鉴权令牌（若配置）
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            request.setAccessToken(properties.getAccessToken());
        }

        // 2. 拼接执行器触发端点 URL
        String url = trimSlash(executorBaseUrl) + "/orbit/executor/run";
        try {
            TriggerResult result = restTemplate.postForObject(url,
                    new HttpEntity<TriggerRequest>(request, jsonHeaders), TriggerResult.class);
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
     * 解析触发调用的 readTimeout（毫秒）。
     *
     * 取 {@code orbit.admin.trigger-timeout-seconds}，配置为 0 或负数时退回全局 readTimeoutMs，
     * 避免负的 readTimeout 在 {@code HttpURLConnection} 中等同于「无限等待」。
     *
     * @param properties 调度中心配置
     * @return 读取超时毫秒数
     */
    private static int triggerReadTimeoutMs(AdminProperties properties) {
        int seconds = properties.getTriggerTimeoutSeconds();
        if (seconds <= 0) {
            return properties.getReadTimeoutMs();
        }
        long ms = seconds * 1000L;
        if (ms > Integer.MAX_VALUE) {
            ms = Integer.MAX_VALUE;
        }
        return (int) ms;
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

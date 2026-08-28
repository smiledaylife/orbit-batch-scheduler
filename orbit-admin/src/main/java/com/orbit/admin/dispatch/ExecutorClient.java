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
 * <p>核心职责：
 * <ul>
 *   <li>负责调用执行器暴露的 {@code POST /orbit/executor/run} 接口；</li>
 *   <li>动态适配不同任务指定的读取超时时间（ReadTimeout）；</li>
 *   <li>携带鉴权安全令牌（{@code X-Orbit-Token}）；</li>
 *   <li>捕获网络连通性异常、超时异常，并优雅封装为失败的 {@link TriggerResult}。</li>
 * </ul>
 */
@Component
public class ExecutorClient {

    private static final Logger log = LoggerFactory.getLogger(ExecutorClient.class);

    /**
     * 安全令牌 Header 字段名称
     */
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    private final AdminProperties properties;

    public ExecutorClient(AdminProperties properties) {
        this.properties = properties;
    }

    /**
     * 向目标执行器发起任务触发请求。
     *
     * @param executorBaseUrl 目标执行器通信基地址（例如：http://10.0.0.1:8081）
     * @param request         任务触发入参（含任务 ID、参数、日志 ID、超时等）
     * @return 执行器返回的执行结果；若请求失败或超时则返回包含错误原因的失败结果
     */
    public TriggerResult trigger(String executorBaseUrl, TriggerRequest request) {
        // 1. 确定本次 HTTP 调用的读取超时时间：优先使用任务自身配置的 timeoutSeconds，兜底使用全局 readTimeoutMs
        int readTimeout = request.getTimeoutSeconds() > 0
                ? request.getTimeoutSeconds() * 1000
                : properties.getReadTimeoutMs();

        // 2. 根据指定的超时时间动态创建 RestTemplate 实例
        RestTemplate rest = buildRest(properties.getConnectTimeoutMs(), readTimeout);

        // 3. 设置鉴权令牌（若配置）
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            request.setAccessToken(properties.getAccessToken());
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            headers.set(TOKEN_HEADER, properties.getAccessToken());
        }

        // 4. 拼接执行器触发端点 URL
        String url = trimSlash(executorBaseUrl) + "/orbit/executor/run";
        try {
            TriggerResult result = rest.postForObject(url, new HttpEntity<TriggerRequest>(request, headers),
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

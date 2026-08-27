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
 * 调度中心调用执行器 /run 接口。
 */
@Component
public class ExecutorClient {

    private static final Logger log = LoggerFactory.getLogger(ExecutorClient.class);

    public static final String TOKEN_HEADER = "X-Orbit-Token";

    private final AdminProperties properties;

    public ExecutorClient(AdminProperties properties) {
        this.properties = properties;
    }

    public TriggerResult trigger(String executorBaseUrl, TriggerRequest request) {
        int readTimeout = request.getTimeoutSeconds() > 0
                ? request.getTimeoutSeconds() * 1000
                : properties.getReadTimeoutMs();
        RestTemplate rest = buildRest(properties.getConnectTimeoutMs(), readTimeout);

        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            request.setAccessToken(properties.getAccessToken());
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            headers.set(TOKEN_HEADER, properties.getAccessToken());
        }

        String url = trimSlash(executorBaseUrl) + "/orbit/executor/run";
        try {
            TriggerResult result = rest.postForObject(url, new HttpEntity<TriggerRequest>(request, headers),
                    TriggerResult.class);
            if (result == null) {
                return TriggerResult.fail(request.getLogId(), request.getJobId(), executorBaseUrl, 0,
                        "empty response from executor");
            }
            if (result.getWorkerNode() == null) {
                result.setWorkerNode(executorBaseUrl);
            }
            return result;
        } catch (Exception e) {
            log.warn("[orbit-admin] trigger {} failed: {}", url, e.getMessage());
            return TriggerResult.fail(request.getLogId(), request.getJobId(), executorBaseUrl, 0,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private static RestTemplate buildRest(int connectMs, int readMs) {
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(connectMs);
        f.setReadTimeout(readMs);
        return new RestTemplate(f);
    }

    private static String trimSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }
}

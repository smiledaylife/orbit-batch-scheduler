package com.orbit.executor;

import com.orbit.core.model.ApiResult;
import com.orbit.core.model.RegistryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

/**
 * 执行器 → 调度中心 注册/心跳客户端。
 */
public class AdminClient {

    private static final Logger log = LoggerFactory.getLogger(AdminClient.class);
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    private final ExecutorProperties properties;
    private final RestTemplate restTemplate;

    public AdminClient(ExecutorProperties properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(3000);
        f.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(f);
    }

    public boolean registry(RegistryRequest req) {
        String admins = properties.getAdminAddresses();
        if (admins == null || admins.trim().isEmpty()) {
            log.warn("[orbit-executor] admin-addresses empty, skip registry");
            return false;
        }
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            req.setAccessToken(properties.getAccessToken());
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            headers.set(TOKEN_HEADER, properties.getAccessToken());
        }
        boolean anyOk = false;
        for (String raw : admins.split(",")) {
            String base = raw.trim();
            if (base.isEmpty()) {
                continue;
            }
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            String url = base + "/orbit/admin/registry";
            try {
                restTemplate.postForObject(url, new HttpEntity<RegistryRequest>(req, headers), ApiResult.class);
                anyOk = true;
            } catch (Exception e) {
                log.warn("[orbit-executor] registry to {} failed: {}", url, e.getMessage());
            }
        }
        return anyOk;
    }

    public void remove(RegistryRequest req) {
        String admins = properties.getAdminAddresses();
        if (admins == null) {
            return;
        }
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            req.setAccessToken(properties.getAccessToken());
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            headers.set(TOKEN_HEADER, properties.getAccessToken());
        }
        for (String raw : admins.split(",")) {
            String base = raw.trim();
            if (base.isEmpty()) {
                continue;
            }
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            try {
                restTemplate.postForObject(base + "/orbit/admin/registry/remove",
                        new HttpEntity<RegistryRequest>(req, headers), ApiResult.class);
            } catch (Exception e) {
                log.warn("[orbit-executor] remove registry failed: {}", e.getMessage());
            }
        }
    }
}

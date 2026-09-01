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
 * 执行器向调度中心进行通信交互的 HTTP 客户端。
 * 核心职责：
 * 
 *   - 负责将执行器的节点信息与心跳定期上报至调度中心集群（支持多地址容灾）；
 *   - 在执行器服务关闭时，负责向调度中心发送下线请求；
 *   - 在 HTTP 请求头中携带双向约定的安全访问令牌（{@code X-Orbit-Token}）。
 * 
 */
public class AdminClient {

    private static final Logger log = LoggerFactory.getLogger(AdminClient.class);

    /**
     * 安全令牌约定的 HTTP Header 名称
     */
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    /**
     * 执行器配置属性
     */
    private final ExecutorProperties properties;

    /**
     * 底层执行 HTTP 调用的 RestTemplate
     */
    private final RestTemplate restTemplate;

    /**
     * 构造方法，初始化 RestTemplate 及连接/读取超时时间
     *
     * @param properties 执行器配置
     */
    public AdminClient(ExecutorProperties properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        // 设置与调度中心建立 HTTP 连接的超时时间为 3 秒
        f.setConnectTimeout(3000);
        // 设置读取响应的超时时间为 5 秒
        f.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(f);
    }

    /**
     * 向调度中心发送注册或周期性心跳上报请求。
     * 若配置了多个调度中心地址（逗号分隔），将逐个尝试发送，只要有一个成功即判定为本轮心跳成功。
     *
     * @param req 注册/心跳请求数据实体
     * @return 是否有至少一个调度中心节点成功接收心跳
     */
    public boolean registry(RegistryRequest req) {
        String admins = properties.getAdminAddresses();
        // 若未配置调度中心地址，打印警告并跳过注册
        if (admins == null || admins.trim().isEmpty()) {
            log.warn("[orbit-executor] admin-addresses empty, skip registry");
            return false;
        }

        // 若配置了访问令牌，同步设置到请求体中
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            req.setAccessToken(properties.getAccessToken());
        }

        // 构建 HTTP 请求头，设置 JSON 格式并附带鉴权 Token Header
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            headers.set(TOKEN_HEADER, properties.getAccessToken());
        }

        boolean anyOk = false;
        // 支持配置多个调度中心地址，以英文逗号分隔进行集群上报
        for (String raw : admins.split(",")) {
            String base = raw.trim();
            if (base.isEmpty()) {
                continue;
            }
            // 去除末尾的斜杠
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

    /**
     * 向调度中心发送执行器服务下线注销通知。
     * 在执行器停机（优雅关闭）时调用，主动摘除注册表中的当前节点，避免调度中心在心跳超时前向已关闭节点分发任务。
     *
     * @param req 包含 appName 与 address 的注销请求数据
     */
    public void remove(RegistryRequest req) {
        String admins = properties.getAdminAddresses();
        if (admins == null) {
            return;
        }

        // 附带访问令牌
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            req.setAccessToken(properties.getAccessToken());
        }

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (properties.getAccessToken() != null && !properties.getAccessToken().isEmpty()) {
            headers.set(TOKEN_HEADER, properties.getAccessToken());
        }

        // 向所有配置的调度中心广播下线请求
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

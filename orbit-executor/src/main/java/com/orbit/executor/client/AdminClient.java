package com.orbit.executor.client;

import com.orbit.core.model.ApiResult;
import com.orbit.core.model.RegistryRequest;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 执行器向调度中心进行通信交互的 HTTP 客户端。
 * 核心职责：
 *
 *   - 负责将执行器的节点信息与心跳定期上报至调度中心集群（支持多地址容灾）；
 *   - 在执行器服务关闭时，负责向调度中心发送下线请求；
 *   - 在 HTTP 请求头中携带双向约定的安全访问令牌（{@code X-Orbit-Token}）。
 *
 * 性能设计：admin 地址列表（逗号分隔）与鉴权请求头在构造时<b>一次性预解析 / 预构建</b>。
 * 心跳默认 20 秒一次、多地址场景下原先每轮都要重复 split、trim、去尾斜杠与 Header 对象分配，
 * 配置在运行期不可变，预构建可完全消除该重复开销。
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
     * 预解析的调度中心基地址列表（去空白、去末尾斜杠），配置为空时为空列表
     */
    private final List<String> adminBases;

    /**
     * 预构建的 JSON + 鉴权 Header（配置不可变，仅依赖构造时的 accessToken）
     */
    private final HttpHeaders jsonHeaders;

    /**
     * 构造方法，初始化 RestTemplate、预解析地址列表与预构建请求头
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
        this.adminBases = parseAdminBases(properties.getAdminAddresses());
        this.jsonHeaders = buildJsonHeaders(properties.getAccessToken());
    }

    /**
     * 向调度中心发送注册或周期性心跳上报请求。
     * 若配置了多个调度中心地址（逗号分隔），将逐个尝试发送，只要有一个成功即判定为本轮心跳成功。
     *
     * @param req 注册/心跳请求数据实体
     * @return 是否有至少一个调度中心节点成功接收心跳
     */
    public boolean registry(RegistryRequest req) {
        // 若未配置调度中心地址，打印警告并跳过注册
        if (adminBases.isEmpty()) {
            log.warn("[orbit-executor] admin-addresses empty, skip registry");
            return false;
        }

        // 若配置了访问令牌，同步设置到请求体中（兼容仅从 Body 读令牌的旧版调度中心）
        applyBodyToken(req);

        boolean anyOk = false;
        HttpEntity<RegistryRequest> entity = new HttpEntity<RegistryRequest>(req, jsonHeaders);
        for (String base : adminBases) {
            String url = base + "/orbit/admin/registry";
            try {
                restTemplate.postForObject(url, entity, ApiResult.class);
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
        if (adminBases.isEmpty()) {
            return;
        }

        // 附带访问令牌
        applyBodyToken(req);

        HttpEntity<RegistryRequest> entity = new HttpEntity<RegistryRequest>(req, jsonHeaders);
        for (String base : adminBases) {
            try {
                restTemplate.postForObject(base + "/orbit/admin/registry/remove", entity, ApiResult.class);
            } catch (Exception e) {
                log.warn("[orbit-executor] remove registry failed: {}", e.getMessage());
            }
        }
    }

    /**
     * 将 accessToken 写入请求体（若配置）。提取为私有方法，避免 registry/remove 两处重复判空。
     */
    private void applyBodyToken(RegistryRequest req) {
        String token = properties.getAccessToken();
        if (token != null && !token.isEmpty()) {
            req.setAccessToken(token);
        }
    }

    /**
     * 预解析调度中心地址列表：逗号分隔、去空白、去末尾斜杠。
     */
    private static List<String> parseAdminBases(String admins) {
        if (admins == null || admins.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> bases = new ArrayList<String>();
        for (String raw : admins.split(",")) {
            String base = raw.trim();
            if (base.isEmpty()) {
                continue;
            }
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            bases.add(base);
        }
        return Collections.unmodifiableList(bases);
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
}

/*
 * AICode Begin
 * AdminJobClient：调度中心任务回写客户端。
 *
 * 用于 TableCheck 任务在检测到最新分区表后，将最新表名回写到调度中心任务参数
 * （替代原 Soma 的 job_infoMapper.updateParamsByTriggerId 落库行为）。
 *
 * 逻辑：
 *   1. GET /orbit/admin/jobs/{name} 获取任务全量信息（JobInfo + quartz 详情）；
 *   2. 将解析出的 params（Map）与本地 latestTableName 合并；
 *   3. PUT /orbit/admin/jobs/{name} 回写（body 为 JobInfo，params 为合并后的 Map）。
 *
 * 认证：与 SDK 一致，access-token 非空时携带 X-Orbit-Token 头（见 AdminAuthInterceptor）。
 */
package com.natpcp.natpcp.dsv2.jobs;

import com.orbit.core.model.JobInfo;
import com.orbit.executor.config.ExecutorProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

/**
 * 调度中心（orbit-admin）任务回写客户端。
 */
@Slf4j
@Component
public class AdminJobClient {

    private static final String TOKEN_HEADER = "X-Orbit-Token";

    @Autowired
    private ExecutorProperties executorProperties;

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 回写任务的 params[key]=value。
     *
     * @param jobName 任务名称（handler 名称）
     * @param key     参数键
     * @param value   参数值
     * @throws IllegalStateException 回写失败（日志保留原始异常链路）
     */
    public void updateJobParam(String jobName, String key, Object value) {
        String baseUrl = trimTrailingSlash(executorProperties.getAdminAddresses());
        final String detailUrl = baseUrl + "/orbit/admin/jobs/" + jobName;
        final String updateUrl = baseUrl + "/orbit/admin/jobs/" + jobName;

        HttpHeaders headers = buildHeaders();

        // 1. 获取任务当前详情
        JobInfo job;
        try {
            ResponseEntity<Map> detailResp = restTemplate.exchange(detailUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);
            job = extractJobInfo(detailResp.getBody());
        } catch (Exception e) {
            throw new IllegalStateException("查询任务详情失败 jobName=" + jobName, e);
        }
        if (job == null) {
            throw new IllegalStateException("任务不存在 jobName=" + jobName);
        }

        // 2. 合并参数
        Map<String, Object> params = job.getParams();
        if (params == null) {
            params = new HashMap<>();
        }
        params.put(key, value);
        job.setParams(params);

        // 3. 回写
        try {
            HttpEntity<JobInfo> entity = new HttpEntity<>(job, headers);
            ResponseEntity<Map> resp = restTemplate.exchange(updateUrl, HttpMethod.PUT, entity, Map.class);
            if (!resp.getStatusCode().is2xxSuccessful()) {
                throw new IllegalStateException("任务回写失败 jobName=" + jobName + ", status=" + resp.getStatusCode());
            }
            log.info("任务参数回写成功 jobName={}, key={}, value={}", jobName, key, value);
        } catch (Exception e) {
            throw new IllegalStateException("任务回写失败 jobName=" + jobName, e);
        }
    }

    /**
     * 从 admin 详情接口返回体提取 JobInfo。
     * <p>接口返回 {code,msg,data:{job:{...},quartz:{...}}}，提取 data.job。</p>
     */
    private JobInfo extractJobInfo(Map body) {
        if (body == null) {
            return null;
        }
        try {
            JsonNode root = objectMapper.convertValue(body, JsonNode.class);
            JsonNode data = root.path("data");
            JsonNode jobNode = data.path("job");
            if (jobNode.isMissingNode() || jobNode.isNull()) {
                jobNode = data;
            }
            if (jobNode.isMissingNode() || jobNode.isNull()) {
                return null;
            }
            return objectMapper.treeToValue(jobNode, JobInfo.class);
        } catch (Exception e) {
            log.warn("解析任务详情失败，将按空任务处理: {}", e.getMessage());
            return null;
        }
    }

    private HttpHeaders buildHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        String token = executorProperties.getAccessToken();
        if (token != null && !token.trim().isEmpty()) {
            headers.set(TOKEN_HEADER, token);
        }
        return headers;
    }

    private String trimTrailingSlash(String url) {
        if (url == null) {
            return "";
        }
        String trimmed = url;
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }
}
/*
 * AICode End
 */

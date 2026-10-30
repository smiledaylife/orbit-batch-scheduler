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
 * 可用性：
 *   - admin-addresses 与 SDK AdminClient 一致支持逗号分隔多地址，按顺序故障转移；
 *   - RestTemplate 显式设置连接/读取超时，避免调度中心无响应时把 Tasklet（及其事务）挂死。
 */
package com.natpcp.natpcp.dsv2.jobs;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.core.model.JobInfo;
import com.orbit.executor.config.ExecutorProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度中心（orbit-admin）任务回写客户端。
 */
@Slf4j
@Component
public class AdminJobClient {

    private static final String TOKEN_HEADER = "X-Orbit-Token";
    private static final String JOBS_PATH = "/orbit/admin/jobs/";
    private static final int CONNECT_TIMEOUT_MS = 3_000;
    private static final int READ_TIMEOUT_MS = 10_000;

    @Autowired
    private ExecutorProperties executorProperties;

    private final RestTemplate restTemplate = buildRestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static RestTemplate buildRestTemplate() {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(CONNECT_TIMEOUT_MS);
        factory.setReadTimeout(READ_TIMEOUT_MS);
        return new RestTemplate(factory);
    }

    /**
     * 回写任务的 params[key]=value。
     *
     * @param jobName 任务名称（handler 名称）
     * @param key     参数键
     * @param value   参数值
     * @throws IllegalStateException 回写失败（日志保留原始异常链路）
     */
    public void updateJobParam(String jobName, String key, Object value) {
        List<String> bases = resolveBases(executorProperties.getAdminAddresses());
        if (bases.isEmpty()) {
            throw new IllegalStateException("orbit.executor.admin-addresses 未配置，无法回写任务 jobName=" + jobName);
        }
        HttpHeaders headers = buildHeaders();

        // 1. 获取任务当前详情（多地址故障转移）
        JobInfo job;
        try {
            job = withFailover(bases, base -> {
                ResponseEntity<Map> detailResp = restTemplate.exchange(base + JOBS_PATH + jobName,
                        HttpMethod.GET, new HttpEntity<>(headers), Map.class);
                return extractJobInfo(detailResp.getBody());
            });
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
        final JobInfo body = job;
        ResponseEntity<Map> resp;
        try {
            resp = withFailover(bases, base -> restTemplate.exchange(base + JOBS_PATH + jobName,
                    HttpMethod.PUT, new HttpEntity<>(body, headers), Map.class));
        } catch (Exception e) {
            throw new IllegalStateException("任务回写失败 jobName=" + jobName, e);
        }
        if (resp == null || !resp.getStatusCode().is2xxSuccessful()) {
            throw new IllegalStateException("任务回写失败 jobName=" + jobName
                    + ", status=" + (resp == null ? "null" : resp.getStatusCode()));
        }
        log.info("任务参数回写成功 jobName={}, key={}, value={}", jobName, key, value);
    }

    /** 单地址调用 */
    @FunctionalInterface
    private interface AdminCall<T> {
        T call(String base) throws Exception;
    }

    /**
     * 依次尝试各 admin 地址，首个成功即返回；全部失败抛出最后一次异常。
     */
    private <T> T withFailover(List<String> bases, AdminCall<T> call) throws Exception {
        Exception last = null;
        for (String base : bases) {
            try {
                return call.call(base);
            } catch (Exception e) {
                last = e;
                log.warn("调用调度中心 {} 失败，尝试下一个地址: {}", base, e.getMessage());
            }
        }
        throw last == null ? new IllegalStateException("no admin address") : last;
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

    /**
     * 解析 admin-addresses（逗号分隔，去空白、去尾斜杠），与 SDK AdminClient 保持一致。
     */
    static List<String> resolveBases(String adminAddresses) {
        List<String> bases = new ArrayList<>();
        if (adminAddresses == null) {
            return bases;
        }
        for (String raw : adminAddresses.split(",")) {
            String base = raw.trim();
            if (base.isEmpty()) {
                continue;
            }
            while (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            bases.add(base);
        }
        return bases;
    }
}
/*
 * AICode End
 */

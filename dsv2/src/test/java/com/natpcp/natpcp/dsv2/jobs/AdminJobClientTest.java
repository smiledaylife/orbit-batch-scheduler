package com.natpcp.natpcp.dsv2.jobs;

import com.natpcp.natpcp.dsv2.jobs.AdminJobClient;
import com.orbit.executor.config.ExecutorProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.client.RestTemplate;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * AdminJobClient unit test.
 *
 * RestTemplate 为 private final 直接实例化，测试中用反射替换为 mock。
 * 注意 JobInfo 无 "name" 字段（为 "jobName"），body 中属性需与其映射。
 */
@DisplayName("AdminJobClient Test")
@ExtendWith(MockitoExtension.class)
class AdminJobClientTest {

    @Mock
    private RestTemplate restTemplate;

    private ExecutorProperties executorProperties;
    private AdminJobClient adminJobClient;

    @BeforeEach
    void setUp() {
        executorProperties = new ExecutorProperties();
        executorProperties.setAdminAddresses("http://localhost:8018/");
        executorProperties.setAccessToken("my-token");
        adminJobClient = new AdminJobClient();
        ReflectionTestUtils.setField(adminJobClient, "executorProperties", executorProperties);
        ReflectionTestUtils.setField(adminJobClient, "restTemplate", restTemplate);
    }

    /** 构造 /orbit/admin/jobs/{name} 返回体 data.job */
    private ResponseEntity<Map> detailResp() {
        Map<String, Object> jobMap = new HashMap<>();
        jobMap.put("id", 1L);
        jobMap.put("jobName", "tableCheck");
        jobMap.put("params", new HashMap<String, Object>());
        Map<String, Object> data = new HashMap<>();
        data.put("job", jobMap);
        Map<String, Object> body = new HashMap<>();
        body.put("code", "0");
        body.put("data", data);
        return new ResponseEntity<>(body, HttpStatus.OK);
    }

    @Test
    @DisplayName("updateJobParam: 正常回写，GET 提取 data.job，PUT 返回2xx")
    void updateJobParam_shouldUpdate_whenResultSuccess() {
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());

        adminJobClient.updateJobParam("tableCheck", "latestTable", "t_part_20260101");

        verify(restTemplate).exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class));
        verify(restTemplate).exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class));
    }

    @Test
    @DisplayName("updateJobParam: params 为 null 时创建新 map")
    void updateJobParam_shouldCreateParams_whenNull() {
        Map<String, Object> jobMap = new HashMap<>();
        jobMap.put("id", 1L);
        jobMap.put("jobName", "tableCheck");
        Map<String, Object> data = new HashMap<>();
        data.put("job", jobMap);
        Map<String, Object> body = new HashMap<>();
        body.put("code", "0");
        body.put("data", data);
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(body, HttpStatus.OK));
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(body, HttpStatus.OK));

        adminJobClient.updateJobParam("tableCheck", "k", "v");
    }

    @Test
    @DisplayName("updateJobParam: GET 失败抛出 IllegalStateException")
    void updateJobParam_shouldThrow_whenGetFails() {
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(new RuntimeException("conn refused"));

        assertThatThrownBy(() -> adminJobClient.updateJobParam("tableCheck", "k", "v"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("查询任务详情失败");
    }

    @Test
    @DisplayName("updateJobParam: data 缺失 job 提取不到抛出任务不存在")
    void updateJobParam_shouldThrow_whenJobMissing() {
        Map<String, Object> body = new HashMap<>();
        body.put("code", "0");
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(body, HttpStatus.OK));

        assertThatThrownBy(() -> adminJobClient.updateJobParam("tableCheck", "k", "v"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("任务不存在");
    }

    @Test
    @DisplayName("updateJobParam: PUT 返回非2xx抛出回写失败")
    void updateJobParam_shouldThrow_whenUpdateStatusNot2xx() {
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(new ResponseEntity<>(detailResp().getBody(), HttpStatus.BAD_REQUEST));

        assertThatThrownBy(() -> adminJobClient.updateJobParam("tableCheck", "k", "v"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("任务回写失败");
    }

    @Test
    @DisplayName("updateJobParam: PUT 抛异常包装为回写失败")
    void updateJobParam_shouldThrow_whenUpdateThrows() {
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class)))
                .thenThrow(new RuntimeException("io error"));

        assertThatThrownBy(() -> adminJobClient.updateJobParam("tableCheck", "k", "v"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("任务回写失败");
    }

    @Test
    @DisplayName("buildHeaders: access-token 为空不阻塞回写流程")
    void buildHeaders_shouldSkipToken_whenBlank() {
        executorProperties.setAccessToken("  ");
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());

        adminJobClient.updateJobParam("tableCheck", "k", "v");
    }

    @Test
    @DisplayName("updateJobParam: admin 地址尾斜杠被去除")
    void updateJobParam_shouldTrimTrailingSlash() {
        executorProperties.setAdminAddresses("http://localhost:8018////");
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.GET), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());
        when(restTemplate.exchange(eq("http://localhost:8018/orbit/admin/jobs/tableCheck"),
                eq(HttpMethod.PUT), any(HttpEntity.class), eq(Map.class)))
                .thenReturn(detailResp());

        adminJobClient.updateJobParam("tableCheck", "k", "v");
    }
}
/*
 * AICode End
 */

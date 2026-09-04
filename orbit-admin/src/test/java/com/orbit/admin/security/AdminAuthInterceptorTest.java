package com.orbit.admin.security;

import com.orbit.admin.config.AdminProperties;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 调度中心管理接口鉴权拦截器测试。
 * 覆盖：未配置令牌放行、X-Orbit-Token / Authorization Bearer 两种写法、
 * 令牌错误或缺失一律 401，以及「所有管理端点都受保护」这一回归点。
 */
class AdminAuthInterceptorTest {

    private static final String TOKEN = "s3cr3t-token";

    private static final String TRIGGER_URI = "/orbit/admin/jobs/dailyReport/trigger";

    /** 全部需要保护的管理端点（与 AdminApiController 一一对应） */
    private static final String[] PROTECTED_URIS = {
            "/orbit/admin/registry",
            "/orbit/admin/registry/remove",
            "/orbit/admin/jobs",
            "/orbit/admin/jobs/dailyReport",
            "/orbit/admin/jobs/dailyReport/pause",
            "/orbit/admin/jobs/dailyReport/resume",
            TRIGGER_URI,
            "/orbit/admin/logs",
            "/orbit/admin/executors",
            "/orbit/admin/overview",
    };

    private static AdminAuthInterceptor interceptorWithToken(String token) {
        AdminProperties properties = new AdminProperties();
        properties.setAccessToken(token);
        return new AdminAuthInterceptor(properties);
    }

    private static MockHttpServletRequest request(String method, String uri) {
        return new MockHttpServletRequest(method, uri);
    }

    @Test
    void permitAllWhenTokenNotConfigured() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken("");
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertTrue(interceptor.preHandle(request("POST", TRIGGER_URI), response, new Object()));
        assertEquals(200, response.getStatus());
    }

    @Test
    void acceptOrbitTokenHeader() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken(TOKEN);
        MockHttpServletRequest req = request("POST", TRIGGER_URI);
        req.addHeader("X-Orbit-Token", TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertTrue(interceptor.preHandle(req, response, new Object()));
    }

    @Test
    void acceptBearerTokenHeader() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken(TOKEN);
        MockHttpServletRequest req = request("DELETE", "/orbit/admin/jobs/dailyReport");
        req.addHeader("Authorization", "Bearer " + TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertTrue(interceptor.preHandle(req, response, new Object()));
    }

    @Test
    void rejectMissingTokenWith401() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken(TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertFalse(interceptor.preHandle(request("POST", TRIGGER_URI), response, new Object()));
        assertEquals(401, response.getStatus());
        // 响应体与 ApiResult 的 JSON 结构保持一致，前端不需要特判 401 的返回格式
        assertTrue(response.getContentAsString().contains("\"code\":401"));
        assertTrue(response.getContentAsString().contains("invalid access token"));
    }

    @Test
    void rejectWrongTokenWith401() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken(TOKEN);
        MockHttpServletRequest req = request("POST", TRIGGER_URI);
        req.addHeader("X-Orbit-Token", "wrong");
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertFalse(interceptor.preHandle(req, response, new Object()));
        assertEquals(401, response.getStatus());
    }

    @Test
    void rejectMalformedBearerHeader() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken(TOKEN);
        MockHttpServletRequest req = request("GET", "/orbit/admin/jobs");
        req.addHeader("Authorization", "Bearer ");
        MockHttpServletResponse response = new MockHttpServletResponse();

        assertFalse(interceptor.preHandle(req, response, new Object()));
        assertEquals(401, response.getStatus());
    }

    @Test
    void everyAdminEndpointIsProtected() throws Exception {
        AdminAuthInterceptor interceptor = interceptorWithToken(TOKEN);
        for (String uri : PROTECTED_URIS) {
            MockHttpServletResponse response = new MockHttpServletResponse();
            assertFalse(interceptor.preHandle(request("GET", uri), response, new Object()),
                    "endpoint should require a token: " + uri);
            assertEquals(401, response.getStatus(), "endpoint should return 401: " + uri);
        }
    }
}

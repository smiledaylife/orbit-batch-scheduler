package com.orbit.admin.security;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.protocol.OrbitProtocol;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AdminAuthInterceptor} 的鉴权闸门测试。
 *
 * 这是 {@code /orbit/admin/**} 唯一的鉴权入口，覆盖三种令牌来源（X-Orbit-Token、
 * Authorization: Bearer、缺失）、未配置令牌时的放行，以及拒绝时的 401 响应体。
 */
class AdminAuthInterceptorTest {

    private static final String TOKEN = "s3cr3t-token";

    private AdminAuthInterceptor interceptorWith(String configuredToken) {
        AdminProperties properties = new AdminProperties();
        properties.setAccessToken(configuredToken);
        return new AdminAuthInterceptor(properties);
    }

    private MockHttpServletRequest post(String path) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", path);
        request.setRequestURI(path);
        return request;
    }

    @Test
    void unconfiguredTokenAllowsEveryRequest() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(interceptorWith("").preHandle(post("/orbit/admin/jobs"), response, new Object()),
                "空令牌应放行，保留开发态开箱即用");
        assertTrue(interceptorWith(null).preHandle(post("/orbit/admin/jobs"), response, new Object()),
                "null 令牌同样应放行");
        assertEquals(200, response.getStatus(), "放行时不应改写响应状态");
    }

    @Test
    void orbitTokenHeaderIsAccepted() throws Exception {
        MockHttpServletRequest request = post("/orbit/admin/jobs");
        request.addHeader(OrbitProtocol.TOKEN_HEADER, TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(interceptorWith(TOKEN).preHandle(request, response, new Object()));
    }

    @Test
    void bearerTokenIsAccepted() throws Exception {
        MockHttpServletRequest request = post("/orbit/admin/jobs/1/trigger");
        request.addHeader("Authorization", "Bearer " + TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(interceptorWith(TOKEN).preHandle(request, response, new Object()),
                "Authorization: Bearer 是为 curl / 网关准备的等价入口");
    }

    @Test
    void configuredTokenIsTrimmedBeforeComparison() throws Exception {
        MockHttpServletRequest request = post("/orbit/admin/jobs");
        request.addHeader(OrbitProtocol.TOKEN_HEADER, TOKEN);
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertTrue(interceptorWith("  " + TOKEN + "\n").preHandle(request, response, new Object()),
                "配置文件里带空白字符的令牌不应导致所有请求被拒");
    }

    @Test
    void wrongTokenIsRejectedWith401() throws Exception {
        MockHttpServletRequest request = post("/orbit/admin/jobs");
        request.addHeader(OrbitProtocol.TOKEN_HEADER, TOKEN + "-wrong");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(interceptorWith(TOKEN).preHandle(request, response, new Object()));
        assertEquals(401, response.getStatus());
        // 拦截器先 setContentType 再 setCharacterEncoding，容器会把 charset 并进 Content-Type，
        // 因此这里分别钉住媒体类型与字符集，而不是比对一个完整字面量
        String contentType = response.getContentType();
        assertTrue(contentType != null && contentType.startsWith("application/json"),
                "拒绝时应声明 JSON 媒体类型，实际为 " + contentType);
        assertTrue(contentType.contains("charset=UTF-8"),
                "响应体含中文提示，必须显式声明 UTF-8，实际为 " + contentType);
        assertTrue(response.getContentAsString().contains("\"code\":401"),
                "拒绝时返回与 ApiResult 同构的 JSON，避免调用方解析出两种结构");
    }

    @Test
    void missingTokenIsRejectedWith401() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(interceptorWith(TOKEN).preHandle(post("/orbit/admin/jobs/1"), response, new Object()));
        assertEquals(401, response.getStatus());
    }

    @Test
    void emptyBearerValueIsRejected() throws Exception {
        MockHttpServletRequest request = post("/orbit/admin/jobs");
        request.addHeader("Authorization", "Bearer   ");
        MockHttpServletResponse response = new MockHttpServletResponse();
        assertFalse(interceptorWith(TOKEN).preHandle(request, response, new Object()),
                "空 Bearer 值不能退化成放行");
    }
}

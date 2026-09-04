package com.orbit.admin.security;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * 调度中心管理接口统一鉴权拦截器。
 *
 * 背景：此前只有 {@code /registry} 与 {@code /registry/remove} 两个端点调用了
 * {@code checkToken}，其余 11 个端点（含 {@code POST /jobs}、{@code DELETE /jobs/{name}}、
 * {@code POST /jobs/{name}/trigger}）即使在配置了 accessToken 的情况下也完全不校验 ——
 * 任何能访问到调度中心端口的人都可以建任务、删任务、立即触发任意 handler。
 *
 * 本拦截器覆盖 {@code /orbit/admin/**} 全部端点，令牌从请求头读取，两种写法任选其一：
 *
 *   - {@code X-Orbit-Token: your-token}（执行器与本拦截器共用同一约定）；
 *   - {@code Authorization: Bearer your-token}（便于 curl / 浏览器 / 网关接入）。
 *
 * 说明：{@code /registry} 额外支持从请求体读取 token（见 {@code AdminApiController.checkToken}），
 * 那是为了兼容执行器的历史行为；拦截器不读请求体，因为 preHandle 阶段消费 body
 * 会影响后续 {@code @RequestBody} 反序列化。执行器两端都会带请求头，因此不受影响。
 *
 * 未配置 accessToken 时的行为：放行，但启动时打印醒目告警。
 * 这样保留了开箱即用的开发体验；生产环境必须配置
 * {@code orbit.admin.access-token}。
 */
public class AdminAuthInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(AdminAuthInterceptor.class);

    /** 未授权响应体，与 {@code ApiResult} 的 JSON 结构保持一致 */
    private static final String UNAUTHORIZED_BODY =
            "{\"code\":401,\"success\":false,\"msg\":\"invalid access token\",\"data\":null}";

    private static final String BEARER_PREFIX = "Bearer ";

    private final AdminProperties properties;

    public AdminAuthInterceptor(AdminProperties properties) {
        this.properties = properties;
    }

    /**
     * 启动时提示鉴权状态，避免「以为配了鉴权其实没配」。
     */
    @PostConstruct
    public void warnIfInsecure() {
        String token = properties.getAccessToken();
        if (token == null || token.trim().isEmpty()) {
            log.warn("==================================================================");
            log.warn(" orbit.admin.access-token 未配置：/orbit/admin/** 全部端点无需鉴权即可访问，");
            log.warn(" 任何能访问该端口的人都可以创建/删除/立即触发任务。");
            log.warn(" 生产环境请务必配置 orbit.admin.access-token。");
            log.warn("==================================================================");
        } else {
            log.info("[orbit-admin] access token configured, /orbit/admin/** requires authentication");
        }
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String expected = properties.getAccessToken();
        if (expected == null || expected.trim().isEmpty()) {
            return true;
        }
        String actual = extractToken(request);
        if (expected.trim().equals(actual)) {
            return true;
        }

        log.warn("[orbit-admin] rejected unauthenticated request: {} {} from {}",
                request.getMethod(), request.getRequestURI(), request.getRemoteAddr());
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        response.setCharacterEncoding("UTF-8");
        response.getWriter().write(UNAUTHORIZED_BODY);
        return false;
    }

    /**
     * 从请求头提取令牌，优先 {@code X-Orbit-Token}，其次 {@code Authorization: Bearer}。
     *
     * @param request 当前请求
     * @return 令牌，取不到时返回 null
     */
    private static String extractToken(HttpServletRequest request) {
        String header = request.getHeader(ExecutorClient.TOKEN_HEADER);
        if (header != null && !header.trim().isEmpty()) {
            return header.trim();
        }
        String authorization = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (authorization != null && authorization.startsWith(BEARER_PREFIX)) {
            String token = authorization.substring(BEARER_PREFIX.length()).trim();
            return token.isEmpty() ? null : token;
        }
        return null;
    }
}

package com.orbit.admin.security;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.protocol.OrbitProtocol;
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
 * {@code /orbit/admin/**} 下的全部端点都必须受令牌保护，包括 {@code POST /jobs}、
 * {@code DELETE /jobs/{name}}、{@code POST /jobs/{name}/trigger} ——
 * 否则任何能访问到调度中心端口的人都可以建任务、删任务、立即触发任意 handler。
 *
 * 本拦截器覆盖 {@code /orbit/admin/**} 全部端点，令牌只从请求头读取：
 *   - {@code X-Orbit-Token: <token>}（与执行器共用 {@link OrbitProtocol#TOKEN_HEADER} 约定）；
 *   - 或 {@code Authorization: Bearer <token>}（便于 curl / 浏览器 / 网关接入）。
 * 不读请求体：preHandle 阶段消费 body 会影响后续 {@code @RequestBody} 反序列化。
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

    /** {@code Authorization} 头的 Bearer 前缀（含末尾空格） */
    private static final String BEARER_PREFIX = "Bearer ";

    private final AdminProperties properties;

    /**
     * @param properties 调度中心配置，提供期望的 access-token；为空表示不启用鉴权
     */
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

    /**
     * 鉴权闸门：比对请求头令牌与配置的 access-token。
     *
     * 未配置 access-token 时放行（开发态开箱即用，启动时另有醒目告警）；
     * 比对失败写 401 与固定 JSON 体后返回 false，请求不再进入 Controller。
     *
     * @param request  当前请求
     * @param response 当前响应
     * @param handler  目标处理器
     * @return 是否放行
     * @throws Exception 写响应体失败时抛出
     */
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
            throws Exception {
        String expected = properties.getAccessToken();
        if (expected == null || expected.trim().isEmpty()) {
            return true;
        }
        String actual = extractToken(request);
        // 常量时间比对：抵御时序侧信道逐字节猜测令牌
        if (OrbitProtocol.constantTimeEquals(expected.trim(), actual)) {
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
        String header = request.getHeader(OrbitProtocol.TOKEN_HEADER);
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

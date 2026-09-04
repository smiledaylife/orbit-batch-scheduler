package com.orbit.admin.config;

import com.orbit.admin.security.AdminAuthInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * 调度中心 Web 层配置。
 *
 * 目前唯一职责：把 {@link AdminAuthInterceptor} 挂到 {@code /orbit/admin/**} 上，
 * 使任务 CRUD、暂停/恢复、手动触发、日志与执行器查询等全部管理端点统一受令牌保护。
 *
 * 说明：Actuator 端点（{@code /actuator/health} 等）不在 {@code /orbit/admin/**} 之下，
 * 因此不受影响，K8s 探针可继续免鉴权访问。
 */
@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

    /** 需要保护的路径 */
    private static final String ADMIN_PATH_PATTERN = "/orbit/admin/**";

    private final AdminProperties properties;

    public WebMvcConfig(AdminProperties properties) {
        this.properties = properties;
    }

    /**
     * 声明鉴权拦截器 Bean。
     *
     * 之所以用 {@code @Bean} 而不是在 {@code AdminAuthInterceptor} 上打 {@code @Component}：
     * 该类属于 security 包，与配置解耦，便于单元测试直接 new。
     *
     * @return 拦截器实例
     */
    @Bean
    public AdminAuthInterceptor adminAuthInterceptor() {
        return new AdminAuthInterceptor(properties);
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 直接调用上面的 @Bean 方法：@Configuration 类默认被 CGLIB 代理，
        // 返回的是容器中的同一个单例（其 @PostConstruct 已由容器执行）。
        registry.addInterceptor(adminAuthInterceptor()).addPathPatterns(ADMIN_PATH_PATTERN);
    }
}

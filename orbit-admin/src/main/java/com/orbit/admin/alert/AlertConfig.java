package com.orbit.admin.alert;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 告警组件装配。
 *
 * 装配规则：
 *
 *   - {@link OrbitAlertHandler}：业务侧未提供实现时，自动注册内置的
 *     {@link LoggingAlertHandler}（WARN 日志）；业务侧只要声明自己的
 *     {@code OrbitAlertHandler} Bean（无需任何注解约定），本条件装配即让位，
 *     保证「用户实现优先」且不会出现双写；
 *   - {@link AlertDispatcher}：始终由框架创建，绑定当前生效的唯一处理器。
 *
 * 注意：本配置类位于调度中心（orbit-admin），与执行器 SDK 无关；
 * 告警只关心调度侧的任务成败，不在执行器业务应用内产生。
 */
@Configuration
public class AlertConfig {

    /**
     * 默认告警处理器：业务侧未定义 {@link OrbitAlertHandler} Bean 时生效。
     *
     * @return 内置 WARN 日志处理器
     */
    @Bean
    @ConditionalOnMissingBean(OrbitAlertHandler.class)
    public OrbitAlertHandler loggingAlertHandler() {
        return new LoggingAlertHandler();
    }

    /**
     * 告警异步分发器，绑定容器内唯一的 {@link OrbitAlertHandler}。
     *
     * @param handler 告警处理器（用户 Bean 或内置兜底，二者必有其一）
     * @return 分发器实例
     */
    @Bean
    public AlertDispatcher alertDispatcher(OrbitAlertHandler handler) {
        return new AlertDispatcher(handler);
    }
}

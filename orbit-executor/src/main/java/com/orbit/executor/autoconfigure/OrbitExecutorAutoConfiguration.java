package com.orbit.executor.autoconfigure;

import com.orbit.executor.bootstrap.ExecutorBootstrap;
import com.orbit.executor.client.AdminClient;
import com.orbit.executor.config.ExecutorProperties;
import com.orbit.executor.handler.JobHandlerRegistry;
import com.orbit.executor.web.ExecutorController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Orbit 执行器 Spring Boot 自动装配配置类。
 * 业务应用只需通过 Maven 引入 {@code orbit-executor} 依赖，在配置中开启
 * （默认开启 {@code orbit.executor.enabled: true}），即可自动装配任务注册表、心跳通信客户端和 HTTP 触发入口。
 */
@Configuration
@EnableConfigurationProperties(ExecutorProperties.class)
@ConditionalOnProperty(prefix = "orbit.executor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrbitExecutorAutoConfiguration {

    /**
     * 注册 @OrbitJob 扫描与方法缓存注册表 Bean
     *
     * @return JobHandlerRegistry 实例
     */
    @Bean
    @ConditionalOnMissingBean
    public JobHandlerRegistry orbitJobHandlerRegistry() {
        return new JobHandlerRegistry();
    }

    /**
     * 注册与调度中心 HTTP 交互的心跳客户端 Bean
     *
     * @param properties 执行器配置属性
     * @return AdminClient 实例
     */
    @Bean
    @ConditionalOnMissingBean
    public AdminClient orbitAdminClient(ExecutorProperties properties) {
        return new AdminClient(properties);
    }

    /**
     * 注册执行器生命周期与心跳自注册启动器 Bean
     *
     * @param properties  执行器配置属性
     * @param registry    JobHandler 注册表
     * @param adminClient 调度中心客户端
     * @return ExecutorBootstrap 实例
     */
    @Bean
    @ConditionalOnMissingBean
    public ExecutorBootstrap orbitExecutorBootstrap(ExecutorProperties properties,
                                                    JobHandlerRegistry registry,
                                                    AdminClient adminClient) {
        return new ExecutorBootstrap(properties, registry, adminClient);
    }

    /**
     * 仅在 Servlet Web 环境下装配执行器 HTTP 对外触发 Controller
     */
    @Configuration
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    static class WebConfig {

        /**
         * 注册接收调度中心 /orbit/executor/run 和 /handlers 调用的控制器
         *
         * @param registry   JobHandler 注册表
         * @param properties 执行器配置属性
         * @param bootstrap  执行器启动引导器
         * @return ExecutorController 实例
         */
        @Bean
        @ConditionalOnMissingBean
        public ExecutorController orbitExecutorController(JobHandlerRegistry registry,
                                                          ExecutorProperties properties,
                                                          ExecutorBootstrap bootstrap) {
            return new ExecutorController(registry, properties, bootstrap);
        }
    }
}

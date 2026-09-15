package com.orbit.executor.autoconfigure;

import com.orbit.executor.bootstrap.ExecutorBootstrap;
import com.orbit.executor.client.AdminClient;
import com.orbit.executor.client.CallbackClient;
import com.orbit.executor.client.ExecutionIdempotency;
import com.orbit.executor.config.ExecutorProperties;
import com.orbit.executor.handler.JobExecutionService;
import com.orbit.executor.handler.JobHandlerRegistry;
import com.orbit.executor.web.ExecutorController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Orbit 执行器 Spring Boot 自动装配入口。
 *
 * <p>当 {@code orbit.executor.enabled=true}（默认）时自动创建任务注册、执行、Admin 通信、
 * callback、幂等保护和 Bootstrap 组件。业务应用只需要引入 starter/依赖并提供配置即可接入。</p>
 */
@Configuration
@EnableConfigurationProperties(ExecutorProperties.class)
@ConditionalOnProperty(prefix = "orbit.executor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrbitExecutorAutoConfiguration {

    /** 创建任务 Handler 注册表，用于保存 {@code @OrbitJob} 暴露的执行方法。 */
    @Bean
    @ConditionalOnMissingBean
    public JobHandlerRegistry orbitJobHandlerRegistry() {
        return new JobHandlerRegistry();
    }

    /**
     * 创建 Redis 执行幂等组件。
     * 幂等 key 以 logId 为粒度，避免 Admin HTTP 超时后重试导致同一任务重复进入执行线程池。
     */
    @Bean
    @ConditionalOnMissingBean
    public ExecutionIdempotency orbitExecutionIdempotency(StringRedisTemplate redis,
                                                          ExecutorProperties properties) {
        return new ExecutionIdempotency(redis, properties);
    }

    /** 创建任务执行服务；容器销毁时调用 {@code destroy()} 完成线程池优雅停止。 */
    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean
    public JobExecutionService orbitJobExecutionService(ExecutorProperties properties,
                                                        CallbackClient callbackClient) {
        return new JobExecutionService(properties, callbackClient);
    }

    /** 创建 Admin HTTP 客户端，负责注册、心跳和任务触发通信。 */
    @Bean
    @ConditionalOnMissingBean
    public AdminClient orbitAdminClient(ExecutorProperties properties) {
        return new AdminClient(properties);
    }

    /** 创建异步 callback 客户端；生产模式使用 Redis Stream 持久化执行结果。 */
    @Bean
    @ConditionalOnMissingBean
    public CallbackClient orbitCallbackClient(ExecutorProperties properties,
                                              AdminClient adminClient,
                                              StringRedisTemplate redis) {
        return new CallbackClient(properties, adminClient, redis);
    }

    /** 创建 Executor 启动引导组件，负责注册自身并启动心跳等后台任务。 */
    @Bean
    @ConditionalOnMissingBean
    public ExecutorBootstrap orbitExecutorBootstrap(ExecutorProperties properties,
                                                    JobHandlerRegistry registry,
                                                    AdminClient adminClient) {
        return new ExecutorBootstrap(properties, registry, adminClient);
    }

    /**
     * Web 场景下才注册 HTTP Controller；非 Web 应用可以复用执行器核心能力而不引入 Servlet 端点。
     */
    @Configuration
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    static class WebConfig {
        /** 暴露 Executor 注册、心跳和任务执行相关 HTTP API。 */
        @Bean
        @ConditionalOnMissingBean
        public ExecutorController orbitExecutorController(JobHandlerRegistry registry,
                                                          ExecutorProperties properties,
                                                          ExecutorBootstrap bootstrap,
                                                          JobExecutionService executionService,
                                                          ExecutionIdempotency idempotency) {
            return new ExecutorController(registry, properties, bootstrap, executionService, idempotency);
        }
    }
}

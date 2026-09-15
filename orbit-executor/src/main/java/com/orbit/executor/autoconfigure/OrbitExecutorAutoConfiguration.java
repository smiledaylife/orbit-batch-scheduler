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

/** Orbit 执行器 Spring Boot 自动装配。 */
@Configuration
@EnableConfigurationProperties(ExecutorProperties.class)
@ConditionalOnProperty(prefix = "orbit.executor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrbitExecutorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public JobHandlerRegistry orbitJobHandlerRegistry() {
        return new JobHandlerRegistry();
    }

    @Bean
    @ConditionalOnMissingBean
    public ExecutionIdempotency orbitExecutionIdempotency(org.springframework.data.redis.core.StringRedisTemplate redis,
                                                          ExecutorProperties properties) {
        return new ExecutionIdempotency(redis, properties);
    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnMissingBean
    public JobExecutionService orbitJobExecutionService(ExecutorProperties properties,
                                                        CallbackClient callbackClient) {
        return new JobExecutionService(properties, callbackClient);
    }

    @Bean
    @ConditionalOnMissingBean
    public AdminClient orbitAdminClient(ExecutorProperties properties) {
        return new AdminClient(properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public CallbackClient orbitCallbackClient(ExecutorProperties properties, AdminClient adminClient) {
        return new CallbackClient(properties, adminClient);
    }

    @Bean
    @ConditionalOnMissingBean
    public ExecutorBootstrap orbitExecutorBootstrap(ExecutorProperties properties,
                                                    JobHandlerRegistry registry,
                                                    AdminClient adminClient) {
        return new ExecutorBootstrap(properties, registry, adminClient);
    }

    @Configuration
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    static class WebConfig {
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

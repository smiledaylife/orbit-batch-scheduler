package com.orbit.executor.autoconfigure;

import com.orbit.executor.AdminClient;
import com.orbit.executor.ExecutorBootstrap;
import com.orbit.executor.ExecutorProperties;
import com.orbit.executor.JobHandlerRegistry;
import com.orbit.executor.web.ExecutorController;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 执行器自动装配：业务应用引入 orbit-executor 依赖即可。
 */
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
    public AdminClient orbitAdminClient(ExecutorProperties properties) {
        return new AdminClient(properties);
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
                                                          ExecutorBootstrap bootstrap) {
            return new ExecutorController(registry, properties, bootstrap);
        }
    }
}

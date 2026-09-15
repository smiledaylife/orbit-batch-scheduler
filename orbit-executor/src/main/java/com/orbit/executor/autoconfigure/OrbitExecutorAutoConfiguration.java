package com.orbit.executor.autoconfigure;

import com.orbit.executor.bootstrap.ExecutorBootstrap;
import com.orbit.executor.client.AdminClient;
import com.orbit.executor.client.CallbackClient;
import com.orbit.executor.client.DurableCallbackClient;
import com.orbit.executor.client.ExecutionIdempotency;
import com.orbit.executor.client.RedisExecutionIdempotency;
import com.orbit.executor.config.ExecutorProperties;
import com.orbit.executor.handler.JobExecutionService;
import com.orbit.executor.handler.JobHandlerRegistry;
import com.orbit.executor.web.ExecutorController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Orbit 执行器 Spring Boot 自动装配入口。
 *
 * 当 {@code orbit.executor.enabled=true}（默认）时自动创建任务注册、执行、Admin 通信、
 * callback、幂等保护和 Bootstrap 组件。业务应用只需要引入 starter/依赖并提供配置即可接入。
 *
 * Redis 是可选能力：{@code orbit-executor} 的 Redis 依赖为 optional，本装配按
 * classpath 拆成两个嵌套配置 —— 有 Redis 时优先使用（跨副本幂等 + 持久化 callback），
 * 没有 Redis 时回退为 JVM 本地幂等 + 纯 HTTP callback，业务应用零 Redis 也能完整运行。
 * 未使用 Redis 的应用也因此不会被动引入 Redis 客户端 jar，不会激活 Redis 健康探针。
 */
@AutoConfiguration
@EnableConfigurationProperties(ExecutorProperties.class)
@ConditionalOnProperty(prefix = "orbit.executor", name = "enabled", havingValue = "true", matchIfMissing = true)
public class OrbitExecutorAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(OrbitExecutorAutoConfiguration.class);

    /** 创建任务 Handler 注册表，用于保存 {@code @OrbitJob} 暴露的执行方法。 */
    @Bean
    @ConditionalOnMissingBean
    public JobHandlerRegistry orbitJobHandlerRegistry() {
        return new JobHandlerRegistry();
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

    /** 创建 Executor 启动引导组件，负责注册自身并启动心跳等后台任务。 */
    @Bean
    @ConditionalOnMissingBean
    public ExecutorBootstrap orbitExecutorBootstrap(ExecutorProperties properties,
                                                    JobHandlerRegistry registry,
                                                    AdminClient adminClient) {
        return new ExecutorBootstrap(properties, registry, adminClient);
    }

    /**
     * classpath 存在 Redis 时的装配：优先使用 Redis 能力。
     *
     * 注意两点：
     *
     *   - 通过 {@link ObjectProvider} 取 StringRedisTemplate：依赖在 classpath 但应用
     *       未启用 Redis 自动装配时容器里可能没有模板 Bean，此时按「无 Redis」回退，
     *       而不是让应用启动失败；
     *   - 嵌套类通过 {@code @ConditionalOnClass} 守门：无 Redis 的应用不会加载本类，
     *       避免方法签名引用缺失类导致 NoClassDefFoundError。
     */
    @Configuration
    @ConditionalOnClass(StringRedisTemplate.class)
    static class RedisPresentConfig {

        /**
         * 创建 Redis 执行幂等组件（跨副本去重）。
         * 容器中存在 StringRedisTemplate 时使用 Redis 实现；
         * 否则退化为 JVM 本地实现（见 {@link ExecutionIdempotency}）。
         */
        @Bean
        @ConditionalOnMissingBean
        public ExecutionIdempotency orbitExecutionIdempotency(ObjectProvider<StringRedisTemplate> redis,
                                                              ExecutorProperties properties) {
            StringRedisTemplate template = redis.getIfAvailable();
            if (template != null) {
                log.info("[orbit-executor] execution idempotency mode: redis (cross-replica)");
                return new RedisExecutionIdempotency(template, properties);
            }
            log.info("[orbit-executor] execution idempotency mode: local jvm fallback "
                    + "(no StringRedisTemplate bean; single-node protection only)");
            return new ExecutionIdempotency(properties);
        }

        /** 创建异步 callback 客户端；Redis 可用时使用持久化回传，否则纯 HTTP。 */
        @Bean
        @ConditionalOnMissingBean
        public CallbackClient orbitCallbackClient(ExecutorProperties properties,
                                                  AdminClient adminClient,
                                                  ObjectProvider<StringRedisTemplate> redis) {
            StringRedisTemplate template = redis.getIfAvailable();
            if (template != null) {
                log.info("[orbit-executor] callback mode: durable redis stream with http fallback");
                return new DurableCallbackClient(properties, adminClient, template);
            }
            log.info("[orbit-executor] callback mode: http only (no StringRedisTemplate bean)");
            return new CallbackClient(properties, adminClient);
        }
    }

    /**
     * classpath 没有 Redis 时的装配：JVM 本地幂等 + 纯 HTTP callback。
     * 业务应用引入 orbit-executor 但没有（也不想要）Redis 依赖时的零配置降级路径。
     */
    @Configuration
    @ConditionalOnMissingClass("org.springframework.data.redis.core.StringRedisTemplate")
    static class RedisAbsentConfig {

        /** 创建 JVM 本地执行幂等组件（单节点防重，见 {@link ExecutionIdempotency}）。 */
        @Bean
        @ConditionalOnMissingBean
        public ExecutionIdempotency orbitExecutionIdempotency(ExecutorProperties properties) {
            log.info("[orbit-executor] execution idempotency mode: local jvm fallback "
                    + "(redis not on classpath; single-node protection only)");
            return new ExecutionIdempotency(properties);
        }

        /** 创建纯 HTTP 回传客户端，不依赖 Redis。 */
        @Bean
        @ConditionalOnMissingBean
        public CallbackClient orbitCallbackClient(ExecutorProperties properties,
                                                  AdminClient adminClient) {
            log.info("[orbit-executor] callback mode: http only (redis not on classpath)");
            return new CallbackClient(properties, adminClient);
        }
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

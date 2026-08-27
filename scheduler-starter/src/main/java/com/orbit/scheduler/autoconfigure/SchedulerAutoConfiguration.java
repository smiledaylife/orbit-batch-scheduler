package com.orbit.scheduler.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.scheduler.bootstrap.SchedulerBootstrap;
import com.orbit.scheduler.core.JobManager;
import com.orbit.scheduler.core.TaskRegistry;
import com.orbit.scheduler.dialect.DialectResolver;
import com.orbit.scheduler.dialect.SchedulerDialect;
import com.orbit.scheduler.discovery.HeadlessDnsServiceEndpointResolver;
import com.orbit.scheduler.discovery.ServiceDnsServiceEndpointResolver;
import com.orbit.scheduler.discovery.StaticServiceEndpointResolver;
import com.orbit.scheduler.health.OrbitSchedulerHealthIndicator;
import com.orbit.scheduler.http.HttpDispatchClient;
import com.orbit.scheduler.lock.JdbcLockProvider;
import com.orbit.scheduler.lock.NoOpLockProvider;
import com.orbit.scheduler.lock.RedissonLockProvider;
import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.spi.JobLogRepository;
import com.orbit.scheduler.spi.LockProvider;
import com.orbit.scheduler.spi.ServiceEndpointResolver;
import com.orbit.scheduler.spi.TaskRepository;
import com.orbit.scheduler.storage.InMemoryJobLogRepository;
import com.orbit.scheduler.storage.InMemoryTaskRepository;
import com.orbit.scheduler.storage.JdbcJobLogRepository;
import com.orbit.scheduler.storage.JdbcTaskRepository;
import com.orbit.scheduler.support.SchedulerProperties;
import com.orbit.scheduler.web.HttpDispatchController;
import com.orbit.scheduler.web.JobController;
import org.quartz.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.quartz.QuartzAutoConfiguration;
import org.springframework.boot.autoconfigure.quartz.SchedulerFactoryBeanCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 调度框架自动装配。
 *
 * <p>装配决策：
 * <table border="1">
 * <tr><th>组件</th><th>规则</th></tr>
 * <tr><td>TaskRepository</td><td>storage.type=database 需数据源；memory 无依赖；auto 自动降级</td></tr>
 * <tr><td>LockProvider</td><td>redis 需 RedissonClient；database 需数据源；auto 优先 Redis → 数据库 → 无锁</td></tr>
 * <tr><td>HttpDispatchClient</td><td>classpath 存在 spring-web 时装配</td></tr>
 * <tr><td>管理 API</td><td>Servlet Web 环境且 orbit.scheduler.api-enabled=true</td></tr>
 * <tr><td>健康检查</td><td>classpath 存在 actuator</td></tr>
 * <tr><td>数据库方言</td><td>database.dialect=auto 时三级探测（URL/元数据/sql_compatibility），自动注入 Quartz driverDelegateClass（用户显式配置优先）与主键生成策略；MySQL/H2 等不干预</td></tr>
 * </table>
 *
 * @author orbit
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(prefix = "orbit.scheduler", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(SchedulerProperties.class)
@AutoConfigureAfter({DataSourceAutoConfiguration.class, QuartzAutoConfiguration.class})
public class SchedulerAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(SchedulerAutoConfiguration.class);

    // ==================================================================
    // 数据库方言（PostgreSQL / GaussDB 双兼容核心）
    // ==================================================================

    /**
     * 数据库方言探测：三级探测（显式配置 → URL/产品名 → sql_compatibility 内核特征）。
     * 仅在存在数据源时装配；探测失败不阻断启动（降级 OTHER，不干预 Quartz delegate）。
     */
    @Bean
    @ConditionalOnMissingBean(SchedulerDialect.class)
    @ConditionalOnBean(DataSource.class)
    public SchedulerDialect orbitSchedulerDialect(SchedulerProperties properties, DataSource dataSource) {
        SchedulerDialect dialect;
        try {
            dialect = DialectResolver.resolve(dataSource, properties.getDatabase().getDialect());
        } catch (Exception e) {
            log.warn("[orbit-scheduler] database dialect probe failed (database unreachable?), " +
                    "fallback to OTHER (quartz delegate keeps user/default config): {}", e.getMessage());
            return SchedulerDialect.OTHER;
        }

        String compatibility = null;
        boolean quartzTablesPresent = false;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            if (dialect == SchedulerDialect.GAUSSDB) {
                compatibility = DialectResolver.probeCompatibilityMode(connection);
            }
            if (dialect == SchedulerDialect.GAUSSDB || dialect == SchedulerDialect.POSTGRESQL) {
                quartzTablesPresent = DialectResolver.quartzTablesPresent(connection);
            }
        } catch (Exception e) {
            log.warn("[orbit-scheduler] dialect post-check failed: {}", e.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception ignore) {
                    // ignore
                }
            }
        }

        log.info("[orbit-scheduler] database dialect resolved: {}{} | quartzDelegate={} | emptyStringAsNull={} | sequenceIdPrefetch={} | auto-injects driverDelegateClass: {}",
                dialect.getDisplayName(),
                compatibility == null ? "" : " (sql_compatibility=" + compatibility + ")",
                dialect.getQuartzDelegateClassName() == null ? "(keep user/default)" : dialect.getQuartzDelegateClassName(),
                !dialect.isEmptyStringDistinctFromNull(),
                dialect.prefetchIdsFromSequence(),
                dialect.injectsQuartzDelegate());

        if (dialect == SchedulerDialect.GAUSSDB) {
            log.info("[orbit-scheduler] GaussDB detected: empty string degrades to NULL, " +
                    "DAO layer normalizes blank strings to NULL; primary keys are prefetched " +
                    "via SEQUENCE nextval (INSERT result feedback is not used).");
        }
        if ((dialect == SchedulerDialect.GAUSSDB || dialect == SchedulerDialect.POSTGRESQL) && !quartzTablesPresent) {
            log.warn("[orbit-scheduler] Quartz cluster tables (QRTZ_*) not found. " +
                    "Execute deploy/sql/schema-{} before using jdbc job-store clustering.",
                    dialect == SchedulerDialect.GAUSSDB ? "gaussdb.sql" : "postgresql.sql");
        }
        return dialect;
    }

    /**
     * Quartz 定制：按方言自动注入 driverDelegateClass。
     *
     * <p>用户在 spring.quartz.properties 里显式配置的 org.quartz.jobStore.driverDelegateClass
     * 优先于框架自动注入（先合并用户配置，再 putIfAbsent）；MySQL/H2 等其他数据库
     * 保持既有行为不变（不注入，沿用 StdJDBCDelegate 等用户配置）。
     */
    @Bean
    @ConditionalOnMissingBean(name = "orbitQuartzDialectCustomizer")
    public SchedulerFactoryBeanCustomizer orbitQuartzDialectCustomizer(ObjectProvider<SchedulerDialect> dialectProvider,
                                                                       Environment environment) {
        return factoryBean -> {
            SchedulerDialect dialect = dialectProvider.getIfAvailable();
            if (dialect == null || !dialect.injectsQuartzDelegate()) {
                // 无数据源（纯内存模式）或非 PostgreSQL/GaussDB：不干预
                return;
            }
            // 与 Boot QuartzAutoConfiguration 相同的绑定方式读取用户 quartz properties，避免覆盖用户配置
            Map<String, String> userProps = Binder.get(environment)
                    .bind("spring.quartz.properties", Bindable.mapOf(String.class, String.class))
                    .orElseGet(HashMap::new);
            Properties merged = new Properties();
            merged.putAll(userProps);
            merged.putIfAbsent("org.quartz.jobStore.driverDelegateClass", dialect.getQuartzDelegateClassName());
            factoryBean.setQuartzProperties(merged);
            log.info("[orbit-scheduler] quartz driverDelegateClass set to {} for {} (user explicit config, if any, takes precedence)",
                    merged.getProperty("org.quartz.jobStore.driverDelegateClass"), dialect.getDisplayName());
        };
    }

    // ==================================================================
    // 基础组件（无第三方可选依赖）
    // ==================================================================

    @Bean
    @ConditionalOnMissingBean
    public TaskRegistry orbitTaskRegistry() {
        return new TaskRegistry();
    }

    /** 任务存储：database（需数据源）/ memory / auto（有数据源则 database，否则 memory） */
    @Bean
    @ConditionalOnMissingBean
    public TaskRepository orbitTaskRepository(SchedulerProperties properties,
                                              ObjectProvider<JdbcTemplateHolder> jdbcTemplateProvider,
                                              ObjectProvider<SchedulerDialect> dialectProvider) {
        String type = properties.getStorage().getType();
        JdbcTemplateHolder holder = jdbcTemplateProvider.getIfAvailable();
        SchedulerDialect dialect = dialectProvider.getIfAvailable();
        boolean databaseCapable = holder != null && holder.get() != null;
        if ("database".equalsIgnoreCase(type)) {
            if (!databaseCapable) {
                throw new IllegalStateException("orbit.scheduler.storage.type=database but no DataSource available. " +
                        "Configure a DataSource or switch to storage.type=memory.");
            }
            return new JdbcTaskRepository(holder.get(), dialect);
        }
        if ("memory".equalsIgnoreCase(type)) {
            return new InMemoryTaskRepository();
        }
        // auto
        return databaseCapable ? new JdbcTaskRepository(holder.get(), dialect) : new InMemoryTaskRepository();
    }

    /** 执行日志存储：database / memory / auto */
    @Bean
    @ConditionalOnMissingBean
    public JobLogRepository orbitJobLogRepository(SchedulerProperties properties,
                                                  ObjectProvider<JdbcTemplateHolder> jdbcTemplateProvider,
                                                  ObjectProvider<SchedulerDialect> dialectProvider) {
        String type = properties.getLog().getStorage();
        JdbcTemplateHolder holder = jdbcTemplateProvider.getIfAvailable();
        SchedulerDialect dialect = dialectProvider.getIfAvailable();
        boolean databaseCapable = holder != null && holder.get() != null;
        if ("database".equalsIgnoreCase(type)) {
            if (!databaseCapable) {
                throw new IllegalStateException("orbit.scheduler.log.storage=database but no DataSource available.");
            }
            return new JdbcJobLogRepository(holder.get(), dialect);
        }
        if ("memory".equalsIgnoreCase(type)) {
            return new InMemoryJobLogRepository(properties.getLog().getMemoryCapacity());
        }
        return databaseCapable ? new JdbcJobLogRepository(holder.get(), dialect)
                : new InMemoryJobLogRepository(properties.getLog().getMemoryCapacity());
    }

    /**
     * 服务端点解析。默认使用普通 Kubernetes Service DNS，由 Service/EndpointSlice
     * 负责 Pod 负载均衡；只有显式配置 headless-dns 才直接解析 Pod IP。
     */
    @Bean
    @ConditionalOnMissingBean
    public ServiceEndpointResolver orbitServiceEndpointResolver(SchedulerProperties properties) {
        String mode = properties.getHttpDispatch().getDiscoveryMode();
        if (mode == null || mode.trim().isEmpty()) {
            mode = "service-dns";
        }
        mode = mode.trim().toLowerCase();

        if ("static".equals(mode)) {
            return new StaticServiceEndpointResolver(properties.getHttpDispatch().getStaticEndpoints());
        }
        if ("headless-dns".equals(mode)) {
            return new HeadlessDnsServiceEndpointResolver(properties);
        }
        if (!"service-dns".equals(mode)) {
            throw new IllegalArgumentException("Unsupported orbit.scheduler.http-dispatch.discovery-mode='"
                    + mode + "'. Supported values: service-dns, headless-dns, static");
        }
        return new ServiceDnsServiceEndpointResolver(properties);
    }

    /** 分布式锁：redis / database / none / auto（优先级 Redis → 数据库 → 无锁） */
    @Bean
    @ConditionalOnMissingBean(LockProvider.class)
    public LockProvider orbitLockProvider(SchedulerProperties properties, ApplicationContext applicationContext,
                                          ObjectProvider<JdbcTemplateHolder> jdbcTemplateProvider) {
        String type = properties.getLock().getType() == null ? "auto" : properties.getLock().getType().trim();
        JdbcTemplateHolder holder = jdbcTemplateProvider.getIfAvailable();
        boolean databaseCapable = holder != null && holder.get() != null;

        if ("none".equalsIgnoreCase(type)) {
            return new NoOpLockProvider();
        }

        boolean wantRedis = "redis".equalsIgnoreCase(type) || "auto".equalsIgnoreCase(type);
        if (wantRedis) {
            Object redissonClient = lookupRedissonClient(applicationContext);
            if (redissonClient != null) {
                return RedissonLockFactory.create(redissonClient);
            }
            if ("redis".equalsIgnoreCase(type)) {
                throw new IllegalStateException("orbit.scheduler.lock.type=redis but no RedissonClient available. " +
                        "Set orbit.scheduler.lock.redis.address, or define a RedissonClient bean, " +
                        "or use lock.type=database.");
            }
        }

        boolean wantDatabase = "database".equalsIgnoreCase(type) || "auto".equalsIgnoreCase(type);
        if (wantDatabase) {
            if (databaseCapable) {
                return new JdbcLockProvider(holder.get());
            }
            if ("database".equalsIgnoreCase(type)) {
                throw new IllegalStateException("orbit.scheduler.lock.type=database but no DataSource available. " +
                        "Configure a DataSource or use lock.type=none.");
            }
        }
        return new NoOpLockProvider();
    }

    /** 调度引擎核心 */
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(Scheduler.class)
    public JobManager orbitJobManager(Scheduler scheduler,
                                      TaskRepository taskRepository,
                                      TaskRegistry taskRegistry,
                                      LockProvider lockProvider,
                                      ObjectProvider<HttpDispatchClient> httpDispatchClient,
                                      JobLogRepository jobLogRepository,
                                      SchedulerProperties properties,
                                      ObjectProvider<ObjectMapper> objectMapper) {
        return new JobManager(scheduler, taskRepository, taskRegistry, lockProvider,
                httpDispatchClient.getIfAvailable(), jobLogRepository, properties,
                objectMapper.getIfAvailable());
    }

    /** 启动引导：注解种子落库 + Quartz 触发器对账 */
    @Bean
    @ConditionalOnMissingBean
    public SchedulerBootstrap orbitSchedulerBootstrap(JobManager jobManager) {
        return new SchedulerBootstrap(jobManager);
    }

    // ==================================================================
    // JdbcTemplate 桥接（spring-jdbc 由 scheduler-core 直接依赖，此处无类加载风险）
    // ==================================================================

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(DataSource.class)
    public JdbcTemplateHolder orbitJdbcTemplateHolder(DataSource dataSource) {
        return new JdbcTemplateHolder(new org.springframework.jdbc.core.JdbcTemplate(dataSource));
    }

    /**
     * JdbcTemplate 持有桥：以普通类形式注入 ObjectProvider，
     * 避免泛型签名引用在极端 classpath 下的解析问题。
     */
    public static class JdbcTemplateHolder {

        private final org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

        public JdbcTemplateHolder(org.springframework.jdbc.core.JdbcTemplate jdbcTemplate) {
            this.jdbcTemplate = jdbcTemplate;
        }

        public org.springframework.jdbc.core.JdbcTemplate get() {
            return jdbcTemplate;
        }
    }

    // ==================================================================
    // Redisson（可选依赖，类存在且配置 address 时生效）
    // ==================================================================

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.redisson.Redisson")
    static class RedissonConfiguration {

        @Bean(destroyMethod = "shutdown")
        @ConditionalOnMissingBean
        @ConditionalOnProperty("orbit.scheduler.lock.redis.address")
        public org.redisson.api.RedissonClient orbitRedissonClient(SchedulerProperties properties) {
            SchedulerProperties.Lock.Redis redis = properties.getLock().getRedis();
            String address = redis.getAddress();
            if (address == null || address.trim().isEmpty()) {
                throw new IllegalStateException("orbit.scheduler.lock.redis.address is empty, " +
                        "remove this property if Redis lock is not needed.");
            }
            org.redisson.config.Config config = new org.redisson.config.Config();
            String fullAddress = address.startsWith("redis://") ? address : "redis://" + address;
            org.redisson.config.SingleServerConfig server =
                    config.useSingleServer().setAddress(fullAddress).setDatabase(redis.getDatabase());
            if (redis.getPassword() != null && !redis.getPassword().isEmpty()) {
                server.setPassword(redis.getPassword());
            }
            return org.redisson.Redisson.create(config);
        }
    }

    /**
     * Redisson 锁工厂：独立类确保仅在 Redisson 存在时被加载。
     */
    static class RedissonLockFactory {

        static LockProvider create(Object redissonClient) {
            return new RedissonLockProvider((org.redisson.api.RedissonClient) redissonClient);
        }
    }

    private static Object lookupRedissonClient(ApplicationContext applicationContext) {
        try {
            Class<?> clazz = ClassUtils.forName("org.redisson.api.RedissonClient",
                    applicationContext.getClassLoader());
            return applicationContext.getBeanProvider(clazz).getIfAvailable();
        } catch (ClassNotFoundException e) {
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    // ==================================================================
    // HTTP 调度客户端（需 spring-web）
    // ==================================================================

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.web.client.RestTemplate")
    static class HttpDispatchConfiguration {

        @Bean
        @ConditionalOnMissingBean
        public HttpDispatchClient orbitHttpDispatchClient(SchedulerProperties properties,
                                                          ServiceEndpointResolver endpointResolver) {
            return new HttpDispatchClient(properties, endpointResolver);
        }
    }

    // ==================================================================
    // Web 管理 API（Servlet 环境）
    // ==================================================================

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.web.bind.annotation.RestController")
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    static class SchedulerWebConfiguration {

        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "orbit.scheduler", name = "api-enabled", havingValue = "true", matchIfMissing = true)
        public JobController orbitJobController(JobManager jobManager, TaskRegistry taskRegistry,
                                                ObjectProvider<HttpDispatchClient> httpDispatchClient) {
            return new JobController(jobManager, taskRegistry, httpDispatchClient.getIfAvailable());
        }

        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "orbit.scheduler.http-dispatch", name = "enabled", havingValue = "true", matchIfMissing = true)
        public HttpDispatchController orbitHttpDispatchController(TaskRegistry taskRegistry,
                                                                  SchedulerProperties properties) {
            return new HttpDispatchController(taskRegistry, properties);
        }
    }

    // ==================================================================
    // Actuator 健康检查（可选依赖）
    // ==================================================================

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = "org.springframework.boot.actuate.health.HealthIndicator")
    static class SchedulerHealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "orbitSchedulerHealthIndicator")
        public OrbitSchedulerHealthIndicator orbitSchedulerHealthIndicator(Scheduler scheduler,
                                                                           JobManager jobManager,
                                                                           ObjectProvider<HttpDispatchClient> httpDispatchClient) {
            return new OrbitSchedulerHealthIndicator(scheduler, jobManager, httpDispatchClient.getIfAvailable());
        }
    }
}

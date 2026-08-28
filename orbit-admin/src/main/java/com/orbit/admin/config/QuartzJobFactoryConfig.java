package com.orbit.admin.config;

import org.springframework.boot.autoconfigure.quartz.SchedulerFactoryBeanCustomizer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

/**
 * Quartz 与 Spring IoC 容器集成配置类。
 * <p>默认情况下 Quartz 实例化的 Job 并不受 Spring 容器托管，无法直接使用 {@code @Autowired} 注入 Spring Bean。
 * 本配置通过自定义 {@link SpringBeanJobFactory}，使 Quartz 任务类（如 {@link com.orbit.admin.quartz.OrbitQuartzJob}）
 * 能够自动注入 {@link com.orbit.admin.service.JobService} 等 Spring 服务组件。
 */
@Configuration
public class QuartzJobFactoryConfig {

    /**
     * 定制 Quartz 的 SchedulerFactoryBean，配置自定义的 JobFactory
     *
     * @param jobFactory 支持 Spring 依赖注入的 JobFactory
     * @return 定制器
     */
    @Bean
    public SchedulerFactoryBeanCustomizer orbitJobFactoryCustomizer(SpringBeanJobFactory jobFactory) {
        return factoryBean -> factoryBean.setJobFactory(jobFactory);
    }

    /**
     * 构建具备 Spring 容器上下文感知的 JobFactory 实例
     *
     * @param ctx Spring 应用上下文
     * @return SpringBeanJobFactory 实例
     */
    @Bean
    public SpringBeanJobFactory springBeanJobFactory(ApplicationContext ctx) {
        SpringBeanJobFactory factory = new SpringBeanJobFactory();
        factory.setApplicationContext(ctx);
        return factory;
    }
}

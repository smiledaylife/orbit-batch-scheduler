package com.orbit.admin.config;

import org.springframework.boot.autoconfigure.quartz.SchedulerFactoryBeanCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.quartz.SpringBeanJobFactory;

/**
 * 让 Quartz Job 支持 Spring @Autowired（OrbitQuartzJob 注入 JobService）。
 */
@Configuration
public class QuartzJobFactoryConfig {

    @Bean
    public SchedulerFactoryBeanCustomizer orbitJobFactoryCustomizer(SpringBeanJobFactory jobFactory) {
        return factoryBean -> factoryBean.setJobFactory(jobFactory);
    }

    @Bean
    public SpringBeanJobFactory springBeanJobFactory(org.springframework.context.ApplicationContext ctx) {
        SpringBeanJobFactory factory = new SpringBeanJobFactory();
        factory.setApplicationContext(ctx);
        return factory;
    }
}

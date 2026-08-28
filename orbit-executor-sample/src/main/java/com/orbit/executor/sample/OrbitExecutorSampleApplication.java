package com.orbit.executor.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Orbit 执行器示例业务服务启动类。
 * <p>通过引入 {@code orbit-executor} 依赖，即可在当前 Spring Boot 业务应用中编写 {@code @OrbitJob} 定时任务。
 */
@SpringBootApplication
public class OrbitExecutorSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrbitExecutorSampleApplication.class, args);
    }
}

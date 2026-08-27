package com.orbit.scheduler.sample;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Orbit Batch Scheduler 演示应用。
 *
 * <p>三种运行模式：
 * <ul>
 *   <li>standalone（默认）：Quartz 内存 + 任务内存存储 + 无锁，零依赖直接跑</li>
 *   <li>local：H2 内存数据库 + JDBC 存储/锁 + HTTP 自调度演示</li>
 *   <li>mysql：MySQL + Quartz JDBC 集群 + Redis/DB 锁（生产形态，K8s 部署用此模式）</li>
 * </ul>
 *
 * @author orbit
 */
@SpringBootApplication
public class OrbitSchedulerSampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrbitSchedulerSampleApplication.class, args);
    }
}

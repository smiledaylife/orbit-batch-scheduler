package com.orbit.admin;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Orbit 分布式调度中心（Admin）应用启动入口。
 * <p>核心职责：
 * <ul>
 *   <li>任务元数据 CRUD 与生命周期维护；</li>
 *   <li>整合 Quartz 提供高精度 Cron 定时触发能力；</li>
 *   <li>维护执行器心跳与动态在线注册表，支持超时剔除；</li>
 *   <li>按照路由策略（轮询/随机/首节点）向执行器进行 HTTP 任务派发与全链路日志记录。</li>
 * </ul>
 */
@SpringBootApplication
@EnableScheduling
public class OrbitAdminApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrbitAdminApplication.class, args);
    }
}

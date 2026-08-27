package com.orbit.scheduler.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 批量任务注解：标记 Spring Bean 中的方法为可调度批量任务。
 *
 * <p>启动时由 {@code TaskRegistry} 自动扫描注册，并按存储模式
 * （数据库 t_job_config / 本地内存）落库，随后注册到 Quartz 触发器。
 *
 * <p>支持的方法签名（参数可选、可组合）：
 * <pre>
 * &#64;BatchTask(name = "demo", cron = "0 0/5 * * * ?")
 * public void demo() { ... }
 *
 * &#64;BatchTask(name = "demo2", cron = "0 0 2 * * ?")
 * public String demo2(TaskContext ctx) { ... }   // 返回值会记录进执行日志
 *
 * &#64;BatchTask(name = "demo3", cron = "0 0 3 * * ?", dispatchType = DispatchType.HTTP)
 * public String demo3(Map&lt;String, Object&gt; params) { ... }
 *
 * // 跨服务：目标业务服务无需接入本框架，只需暴露 HTTP 接口
 * &#64;BatchTask(name = "settleOrders", cron = "0 0 2 * * ?",
 *            dispatchType = DispatchType.REMOTE,
 *            httpService = "order-service", httpPath = "/api/batch/settle")
 * public void settleOrdersPlaceholder() { }
 * </pre>
 *
 * <p>当 {@code dispatchType = REMOTE} 时，本地方法体不会被调用（可留空），
 * 框架按 httpService + httpPath 调用外部微服务。WORKFLOW 同理，编排定义放在数据库
 * {@code workflow_def} 或运行时 params。
 *
 * @author orbit
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface BatchTask {

    /**
     * 任务唯一名称（框架约定全局唯一），建议使用小驼峰英文。
     */
    String name();

    /**
     * 任务描述。
     */
    String description() default "";

    /**
     * 默认 Cron 表达式（Quartz 格式，6 或 7 位）。
     * 留空表示默认不注册定时触发器，仅支持手工/API 触发。
     * 数据库模式下可被 t_job_config 中同名任务的 cron 覆盖。
     */
    String cron() default "";

    /**
     * 调度方式：LOCAL / HTTP（同框架） / REMOTE（外部服务） / WORKFLOW（编排）。
     */
    DispatchType dispatchType() default DispatchType.LOCAL;

    /**
     * REMOTE/HTTP 目标服务名（对应 orbit.scheduler.remote-services 注册名或 K8s Service）。
     * 空则 HTTP 派发取全局 http-dispatch.service-name。
     */
    String httpService() default "";

    /**
     * REMOTE/HTTP 目标路径。REMOTE 时为业务接口路径；HTTP 时默认 /api/scheduler/execute。
     */
    String httpPath() default "";

    /**
     * REMOTE 的 HTTP 方法，默认 POST。
     */
    String httpMethod() default "POST";

    /**
     * 是否在每次启动时用注解中的 cron/description 覆盖数据库中的同名配置。
     * 默认 false（数据库配置优先，便于运维侧改 cron 不被代码回写）。
     */
    boolean overwrite() default false;
}

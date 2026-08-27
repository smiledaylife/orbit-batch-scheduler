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
 * </pre>
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
     * 调度方式：本地进程内执行，或经 Service 远程 HTTP 派发。
     */
    DispatchType dispatchType() default DispatchType.LOCAL;

    /**
     * 是否在每次启动时用注解中的 cron/description 覆盖数据库中的同名配置。
     * 默认 false（数据库配置优先，便于运维侧改 cron 不被代码回写）。
     */
    boolean overwrite() default false;
}

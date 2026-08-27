package com.orbit.executor.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记业务方法为可被调度中心触发的 JobHandler（类似 XXL-JOB 的 @XxlJob）。
 *
 * <pre>
 * &#64;Component
 * public class DemoJobs {
 *     &#64;OrbitJob("dailyReport")
 *     public String dailyReport(JobContext ctx) {
 *         return "ok";
 *     }
 * }
 * </pre>
 *
 * <p>方法参数可选：无参 / {@code JobContext} / {@code Map}。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OrbitJob {

    /** Handler 名称，调度中心任务配置的 handler 字段与此对应 */
    String value();
}

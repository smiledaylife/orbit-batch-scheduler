package com.orbit.executor.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记业务方法为可被调度中心触发调用的任务处理函数（JobHandler）。
 * <p>设计思想对齐 XXL-JOB 的 {@code @XxlJob}，应用启动时由框架自动扫描并注册。
 *
 * <p><b>使用示例：</b>
 * <pre>{@code
 * @Component
 * public class DemoJobs {
 *
 *     // 方式 1：接收完整的 JobContext 上下文对象
 *     @OrbitJob("dailyReport")
 *     public String dailyReport(JobContext ctx) {
 *         String date = ctx.getString("bizDate");
 *         return "ok";
 *     }
 *
 *     // 方式 2：直接接收 Map 类型的参数字典
 *     @OrbitJob("dataSync")
 *     public void dataSync(Map<String, Object> params) {
 *         // 业务逻辑
 *     }
 *
 *     // 方式 3：无参方法
 *     @OrbitJob("manualClean")
 *     public void manualClean() {
 *         // 业务清理逻辑
 *     }
 * }
 * }</pre>
 *
 * <p>方法参数支持：无参 / {@code JobContext} / {@code Map<String, Object>}。
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface OrbitJob {

    /**
     * JobHandler 的全局唯一名称。
     * <p>调度中心在配置任务时，任务属性中的 {@code handler} 字段与此名称严格对应。
     *
     * @return Handler 名称
     */
    String value();
}

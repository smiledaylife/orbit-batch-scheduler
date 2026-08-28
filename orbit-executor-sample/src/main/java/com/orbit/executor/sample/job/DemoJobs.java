package com.orbit.executor.sample.job;

import com.orbit.executor.JobContext;
import com.orbit.executor.annotation.OrbitJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 示例业务任务处理类（JobHandler 集合）。
 * <p>业务方在执行器工程中定义具体的处理函数，使用 {@link OrbitJob} 声明唯一的 Handler 名称，
 * 由调度中心通过 Cron 表达式或手动触发执行。
 *
 * <p>支持的方法入参类型包括：
 * <ul>
 *   <li>{@link JobContext}：推荐方式，包含任务 ID、任务名、日志 ID 及业务参数</li>
 *   <li>{@link Map}：直接获取入参键值对</li>
 *   <li>无参：适用于不需要参数的简单批处理或清理任务</li>
 * </ul>
 */
@Component
public class DemoJobs {

    private static final Logger log = LoggerFactory.getLogger(DemoJobs.class);

    /**
     * 示例 1：日报统计任务（接收完整的 JobContext 上下文对象）。
     *
     * @param ctx 任务执行上下文
     * @return 执行结果描述（将返回给调度中心并保存在调度日志中）
     * @throws InterruptedException 模拟业务耗时中断
     */
    @OrbitJob("dailyReport")
    public String dailyReport(JobContext ctx) throws InterruptedException {
        log.info("[demo] dailyReport start logId={} params={}", ctx.getLogId(), ctx.getParams());
        // 模拟业务处理耗时 500ms ~ 1000ms
        Thread.sleep(500L + ThreadLocalRandom.current().nextInt(500));
        // 从上下文中获取业务参数，支持默认值回退
        String bizDate = ctx.getString("bizDate", "yesterday");
        return "report done, bizDate=" + bizDate + ", orders=" + (8000 + ThreadLocalRandom.current().nextInt(2000));
    }

    /**
     * 示例 2：数据同步任务（接收 Map 类型参数）。
     *
     * @param params 动态入参字典
     * @return 执行结果
     * @throws InterruptedException 模拟业务耗时中断
     */
    @OrbitJob("dataSync")
    public String dataSync(Map<String, Object> params) throws InterruptedException {
        log.info("[demo] dataSync params={}", params);
        Thread.sleep(300);
        return "sync ok";
    }

    /**
     * 示例 3：无参任务（用于常规的定期日志/缓存清理等操作）。
     */
    @OrbitJob("manualClean")
    public void manualClean() {
        log.info("[demo] manualClean executed successfully");
    }
}

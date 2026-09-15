package com.orbit.executor.sample.job;

import com.orbit.executor.JobContext;
import com.orbit.executor.annotation.OrbitJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 示例业务任务处理类（JobHandler 集合）。
 * 业务方在执行器工程中定义具体的处理函数，使用 {@link OrbitJob} 声明唯一的 Handler 名称，
 * 由调度中心通过 Cron 表达式或手动触发执行。
 *
 * 支持的方法入参类型包括：
 *
 *   - {@link JobContext}：推荐方式，包含任务 ID、任务名、日志 ID 及业务参数
 *   - {@link Map}：直接获取入参键值对
 *   - 无参：适用于不需要参数的简单批处理或清理任务
 *
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

    /**
     * 示例 4：可失败任务（演示执行级失败重试）。
     *
     * 同一 logId 的首次执行必定抛异常，重试轮成功 —— 用于本地验证
     * retryCount / retryIntervalSeconds 与 JobContext.attempt 的完整链路：
     * 中间失败不回传，调度日志只看到带 attempt 标注的最终 SUCCESS。
     *
     * @param ctx 任务执行上下文（业务侧据此区分首轮与重试轮次）
     * @return 执行结果
     */
    @OrbitJob("flaky")
    public String flaky(JobContext ctx) {
        if (FAILED_ONCE.add(ctx.getLogId())) {
            log.warn("[demo] flaky first attempt of logId={} fails on purpose", ctx.getLogId());
            throw new IllegalStateException("flaky first attempt always fails");
        }
        return "ok on attempt " + ctx.getAttempt();
    }

    /**
     * 示例 5：永远失败的任务（演示重试耗尽后的失败告警）。
     *
     * 重试全部耗尽后回传最终 FAILED，调度中心投递 EXECUTION_FAILED 告警事件，
     * 未接自定义告警处理器时可在 admin 日志看到 [orbit-alert] WARN 行。
     */
    @OrbitJob("alwaysFail")
    public String alwaysFail() {
        throw new IllegalStateException("intentional failure for retry/alert demo");
    }

    /** 已记录「首败」的 logId 集合（flaky 用，仅演示用途，进程内生效） */
    private static final Set<String> FAILED_ONCE = ConcurrentHashMap.newKeySet();
}

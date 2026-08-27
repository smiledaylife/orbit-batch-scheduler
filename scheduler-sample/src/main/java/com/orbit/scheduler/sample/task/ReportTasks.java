package com.orbit.scheduler.sample.task;

import com.orbit.scheduler.annotation.BatchTask;
import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.core.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 报表类批量任务示例：演示本地调度 + 参数读取 + 返回值记录。
 *
 * @author orbit
 */
@Component
public class ReportTasks {

    private static final Logger log = LoggerFactory.getLogger(ReportTasks.class);

    /**
     * 每日订单报表：凌晨 2 点执行，演示 TaskContext 读取参数。
     */
    @BatchTask(name = "dailyOrderReport", description = "生成前一日订单汇总报表", cron = "0 0 2 * * ?")
    public String dailyOrderReport(TaskContext ctx) {
        log.info("[demo] dailyOrderReport 开始, params={}, requestId={}", ctx.getParams(), ctx.getRequestId());
        simulateWork(1500);
        int orders = 8_000 + ThreadLocalRandom.current().nextInt(2_000);
        String bizDate = ctx.getString("bizDate", "yesterday");
        String result = "订单报表生成完成: bizDate=" + bizDate + ", orders=" + orders;
        log.info("[demo] dailyOrderReport 完成: {}", result);
        return result;
    }

    /**
     * 缓存预热：每 2 分钟，演示高频短任务 + Map 参数签名。
     */
    @BatchTask(name = "cacheWarmUp", description = "热门数据缓存预热", cron = "0 */2 * * * ?")
    public String cacheWarmUp(java.util.Map<String, Object> params) {
        int keys = 100 + ThreadLocalRandom.current().nextInt(50);
        simulateWork(300);
        return "缓存预热完成, 预热 key 数=" + keys + ", 参数=" + params;
    }

    /**
     * 积分结算：凌晨 1:30，演示无参数签名。
     */
    @BatchTask(name = "pointsSettle", description = "用户积分日结算", cron = "0 30 1 * * ?")
    public void pointsSettle() {
        log.info("[demo] pointsSettle 开始");
        simulateWork(1000);
        log.info("[demo] pointsSettle 完成");
    }

    /**
     * 手动归档：无 cron，仅通过 REST API 手动触发。
     */
    @BatchTask(name = "manualArchive", description = "历史数据归档（手动触发示例）", cron = "")
    public String manualArchive(TaskContext ctx) {
        simulateWork(800);
        return "历史数据归档完成, 触发节点=" + ctx.getDispatchNode() + ", 执行节点=" + ctx.getWorkerNode();
    }

    private static void simulateWork(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

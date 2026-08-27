package com.orbit.executor.sample.job;

import com.orbit.executor.JobContext;
import com.orbit.executor.annotation.OrbitJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 示例 JobHandler：业务写在执行器侧，由调度中心 Cron 触发。
 */
@Component
public class DemoJobs {

    private static final Logger log = LoggerFactory.getLogger(DemoJobs.class);

    @OrbitJob("dailyReport")
    public String dailyReport(JobContext ctx) throws InterruptedException {
        log.info("[demo] dailyReport start logId={} params={}", ctx.getLogId(), ctx.getParams());
        Thread.sleep(500L + ThreadLocalRandom.current().nextInt(500));
        String bizDate = ctx.getString("bizDate", "yesterday");
        return "report done, bizDate=" + bizDate + ", orders=" + (8000 + ThreadLocalRandom.current().nextInt(2000));
    }

    @OrbitJob("dataSync")
    public String dataSync(Map<String, Object> params) throws InterruptedException {
        log.info("[demo] dataSync params={}", params);
        Thread.sleep(300);
        return "sync ok";
    }

    @OrbitJob("manualClean")
    public void manualClean() {
        log.info("[demo] manualClean");
    }
}

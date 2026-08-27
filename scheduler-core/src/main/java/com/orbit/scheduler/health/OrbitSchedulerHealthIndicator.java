package com.orbit.scheduler.health;

import com.orbit.scheduler.core.JobManager;
import com.orbit.scheduler.http.HttpDispatchClient;
import org.quartz.Scheduler;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 调度器健康指标：暴露调度器运行状态、节点 ID、存储/锁类型、集群端点数等。
 *
 * @author orbit
 */
public class OrbitSchedulerHealthIndicator extends AbstractHealthIndicator {

    private final Scheduler scheduler;
    private final JobManager jobManager;
    private final HttpDispatchClient httpDispatchClient;

    public OrbitSchedulerHealthIndicator(Scheduler scheduler, JobManager jobManager,
                                         HttpDispatchClient httpDispatchClient) {
        this.scheduler = scheduler;
        this.jobManager = jobManager;
        this.httpDispatchClient = httpDispatchClient;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        Map<String, Object> details = new LinkedHashMap<String, Object>();
        boolean started;
        try {
            started = scheduler != null && scheduler.isStarted();
        } catch (Exception e) {
            started = false;
            details.put("schedulerError", e.getMessage());
        }
        details.put("nodeId", jobManager.getNodeId());
        Map<String, Object> overview = jobManager.overview();
        details.putAll(overview);
        if (httpDispatchClient != null) {
            try {
                details.put("endpoints", httpDispatchClient.listEndpoints(null).size());
            } catch (Exception e) {
                details.put("endpoints", -1);
            }
        }
        if (started) {
            builder.up().withDetails(details);
        } else {
            builder.down().withDetails(details);
        }
    }
}

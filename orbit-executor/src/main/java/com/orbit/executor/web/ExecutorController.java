package com.orbit.executor.web;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.ExecutorBootstrap;
import com.orbit.executor.ExecutorProperties;
import com.orbit.executor.JobContext;
import com.orbit.executor.JobHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 执行器接收调度中心触发的入口。
 */
@RestController
@RequestMapping("/orbit/executor")
public class ExecutorController {

    private static final Logger log = LoggerFactory.getLogger(ExecutorController.class);
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    private final JobHandlerRegistry registry;
    private final ExecutorProperties properties;
    private final ExecutorBootstrap bootstrap;

    public ExecutorController(JobHandlerRegistry registry, ExecutorProperties properties,
                              ExecutorBootstrap bootstrap) {
        this.registry = registry;
        this.properties = properties;
        this.bootstrap = bootstrap;
    }

    @PostMapping("/run")
    public TriggerResult run(@RequestBody TriggerRequest request,
                             @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        checkToken(token, request.getAccessToken());
        String handler = request.getHandler();
        String node = bootstrap.getResolvedNodeId() == null ? "executor" : bootstrap.getResolvedNodeId();
        long start = System.currentTimeMillis();
        if (handler == null || handler.trim().isEmpty()) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, 0, "handler required");
        }
        if (!registry.has(handler)) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, 0,
                    "handler not found on this executor: " + handler);
        }
        try {
            JobContext ctx = new JobContext(request.getJobId(), request.getJobName(), handler,
                    request.getLogId(), request.getParams());
            Object ret = registry.invoke(handler, ctx);
            long cost = System.currentTimeMillis() - start;
            String msg = ret == null ? "OK" : String.valueOf(ret);
            log.info("[orbit-executor] run handler={} job={} logId={} {}ms",
                    handler, request.getJobName(), request.getLogId(), cost);
            return TriggerResult.ok(request.getLogId(), request.getJobId(), node, cost, msg);
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            log.error("[orbit-executor] handler '{}' failed", handler, e);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, cost,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    @GetMapping("/handlers")
    public Map<String, Object> handlers() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("appName", properties.getAppName());
        m.put("address", bootstrap.getResolvedAddress());
        m.put("nodeId", bootstrap.getResolvedNodeId());
        m.put("handlers", registry.listNames());
        return m;
    }

    private void checkToken(String header, String body) {
        String expect = properties.getAccessToken();
        if (expect == null || expect.isEmpty()) {
            return;
        }
        String actual = header != null && !header.isEmpty() ? header : body;
        if (!expect.equals(actual)) {
            throw new IllegalArgumentException("invalid access token");
        }
    }
}

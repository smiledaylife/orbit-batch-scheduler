package com.orbit.executor.web;

import com.orbit.core.model.ApiResult;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.core.protocol.OrbitProtocol;
import com.orbit.executor.bootstrap.ExecutorBootstrap;
import com.orbit.executor.client.ExecutionIdempotency;
import com.orbit.executor.config.ExecutorProperties;
import com.orbit.executor.handler.JobExecutionService;
import com.orbit.executor.handler.JobHandlerRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/** 执行器对外 HTTP API。 */
@RestController
@RequestMapping("/orbit/executor")
public class ExecutorController {
    private static final Logger log = LoggerFactory.getLogger(ExecutorController.class);
    private final JobHandlerRegistry registry;
    private final ExecutorProperties properties;
    private final ExecutorBootstrap bootstrap;
    private final JobExecutionService executionService;
    private final ExecutionIdempotency idempotency;

    public ExecutorController(JobHandlerRegistry registry, ExecutorProperties properties,
                              ExecutorBootstrap bootstrap, JobExecutionService executionService,
                              ExecutionIdempotency idempotency) {
        this.registry = registry;
        this.properties = properties;
        this.bootstrap = bootstrap;
        this.executionService = executionService;
        this.idempotency = idempotency;
    }

    @PostMapping("/run")
    public TriggerResult run(@RequestBody TriggerRequest request,
                             @RequestHeader(value = OrbitProtocol.TOKEN_HEADER, required = false) String token) {
        checkToken(token);
        String handler = request.getHandler();
        String node = bootstrap.getResolvedNodeId() == null ? "executor" : bootstrap.getResolvedNodeId();
        if (handler == null || handler.trim().isEmpty()) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, 0, "handler required");
        }
        if (!registry.has(handler)) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, 0,
                    "handler not found on this executor: " + handler);
        }

        // logId 是一次调度执行的幂等主键：HTTP 超时后重试不会再次进入线程池。
        boolean first = idempotency.tryReserve(request.getLogId());
        if (!first) {
            return TriggerResult.accepted(request.getLogId(), request.getJobId(), node,
                    "duplicate request ignored: logId already accepted");
        }

        TriggerResult result;
        try {
            result = executionService.submit(request, registry, node);
        } catch (RuntimeException e) {
            idempotency.release(request.getLogId());
            throw e;
        }
        // 线程池饱和等同步拒绝意味着任务没有执行，必须释放幂等占位，允许后续重试。
        if (!result.isAccepted()) {
            idempotency.release(request.getLogId());
        }
        return result;
    }

    @GetMapping("/handlers")
    public Map<String, Object> handlers(
            @RequestHeader(value = OrbitProtocol.TOKEN_HEADER, required = false) String token) {
        checkToken(token);
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("appName", properties.getAppName());
        m.put("address", bootstrap.getResolvedAddress());
        m.put("nodeId", bootstrap.getResolvedNodeId());
        m.put("handlers", registry.listNames());
        return m;
    }

    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ApiResult<Void> forbidden(IllegalArgumentException e) {
        log.warn("[orbit-executor] rejected unauthorized request: {}", e.getMessage());
        return ApiResult.fail(403, e.getMessage());
    }

    private void checkToken(String header) {
        String expect = properties.getAccessToken();
        if (expect == null || expect.isEmpty()) {
            return;
        }
        if (!OrbitProtocol.constantTimeEquals(expect, header)) {
            throw new IllegalArgumentException("invalid access token");
        }
    }
}

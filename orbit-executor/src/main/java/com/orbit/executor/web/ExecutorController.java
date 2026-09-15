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

/**
 * 执行器对外 HTTP API。
 *
 * 负责接收 Admin 的任务触发请求、查询已注册 Handler，并在入口处完成访问令牌校验和
 * logId 幂等保护。真正的任务执行由 {@link JobExecutionService} 异步完成。
 */
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

    /**
     * 接收一次任务执行请求。
     *
     * 该接口采用「受理即返回」语义：HTTP 返回 accepted=true 只表示任务已进入执行器，
     * 最终 SUCCESS/FAILED 必须通过 callback 回传 Admin。logId 是一次调度执行的全局幂等主键，
     * Admin 因 HTTP 超时而重试时不会再次进入执行线程池。
     */
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
            // submit 在同步阶段失败时没有真正执行任务，因此释放幂等占位允许后续重试。
            idempotency.release(request.getLogId());
            throw e;
        }
        // 线程池饱和等同步拒绝意味着任务没有执行，必须释放幂等占位，允许后续重试。
        if (!result.isAccepted()) {
            idempotency.release(request.getLogId());
        }
        return result;
    }

    /**
     * 查询当前 Executor 注册的 Handler 和节点信息。
     * Admin 可用该接口辅助故障排查和确认任务是否部署到目标 Executor。
     */
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

    /** 把认证失败转换为统一的 403 API 响应，并记录安全审计日志。 */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ApiResult<Void> forbidden(IllegalArgumentException e) {
        log.warn("[orbit-executor] rejected unauthorized request: {}", e.getMessage());
        return ApiResult.fail(403, e.getMessage());
    }

    /**
     * 校验 Admin -> Executor 的共享令牌。
     * {@link OrbitProtocol#constantTimeEquals(String, String)} 用于降低令牌比较的时序侧信道风险。
     */
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

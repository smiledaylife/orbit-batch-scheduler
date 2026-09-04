package com.orbit.executor.web;

import com.orbit.core.model.ApiResult;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.JobContext;
import com.orbit.executor.bootstrap.ExecutorBootstrap;
import com.orbit.executor.config.ExecutorProperties;
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
 * 执行器对外暴露的 HTTP RESTful API 控制器。
 * 核心职责：
 * 
 *   - 接收调度中心派发的任务触发请求（{@code POST /orbit/executor/run}）；
 *   - 校验安全访问令牌（{@code X-Orbit-Token}）；
 *   - 定位并执行本地对应的 JobHandler，记录耗时并返回执行结果；
 *   - 提供当前节点在线信息与支持的 Handler 查询端点（{@code GET /orbit/executor/handlers}）。
 * 
 */
@RestController
@RequestMapping("/orbit/executor")
public class ExecutorController {

    private static final Logger log = LoggerFactory.getLogger(ExecutorController.class);

    /**
     * 安全鉴权请求头名称
     */
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    private final JobHandlerRegistry registry;
    private final ExecutorProperties properties;
    private final ExecutorBootstrap bootstrap;

    /**
     * 构造控制器，注入核心依赖组件
     *
     * @param registry   JobHandler 注册表
     * @param properties 执行器配置属性
     * @param bootstrap  执行器引导器
     */
    public ExecutorController(JobHandlerRegistry registry, ExecutorProperties properties,
                              ExecutorBootstrap bootstrap) {
        this.registry = registry;
        this.properties = properties;
        this.bootstrap = bootstrap;
    }

    /**
     * 接收调度中心的任务触发执行请求。
     *
     * @param request 任务触发参数实体
     * @param token   HTTP Header 中的鉴权令牌
     * @return 任务执行结果
     */
    @PostMapping("/run")
    public TriggerResult run(@RequestBody TriggerRequest request,
                             @RequestHeader(value = TOKEN_HEADER, required = false) String token) {
        // 1. 安全访问令牌校验
        checkToken(token, request.getAccessToken());

        String handler = request.getHandler();
        String node = bootstrap.getResolvedNodeId() == null ? "executor" : bootstrap.getResolvedNodeId();
        long start = System.currentTimeMillis();

        // 2. 基础参数校验：Handler 名称必填
        if (handler == null || handler.trim().isEmpty()) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, 0, "handler required");
        }

        // 3. 检查当前执行器是否已注册该 Handler
        if (!registry.has(handler)) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, 0,
                    "handler not found on this executor: " + handler);
        }

        // 4. 构建任务执行上下文并执行具体业务函数
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
            // 捕获业务异常并封装为失败结果返回
            long cost = System.currentTimeMillis() - start;
            log.error("[orbit-executor] handler '{}' failed", handler, e);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), node, cost,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    /**
     * 查询当前执行器节点的信息以及已注册的所有 JobHandler 列表。
     *
     * @return 执行器概况与 Handler 列表数据
     */
    @GetMapping("/handlers")
    public Map<String, Object> handlers() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("appName", properties.getAppName());
        m.put("address", bootstrap.getResolvedAddress());
        m.put("nodeId", bootstrap.getResolvedNodeId());
        m.put("handlers", registry.listNames());
        return m;
    }

    /**
     * 鉴权失败（令牌不匹配）返回 403，避免落入 Spring 默认 500 白页。
     *
     * @param e 非法参数异常
     * @return 标准失败响应
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    public ApiResult<Void> forbidden(IllegalArgumentException e) {
        return ApiResult.fail(403, e.getMessage());
    }

    /**
     * 双向安全令牌校验逻辑。
     * 若本地未配置 accessToken，则跳过校验；若配置了 accessToken，优先比对 Header 中的 Token，其次比对 Body 中的 Token。
     *
     * @param header Header 携带的令牌
     * @param body   Body 携带的令牌
     */
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

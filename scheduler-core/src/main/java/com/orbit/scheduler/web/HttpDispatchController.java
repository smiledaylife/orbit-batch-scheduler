package com.orbit.scheduler.web;

import com.orbit.scheduler.core.TaskContext;
import com.orbit.scheduler.core.TaskRegistry;
import com.orbit.scheduler.http.HttpDispatchClient;
import com.orbit.scheduler.model.HttpDispatchRequest;
import com.orbit.scheduler.model.HttpDispatchResponse;
import com.orbit.scheduler.support.NodeIdProvider;
import com.orbit.scheduler.support.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Date;

/**
 * 远程执行端点：接收来自其它调度节点的 HTTP 派发请求（经 K8s Service 路由），
 * 在本节点查找执行器并执行。
 *
 * <p>注意：分布式锁由发起调度的节点持有，本端点不再重复加锁。
 *
 * @author orbit
 */
@RestController
public class HttpDispatchController {

    private static final Logger log = LoggerFactory.getLogger(HttpDispatchController.class);

    private final TaskRegistry taskRegistry;
    private final SchedulerProperties properties;
    private final String nodeId;

    public HttpDispatchController(TaskRegistry taskRegistry, SchedulerProperties properties) {
        this.taskRegistry = taskRegistry;
        this.properties = properties;
        this.nodeId = NodeIdProvider.resolve(properties.getNodeId());
    }

    @PostMapping("${orbit.scheduler.http-dispatch.path:/api/scheduler/execute}")
    public HttpDispatchResponse execute(@RequestBody HttpDispatchRequest request,
                                        @RequestHeader(value = HttpDispatchClient.TOKEN_HEADER, required = false) String token) {
        verifyToken(token);
        String taskName = request.getTaskName();
        if (taskName == null || taskName.trim().isEmpty()) {
            return HttpDispatchResponse.failure(request.getRequestId(), nodeId, "taskName is required");
        }
        if (!taskRegistry.hasTask(taskName)) {
            // 标记 taskMissing，调度端将自动故障转移到下一端点
            return HttpDispatchResponse.taskMissing(request.getRequestId(), nodeId,
                    "task '" + taskName + "' has no executor on node " + nodeId);
        }
        long start = System.currentTimeMillis();
        try {
            TaskContext context = new TaskContext(
                    taskName, properties.getGroup(), request.getParams(),
                    new Date(request.getFireTime() <= 0 ? start : request.getFireTime()),
                    request.getDispatchNode(), nodeId, request.getRequestId());
            Object ret = taskRegistry.execute(taskName, context);
            long cost = System.currentTimeMillis() - start;
            String message = ret == null ? "OK" : String.valueOf(ret);
            log.info("[orbit-scheduler] remote dispatch executed task '{}' from node {} in {}ms",
                    taskName, request.getDispatchNode(), cost);
            return HttpDispatchResponse.success(request.getRequestId(), nodeId, cost, message);
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            log.error("[orbit-scheduler] remote dispatch failed for task '{}'", taskName, e);
            return HttpDispatchResponse.failure(request.getRequestId(), nodeId,
                    (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
    }

    private void verifyToken(String token) {
        String secret = properties.getHttpDispatch().getSecret();
        if (secret != null && !secret.isEmpty() && !secret.equals(token)) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "invalid scheduler dispatch token");
        }
    }
}

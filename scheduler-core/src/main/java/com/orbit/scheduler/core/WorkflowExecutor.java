package com.orbit.scheduler.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.http.HttpDispatchClient;
import com.orbit.scheduler.http.RemoteServiceClient;
import com.orbit.scheduler.model.HttpDispatchResponse;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.WorkflowDefinition;
import com.orbit.scheduler.model.WorkflowStepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 工作流编排执行器：按 {@link WorkflowDefinition} 串行/并行调度多个 LOCAL / HTTP / REMOTE 步骤，
 * 实现"调度中心编排、业务在其他服务执行"的批量流水线。
 *
 * @author orbit
 */
public class WorkflowExecutor {

    private static final Logger log = LoggerFactory.getLogger(WorkflowExecutor.class);

    private final TaskRegistry taskRegistry;
    private final HttpDispatchClient httpDispatchClient;
    private final RemoteServiceClient remoteServiceClient;
    private final ObjectMapper objectMapper;
    private final String nodeId;

    public WorkflowExecutor(TaskRegistry taskRegistry,
                            HttpDispatchClient httpDispatchClient,
                            RemoteServiceClient remoteServiceClient,
                            ObjectMapper objectMapper,
                            String nodeId) {
        this.taskRegistry = taskRegistry;
        this.httpDispatchClient = httpDispatchClient;
        this.remoteServiceClient = remoteServiceClient;
        this.objectMapper = objectMapper == null ? new ObjectMapper() : objectMapper;
        this.nodeId = nodeId;
    }

    /**
     * 执行工作流。
     *
     * @param cfg       工作流任务配置（workflowDef 存放在 params 的 {@code __workflow} 或
     *                  {@link JobConfig#getWorkflowDef()}）
     * @param params    触发参数
     * @param requestId 追踪 ID
     * @param fireTime  触发时间
     * @return 汇总消息与逐步结果
     */
    public WorkflowResult execute(JobConfig cfg, Map<String, Object> params,
                                  String requestId, long fireTime) {
        WorkflowDefinition def = resolveDefinition(cfg);
        if (def == null || def.getSteps() == null || def.getSteps().isEmpty()) {
            throw new IllegalStateException("workflow task '" + cfg.getTaskName()
                    + "' has empty workflowDef (set workflowDef JSON or params.__workflow)");
        }

        List<WorkflowDefinition.Step> enabled = new ArrayList<WorkflowDefinition.Step>();
        for (WorkflowDefinition.Step step : def.getSteps()) {
            if (step != null && step.isEnabled()) {
                if (step.getName() == null || step.getName().trim().isEmpty()) {
                    step.setName("step-" + (enabled.size() + 1));
                }
                enabled.add(step);
            }
        }
        if (enabled.isEmpty()) {
            return WorkflowResult.empty("all steps disabled");
        }

        int defaultTimeout = (cfg.getTimeoutSeconds() != null && cfg.getTimeoutSeconds() > 0)
                ? cfg.getTimeoutSeconds() : 300;

        if (def.isParallel()) {
            return executeParallel(cfg, enabled, params, requestId, fireTime, defaultTimeout, def.isFailFast());
        }
        return executeSequential(cfg, enabled, params, requestId, fireTime, defaultTimeout, def.isFailFast());
    }

    private WorkflowResult executeSequential(JobConfig cfg, List<WorkflowDefinition.Step> steps,
                                             Map<String, Object> params, String requestId,
                                             long fireTime, int defaultTimeout, boolean failFast) {
        List<WorkflowStepResult> results = new ArrayList<WorkflowStepResult>();
        Map<String, Object> contextParams = mergeParams(cfg.getParamsView(), params);
        boolean aborted = false;
        String abortReason = null;

        for (WorkflowDefinition.Step step : steps) {
            if (aborted) {
                results.add(WorkflowStepResult.skipped(step.getName(),
                        "aborted due to previous failure: " + abortReason));
                continue;
            }
            WorkflowStepResult result = runStep(cfg, step, contextParams, requestId, fireTime, defaultTimeout);
            results.add(result);
            // 将步骤输出写入上下文（后续步骤可读取 prev.* ）
            if (result.getMessage() != null) {
                contextParams.put("prev." + step.getName(), result.getMessage());
                contextParams.put("prev.status", result.getStatus());
            }
            if (!result.isSuccess() && !step.isContinueOnFailure() && failFast) {
                aborted = true;
                abortReason = step.getName() + " " + result.getStatus() + ": " + result.getMessage();
            }
        }
        return WorkflowResult.of(results);
    }

    private WorkflowResult executeParallel(JobConfig cfg, List<WorkflowDefinition.Step> steps,
                                           Map<String, Object> params, String requestId,
                                           long fireTime, int defaultTimeout, boolean failFast) {
        // 简化并行：无 dependsOn 的一起跑；有 dependsOn 的等依赖完成后再跑（分层）
        Map<String, WorkflowStepResult> done = new LinkedHashMap<String, WorkflowStepResult>();
        Map<String, Object> contextParams = mergeParams(cfg.getParamsView(), params);
        List<WorkflowDefinition.Step> remaining = new ArrayList<WorkflowDefinition.Step>(steps);
        int guard = steps.size() + 2;

        while (!remaining.isEmpty() && guard-- > 0) {
            List<WorkflowDefinition.Step> ready = new ArrayList<WorkflowDefinition.Step>();
            List<WorkflowDefinition.Step> blocked = new ArrayList<WorkflowDefinition.Step>();
            for (WorkflowDefinition.Step step : remaining) {
                if (dependenciesSatisfied(step, done)) {
                    ready.add(step);
                } else if (dependenciesFailed(step, done) && failFast && !step.isContinueOnFailure()) {
                    done.put(step.getName(), WorkflowStepResult.skipped(step.getName(),
                            "dependency failed"));
                } else if (dependenciesFailed(step, done)) {
                    // 依赖失败但 continue：仍跳过
                    done.put(step.getName(), WorkflowStepResult.skipped(step.getName(),
                            "dependency failed (continueOnFailure)"));
                } else {
                    blocked.add(step);
                }
            }
            if (ready.isEmpty()) {
                // 死锁或循环依赖
                for (WorkflowDefinition.Step step : blocked) {
                    done.put(step.getName(), WorkflowStepResult.skipped(step.getName(),
                            "unsatisfied dependsOn (possible cycle)"));
                }
                break;
            }

            if (ready.size() == 1) {
                WorkflowDefinition.Step step = ready.get(0);
                WorkflowStepResult r = runStep(cfg, step, contextParams, requestId, fireTime, defaultTimeout);
                done.put(step.getName(), r);
                if (r.getMessage() != null) {
                    contextParams.put("prev." + step.getName(), r.getMessage());
                }
            } else {
                ExecutorService pool = Executors.newFixedThreadPool(Math.min(ready.size(), 8), r -> {
                    Thread t = new Thread(r, "orbit-workflow-" + cfg.getTaskName());
                    t.setDaemon(true);
                    return t;
                });
                try {
                    Map<String, Future<WorkflowStepResult>> futures =
                            new LinkedHashMap<String, Future<WorkflowStepResult>>();
                    for (final WorkflowDefinition.Step step : ready) {
                        final Map<String, Object> snap = new LinkedHashMap<String, Object>(contextParams);
                        futures.put(step.getName(), pool.submit(new Callable<WorkflowStepResult>() {
                            @Override
                            public WorkflowStepResult call() {
                                return runStep(cfg, step, snap, requestId, fireTime, defaultTimeout);
                            }
                        }));
                    }
                    for (Map.Entry<String, Future<WorkflowStepResult>> e : futures.entrySet()) {
                        try {
                            WorkflowStepResult r = e.getValue().get(defaultTimeout + 30L, TimeUnit.SECONDS);
                            done.put(e.getKey(), r);
                            if (r.getMessage() != null) {
                                contextParams.put("prev." + e.getKey(), r.getMessage());
                            }
                        } catch (Exception ex) {
                            done.put(e.getKey(), new WorkflowStepResult(e.getKey(), "PARALLEL",
                                    DispatchSummary.STATUS_FAILED, 0, nodeId,
                                    ex.getMessage() == null ? ex.getClass().getSimpleName() : ex.getMessage()));
                        }
                    }
                } finally {
                    pool.shutdownNow();
                }
            }
            remaining = blocked;
            // 若 failFast 且已有失败，剩余全部 skip
            if (failFast && hasFailure(done)) {
                for (WorkflowDefinition.Step step : remaining) {
                    if (!done.containsKey(step.getName())) {
                        done.put(step.getName(), WorkflowStepResult.skipped(step.getName(),
                                "aborted due to parallel failure"));
                    }
                }
                break;
            }
        }

        // 按原顺序输出
        List<WorkflowStepResult> ordered = new ArrayList<WorkflowStepResult>();
        for (WorkflowDefinition.Step step : steps) {
            WorkflowStepResult r = done.get(step.getName());
            if (r != null) {
                ordered.add(r);
            }
        }
        return WorkflowResult.of(ordered);
    }

    private boolean dependenciesSatisfied(WorkflowDefinition.Step step, Map<String, WorkflowStepResult> done) {
        if (step.getDependsOn() == null || step.getDependsOn().isEmpty()) {
            return true;
        }
        for (String dep : step.getDependsOn()) {
            WorkflowStepResult r = done.get(dep);
            if (r == null) {
                return false;
            }
            if (!r.isSuccess()) {
                return false;
            }
        }
        return true;
    }

    private boolean dependenciesFailed(WorkflowDefinition.Step step, Map<String, WorkflowStepResult> done) {
        if (step.getDependsOn() == null || step.getDependsOn().isEmpty()) {
            return false;
        }
        for (String dep : step.getDependsOn()) {
            WorkflowStepResult r = done.get(dep);
            if (r != null && !r.isSuccess()) {
                return true;
            }
        }
        return false;
    }

    private boolean hasFailure(Map<String, WorkflowStepResult> done) {
        for (WorkflowStepResult r : done.values()) {
            if (r != null && !r.isSuccess()) {
                return true;
            }
        }
        return false;
    }

    private WorkflowStepResult runStep(JobConfig parent, WorkflowDefinition.Step step,
                                       Map<String, Object> contextParams, String requestId,
                                       long fireTime, int defaultTimeout) {
        long start = System.currentTimeMillis();
        DispatchType type = step.resolveDispatchType();
        Map<String, Object> stepParams = mergeParams(contextParams, step.getParams());
        String stepRequestId = requestId + "-" + step.getName();
        try {
            switch (type) {
                case REMOTE:
                    return runRemote(step, stepParams, defaultTimeout, stepRequestId, start);
                case HTTP:
                    return runHttp(parent, step, stepParams, stepRequestId, start);
                case WORKFLOW:
                    return new WorkflowStepResult(step.getName(), "WORKFLOW",
                            DispatchSummary.STATUS_FAILED, 0, nodeId,
                            "nested WORKFLOW step is not supported; flatten steps into one workflow");
                case LOCAL:
                default:
                    return runLocal(step, stepParams, fireTime, stepRequestId, start);
            }
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            log.error("[orbit-scheduler] workflow step '{}' failed", step.getName(), e);
            return new WorkflowStepResult(step.getName(), type.name(),
                    DispatchSummary.STATUS_FAILED, cost, nodeId,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    private WorkflowStepResult runLocal(WorkflowDefinition.Step step, Map<String, Object> params,
                                        long fireTime, String requestId, long start) {
        String taskName = step.getTaskName();
        if (taskName == null || taskName.trim().isEmpty()) {
            taskName = step.getName();
        }
        if (taskRegistry == null || !taskRegistry.hasTask(taskName)) {
            return new WorkflowStepResult(step.getName(), "LOCAL", DispatchSummary.STATUS_FAILED,
                    System.currentTimeMillis() - start, nodeId,
                    "no local executor for task '" + taskName + "'");
        }
        TaskContext ctx = new TaskContext(taskName, "ORBIT", params,
                new Date(fireTime), nodeId, nodeId, requestId);
        Object ret = taskRegistry.execute(taskName, ctx);
        long cost = System.currentTimeMillis() - start;
        String msg = ret == null ? "OK" : String.valueOf(ret);
        return new WorkflowStepResult(step.getName(), "LOCAL", DispatchSummary.STATUS_SUCCESS,
                cost, nodeId, msg);
    }

    private WorkflowStepResult runHttp(JobConfig parent, WorkflowDefinition.Step step,
                                       Map<String, Object> params, String requestId, long start) {
        if (httpDispatchClient == null) {
            return new WorkflowStepResult(step.getName(), "HTTP", DispatchSummary.STATUS_FAILED,
                    0, nodeId, "HTTP dispatch client unavailable");
        }
        JobConfig proxy = new JobConfig();
        proxy.setTaskName(step.getTaskName() == null || step.getTaskName().isEmpty()
                ? step.getName() : step.getTaskName());
        proxy.setDispatchType(DispatchType.HTTP);
        proxy.setHttpServiceName(step.getService() != null ? step.getService() : parent.getHttpServiceName());
        proxy.setHttpPath(step.getPath() != null ? step.getPath() : parent.getHttpPath());
        proxy.setTimeoutSeconds(step.getTimeoutSeconds() != null
                ? step.getTimeoutSeconds() : parent.getTimeoutSeconds());
        HttpDispatchResponse resp = httpDispatchClient.dispatch(proxy, params, requestId);
        long cost = System.currentTimeMillis() - start;
        return new WorkflowStepResult(step.getName(), "HTTP",
                resp.isSuccess() ? DispatchSummary.STATUS_SUCCESS : DispatchSummary.STATUS_FAILED,
                cost, resp.getWorkerNode(), resp.getMessage());
    }

    private WorkflowStepResult runRemote(WorkflowDefinition.Step step, Map<String, Object> params,
                                         int defaultTimeout, String requestId, long start) {
        if (remoteServiceClient == null) {
            return new WorkflowStepResult(step.getName(), "REMOTE", DispatchSummary.STATUS_FAILED,
                    0, nodeId, "RemoteServiceClient unavailable");
        }
        HttpDispatchResponse resp = remoteServiceClient.invokeStep(step, params, defaultTimeout, requestId);
        long cost = System.currentTimeMillis() - start;
        return new WorkflowStepResult(step.getName(), "REMOTE",
                resp.isSuccess() ? DispatchSummary.STATUS_SUCCESS : DispatchSummary.STATUS_FAILED,
                cost, resp.getWorkerNode(), resp.getMessage());
    }

    private WorkflowDefinition resolveDefinition(JobConfig cfg) {
        // 1) 专用字段
        String json = cfg.getWorkflowDef();
        if (json == null || json.trim().isEmpty()) {
            // 2) params.__workflow
            Map<String, Object> params = cfg.getParamsView();
            Object w = params.get("__workflow");
            if (w instanceof Map) {
                try {
                    return objectMapper.convertValue(w, WorkflowDefinition.class);
                } catch (Exception e) {
                    log.warn("[orbit-scheduler] convert params.__workflow failed: {}", e.getMessage());
                }
            } else if (w instanceof String) {
                json = (String) w;
            } else if (params.containsKey("steps")) {
                try {
                    return objectMapper.convertValue(params, WorkflowDefinition.class);
                } catch (Exception e) {
                    log.warn("[orbit-scheduler] convert params as workflow failed: {}", e.getMessage());
                }
            }
        }
        if (json == null || json.trim().isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(json, WorkflowDefinition.class);
        } catch (Exception e) {
            throw new IllegalStateException("invalid workflowDef JSON: " + e.getMessage(), e);
        }
    }

    private static Map<String, Object> mergeParams(Map<String, Object> base, Map<String, Object> override) {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        if (base != null) {
            m.putAll(base);
        }
        if (override != null) {
            m.putAll(override);
        }
        // 不把 __workflow 传给下游
        m.remove("__workflow");
        m.remove("steps");
        m.remove("mode");
        m.remove("failFast");
        return m;
    }

    /**
     * 工作流整体执行结果。
     */
    public static final class WorkflowResult {
        private final List<WorkflowStepResult> steps;
        private final boolean success;
        private final String message;

        private WorkflowResult(List<WorkflowStepResult> steps, boolean success, String message) {
            this.steps = steps;
            this.success = success;
            this.message = message;
        }

        static WorkflowResult empty(String message) {
            return new WorkflowResult(Collections.<WorkflowStepResult>emptyList(), true, message);
        }

        static WorkflowResult of(List<WorkflowStepResult> steps) {
            boolean ok = true;
            StringBuilder sb = new StringBuilder();
            sb.append("workflow steps=").append(steps.size()).append(" [");
            for (int i = 0; i < steps.size(); i++) {
                WorkflowStepResult s = steps.get(i);
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(s.getStepName()).append('=').append(s.getStatus())
                        .append('(').append(s.getCostMs()).append("ms)");
                if (!s.isSuccess()) {
                    ok = false;
                }
            }
            sb.append(']');
            return new WorkflowResult(steps, ok, sb.toString());
        }

        public List<WorkflowStepResult> getSteps() { return steps; }

        public boolean isSuccess() { return success; }

        public String getMessage() { return message; }
    }
}

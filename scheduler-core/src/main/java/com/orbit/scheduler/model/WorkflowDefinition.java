package com.orbit.scheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.orbit.scheduler.annotation.DispatchType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 工作流编排定义：描述一次批量调度中需要串行/并行执行的多个步骤。
 *
 * <p>JSON 示例：
 * <pre>
 * {
 *   "mode": "SEQUENTIAL",
 *   "failFast": true,
 *   "steps": [
 *     {
 *       "name": "settle-orders",
 *       "dispatchType": "REMOTE",
 *       "service": "order-service",
 *       "path": "/api/batch/settle",
 *       "method": "POST",
 *       "params": { "bizDate": "yesterday" }
 *     },
 *     {
 *       "name": "sync-inventory",
 *       "dispatchType": "REMOTE",
 *       "service": "inventory-service",
 *       "path": "/api/batch/sync",
 *       "dependsOn": ["settle-orders"]
 *     },
 *     {
 *       "name": "local-report",
 *       "dispatchType": "LOCAL",
 *       "taskName": "dailyOrderReport"
 *     }
 *   ]
 * }
 * </pre>
 *
 * @author orbit
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class WorkflowDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 执行模式：SEQUENTIAL（默认串行）/ PARALLEL（全部并行） */
    private String mode = "SEQUENTIAL";

    /** 任一步失败是否立即中止后续步骤（串行模式）；并行模式下仍会等待已启动步骤结束 */
    private boolean failFast = true;

    /** 步骤列表 */
    private List<Step> steps = new ArrayList<Step>();

    public String getMode() { return mode; }

    public void setMode(String mode) { this.mode = mode; }

    public boolean isFailFast() { return failFast; }

    public void setFailFast(boolean failFast) { this.failFast = failFast; }

    public List<Step> getSteps() { return steps; }

    public void setSteps(List<Step> steps) {
        this.steps = steps == null ? new ArrayList<Step>() : steps;
    }

    public boolean isParallel() {
        return mode != null && "PARALLEL".equalsIgnoreCase(mode.trim());
    }

    /**
     * 单个编排步骤。
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Step implements Serializable {

        private static final long serialVersionUID = 1L;

        /** 步骤名（同工作流内唯一，用于 dependsOn 与日志） */
        private String name;

        /** 调度类型：LOCAL / HTTP / REMOTE；空则按字段推断（有 service/path → REMOTE，有 taskName → LOCAL） */
        private DispatchType dispatchType;

        /** 本地/HTTP 同框架任务名 */
        private String taskName;

        /** REMOTE：远程服务注册名（orbit.scheduler.remote-services 的 key） */
        private String service;

        /** REMOTE/HTTP：请求路径 */
        private String path;

        /** REMOTE：HTTP 方法，默认 POST */
        private String method = "POST";

        /** 步骤级超时秒数；空则继承工作流任务 timeoutSeconds */
        private Integer timeoutSeconds;

        /** 步骤参数（与工作流触发 params 合并，步骤优先） */
        private Map<String, Object> params = new LinkedHashMap<String, Object>();

        /** 额外请求头（REMOTE） */
        private Map<String, String> headers = new LinkedHashMap<String, String>();

        /** 依赖的前置步骤名（PARALLEL 模式下生效；SEQUENTIAL 默认按列表顺序） */
        private List<String> dependsOn = new ArrayList<String>();

        /** 失败时是否继续后续步骤（覆盖 failFast） */
        private boolean continueOnFailure = false;

        /** 是否启用；false 时跳过 */
        private boolean enabled = true;

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }

        public DispatchType getDispatchType() { return dispatchType; }

        public void setDispatchType(DispatchType dispatchType) { this.dispatchType = dispatchType; }

        public String getTaskName() { return taskName; }

        public void setTaskName(String taskName) { this.taskName = taskName; }

        public String getService() { return service; }

        public void setService(String service) { this.service = service; }

        public String getPath() { return path; }

        public void setPath(String path) { this.path = path; }

        public String getMethod() { return method; }

        public void setMethod(String method) { this.method = method; }

        public Integer getTimeoutSeconds() { return timeoutSeconds; }

        public void setTimeoutSeconds(Integer timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }

        public Map<String, Object> getParams() { return params; }

        public void setParams(Map<String, Object> params) {
            this.params = params == null ? new LinkedHashMap<String, Object>() : params;
        }

        public Map<String, String> getHeaders() { return headers; }

        public void setHeaders(Map<String, String> headers) {
            this.headers = headers == null ? new LinkedHashMap<String, String>() : headers;
        }

        public List<String> getDependsOn() { return dependsOn; }

        public void setDependsOn(List<String> dependsOn) {
            this.dependsOn = dependsOn == null ? new ArrayList<String>() : dependsOn;
        }

        public boolean isContinueOnFailure() { return continueOnFailure; }

        public void setContinueOnFailure(boolean continueOnFailure) {
            this.continueOnFailure = continueOnFailure;
        }

        public boolean isEnabled() { return enabled; }

        public void setEnabled(boolean enabled) { this.enabled = enabled; }

        /**
         * 推断实际调度类型。
         */
        public DispatchType resolveDispatchType() {
            if (dispatchType != null) {
                return dispatchType;
            }
            if (service != null && !service.trim().isEmpty()) {
                return DispatchType.REMOTE;
            }
            if (path != null && (path.startsWith("http://") || path.startsWith("https://"))) {
                return DispatchType.REMOTE;
            }
            if (taskName != null && !taskName.trim().isEmpty()) {
                return DispatchType.LOCAL;
            }
            return DispatchType.REMOTE;
        }
    }
}

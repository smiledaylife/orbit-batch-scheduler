package com.orbit.scheduler.model;

import java.io.Serializable;

/**
 * HTTP 远程派发响应体：执行节点 → 调度节点。
 *
 * @author orbit
 */
public class HttpDispatchResponse implements Serializable {

    private static final long serialVersionUID = 1L;

    private boolean success;
    /** 目标节点不存在该任务执行器时为 true（仅 Headless/Static 模式会尝试下一端点） */
    private boolean taskMissing;
    private String message;
    private long costMs;
    /** 实际执行节点 ID */
    private String workerNode;
    private String requestId;

    public HttpDispatchResponse() {
    }

    public static HttpDispatchResponse success(String requestId, String workerNode, long costMs, String message) {
        HttpDispatchResponse r = new HttpDispatchResponse();
        r.success = true;
        r.requestId = requestId;
        r.workerNode = workerNode;
        r.costMs = costMs;
        r.message = message;
        return r;
    }

    public static HttpDispatchResponse failure(String requestId, String workerNode, String message) {
        HttpDispatchResponse r = new HttpDispatchResponse();
        r.success = false;
        r.requestId = requestId;
        r.workerNode = workerNode;
        r.message = message;
        return r;
    }

    public static HttpDispatchResponse taskMissing(String requestId, String workerNode, String message) {
        HttpDispatchResponse r = failure(requestId, workerNode, message);
        r.taskMissing = true;
        return r;
    }

    public boolean isSuccess() { return success; }

    public void setSuccess(boolean success) { this.success = success; }

    public boolean isTaskMissing() { return taskMissing; }

    public void setTaskMissing(boolean taskMissing) { this.taskMissing = taskMissing; }

    public String getMessage() { return message; }

    public void setMessage(String message) { this.message = message; }

    public long getCostMs() { return costMs; }

    public void setCostMs(long costMs) { this.costMs = costMs; }

    public String getWorkerNode() { return workerNode; }

    public void setWorkerNode(String workerNode) { this.workerNode = workerNode; }

    public String getRequestId() { return requestId; }

    public void setRequestId(String requestId) { this.requestId = requestId; }
}

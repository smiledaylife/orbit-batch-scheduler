package com.orbit.scheduler.model;

import java.io.Serializable;
import java.util.Map;

/**
 * HTTP 远程派发请求体：调度节点 → 执行节点（经 K8s Service）。
 *
 * @author orbit
 */
public class HttpDispatchRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private String requestId;
    private String taskName;
    private Map<String, Object> params;
    private long fireTime;
    /** 发起派发的节点 ID */
    private String dispatchNode;
    /** 期望的超时秒数（执行节点可据此控制自身超时） */
    private int timeoutSeconds;

    public HttpDispatchRequest() {
    }

    public HttpDispatchRequest(String requestId, String taskName, Map<String, Object> params,
                                long fireTime, String dispatchNode, int timeoutSeconds) {
        this.requestId = requestId;
        this.taskName = taskName;
        this.params = params;
        this.fireTime = fireTime;
        this.dispatchNode = dispatchNode;
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getRequestId() { return requestId; }

    public void setRequestId(String requestId) { this.requestId = requestId; }

    public String getTaskName() { return taskName; }

    public void setTaskName(String taskName) { this.taskName = taskName; }

    public Map<String, Object> getParams() { return params; }

    public void setParams(Map<String, Object> params) { this.params = params; }

    public long getFireTime() { return fireTime; }

    public void setFireTime(long fireTime) { this.fireTime = fireTime; }

    public String getDispatchNode() { return dispatchNode; }

    public void setDispatchNode(String dispatchNode) { this.dispatchNode = dispatchNode; }

    public int getTimeoutSeconds() { return timeoutSeconds; }

    public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
}

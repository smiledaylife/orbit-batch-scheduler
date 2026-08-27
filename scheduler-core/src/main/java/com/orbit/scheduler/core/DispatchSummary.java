package com.orbit.scheduler.core;

/**
 * 单次派发的执行结果摘要。
 *
 * @author orbit
 */
public class DispatchSummary {

    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_SKIPPED = "SKIPPED";

    private final String taskName;
    private final String status;
    private final long costMs;
    private final String workerNode;
    private final String requestId;
    private final String message;

    public DispatchSummary(String taskName, String status, long costMs,
                           String workerNode, String requestId, String message) {
        this.taskName = taskName;
        this.status = status;
        this.costMs = costMs;
        this.workerNode = workerNode;
        this.requestId = requestId;
        this.message = message;
    }

    public String getTaskName() { return taskName; }

    public String getStatus() { return status; }

    public long getCostMs() { return costMs; }

    public String getWorkerNode() { return workerNode; }

    public String getRequestId() { return requestId; }

    public String getMessage() { return message; }

    @Override
    public String toString() {
        return "DispatchSummary{taskName='" + taskName + "', status=" + status +
                ", costMs=" + costMs + ", workerNode='" + workerNode +
                "', requestId='" + requestId + "'}";
    }
}

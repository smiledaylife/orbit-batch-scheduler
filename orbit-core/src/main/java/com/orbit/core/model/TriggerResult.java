package com.orbit.core.model;

import java.io.Serializable;

/**
 * 执行器 → 调度中心：执行结果回调（也可同步返回）。
 */
public class TriggerResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String logId;
    private long jobId;
    private boolean success;
    private String message;
    private long costMs;
    private String workerNode;

    public static TriggerResult ok(String logId, long jobId, String workerNode, long costMs, String message) {
        TriggerResult r = new TriggerResult();
        r.logId = logId;
        r.jobId = jobId;
        r.success = true;
        r.workerNode = workerNode;
        r.costMs = costMs;
        r.message = message;
        return r;
    }

    public static TriggerResult fail(String logId, long jobId, String workerNode, long costMs, String message) {
        TriggerResult r = new TriggerResult();
        r.logId = logId;
        r.jobId = jobId;
        r.success = false;
        r.workerNode = workerNode;
        r.costMs = costMs;
        r.message = message;
        return r;
    }

    public String getLogId() { return logId; }
    public void setLogId(String logId) { this.logId = logId; }
    public long getJobId() { return jobId; }
    public void setJobId(long jobId) { this.jobId = jobId; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public long getCostMs() { return costMs; }
    public void setCostMs(long costMs) { this.costMs = costMs; }
    public String getWorkerNode() { return workerNode; }
    public void setWorkerNode(String workerNode) { this.workerNode = workerNode; }
}

package com.orbit.scheduler.model;

import java.io.Serializable;

/**
 * 工作流单步执行结果。
 *
 * @author orbit
 */
public class WorkflowStepResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private String stepName;
    private String dispatchType;
    private String status;
    private long costMs;
    private String workerNode;
    private String message;
    private boolean skipped;

    public WorkflowStepResult() {
    }

    public WorkflowStepResult(String stepName, String dispatchType, String status,
                              long costMs, String workerNode, String message) {
        this.stepName = stepName;
        this.dispatchType = dispatchType;
        this.status = status;
        this.costMs = costMs;
        this.workerNode = workerNode;
        this.message = message;
    }

    public static WorkflowStepResult skipped(String stepName, String reason) {
        WorkflowStepResult r = new WorkflowStepResult(stepName, null, "SKIPPED", 0, null, reason);
        r.skipped = true;
        return r;
    }

    public String getStepName() { return stepName; }

    public void setStepName(String stepName) { this.stepName = stepName; }

    public String getDispatchType() { return dispatchType; }

    public void setDispatchType(String dispatchType) { this.dispatchType = dispatchType; }

    public String getStatus() { return status; }

    public void setStatus(String status) { this.status = status; }

    public long getCostMs() { return costMs; }

    public void setCostMs(long costMs) { this.costMs = costMs; }

    public String getWorkerNode() { return workerNode; }

    public void setWorkerNode(String workerNode) { this.workerNode = workerNode; }

    public String getMessage() { return message; }

    public void setMessage(String message) { this.message = message; }

    public boolean isSkipped() { return skipped; }

    public void setSkipped(boolean skipped) { this.skipped = skipped; }

    public boolean isSuccess() {
        return "SUCCESS".equalsIgnoreCase(status) || "SKIPPED".equalsIgnoreCase(status);
    }

    @Override
    public String toString() {
        return stepName + "[" + status + "," + costMs + "ms]";
    }
}

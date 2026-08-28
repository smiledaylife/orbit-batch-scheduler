package com.orbit.core.model;

import java.io.Serializable;

/**
 * 任务执行结果响应实体。
 * <p>执行器执行完具体的 @OrbitJob 方法后，将结果封装并同步返回给调度中心。
 */
public class TriggerResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 关联的调度日志 ID
     */
    private String logId;

    /**
     * 关联的任务 ID
     */
    private long jobId;

    /**
     * 任务是否执行成功的标识（true: 成功，false: 失败）
     */
    private boolean success;

    /**
     * 执行结果描述信息或异常堆栈摘要
     */
    private String message;

    /**
     * 执行器本地方法调用的实际耗时（毫秒）
     */
    private long costMs;

    /**
     * 实际承接并执行任务的工作节点标识或节点网络地址
     */
    private String workerNode;

    /**
     * 构建执行成功的 TriggerResult 结果对象
     *
     * @param logId      日志 ID
     * @param jobId      任务 ID
     * @param workerNode 执行节点标识
     * @param costMs     耗时（毫秒）
     * @param message    执行成功结果文本
     * @return 成功响应对象
     */
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

    /**
     * 构建执行失败的 TriggerResult 结果对象
     *
     * @param logId      日志 ID
     * @param jobId      任务 ID
     * @param workerNode 执行节点标识
     * @param costMs     耗时（毫秒）
     * @param message    失败原因或异常描述
     * @return 失败响应对象
     */
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

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getCostMs() {
        return costMs;
    }

    public void setCostMs(long costMs) {
        this.costMs = costMs;
    }

    public String getWorkerNode() {
        return workerNode;
    }

    public void setWorkerNode(String workerNode) {
        this.workerNode = workerNode;
    }
}

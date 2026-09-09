package com.orbit.core.model;

import java.io.Serializable;

/**
 * 任务执行结果响应实体，承载两个方向、两种语义的载荷：
 *
 * 1. 触发回执（执行器 -> 调度中心，{@code POST /orbit/executor/run} 的同步响应）：
 *    {@code accepted=true} 表示执行器已受理本次触发并已入队，任务尚未执行完毕。
 *    该响应只反映「收没收到」，与任务成败无关，因此调度中心必须把日志留在 RUNNING；
 * 2. 执行结果（执行器 -> 调度中心，{@code POST /orbit/admin/callback} 的请求体）：
 *    {@code accepted=false}，{@code success} 表示业务方法的真实成败，
 *    {@code costMs} 为执行器本地计时。
 *
 * 两个方向复用同一实体，是为了让 logId / jobId / workerNode 等字段只有一处定义。
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
     * 本载荷是否为「触发回执」：true 表示执行器已受理并入队（任务尚未跑完），
     * false 表示这是任务的最终执行结果。
     */
    private boolean accepted;

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
    /**
     * 构建触发回执：执行器已受理本次触发、任务已入队，但尚未执行完毕。
     * success 置为 true 仅表示「受理成功」，不代表任务会成功。
     *
     * @param logId      日志 ID
     * @param jobId      任务 ID
     * @param workerNode 受理节点标识
     * @param message    受理说明
     * @return 触发回执对象
     */
    public static TriggerResult accepted(String logId, long jobId, String workerNode, String message) {
        TriggerResult r = new TriggerResult();
        r.logId = logId;
        r.jobId = jobId;
        r.success = true;
        r.accepted = true;
        r.workerNode = workerNode;
        r.costMs = 0;
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

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public String getWorkerNode() {
        return workerNode;
    }

    public void setWorkerNode(String workerNode) {
        this.workerNode = workerNode;
    }
}

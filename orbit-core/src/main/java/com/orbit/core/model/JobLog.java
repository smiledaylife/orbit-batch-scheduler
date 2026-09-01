package com.orbit.core.model;

import java.io.Serializable;
import java.util.Date;

/**
 * 任务执行日志实体。
 * 用于记录每次任务调度派发的执行明细、选中的执行器节点、执行结果与耗时。
 */
public class JobLog implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 日志主键自增 ID
     */
    private Long id;

    /**
     * 全局调度日志跟踪 ID（UUID 生成，单次调度链路唯一）
     */
    private String logId;

    /**
     * 关联的任务 ID（JobInfo.id）
     */
    private Long jobId;

    /**
     * 关联的任务名称
     */
    private String jobName;

    /**
     * 关联的执行器应用名称
     */
    private String appName;

    /**
     * 触发的 JobHandler 处理函数名
     */
    private String handler;

    /**
     * 实际派发执行的目标执行器地址（如 http://10.0.1.5:8081）
     */
    private String executorAddress;

    /**
     * 执行状态：RUNNING（执行中）、SUCCESS（执行成功）、FAILED（执行失败）
     */
    private String status;

    /**
     * 执行结果信息或异常错误堆栈摘要（超长自动截断保存）
     */
    private String message;

    /**
     * 执行耗时（毫秒）
     */
    private long costMs;

    /**
     * 调度触发开始时间
     */
    private Date startTime;

    /**
     * 调度执行结束时间
     */
    private Date endTime;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public Long getJobId() {
        return jobId;
    }

    public void setJobId(Long jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getHandler() {
        return handler;
    }

    public void setHandler(String handler) {
        this.handler = handler;
    }

    public String getExecutorAddress() {
        return executorAddress;
    }

    public void setExecutorAddress(String executorAddress) {
        this.executorAddress = executorAddress;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
}

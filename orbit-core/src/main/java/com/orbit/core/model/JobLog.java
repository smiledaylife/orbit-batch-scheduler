package com.orbit.core.model;

import java.io.Serializable;
import java.util.Date;

/**
 * 任务执行日志。
 */
public class JobLog implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;
    private String logId;
    private Long jobId;
    private String jobName;
    private String appName;
    private String handler;
    private String executorAddress;
    private String status;
    private String message;
    private long costMs;
    private Date startTime;
    private Date endTime;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getLogId() { return logId; }
    public void setLogId(String logId) { this.logId = logId; }
    public Long getJobId() { return jobId; }
    public void setJobId(Long jobId) { this.jobId = jobId; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }
    public String getHandler() { return handler; }
    public void setHandler(String handler) { this.handler = handler; }
    public String getExecutorAddress() { return executorAddress; }
    public void setExecutorAddress(String executorAddress) { this.executorAddress = executorAddress; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public long getCostMs() { return costMs; }
    public void setCostMs(long costMs) { this.costMs = costMs; }
    public Date getStartTime() { return startTime; }
    public void setStartTime(Date startTime) { this.startTime = startTime; }
    public Date getEndTime() { return endTime; }
    public void setEndTime(Date endTime) { this.endTime = endTime; }
}

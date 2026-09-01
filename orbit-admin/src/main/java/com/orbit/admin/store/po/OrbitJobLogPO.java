package com.orbit.admin.store.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;
import java.util.Date;

/**
 * 调度执行日志表 {@code orbit_job_log} 的持久化实体（PO）。
 * 仅在调度中心持久层使用；对外协议模型见 {@code com.orbit.core.model.JobLog}。
 */
@TableName("orbit_job_log")
public class OrbitJobLogPO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 主键自增 ID */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /** 单次调度链路唯一追踪 ID（UUID） */
    private String logId;

    /** 关联任务 ID */
    private Long jobId;

    /** 关联任务名称 */
    private String jobName;

    /** 关联执行器应用名 */
    private String appName;

    /** 触发的 Handler 名 */
    private String handler;

    /** 实际派发的执行器地址 */
    private String executorAddress;

    /** 执行状态：RUNNING / SUCCESS / FAILED */
    private String status;

    /** 执行结果或异常摘要（超长截断） */
    private String message;

    /** 执行耗时（毫秒） */
    private Long costMs;

    /** 触发开始时间 */
    private Date startTime;

    /** 执行结束时间 */
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

    public Long getCostMs() {
        return costMs;
    }

    public void setCostMs(Long costMs) {
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

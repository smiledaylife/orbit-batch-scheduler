package com.orbit.scheduler.model;

import java.util.Date;

/**
 * 任务执行日志实体：对应表 t_job_log。
 *
 * @author orbit
 */
public class JobLog {

    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_SKIPPED = "SKIPPED";

    private Long id;
    private String requestId;
    private String taskName;
    private String taskGroup;
    private String dispatchType;
    /** 发起调度节点（Quartz 触发所在节点） */
    private String dispatchNode;
    /** 实际执行节点（本地执行=dispatchNode；HTTP 派发=远端 Pod） */
    private String workerNode;
    private String status;
    private Date startTime;
    private Date endTime;
    private Long costMs;
    private String message;

    public JobLog() {
    }

    public static JobLog startOf(String requestId, String taskName, String taskGroup,
                                 String dispatchType, String dispatchNode) {
        JobLog log = new JobLog();
        log.requestId = requestId;
        log.taskName = taskName;
        log.taskGroup = taskGroup;
        log.dispatchType = dispatchType;
        log.dispatchNode = dispatchNode;
        log.workerNode = dispatchNode;
        log.status = STATUS_RUNNING;
        log.startTime = new Date();
        return log;
    }

    public Long getId() { return id; }

    public void setId(Long id) { this.id = id; }

    public String getRequestId() { return requestId; }

    public void setRequestId(String requestId) { this.requestId = requestId; }

    public String getTaskName() { return taskName; }

    public void setTaskName(String taskName) { this.taskName = taskName; }

    public String getTaskGroup() { return taskGroup; }

    public void setTaskGroup(String taskGroup) { this.taskGroup = taskGroup; }

    public String getDispatchType() { return dispatchType; }

    public void setDispatchType(String dispatchType) { this.dispatchType = dispatchType; }

    public String getDispatchNode() { return dispatchNode; }

    public void setDispatchNode(String dispatchNode) { this.dispatchNode = dispatchNode; }

    public String getWorkerNode() { return workerNode; }

    public void setWorkerNode(String workerNode) { this.workerNode = workerNode; }

    public String getStatus() { return status; }

    public void setStatus(String status) { this.status = status; }

    public Date getStartTime() { return startTime; }

    public void setStartTime(Date startTime) { this.startTime = startTime; }

    public Date getEndTime() { return endTime; }

    public void setEndTime(Date endTime) { this.endTime = endTime; }

    public Long getCostMs() { return costMs; }

    public void setCostMs(Long costMs) { this.costMs = costMs; }

    public String getMessage() { return message; }

    public void setMessage(String message) { this.message = message; }
}

package com.orbit.core.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务触发执行请求实体。
 * 调度中心根据定时调度或手动触发，通过 HTTP POST 方式调用执行器端 /orbit/executor/run 时透传的请求体。
 */
public class TriggerRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 任务 ID（对应 orbit_job 表主键）
     */
    private long jobId;

    /**
     * 任务唯一名称（对应 orbit_job 表 job_name）
     */
    private String jobName;

    /**
     * 目标任务处理函数名称（与方法上的 @OrbitJob 标注值对应）
     */
    private String handler;

    /**
     * 本次执行链路唯一追踪日志 ID（UUID 格式）
     */
    private String logId;

    /**
     * 任务执行入参字典（合并了任务配置中的静态参数与手动触发时的动态参数）
     */
    private Map<String, Object> params = new HashMap<String, Object>();

    /**
     * 任务执行超时时间（秒），默认 300 秒
     */
    private int timeoutSeconds = 300;

    /**
     * 安全访问令牌，用于执行器端安全校验
     */
    private String accessToken;

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getHandler() {
        return handler;
    }

    public void setHandler(String handler) {
        this.handler = handler;
    }

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
}

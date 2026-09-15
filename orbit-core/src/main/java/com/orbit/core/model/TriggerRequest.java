package com.orbit.core.model;

import java.io.Serial;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务触发执行请求实体。
 * 调度中心根据定时调度或手动触发，通过 HTTP POST 方式调用执行器端 /orbit/executor/run 时透传的请求体。
 */
public class TriggerRequest implements Serializable {

    @Serial
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
     * 执行器应用名（对应 orbit_job 表 app_name）；
     * 执行器回传结果时原样带回，供调度中心告警事件直接使用，免去反查库。
     */
    private String appName;

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
     * 失败重试次数（不含首次执行）：执行失败/超时后由执行器本地重试的次数上限，0 表示不重试。
     * 由调度中心随触发下发（来自任务定义 orbit_job.retry_count）。
     */
    private int retryCount = 0;

    /**
     * 失败重试间隔（秒），0 表示立即重试，默认 10 秒。
     * 由调度中心随触发下发（来自任务定义 orbit_job.retry_interval_seconds）。
     */
    private int retryIntervalSeconds = 10;

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

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getRetryIntervalSeconds() {
        return retryIntervalSeconds;
    }

    public void setRetryIntervalSeconds(int retryIntervalSeconds) {
        this.retryIntervalSeconds = retryIntervalSeconds;
    }

}

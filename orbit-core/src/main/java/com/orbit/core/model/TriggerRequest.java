package com.orbit.core.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 调度中心 → 执行器：触发任务执行。
 */
public class TriggerRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    private long jobId;
    private String jobName;
    /** JobHandler 名称（@OrbitJob value） */
    private String handler;
    private String logId;
    private Map<String, Object> params = new HashMap<String, Object>();
    private int timeoutSeconds = 300;
    private String accessToken;

    public long getJobId() { return jobId; }
    public void setJobId(long jobId) { this.jobId = jobId; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getHandler() { return handler; }
    public void setHandler(String handler) { this.handler = handler; }
    public String getLogId() { return logId; }
    public void setLogId(String logId) { this.logId = logId; }
    public Map<String, Object> getParams() { return params; }
    public void setParams(Map<String, Object> params) { this.params = params; }
    public int getTimeoutSeconds() { return timeoutSeconds; }
    public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
}

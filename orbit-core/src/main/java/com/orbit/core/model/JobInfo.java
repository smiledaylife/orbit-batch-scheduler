package com.orbit.core.model;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务定义（调度中心持久化）。
 */
public class JobInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private Long id;
    /** 任务名（唯一） */
    private String jobName;
    private String description;
    /** 执行器应用名 */
    private String appName;
    /** JobHandler 名 */
    private String handler;
    /** Quartz Cron */
    private String cron;
    private Map<String, Object> params = new HashMap<String, Object>();
    private int timeoutSeconds = 300;
    /** ROUND / RANDOM / FIRST */
    private String routeStrategy = "ROUND";
    private boolean enabled = true;
    private int version;
    private Date createdAt;
    private Date updatedAt;

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getJobName() { return jobName; }
    public void setJobName(String jobName) { this.jobName = jobName; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }
    public String getHandler() { return handler; }
    public void setHandler(String handler) { this.handler = handler; }
    public String getCron() { return cron; }
    public void setCron(String cron) { this.cron = cron; }
    public Map<String, Object> getParams() { return params; }
    public void setParams(Map<String, Object> params) {
        this.params = params == null ? new HashMap<String, Object>() : params;
    }
    public int getTimeoutSeconds() { return timeoutSeconds; }
    public void setTimeoutSeconds(int timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }
    public String getRouteStrategy() { return routeStrategy; }
    public void setRouteStrategy(String routeStrategy) { this.routeStrategy = routeStrategy; }
    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public int getVersion() { return version; }
    public void setVersion(int version) { this.version = version; }
    public Date getCreatedAt() { return createdAt; }
    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }
    public Date getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }
}

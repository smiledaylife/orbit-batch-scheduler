package com.orbit.core.model;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务元数据定义模型。
 * 持久化存储于调度中心数据库（如 PostgreSQL / GaussDB / H2）的 orbit_job 表中，
 * 并与 Quartz 的 JobDetail/Trigger 保持生命周期同步。
 */
public class JobInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 任务主键自增 ID
     */
    private Long id;

    /**
     * 任务唯一名称（标识符，符合正则 [A-Za-z0-9_-.]{1,64}）
     */
    private String jobName;

    /**
     * 任务描述信息或业务备注
     */
    private String description;

    /**
     * 目标执行器应用名（与执行器端的 orbit.executor.app-name 对应）
     */
    private String appName;

    /**
     * 目标任务处理函数名（与执行器方法上的 @OrbitJob("xxx") 对应）
     */
    private String handler;

    /**
     * Quartz 格式的标准 Cron 表达式（若为空表示仅支持手动触发，不注册自动定时调度）
     */
    private String cron;

    /**
     * 静态执行参数（JSON 格式持久化），任务触发时透传给执行器
     */
    private Map<String, Object> params = new HashMap<String, Object>();

    /**
     * 任务执行超时时间（秒），默认 300 秒，同时作用于调度中心 HTTP 调用的 ReadTimeout
     */
    private int timeoutSeconds = 300;

    /**
     * 多实例路由策略，支持：
     * 
     *   - ROUND: 轮询（默认）
     *   - RANDOM: 随机
     *   - FIRST: 首个节点
     * 
     */
    private String routeStrategy = "ROUND";

    /**
     * 调度开关（true: 启用定时触发，false: 暂停调度）
     */
    private boolean enabled = true;

    /**
     * 乐观锁版本号，防止并发修改冲突
     */
    private int version;

    /**
     * 创建时间
     */
    private Date createdAt;

    /**
     * 最近更新时间
     */
    private Date updatedAt;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
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

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params == null ? new HashMap<String, Object>() : params;
    }

    public int getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getRouteStrategy() {
        return routeStrategy;
    }

    public void setRouteStrategy(String routeStrategy) {
        this.routeStrategy = routeStrategy;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Date updatedAt) {
        this.updatedAt = updatedAt;
    }
}

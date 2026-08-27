package com.orbit.scheduler.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.orbit.scheduler.annotation.DispatchType;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 任务配置实体：对应表 t_job_config（数据库存储）或本地内存注册表。
 *
 * <p>REST API 中 {@code params} 字段以 JSON 对象形式读写，底层持久化为 JSON 字符串。
 *
 * @author orbit
 */
public class JobConfig {

    private Long id;
    private String taskName;
    private String taskGroup;
    private String description;
    /** Quartz Cron 表达式；空表示不注册定时触发器（仅手工触发） */
    private String cronExpression;
    /** 调度方式：LOCAL / HTTP */
    private DispatchType dispatchType = DispatchType.LOCAL;
    /** HTTP 调度目标服务名（K8s Headless Service DNS，空则取全局配置） */
    private String httpServiceName;
    /** HTTP 调度远端执行路径（空则取全局配置） */
    private String httpPath;
    /** 任务超时秒数（HTTP 调度读超时依据） */
    private Integer timeoutSeconds = 300;
    /** 任务参数 JSON */
    private String paramsJson;
    /** 是否启用（false 时不会被调度） */
    private boolean enabled = true;
    /** 乐观锁版本号 */
    private int version;
    private Date createdAt;
    private Date updatedAt;

    public JobConfig() {
    }

    public JobConfig(String taskName, String taskGroup, String cronExpression, DispatchType dispatchType) {
        this.taskName = taskName;
        this.taskGroup = taskGroup;
        this.cronExpression = cronExpression;
        this.dispatchType = dispatchType;
    }

    /** 拷贝可编辑字段（用于更新接口，保留 id/版本/时间戳） */
    public void copyEditableFrom(JobConfig input) {
        this.description = input.description;
        this.cronExpression = input.cronExpression;
        this.dispatchType = input.dispatchType == null ? DispatchType.LOCAL : input.dispatchType;
        this.httpServiceName = input.httpServiceName;
        this.httpPath = input.httpPath;
        this.timeoutSeconds = input.timeoutSeconds == null || input.timeoutSeconds <= 0 ? 300 : input.timeoutSeconds;
        this.paramsJson = input.paramsJson;
        this.enabled = input.enabled;
    }

    // ---------------- JSON 视图：params 以对象形式对外 ----------------

    @JsonProperty("params")
    public Map<String, Object> getParamsView() {
        return JsonHolder.parse(paramsJson);
    }

    @JsonProperty("params")
    public void setParamsView(Map<String, Object> params) {
        this.paramsJson = JsonHolder.toJson(params);
    }

    @JsonIgnore
    public String getParamsJson() { return paramsJson; }

    public void setParamsJson(String paramsJson) { this.paramsJson = paramsJson; }

    // ---------------- getter / setter ----------------

    public Long getId() { return id; }

    public void setId(Long id) { this.id = id; }

    public String getTaskName() { return taskName; }

    public void setTaskName(String taskName) { this.taskName = taskName; }

    public String getTaskGroup() { return taskGroup; }

    public void setTaskGroup(String taskGroup) { this.taskGroup = taskGroup; }

    public String getDescription() { return description; }

    public void setDescription(String description) { this.description = description; }

    public String getCronExpression() { return cronExpression; }

    public void setCronExpression(String cronExpression) { this.cronExpression = cronExpression; }

    public DispatchType getDispatchType() { return dispatchType; }

    public void setDispatchType(DispatchType dispatchType) { this.dispatchType = dispatchType; }

    public String getHttpServiceName() { return httpServiceName; }

    public void setHttpServiceName(String httpServiceName) { this.httpServiceName = httpServiceName; }

    public String getHttpPath() { return httpPath; }

    public void setHttpPath(String httpPath) { this.httpPath = httpPath; }

    public Integer getTimeoutSeconds() { return timeoutSeconds; }

    public void setTimeoutSeconds(Integer timeoutSeconds) { this.timeoutSeconds = timeoutSeconds; }

    public boolean isEnabled() { return enabled; }

    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getVersion() { return version; }

    public void setVersion(int version) { this.version = version; }

    public Date getCreatedAt() { return createdAt; }

    public void setCreatedAt(Date createdAt) { this.createdAt = createdAt; }

    public Date getUpdatedAt() { return updatedAt; }

    public void setUpdatedAt(Date updatedAt) { this.updatedAt = updatedAt; }

    /** 静态内部类避免实体强依赖 Spring 上下文中的 ObjectMapper */
    static final class JsonHolder {
        /** 单例 ObjectMapper，避免每实体构造（Mapper 本身线程安全） */
        private static final com.fasterxml.jackson.databind.ObjectMapper MAPPER = buildMapper();

        private static com.fasterxml.jackson.databind.ObjectMapper buildMapper() {
            com.fasterxml.jackson.databind.ObjectMapper m =
                    new com.fasterxml.jackson.databind.ObjectMapper();
            // 未知字段反序列化时不抛异常，增强对前端附加字段的容忍
            m.disable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
            return m;
        }

        static Map<String, Object> parse(String json) {
            if (json == null || json.trim().isEmpty()) {
                return new LinkedHashMap<String, Object>();
            }
            try {
                Map<String, Object> m = MAPPER.readValue(json, Map.class);
                return m == null ? new LinkedHashMap<String, Object>() : m;
            } catch (Exception e) {
                return new LinkedHashMap<String, Object>();
            }
        }

        static String toJson(Map<String, Object> params) {
            if (params == null || params.isEmpty()) {
                return null;
            }
            try {
                return MAPPER.writeValueAsString(params);
            } catch (Exception e) {
                return null;
            }
        }
    }
}

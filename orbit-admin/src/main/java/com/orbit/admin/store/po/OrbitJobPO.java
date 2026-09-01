package com.orbit.admin.store.po;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.annotation.Version;

import java.io.Serializable;
import java.util.Date;

/**
 * 任务定义表 {@code orbit_job} 的持久化实体（PO）。
 * 仅在调度中心持久层使用；对外协议模型见 {@code com.orbit.core.model.JobInfo}，
 * 二者通过 {@code JobStore} 做相互转换，保证 orbit-core 不依赖任何 ORM 框架。
 * 其中 {@code params} 以 JSON 字符串落库，{@code version} 为乐观锁版本号。
 */
@TableName("orbit_job")
public class OrbitJobPO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 主键自增 ID */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /** 任务唯一名 */
    private String jobName;

    /** 任务描述；可空，更新策略设为 IGNORED 以允许清空为 NULL */
    @TableField(value = "description", updateStrategy = FieldStrategy.IGNORED)
    private String description;

    /** 目标执行器应用名 */
    private String appName;

    /** JobHandler 名称（对应 @OrbitJob 值） */
    private String handler;

    /** Cron 表达式（为空表示仅手动触发）；可空，更新策略设为 IGNORED 以允许写 NULL */
    @TableField(value = "cron_expr", updateStrategy = FieldStrategy.IGNORED)
    private String cronExpr;

    /** 静态执行参数（JSON 字符串）；可空，更新策略设为 IGNORED 以允许写 NULL */
    @TableField(value = "params", updateStrategy = FieldStrategy.IGNORED)
    private String params;

    /** 执行超时（秒） */
    private Integer timeoutSeconds;

    /** 路由策略：ROUND / RANDOM / FIRST */
    private String routeStrategy;

    /** 是否启用调度 */
    private Boolean enabled;

    /** 乐观锁版本号（MyBatis-Plus 乐观锁插件会在更新时自动 +1 并写入 WHERE 条件） */
    @Version
    private Integer version;

    /** 创建时间 */
    private Date createdAt;

    /** 最近更新时间 */
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

    public String getCronExpr() {
        return cronExpr;
    }

    public void setCronExpr(String cronExpr) {
        this.cronExpr = cronExpr;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public Integer getTimeoutSeconds() {
        return timeoutSeconds;
    }

    public void setTimeoutSeconds(Integer timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public String getRouteStrategy() {
        return routeStrategy;
    }

    public void setRouteStrategy(String routeStrategy) {
        this.routeStrategy = routeStrategy;
    }

    public Boolean getEnabled() {
        return enabled;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
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

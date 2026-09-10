package com.orbit.admin.store.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;
import java.util.Date;

/**
 * 执行器注册表 {@code orbit_executor_registry} 持久化实体。
 * 对齐 XXL-JOB 的 {@code xxl_job_registry}：心跳写入共享库，任意 admin 副本可读，
 * 因此调度中心可以无状态 Deployment，不必 StatefulSet / Headless DNS。
 */
@TableName("orbit_executor_registry")
public class OrbitExecutorRegistryPO implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 主键，数据库自增 */
    @TableId(value = "id", type = IdType.AUTO)
    private Long id;

    /** 执行器应用名，对应 orbit.executor.app-name */
    private String appName;

    /** 执行器通信基地址（http://host:port），派发时直接拼接 /orbit/executor/run */
    private String address;

    /** 节点标识：K8s 下取 POD_NAME，用于区分同一应用的不同副本 */
    private String nodeId;

    /** JobHandler 名称列表，JSON 数组字符串 */
    private String handlers;

    /** 最近一次心跳时间；距今超过 heartbeat-timeout-seconds 即判定失联并被摘除 */
    private Date lastHeartbeat;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getHandlers() {
        return handlers;
    }

    public void setHandlers(String handlers) {
        this.handlers = handlers;
    }

    public Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Date lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }
}

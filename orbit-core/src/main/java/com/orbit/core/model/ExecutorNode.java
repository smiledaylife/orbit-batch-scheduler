package com.orbit.core.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 在线执行器节点模型。
 * <p>表示当前注册在调度中心内存注册表中的一个执行器实例状态信息。
 */
public class ExecutorNode implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 执行器应用名称（例如：order-service），用于任务调度时的应用分组匹配
     */
    private String appName;

    /**
     * 执行器对外暴露的可访问基础 HTTP 地址（例如：http://192.168.1.100:8081）
     */
    private String address;

    /**
     * 执行器节点唯一标识（K8s 中通常对应 POD_NAME，本地对应主机名）
     */
    private String nodeId;

    /**
     * 该执行器节点当前支持并已扫描注册的所有 JobHandler 名称集合
     */
    private List<String> handlers = new ArrayList<String>();

    /**
     * 最近一次接收到心跳的时间戳，用于调度中心后台定时任务判断是否超时离线
     */
    private Date lastHeartbeat;

    /**
     * 节点在线状态标识（true: 在线，false: 离线或下线）
     */
    private boolean online;

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

    public List<String> getHandlers() {
        return handlers;
    }

    public void setHandlers(List<String> handlers) {
        this.handlers = handlers;
    }

    public Date getLastHeartbeat() {
        return lastHeartbeat;
    }

    public void setLastHeartbeat(Date lastHeartbeat) {
        this.lastHeartbeat = lastHeartbeat;
    }

    public boolean isOnline() {
        return online;
    }

    public void setOnline(boolean online) {
        this.online = online;
    }
}

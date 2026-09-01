package com.orbit.core.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 执行器注册 / 心跳上报请求实体。
 * 执行器启动或周期性心跳时，向调度中心 /orbit/admin/registry 发送此实体，
 * 声明自身的应用名、通信网络地址、支持的 JobHandler 列表以及安全访问凭证。
 */
public class RegistryRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 执行器应用名称（例如：order-service），用于任务调度时的应用分组匹配
     */
    private String appName;

    /**
     * 执行器对外暴露的可访问基础 HTTP 地址（例如：http://10.0.1.8:8081）
     */
    private String address;

    /**
     * 本机节点唯一标识（K8s 中对应 POD_NAME，物理机/虚拟机对应主机名或随机 ID）
     */
    private String nodeId;

    /**
     * 当前节点本地扫描并注册的所有 JobHandler 处理函数名称列表
     */
    private List<String> handlers = new ArrayList<String>();

    /**
     * 安全访问令牌（与调度中心 orbit.admin.access-token 配置一致时方可通过鉴权）
     */
    private String accessToken;

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

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
}

package com.orbit.core.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 执行器注册 / 心跳请求。
 */
public class RegistryRequest implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 执行器应用名（对应任务的 appName 路由） */
    private String appName;
    /** 执行器地址，如 http://10.0.1.8:8081 */
    private String address;
    /** 本机节点标识 */
    private String nodeId;
    /** 已注册的 JobHandler 名称列表 */
    private List<String> handlers = new ArrayList<String>();
    /** 访问令牌（与 admin 配置一致） */
    private String accessToken;

    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public List<String> getHandlers() { return handlers; }
    public void setHandlers(List<String> handlers) { this.handlers = handlers; }
    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
}

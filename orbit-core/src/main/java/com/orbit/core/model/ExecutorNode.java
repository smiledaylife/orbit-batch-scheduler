package com.orbit.core.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 在线执行器节点。
 */
public class ExecutorNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private String appName;
    private String address;
    private String nodeId;
    private List<String> handlers = new ArrayList<String>();
    private Date lastHeartbeat;
    private boolean online;

    public String getAppName() { return appName; }
    public void setAppName(String appName) { this.appName = appName; }
    public String getAddress() { return address; }
    public void setAddress(String address) { this.address = address; }
    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public List<String> getHandlers() { return handlers; }
    public void setHandlers(List<String> handlers) { this.handlers = handlers; }
    public Date getLastHeartbeat() { return lastHeartbeat; }
    public void setLastHeartbeat(Date lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }
    public boolean isOnline() { return online; }
    public void setOnline(boolean online) { this.online = online; }
}

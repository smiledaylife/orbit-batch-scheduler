package com.orbit.admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 调度中心配置 orbit.admin.*
 */
@Component
@ConfigurationProperties(prefix = "orbit.admin")
public class AdminProperties {

    /** 访问令牌，执行器注册/触发时校验；空则不校验 */
    private String accessToken = "";
    /** 执行器心跳超时（秒），超时视为离线 */
    private int heartbeatTimeoutSeconds = 90;
    /** 触发执行器 HTTP 连接超时毫秒 */
    private int connectTimeoutMs = 3000;
    /** 触发执行器默认读超时毫秒（任务级 timeout 优先） */
    private int readTimeoutMs = 300000;
    /** Quartz 任务分组 */
    private String group = "ORBIT";
    private String timezone = "Asia/Shanghai";

    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
    public int getHeartbeatTimeoutSeconds() { return heartbeatTimeoutSeconds; }
    public void setHeartbeatTimeoutSeconds(int heartbeatTimeoutSeconds) {
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
    }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int connectTimeoutMs) { this.connectTimeoutMs = connectTimeoutMs; }
    public int getReadTimeoutMs() { return readTimeoutMs; }
    public void setReadTimeoutMs(int readTimeoutMs) { this.readTimeoutMs = readTimeoutMs; }
    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }
    public String getTimezone() { return timezone; }
    public void setTimezone(String timezone) { this.timezone = timezone; }
}

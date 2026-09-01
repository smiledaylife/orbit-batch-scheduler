package com.orbit.admin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 调度中心配置属性类（对应配置前缀：{@code orbit.admin.*}）。
 * 涵盖调度中心鉴权令牌、执行器心跳超时阈值、派发超时控制以及 Quartz 分组和时区配置。
 */
@Component
@ConfigurationProperties(prefix = "orbit.admin")
public class AdminProperties {

    /**
     * 安全访问令牌（Token）。
     * 用于调度中心与执行器之间的鉴权认证：执行器注册/心跳，以及调度中心向执行器派发任务时进行双向校验。
     * 为空时跳过鉴权。
     */
    private String accessToken = "";

    /**
     * 执行器心跳超时剔除时间（秒），默认为 90 秒。
     * 调度中心后台任务将周期性扫描注册表，若节点的最近心跳时间距当前时间超过该阈值，
     * 则判定该执行器实例失联并从内存注册表中剔除。
     */
    private int heartbeatTimeoutSeconds = 90;

    /**
     * 调度中心向执行器发起 HTTP 调用时的建立连接超时时间（毫秒），默认为 3000ms（3秒）。
     */
    private int connectTimeoutMs = 3000;

    /**
     * 调度中心调用执行器时的全局默认数据读取超时时间（毫秒），默认为 300000ms（5分钟）。
     * 若具体任务中单独配置了 {@code timeoutSeconds}，则优先以任务自身的超时配置为准。
     */
    private int readTimeoutMs = 300000;

    /**
     * Quartz 调度框架内部的任务分组名称，默认为 "ORBIT"。
     */
    private String group = "ORBIT";

    /**
     * 定时任务 Cron 表达式计算所依据的时区标识，默认为 "Asia/Shanghai"。
     */
    private String timezone = "Asia/Shanghai";

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public int getHeartbeatTimeoutSeconds() {
        return heartbeatTimeoutSeconds;
    }

    public void setHeartbeatTimeoutSeconds(int heartbeatTimeoutSeconds) {
        this.heartbeatTimeoutSeconds = heartbeatTimeoutSeconds;
    }

    public int getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public void setConnectTimeoutMs(int connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
    }

    public int getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public void setReadTimeoutMs(int readTimeoutMs) {
        this.readTimeoutMs = readTimeoutMs;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}

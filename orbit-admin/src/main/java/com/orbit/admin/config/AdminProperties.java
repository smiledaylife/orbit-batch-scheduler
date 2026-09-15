package com.orbit.admin.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/** 调度中心配置属性类。 */
@Component
@ConfigurationProperties(prefix = "orbit.admin")
public class AdminProperties {

    private String accessToken = "";
    private int heartbeatTimeoutSeconds = 90;
    private int connectTimeoutMs = 3000;
    private String group = "ORBIT";
    private int maxTimeoutSeconds = 3600;
    private String executorAddressAllowPattern = "";
    private String timezone = "Asia/Shanghai";
    private long registryCacheTtlMs = 3000;
    private int logRetentionDays = 30;
    private int dispatchThreads = 64;
    private int dispatchQueueCapacity = 256;
    private boolean dispatchSerialPerJob = true;
    private int triggerTimeoutSeconds = 10;

    /** Spring 当前激活的 profile；仅对 cluster profile 强制生产安全配置。 */
    @Value("${spring.profiles.active:}")
    private String activeProfiles;

    @PostConstruct
    public void validate() {
        if (heartbeatTimeoutSeconds < 5) {
            throw new IllegalStateException("orbit.admin.heartbeat-timeout-seconds must be >= 5");
        }
        if (maxTimeoutSeconds < 1) {
            throw new IllegalStateException("orbit.admin.max-timeout-seconds must be >= 1");
        }
        if (connectTimeoutMs < 1 || triggerTimeoutSeconds < 1) {
            throw new IllegalStateException("orbit.admin HTTP timeout settings must be > 0");
        }
        if (dispatchThreads < 1 || dispatchQueueCapacity < 0) {
            throw new IllegalStateException("orbit.admin dispatch thread/queue settings are invalid");
        }
        if (isClusterProfile()) {
            if (accessToken == null || accessToken.trim().isEmpty()) {
                throw new IllegalStateException("orbit.admin.access-token is required in cluster profile");
            }
            if (executorAddressAllowPattern == null || executorAddressAllowPattern.trim().isEmpty()) {
                throw new IllegalStateException(
                        "orbit.admin.executor-address-allow-pattern is required in cluster profile");
            }
        }
    }

    private boolean isClusterProfile() {
        if (activeProfiles == null) {
            return false;
        }
        for (String profile : activeProfiles.split(",")) {
            if ("cluster".equalsIgnoreCase(profile.trim())) {
                return true;
            }
        }
        return false;
    }

    public String getAccessToken() { return accessToken; }
    public void setAccessToken(String accessToken) { this.accessToken = accessToken; }
    public int getHeartbeatTimeoutSeconds() { return heartbeatTimeoutSeconds; }
    public void setHeartbeatTimeoutSeconds(int value) { this.heartbeatTimeoutSeconds = value; }
    public int getConnectTimeoutMs() { return connectTimeoutMs; }
    public void setConnectTimeoutMs(int value) { this.connectTimeoutMs = value; }
    public String getGroup() { return group; }
    public void setGroup(String value) { this.group = value; }
    public String getTimezone() { return timezone; }
    public void setTimezone(String value) { this.timezone = value; }
    public int getMaxTimeoutSeconds() { return maxTimeoutSeconds; }
    public void setMaxTimeoutSeconds(int value) { this.maxTimeoutSeconds = value; }
    public String getExecutorAddressAllowPattern() { return executorAddressAllowPattern; }
    public void setExecutorAddressAllowPattern(String value) { this.executorAddressAllowPattern = value; }
    public long getRegistryCacheTtlMs() { return registryCacheTtlMs; }
    public void setRegistryCacheTtlMs(long value) { this.registryCacheTtlMs = value; }
    public int getLogRetentionDays() { return logRetentionDays; }
    public void setLogRetentionDays(int value) { this.logRetentionDays = value; }
    public int getDispatchThreads() { return dispatchThreads; }
    public void setDispatchThreads(int value) { this.dispatchThreads = value; }
    public int getDispatchQueueCapacity() { return dispatchQueueCapacity; }
    public void setDispatchQueueCapacity(int value) { this.dispatchQueueCapacity = value; }
    public boolean isDispatchSerialPerJob() { return dispatchSerialPerJob; }
    public void setDispatchSerialPerJob(boolean value) { this.dispatchSerialPerJob = value; }
    public int getTriggerTimeoutSeconds() { return triggerTimeoutSeconds; }
    public void setTriggerTimeoutSeconds(int value) { this.triggerTimeoutSeconds = value; }
}

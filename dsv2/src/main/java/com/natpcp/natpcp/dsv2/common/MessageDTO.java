/*
 * AICode Begin
 * MessageDTO：监控消息统一信封（源码入驻）。
 *
 * 原类型来自二方 jar com.cd:monitor-agent-api（com.cd.monitor.agent.common.model 包）。
 * 遵守本项目「零外部二方 jar」约束，以源码形式入驻、保持原包名与结构，
 * 用于无监控 agent 环境下手动推送 Kafka（monitor_common_bus 主题）的消息壳。
 *
 * Kafka 消息 JSON 示例：
 * {"tableName":"TB_MONITOR_COMMON_TARGET_ERR","isArray":false,
 *  "data":{...MonitorCommonTargetErrDTO...},"version":"2.0"}
 */
package com.natpcp.natpcp.dsv2.common;

/**
 * 监控消息信封，包装业务 DTO 并携带路由表名与消息版本。
 *
 * @param <T> 业务消息体类型
 */
public class MessageDTO<T> {

    /** 消费侧业务表名。 */
    private String tableName;

    /** 是否批量消息。 */
    private Boolean isArray;

    /** 业务消息体（单条时为业务 DTO，批量时为 List）。 */
    private T data;

    /** 消息版本（当前固定 2.0）。 */
    private String version;

    public MessageDTO() {
    }

    public MessageDTO(String tableName, T data) {
        this.tableName = tableName;
        this.data = data;
        this.version = "2.0";
        this.isArray = false;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Boolean getIsArray() {
        return isArray;
    }

    public void setIsArray(Boolean isArray) {
        this.isArray = isArray;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "MessageDTO{" +
                "tableName='" + tableName + '\'' +
                ", isArray=" + isArray +
                ", data=" + data +
                ", version='" + version + '\'' +
                '}';
    }
}
/*
 * AICode End
 */

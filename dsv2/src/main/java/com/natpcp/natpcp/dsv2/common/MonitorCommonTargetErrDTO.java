/*
 * AICode Begin
 * MonitorCommonTargetErrDTO：监控中心通用目标错误 DTO（源码入驻）。
 *
 * 原类型来自二方 jar com.cd:monitor-agent-api（com.cd.monitor.api.model 包）。
 * 遵守本项目「零外部二方 jar」约束，以源码形式入驻、保持原包名与字段结构，
 * 用于无监控 agent 环境下手动推送 Kafka（monitor_common_bus 主题）的消息体。
 */
package com.natpcp.natpcp.dsv2.common;

/**
 * 监控中心通用目标错误 DTO。
 *
 * <p>消息结构中 data 字段的业务载体，承载单个业务监控告警事件。
 */
public class MonitorCommonTargetErrDTO {

    /** 对应消费侧业务表名（监控中心统一注册表名）。 */
    public static final String TABLE_NAME = "TB_MONITOR_COMMON_TARGET_ERR";

    /** 目标编码。 */
    private String targetCode;

    /** 目标名称。 */
    private String targetName;

    /** 中心标志（HF/YZ/FT/LF，对应 framework.dataCenter 映射）。 */
    private String centerFlag;

    /** 子项编码（通常与目标编码相同）。 */
    private String subitemCode;

    /** 清算日期（yyyyMMdd）。 */
    private String clrDate;

    /** 采集时间（yyyyMMddHHmmss）。 */
    private String collectDateTime;

    /** 错误信息。 */
    private String errMsg;

    /** 告警状态。 */
    private Boolean alarmStatus;

    public String getTableName() {
        return TABLE_NAME;
    }

    public String getTargetCode() {
        return targetCode;
    }

    public void setTargetCode(String targetCode) {
        this.targetCode = targetCode;
    }

    public String getTargetName() {
        return targetName;
    }

    public void setTargetName(String targetName) {
        this.targetName = targetName;
    }

    public String getCenterFlag() {
        return centerFlag;
    }

    public void setCenterFlag(String centerFlag) {
        this.centerFlag = centerFlag;
    }

    public String getSubitemCode() {
        return subitemCode;
    }

    public void setSubitemCode(String subitemCode) {
        this.subitemCode = subitemCode;
    }

    public String getClrDate() {
        return clrDate;
    }

    public void setClrDate(String clrDate) {
        this.clrDate = clrDate;
    }

    public String getCollectDateTime() {
        return collectDateTime;
    }

    public void setCollectDateTime(String collectDateTime) {
        this.collectDateTime = collectDateTime;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public Boolean getAlarmStatus() {
        return alarmStatus;
    }

    public void setAlarmStatus(Boolean alarmStatus) {
        this.alarmStatus = alarmStatus;
    }

    @Override
    public String toString() {
        return "MonitorCommonTargetErrDTO{" +
                "targetCode='" + targetCode + '\'' +
                ", targetName='" + targetName + '\'' +
                ", centerFlag='" + centerFlag + '\'' +
                ", subitemCode='" + subitemCode + '\'' +
                ", clrDate='" + clrDate + '\'' +
                ", collectDateTime='" + collectDateTime + '\'' +
                ", errMsg='" + errMsg + '\'' +
                ", alarmStatus=" + alarmStatus +
                '}';
    }
}
/*
 * AICode End
 */

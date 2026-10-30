/*
 * AICode Begin
 * ServiceTool：监控服务工具（源码入驻）。
 *
 * 原类型来自二方 jar com.cd:monitor-agent-api（com.cd.monitor.api.tool 包），
 * 原实现由监控 agent 字节码增强注入真实值（getCenterFlag 等）。
 * 遵守本项目「零外部二方 jar」约束，以源码形式入驻、保持原包名与 API。
 *
 * 无 agent 环境下 getCenterFlag() 按 framework.dataCenter 配置映射中心标志：
 *   H(华南) -> HF，Y(庄行) -> YZ，F(分台) -> FT，L(南丰) -> LF；
 * 默认（未配置 / 无法识别）返回 HF，与监控中心枚举 CenterFlagEnum 对齐。
 */
package com.natpcp.natpcp.dsv2.common;

import lombok.extern.slf4j.Slf4j;

/**
 * 监控服务工具：提供中心标志等平台信息读取。
 */
@Slf4j
public class ServiceTool {

    /** 中心标志：华南。 */
    public static final String HF = "HF";
    /** 中心标志：庄行。 */
    public static final String YZ = "YZ";
    /** 中心标志：分台。 */
    public static final String FT = "FT";
    /** 中心标志：南丰。 */
    public static final String LF = "LF";

    private static volatile String centerFlag = HF;

    private ServiceTool() {
    }

    /**
     * 设置中心标志（由 Spring 配置 framework.dataCenter 初始化时调用）。
     *
     * @param dataCenter 数据中心的 single char 编码（H/Y/F/L）
     */
    public static void initCenterFlag(String dataCenter) {
        if (dataCenter == null || dataCenter.isEmpty()) {
            log.warn("[ServiceTool] framework.dataCenter 未配置，centerFlag 使用默认值 {}", centerFlag);
            return;
        }
        String mapped = mapToCenterFlag(dataCenter.trim().toUpperCase());
        if (mapped == null) {
            log.warn("[ServiceTool] 无法识别 dataCenter={}，centerFlag 使用默认值 {}", dataCenter, HF);
            return;
        }
        centerFlag = mapped;
        log.info("[ServiceTool] centerFlag 已初始化：dataCenter={} -> {}", dataCenter, centerFlag);
    }

    /**
     * 获取中心标志（HF/YZ/FT/LF）。
     *
     * @return 中心标志编码
     */
    public static String getCenterFlag() {
        return centerFlag;
    }

    private static String mapToCenterFlag(String dataCenter) {
        return switch (dataCenter) {
            case "H" -> HF;
            case "Y" -> YZ;
            case "F" -> FT;
            case "L" -> LF;
            default -> null;
        };
    }
}
/*
 * AICode End
 */

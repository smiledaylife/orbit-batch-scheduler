package com.orbit.core.model;

/**
 * 多实例路由策略常量。
 * <p>
 * 集中定义路由策略取值（ROUND / RANDOM / FIRST），
 * 供调度中心入参校验与路由分发统一引用，避免魔法字符串漂移。
 * 常量值为协议的一部分，保持字符串形式以兼容既有 JSON 协议与历史数据。
 */
public final class RouteStrategy {

    /** 轮询（默认）：按地址序轮流分发 */
    public static final String ROUND = "ROUND";

    /** 随机：在在线节点中等概率选取 */
    public static final String RANDOM = "RANDOM";

    /** 首节点：固定选取地址序第一个在线节点 */
    public static final String FIRST = "FIRST";

    private RouteStrategy() {
    }
}

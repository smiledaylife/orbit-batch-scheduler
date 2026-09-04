package com.orbit.core.model;

/**
 * 调度执行日志状态常量。
 * <p>
 * 集中定义原先散落在调度中心各处的魔法字符串（"RUNNING" / "SUCCESS" / "FAILED"），
 * 保证日志状态值在写入、更新、查询与 API 响应之间始终一致。
 * 常量值为协议的一部分，保持字符串形式以兼容既有 JSON 协议与历史数据。
 */
public final class JobLogStatus {

    /** 执行中（日志首插状态，派发开始时写入） */
    public static final String RUNNING = "RUNNING";

    /** 执行成功（终态） */
    public static final String SUCCESS = "SUCCESS";

    /** 执行失败（终态，含路由失败、执行异常、超时等） */
    public static final String FAILED = "FAILED";

    private JobLogStatus() {
    }
}

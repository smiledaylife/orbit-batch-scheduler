package com.orbit.admin.alert;

import java.util.Date;

/**
 * 任务失败告警事件（不可变值对象，JDK 21 record）。
 *
 * 由调度中心在任务链路到达「最终失败」时构造，经 {@link AlertDispatcher}
 * 异步投递给全部已注册的 {@link OrbitAlertHandler}。字段是告警渠道需要的最小集合：
 * 定位任务（jobName / handler / appName）、定位链路（logId）、定位现场（executorAddress、
 * costMs、message、eventTime）。后续如需扩展字段，直接在 record 上加组件即可，
 * 处理器实现不受影响（解构与访问器自动生成）。
 *
 * eventType 取值见本类的常量；保持字符串形式与框架内其它协议常量
 * （{@code JobLogStatus} / {@code RouteStrategy}）风格一致。
 */
public record JobAlertEvent(
        /** 事件类型：见 {@link #TRIGGER_FAILED} / {@link #EXECUTION_FAILED} / {@link #EXECUTION_LOST} */
        String eventType,
        /** 任务名称 */
        String jobName,
        /** 执行器应用名 */
        String appName,
        /** JobHandler 名称 */
        String handler,
        /** 调度日志追踪 ID（触发级重试时每次尝试各有一个 logId，最终失败的是最后一条） */
        String logId,
        /** 派发/承接的执行器地址（可能为 null：如未找到在线节点） */
        String executorAddress,
        /** 执行耗时（毫秒；触发级失败为 0 或触发往返耗时） */
        long costMs,
        /** 失败原因或回收说明 */
        String message,
        /** 事件产生时间 */
        Date eventTime) {

    /** 触发失败：向执行器派发同步失败（含触发级重试耗尽）或派发通道饱和拒绝 */
    public static final String TRIGGER_FAILED = "TRIGGER_FAILED";

    /** 执行失败：任务真正执行了，但最终失败（执行器侧重试已耗尽后的回传结果） */
    public static final String EXECUTION_FAILED = "EXECUTION_FAILED";

    /** 执行结果丢失：日志悬挂 RUNNING 被孤儿回收（执行器崩溃/回传丢失/超过最大超时） */
    public static final String EXECUTION_LOST = "EXECUTION_LOST";
}

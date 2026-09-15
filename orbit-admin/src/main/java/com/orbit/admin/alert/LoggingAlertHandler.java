package com.orbit.admin.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认告警实现：仅把失败事件以 WARN 级别写入调度中心自身日志。
 *
 * 作用：
 *
 *   - 作为 {@code OrbitAlertHandler} 的兜底实现，保证未接入任何告警渠道时
 *     失败事件在日志里可见、可被日志采集器（ELK / Loki 等）收集；
 *   - 业务侧注册了自己的 {@link OrbitAlertHandler} Bean 后，本实现自动让位，
 *     不会重复投递（见 AlertConfig 的 {@code @ConditionalOnMissingBean}）。
 *
 * 刻意不做的事：不打堆栈（事件本身已携带现场信息）、不做节流去重
 * （同一任务连续失败会产生多条事件，需要去重请在自定义实现里做，
 * 例如按 jobName + eventType 做滑动窗口抑制）。
 */
public class LoggingAlertHandler implements OrbitAlertHandler {

    private static final Logger log = LoggerFactory.getLogger(LoggingAlertHandler.class);

    /** 事件类型 -> 中文说明，用于日志可读性 */
    private static String describe(String eventType) {
        if (JobAlertEvent.EXECUTION_FAILED.equals(eventType)) {
            return "任务执行失败";
        }
        if (JobAlertEvent.TRIGGER_FAILED.equals(eventType)) {
            return "任务触发失败";
        }
        if (JobAlertEvent.EXECUTION_LOST.equals(eventType)) {
            return "任务执行结果丢失";
        }
        return "任务告警";
    }

    @Override
    public void onAlert(JobAlertEvent event) {
        log.warn("[orbit-alert] {} type={} job={} handler={} app={} logId={} executor={} costMs={} message={}",
                describe(event.eventType()), event.eventType(), event.jobName(), event.handler(),
                event.appName(), event.logId(), event.executorAddress(), event.costMs(), event.message());
    }
}

package com.orbit.admin.alert;

/**
 * 任务失败告警扩展点（SPI）。
 *
 * 业务侧接入方式：实现本接口并注册为 Spring Bean 即可，
 * 调度中心内置的 {@link LoggingAlertHandler}（只打 WARN 日志）会自动让位
 * （{@code @ConditionalOnMissingBean} 语义），无需任何额外配置。
 *
 * 典型实现：对接钉钉/企业微信/飞书群机器人、SMTP 邮件、PagerDuty /
 * Prometheus Alertmanager webhook、自研告警网关等。
 *
 * 调用契约（由 {@link AlertDispatcher} 保证）：
 *
 *   - 异步：onAlert 在独立的分发线程上执行，绝不阻塞调度主链路
 *     （派发、回传、回收线程只做事件入队）；
 *   - 隔离：实现内部抛出的任何异常都会被捕获并计数，不会影响其它处理器与调度流程；
 *   - 有界：分发队列满时新事件被丢弃并计数（fail-open），因此本方法是
 *     「尽力而为」语义 —— 需要强一致的告警请自行在实现内做重试与落盘。
 *
 * 实现要求：线程安全（单线程分发模型下串行调用，但实现方不应依赖调用线程）；
 * 尽量快返回，慢 IO 请自行异步化，否则队列会积压进而触发丢弃。
 */
public interface OrbitAlertHandler {

    /**
     * 处理一条任务失败告警事件。
     *
     * @param event 告警事件（不可变，可安全持有）
     */
    void onAlert(JobAlertEvent event);
}

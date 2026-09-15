package com.orbit.admin.alert;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 告警异步分发器：把「失败事件产生」与「告警渠道投递」解耦。
 *
 * 设计目标：告警绝不能反噬调度主链路。派发、回传、孤儿回收线程只做一次
 * 有界队列入队（纳秒级）即返回；真正的渠道 IO（HTTP/SMTP）在专职分发线程上串行执行。
 *
 * 三项保障：
 *
 * 1. 异步：fire() 永不阻塞（队列满直接丢弃并计数），
 *    「调度中心因为告警渠道卡顿而拖延任务派发」这类事故从结构上不可能发生；
 * 2. 隔离：处理器实现抛出的任何异常（含 Error 之外的运行时异常）都在分发线程内消化，
 *    计入 alertsFailed，不影响后续事件与调度流程；
 * 3. 可观测：fire / dropped / failed 计数经 metrics() 暴露到 /orbit/admin/overview，
 *    dropped 持续增长说明渠道处理能力不足（实现太慢或队列太小），应扩容处理端。
 *
 * 线程模型：单条平台守护线程（orbit-alert-dispatcher）串行消费。
 * 告警频率通常在「次/分钟」量级，单线程足够；处理器慢导致的积压通过
 * 有界队列 + 丢弃计数显式暴露，而不是无界堆积内存。
 */
public class AlertDispatcher implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(AlertDispatcher.class);

    /** 事件队列容量：超出即丢弃（告警是尽力而为语义，不允许无限堆积） */
    private static final int QUEUE_CAPACITY = 256;

    /** 停机时等待队列发空的最长时间（秒） */
    private static final int SHUTDOWN_GRACE_SECONDS = 3;

    private final OrbitAlertHandler handler;

    private final BlockingQueue<JobAlertEvent> queue = new LinkedBlockingQueue<JobAlertEvent>(QUEUE_CAPACITY);

    /** 成功入队（不保证已投递成功）的事件数 */
    private final AtomicLong firedCount = new AtomicLong();

    /** 因队列满被丢弃的事件数 */
    private final AtomicLong droppedCount = new AtomicLong();

    /** 处理器执行抛异常的事件数 */
    private final AtomicLong failedCount = new AtomicLong();

    /** 已成功投递（处理器正常返回）的事件数 */
    private final AtomicLong deliveredCount = new AtomicLong();

    private final Thread worker;

    private volatile boolean running = true;

    /**
     * @param handler 告警处理器（至少存在内置 LoggingAlertHandler，由 AlertConfig 保证非空）
     */
    public AlertDispatcher(OrbitAlertHandler handler) {
        this.handler = handler;
        this.worker = new Thread(this::drain, "orbit-alert-dispatcher");
        this.worker.setDaemon(true);
        this.worker.start();
        log.info("[orbit-admin] alert dispatcher initialized: handler={}, queueCapacity={}",
                handler.getClass().getSimpleName(), QUEUE_CAPACITY);
    }

    /**
     * 投递一条告警事件（异步、非阻塞）。
     *
     * 队列满时丢弃并计数 —— 这是有意的 fail-open 设计：告警渠道拥塞时宁可少报
     * 也不能拖住派发线程。丢弃动作本身会记 WARN 日志，运维可通过
     * /overview 的 alertDropped 指标感知。
     *
     * @param event 告警事件；null 直接忽略
     */
    public void fire(JobAlertEvent event) {
        if (event == null) {
            return;
        }
        if (!running) {
            // 停机中：直接丢弃，不再入队（worker 已退出，入队只会泄漏）
            droppedCount.incrementAndGet();
            return;
        }
        if (queue.offer(event)) {
            firedCount.incrementAndGet();
        } else {
            droppedCount.incrementAndGet();
            log.warn("[orbit-admin] alert queue full, event dropped: type={} job={} logId={}",
                    event.eventType(), event.jobName(), event.logId());
        }
    }

    /** 分发线程主循环：取事件 -> 调处理器 -> 异常消化计数 */
    private void drain() {
        while (running || !queue.isEmpty()) {
            try {
                JobAlertEvent event = queue.poll(500, TimeUnit.MILLISECONDS);
                if (event == null) {
                    continue;
                }
                try {
                    handler.onAlert(event);
                    deliveredCount.incrementAndGet();
                } catch (Throwable t) {
                    // 处理器异常必须消化在这里：让它逃出去会杀死分发线程，
                    // 之后所有告警都只会堆在队列里直到占满被丢弃。
                    failedCount.incrementAndGet();
                    log.error("[orbit-admin] alert handler {} failed for event type={} job={}: {}",
                            handler.getClass().getSimpleName(), event.eventType(), event.jobName(),
                            t.getMessage(), t);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * 告警通道运行指标，供 /orbit/admin/overview 暴露。
     *
     * @return 指标字典（fired / delivered / dropped / failed / queueSize）
     */
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("alertHandler", handler.getClass().getSimpleName());
        m.put("alertFired", firedCount.get());
        m.put("alertDelivered", deliveredCount.get());
        m.put("alertDropped", droppedCount.get());
        m.put("alertFailed", failedCount.get());
        m.put("alertQueueSize", queue.size());
        return m;
    }

    /**
     * Spring 容器销毁回调：停止接新事件，给队列中剩余事件最多 3 秒投递窗口。
     * 中断只恢复中断位，不让停机阶段的告警收尾异常打断容器关闭流程。
     */
    @Override
    public void destroy() {
        running = false;
        try {
            worker.join(TimeUnit.SECONDS.toMillis(SHUTDOWN_GRACE_SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        int left = queue.size();
        if (left > 0) {
            log.warn("[orbit-admin] alert dispatcher shutdown with {} event(s) undelivered", left);
        }
    }
}

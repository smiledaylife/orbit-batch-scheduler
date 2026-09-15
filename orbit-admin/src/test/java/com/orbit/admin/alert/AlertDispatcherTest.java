package com.orbit.admin.alert;

import org.junit.jupiter.api.Test;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AlertDispatcher} 异步分发契约测试。
 * 重点回归：异步投递、处理器异常隔离、停机后丢弃、指标计数。
 */
class AlertDispatcherTest {

    private static JobAlertEvent event(String type, String jobName) {
        return new JobAlertEvent(type, jobName, "app", "handler", "log-1", "http://n:8081",
                10L, "message", new Date());
    }

    @Test
    void fireDeliversEventAsynchronously() throws Exception {
        List<JobAlertEvent> received = new CopyOnWriteArrayList<JobAlertEvent>();
        AlertDispatcher dispatcher = new AlertDispatcher(received::add);

        dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "jobA"));

        long deadline = System.currentTimeMillis() + 2000L;
        while (received.isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(1, received.size());
        assertEquals("jobA", received.get(0).jobName());
        assertEquals(JobAlertEvent.EXECUTION_FAILED, received.get(0).eventType());
        dispatcher.destroy();
    }

    /** 处理器抛异常必须被消化：不影响后续事件，也不杀死分发线程。 */
    @Test
    void handlerFailureIsIsolatedAndCounted() throws Exception {
        List<JobAlertEvent> received = new CopyOnWriteArrayList<JobAlertEvent>();
        OrbitAlertHandler flaky = e -> {
            received.add(e);
            if (e.jobName().equals("boom")) {
                throw new IllegalStateException("channel down");
            }
        };
        AlertDispatcher dispatcher = new AlertDispatcher(flaky);

        dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "boom"));
        dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "fine"));

        long deadline = System.currentTimeMillis() + 2000L;
        while (received.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20L);
        }
        assertEquals(2, received.size(), "second event must still be delivered");
        // 失败的那条计入 failed；成功投递的只有一条
        assertEquals(1L, dispatcher.metrics().get("alertFailed"));
        assertEquals(1L, dispatcher.metrics().get("alertDelivered"));
        dispatcher.destroy();
    }

    /** null 事件直接忽略；fire 不阻塞。 */
    @Test
    void nullEventIsIgnored() {
        List<JobAlertEvent> received = new CopyOnWriteArrayList<JobAlertEvent>();
        AlertDispatcher dispatcher = new AlertDispatcher(received::add);
        dispatcher.fire(null);
        assertEquals(0L, dispatcher.metrics().get("alertFired"));
        dispatcher.destroy();
    }

    /** 队列满时新事件被丢弃并计数（fail-open），绝不阻塞调用方。 */
    @Test
    void overflowDropsEventAndCounts() throws Exception {
        // 处理器阻塞在第一帧，队列（容量 256）填满后再 fire 一条即被丢弃
        java.util.concurrent.CountDownLatch release = new java.util.concurrent.CountDownLatch(1);
        OrbitAlertHandler blocked = e -> {
            try {
                release.await();
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        };
        AlertDispatcher dispatcher = new AlertDispatcher(blocked);
        try {
            // 工作线程何时完成首次 poll 与填充循环存在竞态：队列剩余空位在 255~256 之间，
            // 因此这里断言语义（发生丢弃且计数准确、fire 不阻塞），不断言精确次数
            for (int i = 0; i < 256; i++) {
                dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "fill-" + i));
            }
            dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "extra-1"));
            dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "extra-2"));

            long deadline = System.currentTimeMillis() + 2000L;
            long dropped = 0L;
            while (System.currentTimeMillis() < deadline) {
                dropped = (Long) dispatcher.metrics().get("alertDropped");
                if (dropped >= 1L) {
                    break;
                }
                Thread.sleep(10L);
            }
            assertTrue(dropped >= 1L, "overflow must drop events (fail-open), dropped=" + dropped);
            assertTrue(dropped <= 2L, "drop count must match capacity math, dropped=" + dropped);
            // fired 只统计成功入队的事件：fired + dropped == 总投递数
            assertEquals(258L, (Long) dispatcher.metrics().get("alertFired") + dropped);
        } finally {
            release.countDown();
            dispatcher.destroy();
        }
        assertTrue(((Long) dispatcher.metrics().get("alertDropped")) >= 1L);
    }

    /** destroy 后新事件直接丢弃，不会在停机后继续投递。 */
    @Test
    void fireAfterDestroyIsDropped() throws Exception {
        List<JobAlertEvent> received = new CopyOnWriteArrayList<JobAlertEvent>();
        AlertDispatcher dispatcher = new AlertDispatcher(received::add);
        dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "before"));
        Thread.sleep(100L);
        dispatcher.destroy();
        int before = received.size();

        dispatcher.fire(event(JobAlertEvent.EXECUTION_FAILED, "after"));

        assertEquals(before, received.size(), "no delivery after destroy");
        assertTrue(dispatcher.metrics().get("alertDropped").equals(1L));
    }
}

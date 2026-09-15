package com.orbit.admin.dispatch;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OutstandingDispatches} 单元测试。
 *
 * 重点回归：logId 预登记语义。
 * 历史缺陷：logId 在「拿到受理回执之后」才登记（bind），而执行器可能毫秒级
 * 跑完任务并回传 —— 回传线程先到时 release 查不到映射而空过，随后 bind 把
 * 槽位永久占用：该任务从此不再被 Cron 触发，且孤儿回收只扫 RUNNING 日志，
 * 无法兜底。修复后 logId 在派发前随 {@code tryAcquire} 一次性登记，
 * 回传必然晚于登记，不存在竞态窗口。
 */
class OutstandingDispatchesTest {

    @Test
    void tryAcquireRegistersBothMappingsAtomically() {
        OutstandingDispatches o = new OutstandingDispatches();

        assertTrue(o.tryAcquire("jobA", "log-1"));
        assertEquals(1, o.outstanding());

        // 预登记后，按 logId 的回传释放必须立刻生效（无需等待任何 bind）
        assertTrue(o.release("log-1"));
        assertEquals(0, o.outstanding());
    }

    @Test
    void secondAcquireOnSameJobIsRejectedAndCounted() {
        OutstandingDispatches o = new OutstandingDispatches();

        assertTrue(o.tryAcquire("jobA", "log-1"));
        assertFalse(o.tryAcquire("jobA", "log-2"));
        assertEquals(1L, o.skipped());
        assertEquals(1, o.outstanding());

        // 释放的是第一个 logId；被拒绝的 log-2 从未占用过槽位
        assertTrue(o.release("log-1"));
        assertFalse(o.release("log-2"));
        assertEquals(0, o.outstanding());
    }

    @Test
    void releaseIsIdempotentAndToleratesUnknownLogId() {
        OutstandingDispatches o = new OutstandingDispatches();

        assertTrue(o.tryAcquire("jobA", "log-1"));
        assertTrue(o.release("log-1"));
        // 重复释放、未登记 logId、null 均为安全空操作
        assertFalse(o.release("log-1"));
        assertFalse(o.release("log-unknown"));
        assertFalse(o.release(null));
        assertEquals(0, o.outstanding());
    }

    @Test
    void releaseByJobCleansBothMappings() {
        OutstandingDispatches o = new OutstandingDispatches();

        assertTrue(o.tryAcquire("jobA", "log-1"));
        o.releaseByJob("jobA");
        assertEquals(0, o.outstanding());

        // 槽位已清空：按 logId 的释放与再次占用都正常
        assertFalse(o.release("log-1"));
        assertTrue(o.tryAcquire("jobA", "log-2"));
        assertEquals(1, o.outstanding());
        assertTrue(o.release("log-2"));
    }

    @Test
    void nullJobNameIsAlwaysAcquirable() {
        OutstandingDispatches o = new OutstandingDispatches();
        assertTrue(o.tryAcquire(null, "log-1"));
        assertEquals(0, o.outstanding());
    }

    /**
     * 并发回归：同一任务的并发 tryAcquire 必须恰好一个胜出（其余全部被跳过），
     * 且胜出者登记的 logId 立即可被回传线程 release（无 bind 窗口）。
     */
    @Test
    void concurrentAcquireHasExactlyOneWinnerAndImmediateRelease() throws Exception {
        final OutstandingDispatches o = new OutstandingDispatches();
        final int threads = 16;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch start = new CountDownLatch(1);
        final CountDownLatch done = new CountDownLatch(threads);
        final AtomicInteger winners = new AtomicInteger();
        // 胜出者登记的 logId（多线程只会有一个写入，竞争写安全）
        final java.util.concurrent.atomic.AtomicReference<String> winnerLogId =
                new java.util.concurrent.atomic.AtomicReference<String>();

        for (int i = 0; i < threads; i++) {
            final String logId = "log-" + i;
            pool.execute(() -> {
                try {
                    start.await();
                    if (o.tryAcquire("jobC", logId)) {
                        winners.incrementAndGet();
                        winnerLogId.set(logId);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }
        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "all workers should finish");
        pool.shutdownNow();

        assertEquals(1, winners.get(), "exactly one dispatcher may win the slot");
        assertEquals(threads - 1, o.skipped());
        // 胜出者的 logId 必须立即可释放（预登记语义），且释放后无残留
        assertTrue(o.release(winnerLogId.get()), "release must succeed immediately after acquire");
        assertEquals(0, o.outstanding(), "release by the winner must leave no residue");
    }
}

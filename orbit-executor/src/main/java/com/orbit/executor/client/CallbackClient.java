package com.orbit.executor.client;

import com.orbit.core.model.TriggerResult;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 执行结果回传客户端：任务跑完后把最终结果异步推回调度中心。
 *
 * 触发是「受理即返回」的，调度中心把日志留在 RUNNING，任务的真实成败只能靠本组件回传，
 * 因此回传失败等于这条调度日志一直停在 RUNNING，直到调度中心的孤儿回收把它判失败。
 *
 * 四项设计：
 *
 * 1. 解耦：结果先进有界内存队列，由专职守护线程发送，不占用业务工作线程；
 * 2. 批量：发送前把队列里已积压的结果一次性打包（上限 {@link #BATCH_LIMIT}）成一个请求，
 *    调度中心积压恢复后不必一条一条补发；
 * 3. 重试不丢：单批失败按 callback-retry-interval-ms 退避重试 callback-retry-times 次；
 *    仍失败则把整批塞回队列并退避，靠队列容量做背压，而不是直接丢弃 ——
 *    只有队列真的满了才丢弃，且丢弃时打 ERROR 并带上 logId；
 * 4. 复用出口：寻址、鉴权请求头与 RestTemplate 全部交给 {@link AdminClient}，
 *    本类只负责排队、批量与重试策略。
 *
 * 调度中心侧对同一 logId 的回传是幂等的（只允许 RUNNING 收敛到终态），
 * 因此重试与重复发送都不会污染日志，重发是安全的。
 */
public class CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(CallbackClient.class);

    /** 回传端点路径 */
    private static final String CALLBACK_PATH = "/orbit/admin/callback";

    /** 单个回传请求最多携带的结果条数 */
    private static final int BATCH_LIMIT = 200;

    /** 执行器配置：队列容量与重试策略 */
    private final ExecutorProperties properties;

    /** 调度中心 HTTP 出口，回传请求全部经它发出 */
    private final AdminClient adminClient;

    /** 待回传队列，有界；容量见 orbit.executor.callback-queue-capacity */
    private final LinkedBlockingQueue<TriggerResult> pending;

    /** 发送线程 */
    private final Thread sender;

    /** 发送线程运行标志；置 false 后循环仍会把队列里剩余的结果发完 */
    private volatile boolean running = true;

    /** 累计发送成功条数 */
    private final AtomicLong sentCount = new AtomicLong();

    /** 累计丢弃条数（仅在队列满时发生） */
    private final AtomicLong droppedCount = new AtomicLong();

    /**
     * 构造回传客户端并立即启动发送线程。
     *
     * 队列容量取 {@code callback-queue-capacity} 的下限保护值（至少 1）：
     * {@link LinkedBlockingQueue} 要求容量为正，误配 0 或负数会在 Bean 创建阶段就抛异常。
     *
     * @param properties  执行器配置，提供队列容量与重试策略
     * @param adminClient 调度中心 HTTP 出口，寻址与鉴权头都由它负责
     */
    public CallbackClient(ExecutorProperties properties, AdminClient adminClient) {
        this.properties = properties;
        this.adminClient = adminClient;
        this.pending = new LinkedBlockingQueue<TriggerResult>(
                Math.max(1, properties.getCallbackQueueCapacity()));

        this.sender = new Thread(new Runnable() {
            @Override
            public void run() {
                drainLoop();
            }
        }, "orbit-callback-sender");
        this.sender.setDaemon(true);
        this.sender.start();
    }

    /**
     * 提交一条待回传的执行结果。本方法不阻塞、不抛异常。
     *
     * @param result 最终执行结果（accepted=false）
     */
    public void send(TriggerResult result) {
        if (result == null) {
            return;
        }
        if (!adminClient.hasAdminAddress()) {
            droppedCount.incrementAndGet();
            log.error("[orbit-executor] admin-addresses empty, dropped callback for logId={}", result.getLogId());
            return;
        }
        if (!pending.offer(result)) {
            // 队列满：丢最旧的一条，给新结果腾位置
            TriggerResult evicted = pending.poll();
            if (evicted != null) {
                droppedCount.incrementAndGet();
                log.error("[orbit-executor] callback queue full (capacity={}), dropped oldest logId={}; "
                                + "raise orbit.executor.callback-queue-capacity or check admin availability",
                        properties.getCallbackQueueCapacity(), evicted.getLogId());
            }
            // 腾位与塞入之间存在竞态：仍塞不进去就说明队列又被填满，这条只能丢弃并记账
            if (!pending.offer(result)) {
                droppedCount.incrementAndGet();
                log.error("[orbit-executor] callback queue still full after eviction, dropped incoming logId={}",
                        result.getLogId());
            }
        }
    }

    /**
     * 发送线程主循环：取一条、把队列里已积压的一起打包发送。
     * 停机时 running 置 false，循环把队列里剩余的结果尽量发完再退出。
     */
    private void drainLoop() {
        while (running || !pending.isEmpty()) {
            TriggerResult first;
            try {
                first = pending.poll(500L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (first == null) {
                continue;
            }
            List<TriggerResult> batch = new ArrayList<TriggerResult>(BATCH_LIMIT);
            batch.add(first);
            pending.drainTo(batch, BATCH_LIMIT - 1);
            sendWithRetry(batch);
        }
    }

    /**
     * 带退避重试的批量发送。
     *
     * 重试耗尽后把整批塞回队列而不是丢弃：调度中心可能只是短暂不可用，
     * 直接丢会让这些任务全部落到孤儿回收里被记成失败。塞不回去的（队列满）才真的丢弃。
     */
    private void sendWithRetry(List<TriggerResult> batch) {
        int attempts = Math.max(1, properties.getCallbackRetryTimes() + 1);
        long interval = Math.max(0L, properties.getCallbackRetryIntervalMs());
        for (int i = 0; i < attempts; i++) {
            if (adminClient.post(CALLBACK_PATH, batch)) {
                sentCount.addAndGet(batch.size());
                return;
            }
            if (i < attempts - 1 && interval > 0) {
                if (!sleep(interval)) {
                    break;
                }
            }
        }
        for (TriggerResult result : batch) {
            if (!pending.offer(result)) {
                droppedCount.incrementAndGet();
                log.error("[orbit-executor] callback queue full while re-queuing, dropped logId={}",
                        result.getLogId());
            }
        }
        log.warn("[orbit-executor] callback batch of {} item(s) failed after {} attempt(s), re-queued",
                batch.size(), attempts);
        if (interval > 0) {
            sleep(interval);
        }
    }

    /**
     * 可中断的休眠：被中断时恢复中断标志并返回 false，让调用方立即退出重试循环，
     * 保证停机时发送线程不会被退避等待拖住。
     *
     * @param ms 休眠毫秒数
     * @return 正常睡醒返回 true，被中断返回 false
     */
    private boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * 回传通道运行指标，供测试与健康检查读取。
     *
     * @return [队列积压, 已发送, 已丢弃]
     */
    public long[] stats() {
        return new long[]{pending.size(), sentCount.get(), droppedCount.get()};
    }

    /**
     * 停止发送线程，并把队列中剩余结果尽量发完（最多等待 grace 秒）。
     */
    public void shutdown(long graceSeconds) {
        running = false;
        try {
            sender.join(TimeUnit.SECONDS.toMillis(Math.max(1L, graceSeconds)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!pending.isEmpty()) {
            log.warn("[orbit-executor] {} callback(s) still unsent after shutdown grace period", pending.size());
        }
    }
}

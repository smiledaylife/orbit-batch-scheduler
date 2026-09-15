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
 * 执行结果回传客户端（纯 HTTP 模式，零 Redis 依赖）。
 *
 * 执行器完成任务后不会阻塞业务工作线程，而是先把结果放入本地待发送队列，
 * 由独立发送线程异步投递到 Admin 的 {@code POST /orbit/admin/callback}。
 *
 * 可靠性边界：投递可靠性由进程内存队列 + 退避重试保障，进程崩溃则未投递结果丢失，
 * 由调度中心孤儿回收兜底把日志判失败；需要跨进程重启不丢结果的场景使用
 * {@link DurableCallbackClient}（Redis Stream 持久化回传，Redis 就绪时由自动装配启用）。
 *
 * Admin 侧对同一 logId 的状态更新采用幂等语义，因此 callback 重试和重复消费是安全的。
 */
public class CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(CallbackClient.class);

    /** Admin callback HTTP 接口路径。 */
    private static final String CALLBACK_PATH = "/orbit/admin/callback";

    /** 单批最多投递的 callback 数量，避免单次 HTTP 请求过大。 */
    private static final int BATCH_LIMIT = 200;

    /** 队列达到该阈值时告警，提示 Admin 可能持续不可用。 */
    private static final int QUEUE_WARN_THRESHOLD = 10000;

    private final ExecutorProperties properties;
    private final AdminClient adminClient;

    /**
     * 本地待发送队列。
     *
     * 使用无界队列是为了避免通过主动丢弃 callback 结果来解决背压；否则任务可能已经
     * SUCCESS，但 Admin 永远收不到结果并长期保持 RUNNING。跨进程可靠性由
     * {@link DurableCallbackClient} 的 Redis Stream 模式提供。
     */
    private final LinkedBlockingQueue<TriggerResult> pending;

    /** 独立 callback 发送线程，不占用任务执行线程。 */
    private final Thread sender;

    /** 控制发送线程优雅退出；退出前会继续处理已入队结果。 */
    private volatile boolean running = true;

    /** 已成功交付到 Redis Stream 或 HTTP 的 callback 数量。 */
    protected final AtomicLong sentCount = new AtomicLong();

    /** 因投递失败而重新进入重试流程的 callback 数量。 */
    private final AtomicLong retryCount = new AtomicLong();

    /** 已成功写入 Redis Stream 的 callback 数量；纯 HTTP 模式恒为 0。 */
    protected final AtomicLong streamCount = new AtomicLong();

    /**
     * 创建纯 HTTP 回传客户端，并启动独立发送线程。
     *
     * 适用于本地开发或未启用 Redis 持久化回传的场景。
     *
     * @param properties  执行器配置，提供重试参数
     * @param adminClient 与调度中心通信的 HTTP 客户端
     */
    public CallbackClient(ExecutorProperties properties, AdminClient adminClient) {
        this.properties = properties;
        this.adminClient = adminClient;
        this.pending = new LinkedBlockingQueue<TriggerResult>();
        this.sender = new Thread(this::drainLoop, "orbit-callback-sender");
        this.sender.setDaemon(true);
        this.sender.start();
    }

    /**
     * 提交一条待回传结果。
     *
     * 该方法只负责入队，快速返回，不等待网络 I/O。无界队列不会因固定容量而丢弃结果。
     *
     * @param result 任务最终执行结果；null 会被忽略
     */
    public void send(TriggerResult result) {
        if (result == null) {
            return;
        }
        pending.offer(result);
        int size = pending.size();
        if (size >= QUEUE_WARN_THRESHOLD) {
            log.warn("[orbit-executor] callback backlog={}, logId={}; admin is unavailable or under pressure",
                    size, result.getLogId());
        }
    }

    /**
     * 后台发送循环。
     * 每次先取出一条结果，再尽量从队列批量拉取更多结果，从而降低网络请求次数。
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

            boolean delivered = deliver(batch);
            if (!delivered) {
                // 投递失败时必须整批重新入队，确保不会因为瞬时网络故障造成结果丢失。
                pending.addAll(batch);
                retryCount.addAndGet(batch.size());
                // 已进入停机流程时不再退避重试：退回队列后直接退出循环，
                // 否则 admin 不可达时会在剩余宽限期内无退避地忙转空旋（打满 CPU、重试计数暴涨）。
                if (!running) {
                    return;
                }
                sleep(Math.max(500L, properties.getCallbackRetryIntervalMs()));
            }
        }
    }

    /**
     * 投递一批 callback（HTTP 通道，按 {@code callback-retry-*} 退避重试）。
     * 子类 {@link DurableCallbackClient} 在本方法前先尝试 Redis Stream 持久化通道。
     */
    protected boolean deliver(List<TriggerResult> batch) {
        int attempts = Math.max(1, properties.getCallbackRetryTimes() + 1);
        long interval = Math.max(0L, properties.getCallbackRetryIntervalMs());
        for (int i = 0; i < attempts; i++) {
            if (adminClient.post(CALLBACK_PATH, batch)) {
                sentCount.addAndGet(batch.size());
                return true;
            }
            if (!running) {
                break;
            }
            if (i < attempts - 1 && interval > 0 && !sleep(interval)) {
                break;
            }
        }
        return false;
    }

    /** 可中断的退避等待，保证 JVM 停机时线程可以尽快退出。 */
    protected final boolean sleep(long ms) {
        try {
            Thread.sleep(ms);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /** 投递通道运行配置（子类复用） */
    protected ExecutorProperties properties() {
        return properties;
    }

    /** 投递通道 HTTP 客户端（子类复用） */
    protected AdminClient adminClient() {
        return adminClient;
    }

    /** 待发送队列（子类复用） */
    protected LinkedBlockingQueue<TriggerResult> pendingQueue() {
        return pending;
    }

    /**
     * 返回 callback 通道运行指标：待发送数、已成功发送数、重试数、Redis Stream 写入数。
     */
    public long[] stats() {
        return new long[]{pending.size(), sentCount.get(), retryCount.get(), streamCount.get()};
    }

    /**
     * 停止 callback 发送线程，并在给定宽限期内尽量发送剩余结果。
     * 宽限期结束后不主动清空队列，以便日志能够明确暴露仍有未投递结果。
     *
     * @param graceSeconds 停机等待秒数
     */
    public void shutdown(long graceSeconds) {
        running = false;
        try {
            sender.join(TimeUnit.SECONDS.toMillis(Math.max(1L, graceSeconds)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!pending.isEmpty()) {
            log.error("[orbit-executor] {} callback(s) still unsent after shutdown grace period", pending.size());
        }
    }
}

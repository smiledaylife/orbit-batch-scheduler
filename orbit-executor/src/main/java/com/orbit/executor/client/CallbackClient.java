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
 * 设计原则：结果不能因为内存队列容量不足而被主动淘汰。队列采用无界内存队列，发送失败时
 * 保留原批次并持续重试；这样在进程存活期间保证 callback 至少一次送达。若要求跨进程重启
 * 仍然不丢，需要进一步把 pending queue 替换为 WAL/MQ 等持久化队列，本类不宣称进程重启可靠。
 *
 * 1. 解耦：结果先进内存队列，由专职守护线程发送，不占用业务工作线程；
 * 2. 批量：发送前把队列里已积压的结果一次性打包（上限 {@link #BATCH_LIMIT}）；
 * 3. 可靠：发送失败不丢弃任何结果，整批保留并退避重试；
 * 4. 复用出口：寻址、鉴权请求头与 RestTemplate 全部交给 {@link AdminClient}。
 *
 * 调度中心侧对同一 logId 的回传是幂等的（只允许 RUNNING 收敛到终态），
 * 因此重试与重复发送都是安全的。
 */
public class CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(CallbackClient.class);

    /** 回传端点路径 */
    private static final String CALLBACK_PATH = "/orbit/admin/callback";

    /** 单个回传请求最多携带的结果条数 */
    private static final int BATCH_LIMIT = 200;

    /** 队列积压达到该值时打印告警，防止 Admin 长时间不可用导致内存持续增长 */
    private static final int QUEUE_WARN_THRESHOLD = 10000;

    private final ExecutorProperties properties;
    private final AdminClient adminClient;

    /**
     * 无界队列：不能通过丢弃结果来解决背压，否则会造成任务状态永久 RUNNING/被误回收。
     * 可靠性边界为「进程存活期间不丢」；跨进程重启的可靠投递需要 WAL/MQ。
     */
    private final LinkedBlockingQueue<TriggerResult> pending;

    private final Thread sender;
    private volatile boolean running = true;
    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong retryCount = new AtomicLong();

    public CallbackClient(ExecutorProperties properties, AdminClient adminClient) {
        this.properties = properties;
        this.adminClient = adminClient;
        this.pending = new LinkedBlockingQueue<TriggerResult>();

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
     * 提交一条待回传的执行结果。本方法不阻塞、不抛异常，也绝不因为队列容量而丢结果。
     */
    public void send(TriggerResult result) {
        if (result == null) {
            return;
        }
        pending.offer(result);
        int size = pending.size();
        if (size >= QUEUE_WARN_THRESHOLD) {
            log.warn("[orbit-executor] callback queue backlog={}, admin may be unavailable; "
                            + "callbacks are retained in memory, logId={}",
                    size, result.getLogId());
        }
    }

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

            boolean sent = sendWithRetry(batch);
            if (!running && !sent) {
                // 停机时失败批次已经重新放回队列，不再空转；由 shutdown 报告剩余数量。
                return;
            }
        }
    }

    /**
     * 批量发送。失败后把整批放回队列，绝不丢弃。
     */
    private boolean sendWithRetry(List<TriggerResult> batch) {
        int attempts = Math.max(1, properties.getCallbackRetryTimes() + 1);
        long interval = Math.max(0L, properties.getCallbackRetryIntervalMs());

        for (int i = 0; i < attempts; i++) {
            if (adminClient.post(CALLBACK_PATH, batch)) {
                sentCount.addAndGet(batch.size());
                return true;
            }
            retryCount.incrementAndGet();
            if (!running) {
                break;
            }
            if (i < attempts - 1 && interval > 0 && !sleep(interval)) {
                break;
            }
        }

        // 重新入队时使用 addAll：无界队列不会因为容量不足淘汰任何结果。
        pending.addAll(batch);
        log.warn("[orbit-executor] callback batch of {} item(s) failed after {} attempt(s), "
                        + "retained for retry; pending={}",
                batch.size(), attempts, pending.size());

        if (interval > 0 && running) {
            sleep(interval);
        }
        return false;
    }

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
     * 回传通道运行指标：队列积压、已发送、重试次数。
     */
    public long[] stats() {
        return new long[]{pending.size(), sentCount.get(), retryCount.get()};
    }

    /**
     * 停止发送线程，并把队列中剩余结果尽量发完（最多等待 grace 秒）。
     * 超出宽限期的结果不会在这里主动删除，仍保留在内存中直到 JVM 退出。
     */
    public void shutdown(long graceSeconds) {
        running = false;
        try {
            sender.join(TimeUnit.SECONDS.toMillis(Math.max(1L, graceSeconds)));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        if (!pending.isEmpty()) {
            log.error("[orbit-executor] {} callback(s) still unsent after shutdown grace period; "
                            + "results were retained but process is shutting down",
                    pending.size());
        }
    }
}

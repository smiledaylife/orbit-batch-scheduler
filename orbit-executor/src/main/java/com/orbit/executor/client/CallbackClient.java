package com.orbit.executor.client;

import com.orbit.core.model.TriggerResult;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.connection.stream.StreamRecords;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 执行结果回传客户端。
 *
 * <p>执行器完成任务后不会阻塞业务工作线程，而是先把结果放入本地待发送队列，
 * 由独立发送线程异步投递。生产模式优先写入 Redis Stream，由 Admin 消费组异步落库；
 * Redis 不可用时再回退到 HTTP callback。</p>
 *
 * <p>可靠性边界：Redis Stream 写入成功后，结果具备跨 Executor 进程重启的持久化能力；
 * 如果 Redis 本身不可用，则只能依赖进程存活期间的内存队列和 HTTP 重试。因此生产环境
 * 应保证 Redis 的高可用、持久化以及合理的 Stream 保留策略。</p>
 *
 * <p>Admin 侧对同一 logId 的状态更新采用幂等语义，因此 callback 重试和重复消费是安全的。</p>
 */
public class CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(CallbackClient.class);

    /** Admin callback HTTP 接口路径，作为 Redis Stream 不可用时的兜底通道。 */
    private static final String CALLBACK_PATH = "/orbit/admin/callback";

    /** 单批最多投递的 callback 数量，避免单次 HTTP/Redis 请求过大。 */
    private static final int BATCH_LIMIT = 200;

    /** 队列达到该阈值时告警，提示 Admin/Redis 可能持续不可用。 */
    private static final int QUEUE_WARN_THRESHOLD = 10000;

    private final ExecutorProperties properties;
    private final AdminClient adminClient;

    /**
     * 本地待发送队列。
     *
     * <p>使用无界队列是为了避免通过主动丢弃 callback 结果来解决背压；否则任务可能已经
     * SUCCESS，但 Admin 永远收不到结果并长期保持 RUNNING。跨进程可靠性由 Redis Stream 提供。</p>
     */
    private final LinkedBlockingQueue<TriggerResult> pending;

    /** Redis Stream 客户端，生产模式用于持久化 callback。 */
    private final StringRedisTemplate redis;

    /** 独立 callback 发送线程，不占用任务执行线程。 */
    private final Thread sender;

    /** 控制发送线程优雅退出；退出前会继续处理已入队结果。 */
    private volatile boolean running = true;

    /** 已成功交付到 Redis Stream 或 HTTP 的 callback 数量。 */
    private final AtomicLong sentCount = new AtomicLong();

    /** 因投递失败而重新进入重试流程的 callback 数量。 */
    private final AtomicLong retryCount = new AtomicLong();

    /** 已成功写入 Redis Stream 的 callback 数量。 */
    private final AtomicLong streamCount = new AtomicLong();

    public CallbackClient(ExecutorProperties properties, AdminClient adminClient,
                          StringRedisTemplate redis) {
        this.properties = properties;
        this.adminClient = adminClient;
        this.redis = redis;
        this.pending = new LinkedBlockingQueue<TriggerResult>();
        this.sender = new Thread(this::drainLoop, "orbit-callback-sender");
        this.sender.setDaemon(true);
        this.sender.start();
    }

    /**
     * 提交一条待回传结果。
     *
     * <p>该方法只负责入队，快速返回，不等待网络 I/O。无界队列不会因固定容量而丢弃结果。</p>
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
            log.warn("[orbit-executor] callback backlog={}, logId={}; durable stream is unavailable or under pressure",
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
                // 投递失败时必须整批重新入队，确保不会因为瞬时网络/Redis故障造成结果丢失。
                pending.addAll(batch);
                retryCount.addAndGet(batch.size());
                if (running) {
                    sleep(Math.max(500L, properties.getCallbackRetryIntervalMs()));
                }
            }
        }
    }

    /**
     * 投递一批 callback。
     *
     * <p>生产模式先写 Redis Stream。只有 Redis Stream 写入失败时才降级到 HTTP，
     * 从而让 Redis 正常时 callback 不依赖 Admin HTTP 实例的瞬时可用性。</p>
     */
    private boolean deliver(List<TriggerResult> batch) {
        if (properties.isDurableCallbackEnabled()) {
            try {
                for (TriggerResult result : batch) {
                    Map<String, String> fields = toFields(result);
                    redis.opsForStream().add(StreamRecords.newRecord()
                            .in(properties.getCallbackStreamKey()).ofMap(fields));
                    streamCount.incrementAndGet();
                }
                sentCount.addAndGet(batch.size());
                return true;
            } catch (RuntimeException e) {
                // Redis 不可用时不能直接丢弃 callback，继续走 HTTP 兜底。
                log.warn("[orbit-executor] redis callback stream unavailable, falling back to HTTP: {}",
                        e.getMessage());
            }
        }

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

    /**
     * 把领域对象转换成 Redis Stream 的字符串字段，避免 Redis 序列化依赖具体 Java 类型。
     */
    private static Map<String, String> toFields(TriggerResult result) {
        Map<String, String> fields = new HashMap<String, String>();
        fields.put("logId", safe(result.getLogId()));
        fields.put("jobId", String.valueOf(result.getJobId()));
        fields.put("success", String.valueOf(result.isSuccess()));
        fields.put("accepted", String.valueOf(result.isAccepted()));
        fields.put("costMs", String.valueOf(result.getCostMs()));
        fields.put("workerNode", safe(result.getWorkerNode()));
        fields.put("message", safe(result.getMessage()));
        return fields;
    }

    /** Redis Stream 字段允许为空字符串，避免 null 值导致序列化问题。 */
    private static String safe(String value) {
        return value == null ? "" : value;
    }

    /** 可中断的退避等待，保证 JVM 停机时线程可以尽快退出。 */
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

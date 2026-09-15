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
 * 生产模式优先把结果写入 Redis Stream，再由 Admin Consumer Group 异步消费并落库。
 * 这样 Executor 进程重启不会丢失已经写入 Redis 的 callback；Admin 短时不可用也不会
 * 阻塞业务线程。Redis 写入失败时保留在内存队列并继续重试，同时尝试 HTTP 直投作为
 * 最后的可用路径。Redis Stream 消费端采用 DB 条件更新，因此重复消息天然幂等。
 */
public class CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(CallbackClient.class);
    private static final String CALLBACK_PATH = "/orbit/admin/callback";
    private static final int BATCH_LIMIT = 200;
    private static final int QUEUE_WARN_THRESHOLD = 10000;

    private final ExecutorProperties properties;
    private final AdminClient adminClient;
    private final StringRedisTemplate redis;
    private final LinkedBlockingQueue<TriggerResult> pending;
    private final Thread sender;
    private volatile boolean running = true;
    private final AtomicLong sentCount = new AtomicLong();
    private final AtomicLong retryCount = new AtomicLong();
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
                pending.addAll(batch);
                retryCount.addAndGet(batch.size());
                if (running) {
                    sleep(Math.max(500L, properties.getCallbackRetryIntervalMs()));
                }
            }
        }
    }

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

    private static String safe(String value) {
        return value == null ? "" : value;
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

    public long[] stats() {
        return new long[]{pending.size(), sentCount.get(), retryCount.get(), streamCount.get()};
    }

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

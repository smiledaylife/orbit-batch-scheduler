package com.orbit.admin.service;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Redis Stream callback 消费器。
 *
 * <p>Executor 先将任务结果写入 Redis Stream，本组件使用 Consumer Group 异步消费并落库。
 * DB 状态更新成功后才 ACK Stream；如果 DB 更新失败，则不 ACK，让消息继续保留在 pending
 * 队列中等待后续重试，从而实现至少一次投递。</p>
 *
 * <p>Admin 多副本共享同一个 Consumer Group。这里使用固定逻辑 consumer 名称，并通过
 * record 级 Redis 锁避免同一时刻多个实例重复处理同一条 pending 消息。重复消费最终由
 * JobStore 的状态条件更新保证幂等。</p>
 */
@Component
@ConditionalOnProperty(prefix = "orbit.admin", name = "durable-callback-enabled", havingValue = "true")
public class CallbackStreamConsumer implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(CallbackStreamConsumer.class);

    /** 每条 Stream record 的处理锁前缀；锁 TTL 防止异常退出留下永久死锁。 */
    private static final String PROCESS_LOCK_PREFIX = "orbit:callback:process:";

    /** 用于创建 Redis Stream 的初始化 marker，不代表真实任务 callback。 */
    private static final String INIT_FIELD = "__orbit_init";

    private final StringRedisTemplate redis;
    private final JobService jobService;
    private final AdminProperties properties;

    /** 独立消费线程，避免占用 Spring MVC/业务线程。 */
    private final Thread worker;

    /** 控制消费线程生命周期。 */
    private volatile boolean running = true;

    public CallbackStreamConsumer(StringRedisTemplate redis, JobService jobService, AdminProperties properties) {
        this.redis = redis;
        this.jobService = jobService;
        this.properties = properties;
        this.worker = new Thread(this::runLoop,
                "orbit-callback-stream-consumer-" + UUID.randomUUID().toString().substring(0, 8));
        this.worker.setDaemon(true);
        this.worker.start();
    }

    /**
     * 主消费循环：先处理当前 Consumer 的 pending，再阻塞等待新消息。
     * Redis 暂时不可用时退避后重试，不因为依赖抖动退出消费线程。
     */
    private void runLoop() {
        while (running) {
            try {
                if (!ensureGroup()) {
                    sleep(2000L);
                    continue;
                }
                StreamOperations<String, String, String> ops = redis.opsForStream();
                Consumer consumer = Consumer.from(properties.getCallbackStreamGroup(), "orbit-admin");

                // 先读取 pending，保证 Admin 重启后可以继续处理尚未 ACK 的消息。
                List<MapRecord<String, String, String>> pending = ops.read(
                        consumer,
                        StreamReadOptions.empty().count(100),
                        StreamOffset.create(properties.getCallbackStreamKey(), ReadOffset.from("0-0")));
                process(pending, ops, consumer);

                if (!running) {
                    return;
                }

                // pending 清理后再读取新消息；block 避免空闲时持续轮询 Redis。
                List<MapRecord<String, String, String>> records = ops.read(
                        consumer,
                        StreamReadOptions.empty().count(100).block(Duration.ofSeconds(2)),
                        StreamOffset.create(properties.getCallbackStreamKey(), ReadOffset.lastConsumed()));
                process(records, ops, consumer);
            } catch (Exception e) {
                log.warn("[orbit-admin] callback stream consumer temporarily unavailable: {}", e.getMessage());
                sleep(2000L);
            }
        }
    }

    /**
     * 确保 Stream 和 Consumer Group 已创建。
     * Redis 的 XGROUP CREATE 要求 Stream 已存在，因此首次启动时通过 marker 创建 Stream。
     */
    private boolean ensureGroup() {
        try {
            StreamOperations<String, String, String> ops = redis.opsForStream();
            try {
                ops.createGroup(properties.getCallbackStreamKey(), ReadOffset.latest(),
                        properties.getCallbackStreamGroup());
            } catch (Exception first) {
                String message = first.getMessage();
                if (isBusyGroup(message)) {
                    return true;
                }
                // Redis XGROUP CREATE 要求 Stream 已存在；写 marker 建立 Stream 后重试。
                ops.add(StreamRecords.newRecord().in(properties.getCallbackStreamKey())
                        .ofMap(Collections.singletonMap(INIT_FIELD, "1")));
                try {
                    ops.createGroup(properties.getCallbackStreamKey(), ReadOffset.latest(),
                            properties.getCallbackStreamGroup());
                } catch (Exception second) {
                    if (isBusyGroup(second.getMessage())) {
                        return true;
                    }
                    throw second;
                }
            }
            return true;
        } catch (Exception e) {
            log.debug("[orbit-admin] callback stream group not ready: {}", e.getMessage());
            return false;
        }
    }

    /** 判断 Redis 返回的异常是否表示 Consumer Group 已经存在。 */
    private static boolean isBusyGroup(String message) {
        if (message == null) {
            return false;
        }
        String m = message.toLowerCase();
        return m.contains("busygroup") || m.contains("group name already exists");
    }

    /**
     * 处理一批 Stream 消息。
     * 无效消息可以安全 ACK；真实 callback 只有 DB 处理成功后才 ACK。
     */
    private void process(List<MapRecord<String, String, String>> records,
                         StreamOperations<String, String, String> ops, Consumer consumer) {
        if (records == null || records.isEmpty()) {
            return;
        }
        for (MapRecord<String, String, String> record : records) {
            String recordId = record.getId().getValue();
            Map<String, String> value = record.getValue();
            if (value.containsKey(INIT_FIELD)) {
                ack(ops, consumer, record);
                continue;
            }
            String logId = value.get("logId");
            if (logId == null || logId.trim().isEmpty()) {
                log.warn("[orbit-admin] invalid callback stream record {}, missing logId", recordId);
                ack(ops, consumer, record);
                continue;
            }
            if (!acquireRecordLock(recordId)) {
                // 其他 Admin 实例正在处理，保持 pending 状态，下一轮继续尝试。
                continue;
            }
            try {
                TriggerResult result = new TriggerResult();
                result.setLogId(logId);
                result.setJobId(parseLong(value.get("jobId")));
                result.setSuccess(Boolean.parseBoolean(value.get("success")));
                result.setAccepted(Boolean.parseBoolean(value.get("accepted")));
                result.setCostMs(parseLong(value.get("costMs")));
                result.setWorkerNode(value.get("workerNode"));
                result.setMessage(value.get("message"));
                if (!result.isAccepted()) {
                    jobService.handleCallback(result);
                }
                // 只有业务处理完成后才确认消息，避免 DB 失败造成 callback 丢失。
                ack(ops, consumer, record);
            } catch (Exception e) {
                // 不 ACK：让消息留在 pending，等待下一次消费重试。
                log.error("[orbit-admin] callback stream processing failed, recordId={}, logId={}",
                        recordId, logId, e);
            } finally {
                releaseRecordLock(recordId);
            }
        }
    }

    /** 获取单条消息的分布式处理锁。 */
    private boolean acquireRecordLock(String recordId) {
        try {
            Boolean ok = redis.opsForValue().setIfAbsent(PROCESS_LOCK_PREFIX + recordId,
                    "1", 60L, java.util.concurrent.TimeUnit.SECONDS);
            return Boolean.TRUE.equals(ok);
        } catch (Exception e) {
            // Redis 故障时宁可暂停消费，也不能退化为本地锁导致多实例并发处理。
            return false;
        }
    }

    /** 释放消息处理锁；释放失败不会影响 Stream 的 pending 机制。 */
    private void releaseRecordLock(String recordId) {
        try {
            redis.delete(PROCESS_LOCK_PREFIX + recordId);
        } catch (Exception ignored) {
        }
    }

    /** ACK 一条已成功处理的 Stream 消息。 */
    private void ack(StreamOperations<String, String, String> ops, Consumer consumer,
                     MapRecord<String, String, String> record) {
        ops.acknowledge(properties.getCallbackStreamKey(), consumer.getGroup(), record.getId());
    }

    /** 将 Stream 中的数字字段解析为 long；异常数据按 0 处理并交由业务层继续判断。 */
    private static long parseLong(String value) {
        if (value == null || value.trim().isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /** 消费线程退避等待，支持线程中断。 */
    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Spring 容器销毁时停止消费线程，避免应用退出阶段继续访问 Redis/数据库。
     */
    @Override
    public void destroy() {
        running = false;
        worker.interrupt();
        try {
            worker.join(3000L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

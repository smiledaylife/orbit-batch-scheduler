package com.orbit.admin.service;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Redis Stream callback 消费器。
 *
 * 使用 at-least-once 投递语义：DB 状态先成功，再 ACK Stream 消息；ACK 失败会导致重复消费，
 * 但 JobStore 的 RUNNING -> 终态条件更新保证重复消费不会覆盖已有结果。
 * Consumer 名称故意使用固定逻辑名，使 Pod 重启后仍可读取该 consumer 的 pending entries；
 * 多 Admin 副本同时消费时再通过 Redis record lock 做单消息互斥。
 */
@Component
public class CallbackStreamConsumer implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(CallbackStreamConsumer.class);
    private static final String PROCESS_LOCK_PREFIX = "orbit:callback:process:";
    private static final String INIT_FIELD = "__orbit_init";

    private final StringRedisTemplate redis;
    private final JobService jobService;
    private final AdminProperties properties;
    private final Thread worker;
    private volatile boolean running = true;

    public CallbackStreamConsumer(StringRedisTemplate redis, JobService jobService, AdminProperties properties) {
        this.redis = redis;
        this.jobService = jobService;
        this.properties = properties;
        this.worker = new Thread(this::runLoop, "orbit-callback-stream-consumer-" + UUID.randomUUID().toString().substring(0, 8));
        this.worker.setDaemon(true);
        this.worker.start();
    }

    private void runLoop() {
        while (running) {
            try {
                if (!ensureGroup()) {
                    sleep(2000L);
                    continue;
                }
                StreamOperations<String, String, String> ops = redis.opsForStream();
                Consumer consumer = Consumer.from(properties.getCallbackStreamGroup(), "orbit-admin");

                // 先处理当前 consumer 的 pending，避免 Admin 重启后留下未 ACK 消息。
                List<MapRecord<String, String, String>> pending = ops.read(
                        consumer,
                        StreamReadOptions.empty().count(100),
                        StreamOffset.create(properties.getCallbackStreamKey(), ReadOffset.from("0-0")));
                process(pending, ops, consumer);

                if (!running) {
                    return;
                }
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

    private boolean ensureGroup() {
        try {
            StreamOperations<String, String, String> ops = redis.opsForStream();
            try {
                ops.createGroup(properties.getCallbackStreamKey(), ReadOffset.latest(),
                        properties.getCallbackStreamGroup());
            } catch (Exception existsOrMissing) {
                String message = existsOrMissing.getMessage();
                if (message != null && message.toLowerCase().contains("busygroup")) {
                    return true;
                }
                // Stream 尚未创建：写一个仅用于建立 Stream 的 marker，随后创建 group。
                if (message != null && (message.toLowerCase().contains("no such key")
                        || message.toLowerCase().contains("does not exist"))) {
                    ops.add(StreamRecords.newRecord().in(properties.getCallbackStreamKey())
                            .ofMap(Collections.singletonMap(INIT_FIELD, "1")));
                    ops.createGroup(properties.getCallbackStreamKey(), ReadOffset.latest(),
                            properties.getCallbackStreamGroup());
                    return true;
                }
                // 某些 Redis 版本在 stream 刚创建的竞态下会返回 BUSYGROUP/已有 group。
                if (message != null && message.toLowerCase().contains("group name already exists")) {
                    return true;
                }
                throw existsOrMissing;
            }
            return true;
        } catch (Exception e) {
            log.debug("[orbit-admin] callback stream group not ready: {}", e.getMessage());
            return false;
        }
    }

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
                // Stream 只承载最终执行结果；accepted=true 的同步触发回执不应进入 Stream。
                if (!result.isAccepted()) {
                    jobService.handleCallback(result);
                }
                ack(ops, consumer, record);
            } catch (Exception e) {
                // 不 ACK：下一次 pending/retry 会再次处理，确保 DB 瞬时故障不会吞消息。
                log.error("[orbit-admin] callback stream processing failed, recordId={}, logId={}",
                        recordId, logId, e);
            } finally {
                releaseRecordLock(recordId);
            }
        }
    }

    private boolean acquireRecordLock(String recordId) {
        try {
            Boolean ok = redis.opsForValue().setIfAbsent(PROCESS_LOCK_PREFIX + recordId,
                    "1", 60L, java.util.concurrent.TimeUnit.SECONDS);
            return Boolean.TRUE.equals(ok);
        } catch (Exception e) {
            // Redis 本身就是消息源，Redis 异常时不应继续消费并产生重复处理。
            return false;
        }
    }

    private void releaseRecordLock(String recordId) {
        try {
            redis.delete(PROCESS_LOCK_PREFIX + recordId);
        } catch (Exception ignored) {
        }
    }

    private void ack(StreamOperations<String, String, String> ops, Consumer consumer,
                     MapRecord<String, String, String> record) {
        ops.acknowledge(properties.getCallbackStreamKey(), consumer.getGroup(), record.getId());
    }

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

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

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

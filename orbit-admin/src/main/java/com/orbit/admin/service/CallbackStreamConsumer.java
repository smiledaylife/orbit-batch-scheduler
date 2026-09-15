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
 * DB 状态先成功，再 ACK Stream；ACK 失败会导致重复消费，但 JobStore 的条件更新保证幂等。
 * 使用固定逻辑 consumer 名称，配合 record lock，使 Admin Pod 重启后可以继续处理 pending。
 */
@Component
@ConditionalOnProperty(prefix = "orbit.admin", name = "durable-callback-enabled", havingValue = "true")
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
        this.worker = new Thread(this::runLoop,
                "orbit-callback-stream-consumer-" + UUID.randomUUID().toString().substring(0, 8));
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

    private static boolean isBusyGroup(String message) {
        if (message == null) {
            return false;
        }
        String m = message.toLowerCase();
        return m.contains("busygroup") || m.contains("group name already exists");
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
                if (!result.isAccepted()) {
                    jobService.handleCallback(result);
                }
                ack(ops, consumer, record);
            } catch (Exception e) {
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

package com.orbit.executor.client;

import com.orbit.core.model.TriggerResult;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Redis Stream 持久化回传客户端。
 *
 * 在纯 HTTP 通道（{@link CallbackClient}）之前优先写入 Redis Stream：写入成功后，
 * 结果具备跨 Executor 进程重启的持久化能力，由 Admin 侧消费组异步落库
 * （{@code orbit.admin.durable-callback-enabled=true}）；Stream 写入失败时才降级到
 * 父类的 HTTP 通道，从而让 Redis 正常时 callback 不依赖 Admin HTTP 实例的瞬时可用性。
 *
 * 仅当 classpath 存在 Redis 且容器中注册了 StringRedisTemplate 时由自动装配创建；
 * 本类的 Redis 字段与工具方法都集中在这里，保证基类在无 Redis 环境可安全反射内省。
 */
public class DurableCallbackClient extends CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(DurableCallbackClient.class);

    /** 「已启用持久化回传但 Redis 不可用」的告警只打一次，避免逐条重试刷日志。 */
    private volatile boolean durableUnavailableWarned = false;

    /** Redis Stream 客户端；持久化回传模式必需 */
    private final StringRedisTemplate redis;

    /**
     * @param properties  执行器配置，提供重试参数、回退开关与 Stream key
     * @param adminClient 与调度中心通信的 HTTP 客户端
     * @param redis       Redis 客户端；为 null 时按纯 HTTP 模式工作并一次性告警
     */
    public DurableCallbackClient(ExecutorProperties properties, AdminClient adminClient,
                                 StringRedisTemplate redis) {
        super(properties, adminClient);
        this.redis = redis;
    }

    /**
     * 先尝试 Redis Stream 持久化写入；失败或不可用时降级到 HTTP 通道（父类逻辑）。
     */
    @Override
    protected boolean deliver(List<TriggerResult> batch) {
        if (properties().isDurableCallbackEnabled()) {
            if (redis == null) {
                // 配置了持久化回传但本实例没有 Redis 客户端。
                // 不能静默走 NPE 兜底：一次性把配置矛盾暴露出来，然后按 HTTP 通道继续投递。
                warnDurableUnavailableOnce();
            } else {
                try {
                    for (TriggerResult result : batch) {
                        Map<String, String> fields = toFields(result);
                        redis.opsForStream().add(StreamRecords.newRecord()
                                .in(properties().getCallbackStreamKey()).ofMap(fields));
                        streamCount.incrementAndGet();
                    }
                    // 与旧实现语义一致：stream 写入成功同时计入总成功数（stats[1]）
                    sentCount.addAndGet(batch.size());
                    return true;
                } catch (RuntimeException e) {
                    // Redis 不可用时不能直接丢弃 callback，继续走 HTTP 兜底。
                    // 计数尚未入账：HTTP 路径成功后由父类统一记账。
                    log.warn("[orbit-executor] redis callback stream unavailable, falling back to HTTP: {}",
                            e.getMessage());
                }
            }
        }
        return super.deliver(batch);
    }

    /** 持久化回传配置与 Redis 客户端缺失的矛盾只告警一次，提示修正配置或补齐依赖。 */
    private void warnDurableUnavailableOnce() {
        if (durableUnavailableWarned) {
            return;
        }
        durableUnavailableWarned = true;
        log.warn("[orbit-executor] orbit.executor.durable-callback-enabled=true but no Redis client "
                + "is available on this CallbackClient; results keep falling back to HTTP callback");
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
}

package com.orbit.executor.client;

import com.orbit.core.model.ApiResult;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 执行结果回传客户端：任务跑完后把最终结果异步推回调度中心。
 *
 * 触发是「受理即返回」的，调度中心把日志留在 RUNNING，任务的真实成败只能靠本组件回传，
 * 因此回传失败等于这条调度日志永远停在 RUNNING，直到调度中心的孤儿回收把它判失败。
 * 为降低这种概率，本组件提供三层保障：
 *
 * 1. 解耦：结果先进有界内存队列，由专职守护线程发送，不占用业务工作线程；
 * 2. 重试：单次发送失败按 callback-retry-interval-ms 退避后重试，最多 callback-retry-times 次；
 *    多调度中心地址时逐个尝试，任一成功即算成功；
 * 3. 可观测：队列积压、发送失败、最终丢弃都有计数与日志。队列满时丢弃最旧的一条
 *    （新结果比旧结果更值得送达），并打 ERROR 日志说明被丢弃的 logId。
 *
 * 调度中心侧对同一 logId 的回传是幂等的（只允许 RUNNING 收敛到终态），
 * 因此重试与重复发送都不会污染日志，重发是安全的。
 */
public class CallbackClient {

    private static final Logger log = LoggerFactory.getLogger(CallbackClient.class);

    /** 回传端点路径 */
    private static final String CALLBACK_PATH = "/orbit/admin/callback";

    private final ExecutorProperties properties;
    private final RestTemplate restTemplate;
    private final List<String> adminBases;
    private final HttpHeaders jsonHeaders;

    /** 待回传队列，有界；容量见 orbit.executor.callback-queue-capacity */
    private final LinkedBlockingQueue<TriggerResult> pending;

    /** 发送线程 */
    private final Thread sender;

    private volatile boolean running = true;

    /** 累计发送成功条数 */
    private final AtomicLong sentCount = new AtomicLong();

    /** 累计最终丢弃条数（重试耗尽或队列满） */
    private final AtomicLong droppedCount = new AtomicLong();

    public CallbackClient(ExecutorProperties properties) {
        this.properties = properties;
        SimpleClientHttpRequestFactory f = new SimpleClientHttpRequestFactory();
        f.setConnectTimeout(3000);
        f.setReadTimeout(5000);
        this.restTemplate = new RestTemplate(f);
        this.adminBases = parseAdminBases(properties.getAdminAddresses());
        this.jsonHeaders = buildJsonHeaders(properties.getAccessToken());

        int capacity = Math.max(1, properties.getCallbackQueueCapacity());
        this.pending = new LinkedBlockingQueue<TriggerResult>(capacity);

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
        if (adminBases.isEmpty()) {
            log.warn("[orbit-executor] admin-addresses empty, drop callback for logId={}", result.getLogId());
            droppedCount.incrementAndGet();
            return;
        }
        while (!pending.offer(result)) {
            // 队列满：丢最旧的一条，给新结果腾位置
            TriggerResult evicted = pending.poll();
            if (evicted != null) {
                droppedCount.incrementAndGet();
                log.error("[orbit-executor] callback queue full (capacity={}), dropped oldest logId={}; "
                                + "raise orbit.executor.callback-queue-capacity or check admin availability",
                        properties.getCallbackQueueCapacity(), evicted.getLogId());
            }
        }
    }

    /**
     * 发送线程主循环：取一条、发一条（含重试）。
     * 停机时 running 置 false，循环把队列里剩余的结果尽量发完再退出。
     */
    private void drainLoop() {
        while (running || !pending.isEmpty()) {
            TriggerResult result;
            try {
                result = pending.poll(500L, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (result == null) {
                continue;
            }
            sendWithRetry(result);
        }
    }

    /**
     * 带退避重试的发送：任一轮成功即返回；重试耗尽则计入丢弃并打 ERROR。
     */
    private void sendWithRetry(TriggerResult result) {
        int attempts = Math.max(1, properties.getCallbackRetryTimes() + 1);
        long interval = Math.max(0L, properties.getCallbackRetryIntervalMs());
        for (int i = 0; i < attempts; i++) {
            if (postOnce(result)) {
                sentCount.incrementAndGet();
                return;
            }
            if (i < attempts - 1 && interval > 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        droppedCount.incrementAndGet();
        log.error("[orbit-executor] gave up callback for logId={} job={} after {} attempt(s); "
                        + "the admin will leave this log RUNNING until orphan reaping",
                result.getLogId(), result.getJobId(), attempts);
    }

    /**
     * 向所有已配置的调度中心地址各试一次，任一成功即算成功。
     */
    private boolean postOnce(TriggerResult result) {
        HttpEntity<TriggerResult> entity = new HttpEntity<TriggerResult>(result, jsonHeaders);
        for (String base : adminBases) {
            String url = base + CALLBACK_PATH;
            try {
                restTemplate.postForObject(url, entity, ApiResult.class);
                return true;
            } catch (Exception e) {
                log.warn("[orbit-executor] callback to {} failed for logId={}: {}",
                        url, result.getLogId(), e.getMessage());
            }
        }
        return false;
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

    private static List<String> parseAdminBases(String admins) {
        if (admins == null || admins.trim().isEmpty()) {
            return Collections.emptyList();
        }
        List<String> bases = new ArrayList<String>();
        for (String raw : admins.split(",")) {
            String base = raw.trim();
            if (base.isEmpty()) {
                continue;
            }
            if (base.endsWith("/")) {
                base = base.substring(0, base.length() - 1);
            }
            bases.add(base);
        }
        return Collections.unmodifiableList(bases);
    }

    private static HttpHeaders buildJsonHeaders(String accessToken) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (accessToken != null && !accessToken.isEmpty()) {
            headers.set(AdminClient.TOKEN_HEADER, accessToken);
        }
        return headers;
    }
}

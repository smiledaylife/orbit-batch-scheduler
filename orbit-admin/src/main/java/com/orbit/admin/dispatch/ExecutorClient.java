package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.core.protocol.OrbitProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Locale;

/**
 * 调度中心向执行器派发任务的 HTTP 通信客户端。
 * 核心职责：
 *
 *   - 负责调用执行器暴露的 {@code POST /orbit/executor/run} 接口；
 *   - 使用固定的短读超时（orbit.admin.trigger-timeout-seconds）：触发是「受理即返回」的，
 *       超时只需覆盖网络往返与入队，与任务真实耗时无关；
 *   - 携带鉴权安全令牌（{@code X-Orbit-Token}）；
 *   - 捕获网络连通性异常、超时异常，并优雅封装为失败的 {@link TriggerResult}。
 *
 * HTTP 客户端选用基于 JDK {@link HttpClient} 的 {@link JdkClientHttpRequestFactory}：
 * 基于 NIO 实现，阻塞等待不持有 synchronized 监视器，不会钉住（pin）JDK 21 虚拟线程的载体线程，
 * 是 {@code dispatch-virtual-threads=true} 时触发通道能够安全跑在虚拟线程上的前提；
 * 平台线程模式下与 HttpURLConnection 相比也无额外开销。
 * 故障描述经 {@link #describeTriggerFailure} 归一化，保证调度中心 failover 分级
 * （{@code JobService#looksUnreachable} / {@code looksAmbiguous}）依赖的关键词与客户端实现解耦。
 *
 * 返回值语义：{@code accepted=true} 表示执行器已受理并入队（任务尚未跑完），
 * 此时调度中心必须把日志留在 RUNNING；{@code accepted=false} 表示触发本身失败，日志应立刻判失败。
 *
 */
@Component
public class ExecutorClient {

    private static final Logger log = LoggerFactory.getLogger(ExecutorClient.class);

    /**
     * 预构建的 JSON + 鉴权 Header（accessToken 在运行期不可变，构造时一次性构建）
     */
    private final HttpHeaders jsonHeaders;

    /**
     * 触发调用共用的 RestTemplate。读超时对所有任务一致，因此一个实例即可复用
     * （RestTemplate 配置完成后是线程安全的）。
     */
    private final RestTemplate restTemplate;

    /**
     * 预构建鉴权 Header 与 RestTemplate：accessToken 和读超时在运行期都不变，
     * 构造时一次性算好，派发热路径上无需重复构建。
     *
     * @param properties 调度中心配置，提供 access-token、连接超时与触发读超时
     */
    public ExecutorClient(AdminProperties properties) {
        this.jsonHeaders = buildJsonHeaders(properties.getAccessToken());
        this.restTemplate = buildRest(properties.getConnectTimeoutMs(), triggerReadTimeoutMs(properties));
    }

    /**
     * 向目标执行器发起任务触发请求。
     *
     * @param executorBaseUrl 目标执行器通信基地址（例如：http://10.0.0.1:8081）
     * @param request         任务触发入参（含任务 ID、参数、日志 ID、超时等）
     * @return 受理回执（accepted=true）；若请求失败或超时则返回包含错误原因的失败结果（accepted=false）
     */
    public TriggerResult trigger(String executorBaseUrl, TriggerRequest request) {
        // 1. 拼接执行器触发端点 URL（令牌在构造时已预置进 jsonHeaders）
        String url = OrbitProtocol.trimTrailingSlash(executorBaseUrl) + "/orbit/executor/run";
        try {
            TriggerResult result = restTemplate.postForObject(url,
                    new HttpEntity<TriggerRequest>(request, jsonHeaders), TriggerResult.class);
            // 处理空响应异常场景
            if (result == null) {
                return TriggerResult.fail(request.getLogId(), request.getJobId(), executorBaseUrl, 0,
                        "empty response from executor");
            }
            // 补充执行节点标识
            if (result.getWorkerNode() == null) {
                result.setWorkerNode(executorBaseUrl);
            }
            return result;
        } catch (Exception e) {
            // 捕获 ConnectTimeout、ReadTimeout、404、500 等网络或远端服务异常
            log.warn("[orbit-admin] trigger {} failed: {}", url, e.getMessage());
            return TriggerResult.fail(request.getLogId(), request.getJobId(), executorBaseUrl, 0,
                    describeTriggerFailure(e));
        }
    }

    /**
     * 把触发过程中的任意异常归一化为稳定的故障描述。
     *
     * 调度中心的 failover 分级（可达性摘除 / 模糊失败转移 / 业务拒绝终止）依赖
     * 失败消息中的关键词。不同 HTTP 客户端对同类故障的措辞并不一致
     * （例如读超时在 HttpURLConnection 下是 "Read timed out"，JDK HttpClient 下是
     * "request timed out"），若直接透传原始消息，切换客户端会静默改变 failover 语义。
     * 因此在源头按异常**类型**翻译成稳定关键词，原始消息附在后面供人工定位。
     *
     * 使用 JDK 21 的 switch 模式匹配（JEP 441）按异常层级分发；
     * 注意 case 顺序即匹配优先级：HttpConnectTimeoutException 必须排在父类 HttpTimeoutException 之前。
     *
     * @param e 触发调用抛出的异常
     * @return 以稳定关键词开头的故障描述
     */
    private static String describeTriggerFailure(Throwable e) {
        // RestTemplate 会把底层 IO 异常包在 ResourceAccessException 里：解到最内层真实原因再分类
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return switch (t) {
            // JDK HttpClient：连接超时（必须排在父类 HttpTimeoutException 之前）
            case HttpConnectTimeoutException c -> "connect timed out: " + msgOf(c);
            // JDK HttpClient：响应读超时
            case HttpTimeoutException h -> "read timed out: " + msgOf(h);
            case UnknownHostException u -> "unknownhost: " + msgOf(u);
            // HttpURLConnection：连接阶段超时的消息即 "connect timed out"，此处显式归一
            case SocketTimeoutException s when isConnectTimeout(s) -> "connect timed out: " + msgOf(s);
            case SocketTimeoutException s -> "read timed out: " + msgOf(s);
            case ConnectException c -> "connection refused: " + msgOf(c);
            // No route to host / Network is unreachable 等链路级故障均为 SocketException
            case java.net.SocketException se -> "network unreachable: " + msgOf(se);
            // 4xx/5xx（执行器拒绝、令牌错误等）与其它异常：保留原始消息，由调度中心按业务拒绝处理
            default -> msgOf(t);
        };
    }

    /** 异常消息为空时退回异常类名，保证故障描述永远非空。 */
    private static String msgOf(Throwable t) {
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    /** 区分连接阶段与读取阶段的 SocketTimeoutException（HttpURLConnection 下消息措辞不同）。 */
    private static boolean isConnectTimeout(SocketTimeoutException s) {
        String m = s.getMessage();
        return m != null && m.toLowerCase(Locale.ROOT).contains("connect");
    }

    /**
     * 解析触发调用的 readTimeout（毫秒）。
     *
     * 取 {@code orbit.admin.trigger-timeout-seconds}，下限 1 秒，
     * 避免负的 readTimeout 在 {@code HttpURLConnection} 中等同于「无限等待」。
     *
     * @param properties 调度中心配置
     * @return 读取超时毫秒数
     */
    private static int triggerReadTimeoutMs(AdminProperties properties) {
        // 下限 1 秒：配成 0 或负数会让 HttpURLConnection 的 readTimeout 变成「无限等待」，
        // 表现为触发线程被永久占住且没有任何报错。
        int seconds = properties.getTriggerTimeoutSeconds();
        if (seconds < 1) {
            seconds = 1;
        }
        long ms = seconds * 1000L;
        if (ms > Integer.MAX_VALUE) {
            ms = Integer.MAX_VALUE;
        }
        return (int) ms;
    }

    /**
     * 预构建 JSON + 鉴权 Header。
     */
    private static HttpHeaders buildJsonHeaders(String accessToken) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        if (accessToken != null && !accessToken.isEmpty()) {
            headers.set(OrbitProtocol.TOKEN_HEADER, accessToken);
        }
        return headers;
    }

    /**
     * 辅助工厂方法：根据指定的连接超时和读取超时构建 RestTemplate。
     *
     * 底层使用 JDK HttpClient（NIO）：不会在阻塞等待时持有监视器，
     * 对 JDK 21 虚拟线程友好（见类注释），且与平台线程模式行为一致。
     *
     * @param connectMs 连接超时毫秒数
     * @param readMs    读取超时毫秒数
     * @return RestTemplate 实例
     */
    private static RestTemplate buildRest(int connectMs, int readMs) {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectMs))
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(Duration.ofMillis(readMs));
        return new RestTemplate(factory);
    }

}

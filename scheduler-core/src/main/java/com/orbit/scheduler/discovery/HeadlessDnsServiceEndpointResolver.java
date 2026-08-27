package com.orbit.scheduler.discovery;

import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.spi.ServiceEndpointResolver;
import com.orbit.scheduler.support.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * K8s Headless Service DNS 端点解析器（云原生模式）。
 *
 * <p>通过 {@code InetAddress.getAllByName(serviceName)} 解析 Headless Service
 * 对应的全部 Pod IP（CoreDNS 直接返回 A 记录列表），零额外依赖、天然支持
 * Pod 扩缩容。解析结果按 TTL 缓存（默认 10s）以降低 DNS 压力。
 *
 * <p>非 K8s 环境可退化为普通域名解析（如 docker-compose 中的服务名），
 * 或改用静态端点列表 {@code StaticServiceEndpointResolver}。
 *
 * <p><b>性能优化</b>：使用 {@link AtomicReference} 替代 synchronized + volatile，
 * 读路径无锁；DNS 解析失败时引入退避（{@link #FAILURE_BACKOFF_MS}）避免每次请求
 * 都重试 DNS 触发雪崩，期间仍返回最近一次成功的快照。
 *
 * @author orbit
 */
public class HeadlessDnsServiceEndpointResolver implements ServiceEndpointResolver {

    private static final Logger log = LoggerFactory.getLogger(HeadlessDnsServiceEndpointResolver.class);

    /** DNS 解析缓存 TTL（毫秒） */
    static final long CACHE_TTL_MS = 10_000L;

    /** DNS 解析失败后的退避时间（毫秒），避免每次请求都重试 DNS */
    static final long FAILURE_BACKOFF_MS = 2_000L;

    private final SchedulerProperties properties;

    /** 缓存快照：不可变 List，整体替换以无锁读 */
    private final AtomicReference<CacheEntry> cacheRef = new AtomicReference<CacheEntry>(CacheEntry.EMPTY);

    public HeadlessDnsServiceEndpointResolver(SchedulerProperties properties) {
        this.properties = properties;
    }

    @Override
    public List<ServiceEndpoint> resolve(String serviceName) {
        String host = (serviceName == null || serviceName.trim().isEmpty())
                ? properties.getHttpDispatch().getServiceName()
                : serviceName.trim();
        if (host == null || host.trim().isEmpty()) {
            log.warn("[orbit-scheduler] no service name configured for endpoint resolution " +
                    "(set orbit.scheduler.http-dispatch.service-name)");
            return Collections.emptyList();
        }

        long now = System.currentTimeMillis();
        CacheEntry snapshot = cacheRef.get();
        if (snapshot != null && snapshot.endpoints != null && !snapshot.endpoints.isEmpty()
                && now - snapshot.resolvedAt < (snapshot.lastResolveOk ? CACHE_TTL_MS : FAILURE_BACKOFF_MS)) {
            return snapshot.endpoints;
        }

        synchronized (this) {
            snapshot = cacheRef.get();
            if (snapshot != null && snapshot.endpoints != null && !snapshot.endpoints.isEmpty()
                    && now - snapshot.resolvedAt < (snapshot.lastResolveOk ? CACHE_TTL_MS : FAILURE_BACKOFF_MS)) {
                return snapshot.endpoints;
            }
            try {
                InetAddress[] addresses = InetAddress.getAllByName(host);
                Set<String> urls = new LinkedHashSet<String>();
                int port = properties.getHttpDispatch().getPort();
                for (InetAddress addr : addresses) {
                    // 过滤回环，避免单机自路由打满；本机真实 IP 保留（自路由可工作）
                    String ip = addr.getHostAddress();
                    if (ip != null && !"127.0.0.1".equals(ip) && !"0:0:0:0:0:0:0:1".equals(ip)) {
                        urls.add("http://" + formatHost(ip) + ":" + port);
                    }
                }
                List<ServiceEndpoint> newEndpoints = new ArrayList<ServiceEndpoint>(
                        urls.isEmpty() ? Collections.<ServiceEndpoint>emptyList() : toEndpoints(urls));
                if (!newEndpoints.isEmpty()) {
                    cacheRef.set(new CacheEntry(host, newEndpoints, now, true));
                    if (log.isDebugEnabled()) {
                        log.debug("[orbit-scheduler] resolved {} endpoint(s) for service '{}'",
                                newEndpoints.size(), host);
                    }
                    return newEndpoints;
                }
                cacheRef.set(new CacheEntry(host, lastNonEmptyOrNull(snapshot), now, false));
                log.warn("[orbit-scheduler] resolved service '{}' to empty endpoint list (after loopback filter)", host);
                List<ServiceEndpoint> fallback = lastNonEmptyOrNull(snapshot);
                return fallback == null ? Collections.<ServiceEndpoint>emptyList() : fallback;
            } catch (Exception e) {
                cacheRef.set(new CacheEntry(host, lastNonEmptyOrNull(snapshot), now, false));
                log.warn("[orbit-scheduler] resolve service '{}' failed: {} (return last cache if any, " +
                        "backoff {}ms)", host, e.getMessage(), FAILURE_BACKOFF_MS);
                List<ServiceEndpoint> fallback = lastNonEmptyOrNull(snapshot);
                return fallback == null ? Collections.<ServiceEndpoint>emptyList() : fallback;
            }
        }
    }

    private static List<ServiceEndpoint> lastNonEmptyOrNull(CacheEntry entry) {
        if (entry == null || entry.endpoints == null || entry.endpoints.isEmpty()) {
            return null;
        }
        return entry.endpoints;
    }

    private static String formatHost(String host) {
        return host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
    }

    private static List<ServiceEndpoint> toEndpoints(Set<String> urls) {
        List<ServiceEndpoint> list = new ArrayList<ServiceEndpoint>(urls.size());
        for (String url : urls) {
            list.add(new ServiceEndpoint(url));
        }
        return list;
    }

    /** 不可变缓存条目 */
    private static final class CacheEntry {
        static final CacheEntry EMPTY = new CacheEntry("", Collections.<ServiceEndpoint>emptyList(), 0L, true);

        final String serviceName;
        final List<ServiceEndpoint> endpoints;
        final long resolvedAt;
        final boolean lastResolveOk;

        CacheEntry(String serviceName, List<ServiceEndpoint> endpoints, long resolvedAt, boolean lastResolveOk) {
            this.serviceName = serviceName == null ? "" : serviceName;
            this.endpoints = endpoints == null ? Collections.<ServiceEndpoint>emptyList() : endpoints;
            this.resolvedAt = resolvedAt;
            this.lastResolveOk = lastResolveOk;
        }
    }
}

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
 * 普通 Kubernetes Service DNS 端点解析器。
 *
 * <p>默认面向 ClusterIP Service：DNS 通常解析为 Service 的 ClusterIP，
 * HTTP 请求发送到 Service 后由 Kubernetes Service 负责 Pod 级负载均衡和
 * EndpointSlice 故障摘除。这样业务侧无需额外维护 Headless Service。</p>
 *
 * <p>在非 K8s 环境也可以解析普通域名；若需要客户端直接获得多个 Pod IP，
 * 请显式使用 {@code headless-dns} 模式。</p>
 *
 * <p><b>性能优化</b>：使用 {@link AtomicReference} + 双重判定取代 synchronized，
 * 减少热点路径上的锁争用；DNS 解析失败时返回最近一次成功的快照（不延长 TTL），
 * 同时记录连续失败计数，避免每次请求都触发 DNS 查询的雪崩。
 *
 * @author orbit
 */
public class ServiceDnsServiceEndpointResolver implements ServiceEndpointResolver {

    private static final Logger log = LoggerFactory.getLogger(ServiceDnsServiceEndpointResolver.class);

    /** DNS 解析缓存 TTL（毫秒） */
    static final long CACHE_TTL_MS = 10_000L;

    /** DNS 解析失败后的退避时间（毫秒），避免每次请求都重试 DNS */
    static final long FAILURE_BACKOFF_MS = 2_000L;

    private final SchedulerProperties properties;

    /** 缓存快照：不可变 List，整体替换以无锁读 */
    private final AtomicReference<CacheEntry> cacheRef = new AtomicReference<CacheEntry>(CacheEntry.EMPTY);

    public ServiceDnsServiceEndpointResolver(SchedulerProperties properties) {
        this.properties = properties;
    }

    @Override
    public boolean isLoadBalanced() {
        return true;
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
        // 命中缓存：服务名相同且未过期，直接返回
        if (snapshot != null && snapshot.serviceName.equals(host)
                && snapshot.endpoints != null && !snapshot.endpoints.isEmpty()
                && now - snapshot.resolvedAt < (snapshot.lastResolveOk ? CACHE_TTL_MS : FAILURE_BACKOFF_MS)) {
            return snapshot.endpoints;
        }

        // 进入单线程内做 DNS 查询，避免高并发下并发解析
        synchronized (this) {
            snapshot = cacheRef.get();
            if (snapshot != null && snapshot.serviceName.equals(host)
                    && snapshot.endpoints != null && !snapshot.endpoints.isEmpty()
                    && now - snapshot.resolvedAt < (snapshot.lastResolveOk ? CACHE_TTL_MS : FAILURE_BACKOFF_MS)) {
                return snapshot.endpoints;
            }
            try {
                InetAddress[] addresses = InetAddress.getAllByName(host);
                Set<String> urls = new LinkedHashSet<String>();
                int port = properties.getHttpDispatch().getPort();
                for (InetAddress addr : addresses) {
                    String ip = addr.getHostAddress();
                    if (ip == null || ip.isEmpty() || "127.0.0.1".equals(ip)
                            || "0:0:0:0:0:0:0:1".equals(ip)) {
                        continue;
                    }
                    urls.add("http://" + formatHost(ip) + ":" + port);
                }
                List<ServiceEndpoint> newEndpoints = toEndpoints(urls);
                if (!newEndpoints.isEmpty()) {
                    cacheRef.set(new CacheEntry(host, newEndpoints, now, true));
                    if (log.isDebugEnabled()) {
                        log.debug("[orbit-scheduler] resolved {} endpoint(s) for Service/domain '{}'",
                                newEndpoints.size(), host);
                    }
                    return newEndpoints;
                }
                // 解析成功但被过滤空（罕见：仅回环地址），按解析失败处理
                cacheRef.set(new CacheEntry(host, lastNonEmptyOrNull(snapshot), now, false));
                log.warn("[orbit-scheduler] resolved Service/domain '{}' to empty endpoint list " +
                        "(after loopback filter), keep last cache if any", host);
                return cacheRef.get().endpoints == null
                        ? Collections.<ServiceEndpoint>emptyList() : cacheRef.get().endpoints;
            } catch (Exception e) {
                cacheRef.set(new CacheEntry(host, lastNonEmptyOrNull(snapshot), now, false));
                log.warn("[orbit-scheduler] resolve Service/domain '{}' failed: {} (return last cache if any, " +
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

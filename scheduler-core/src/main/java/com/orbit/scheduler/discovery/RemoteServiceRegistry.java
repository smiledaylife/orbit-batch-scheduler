package com.orbit.scheduler.discovery;

import com.orbit.scheduler.model.RemoteServiceDefinition;
import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.support.SchedulerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 远程业务服务注册表：管理可被 REMOTE / WORKFLOW 调度的外部微服务端点。
 *
 * <p>来源：
 * <ol>
 *   <li>{@code orbit.scheduler.remote-services.<name>.*} 配置</li>
 *   <li>运行时 {@link #register(RemoteServiceDefinition)} 动态注册</li>
 * </ol>
 *
 * <p>解析优先级（单服务）：staticEndpoints → baseUrl → DNS(serviceName/name)。
 *
 * @author orbit
 */
public class RemoteServiceRegistry {

    private static final Logger log = LoggerFactory.getLogger(RemoteServiceRegistry.class);

    private final Map<String, RemoteServiceDefinition> services =
            new ConcurrentHashMap<String, RemoteServiceDefinition>();

    /** DNS 解析缓存：serviceKey → (resolvedAt, endpoints) */
    private final ConcurrentHashMap<String, CacheEntry> dnsCache =
            new ConcurrentHashMap<String, CacheEntry>();

    private static final long CACHE_TTL_MS = 10_000L;
    private static final long FAILURE_BACKOFF_MS = 2_000L;

    public RemoteServiceRegistry() {
    }

    public RemoteServiceRegistry(SchedulerProperties properties) {
        if (properties != null && properties.getRemoteServices() != null) {
            for (Map.Entry<String, RemoteServiceDefinition> e : properties.getRemoteServices().entrySet()) {
                RemoteServiceDefinition def = e.getValue();
                if (def == null) {
                    continue;
                }
                if (def.getName() == null || def.getName().trim().isEmpty()) {
                    def.setName(e.getKey());
                }
                register(def);
            }
        }
    }

    /** 注册或覆盖远程服务定义 */
    public void register(RemoteServiceDefinition definition) {
        if (definition == null || definition.getName() == null || definition.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("remote service name must not be blank");
        }
        String key = definition.getName().trim();
        definition.setName(key);
        services.put(key, definition);
        dnsCache.remove(key);
        log.info("[orbit-scheduler] registered remote service '{}' (host={}, baseUrl={}, static={})",
                key, definition.resolveHost(), blankToDash(definition.getBaseUrl()),
                definition.getStaticEndpoints() == null ? 0 : definition.getStaticEndpoints().size());
    }

    public void unregister(String name) {
        if (name == null) {
            return;
        }
        services.remove(name.trim());
        dnsCache.remove(name.trim());
    }

    public RemoteServiceDefinition get(String name) {
        if (name == null || name.trim().isEmpty()) {
            return null;
        }
        return services.get(name.trim());
    }

    public boolean contains(String name) {
        return get(name) != null;
    }

    public Map<String, RemoteServiceDefinition> listAll() {
        return Collections.unmodifiableMap(new LinkedHashMap<String, RemoteServiceDefinition>(services));
    }

    /**
     * 解析远程服务的可用端点列表。
     *
     * @param serviceName 注册名；若未注册，则按 serviceName 本身做 DNS 解析（端口取全局默认）
     * @param defaultPort 未注册服务时的默认端口
     */
    public List<ServiceEndpoint> resolveEndpoints(String serviceName, int defaultPort) {
        if (serviceName == null || serviceName.trim().isEmpty()) {
            return Collections.emptyList();
        }
        String key = serviceName.trim();

        // 完整 URL 直接作为单端点
        if (key.startsWith("http://") || key.startsWith("https://")) {
            return Collections.singletonList(new ServiceEndpoint(trimTrailingSlash(key)));
        }

        RemoteServiceDefinition def = services.get(key);
        if (def != null) {
            return resolveFromDefinition(def);
        }

        // 未注册：按 DNS 名解析，使用 defaultPort
        return resolveDns(key, key, defaultPort);
    }

    private List<ServiceEndpoint> resolveFromDefinition(RemoteServiceDefinition def) {
        // 1) 静态端点
        if (def.getStaticEndpoints() != null && !def.getStaticEndpoints().isEmpty()) {
            List<ServiceEndpoint> list = new ArrayList<ServiceEndpoint>();
            for (String url : def.getStaticEndpoints()) {
                if (url != null && !url.trim().isEmpty()) {
                    list.add(new ServiceEndpoint(trimTrailingSlash(url.trim())));
                }
            }
            if (!list.isEmpty()) {
                return list;
            }
        }

        // 2) baseUrl
        if (def.getBaseUrl() != null && !def.getBaseUrl().trim().isEmpty()) {
            return Collections.singletonList(new ServiceEndpoint(trimTrailingSlash(def.getBaseUrl().trim())));
        }

        // 3) DNS
        String host = def.resolveHost();
        if (host.isEmpty()) {
            return Collections.emptyList();
        }
        return resolveDns(def.getName(), host, def.getPort() > 0 ? def.getPort() : 8080);
    }

    private List<ServiceEndpoint> resolveDns(String cacheKey, String host, int port) {
        long now = System.currentTimeMillis();
        CacheEntry cached = dnsCache.get(cacheKey);
        if (cached != null && cached.endpoints != null && !cached.endpoints.isEmpty()
                && now - cached.resolvedAt < (cached.ok ? CACHE_TTL_MS : FAILURE_BACKOFF_MS)) {
            return cached.endpoints;
        }

        synchronized (this) {
            cached = dnsCache.get(cacheKey);
            if (cached != null && cached.endpoints != null && !cached.endpoints.isEmpty()
                    && now - cached.resolvedAt < (cached.ok ? CACHE_TTL_MS : FAILURE_BACKOFF_MS)) {
                return cached.endpoints;
            }
            try {
                InetAddress[] addresses = InetAddress.getAllByName(host);
                Set<String> urls = new LinkedHashSet<String>();
                for (InetAddress addr : addresses) {
                    String ip = addr.getHostAddress();
                    if (ip == null || ip.isEmpty()
                            || "127.0.0.1".equals(ip) || "0:0:0:0:0:0:0:1".equals(ip)) {
                        continue;
                    }
                    urls.add("http://" + formatHost(ip) + ":" + port);
                }
                // 若全部被过滤（本地开发常见），回退使用原始 host 名
                if (urls.isEmpty()) {
                    urls.add("http://" + formatHost(host) + ":" + port);
                }
                List<ServiceEndpoint> endpoints = new ArrayList<ServiceEndpoint>(urls.size());
                for (String u : urls) {
                    endpoints.add(new ServiceEndpoint(u));
                }
                List<ServiceEndpoint> immutable = Collections.unmodifiableList(endpoints);
                dnsCache.put(cacheKey, new CacheEntry(immutable, now, true));
                return immutable;
            } catch (Exception e) {
                log.warn("[orbit-scheduler] resolve remote service '{}' (host={}) failed: {}",
                        cacheKey, host, e.getMessage());
                List<ServiceEndpoint> fallback = cached == null ? null : cached.endpoints;
                if (fallback == null || fallback.isEmpty()) {
                    // 最后兜底：直接用 host 拼 URL，交给 RestTemplate 报错
                    fallback = Collections.singletonList(
                            new ServiceEndpoint("http://" + formatHost(host) + ":" + port));
                }
                dnsCache.put(cacheKey, new CacheEntry(fallback, now, false));
                return fallback;
            }
        }
    }

    private static String formatHost(String host) {
        return host.indexOf(':') >= 0 && !host.startsWith("[") ? "[" + host + "]" : host;
    }

    private static String trimTrailingSlash(String url) {
        if (url.endsWith("/") && url.length() > 1) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }

    private static String blankToDash(String s) {
        return s == null || s.trim().isEmpty() ? "-" : s;
    }

    private static final class CacheEntry {
        final List<ServiceEndpoint> endpoints;
        final long resolvedAt;
        final boolean ok;

        CacheEntry(List<ServiceEndpoint> endpoints, long resolvedAt, boolean ok) {
            this.endpoints = endpoints;
            this.resolvedAt = resolvedAt;
            this.ok = ok;
        }
    }
}

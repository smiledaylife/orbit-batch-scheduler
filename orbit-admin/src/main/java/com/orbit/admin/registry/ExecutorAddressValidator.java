package com.orbit.admin.registry;

import com.orbit.core.protocol.OrbitProtocol;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 执行器注册地址校验器。
 * 注册地址最终会被调度中心作为 HTTP 出口访问，因此同时防护 IP 字面量和 DNS
 * 解析后的保留地址，避免通过恶意域名绕过 SSRF 检查。RFC1918 私网地址允许使用，
 * 因为执行器通常部署在 VPC/K8S 私网中。
 *
 * 结果缓存：心跳每 20 秒一次，若每次都做 DNS 解析（{@code InetAddress.getAllByName}），
 * 会在心跳路径上引入毫秒级~秒级延迟，并在 K8s 环境下给 DNS 服务造成 N 副本 × 每 20 秒
 * 的稳态压力；DNS 抖动还会让心跳间歇性失败。因此校验结果（含拒绝原因）按
 * {@value #CACHE_TTL_MS} 毫秒缓存 —— 收益与代价的折中：域名到保留地址的恶意切换
 * 最多延迟一个 TTL 周期被发现，而正常执行器的 DNS 查询频率下降一个数量级。
 */
public final class ExecutorAddressValidator {

    /** IPv4 字面量格式（用于提取每段并校验范围） */
    private static final Pattern IPV4 = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

    /** 校验结果缓存时长（毫秒） */
    private static final long CACHE_TTL_MS = 60_000L;

    /** 缓存容量上限：执行器地址数量有限，超限说明出现异常流量，先清扫再半量淘汰 */
    private static final int CACHE_MAX_ENTRIES = 512;

    /**
     * 校验结果缓存条目：成功存规范化地址，失败存异常。
     * JDK 21 record：三个只读组件天然不可变，配合 volatile 快照写法避免加锁。
     */
    private record CachedResult(
            /** 缓存失效时刻（epoch 毫秒） */
            long expiresAtMs,
            /** 成功时的规范化地址；失败时为 null */
            String normalized,
            /** 失败时的拒绝原因；成功时为 null */
            IllegalArgumentException error) {
    }

    /** 校验结果缓存：key = rawAddress + '|' + allowPattern */
    private static final ConcurrentHashMap<String, CachedResult> CACHE =
            new ConcurrentHashMap<String, CachedResult>();

    private ExecutorAddressValidator() {
    }

    /**
     * 校验并规范化执行器注册地址（防 SSRF 入口）。
     *
     * 校验链：
     *   1. 协议必须为 http/https，且不允许 user-info、query、fragment；
     *   2. 必须携带 host，端口（如有）合法；
     *   3. 拒绝保留地址字面量（0/8、127/8、169.254/16 云元数据、组播及以上、链路本地 IPv6 等）；
     *   4. 对域名做 DNS 解析，解析到保留地址同样拒绝（防恶意域名绕过字面量检查）；
     *   5. 配置了白名单正则时，整串匹配不通过则拒绝。
     *
     * 结果在 {@value #CACHE_TTL_MS} 毫秒内直接复用（见类注释的缓存动机）。
     *
     * RFC1918 私网地址放行：执行器通常部署在 VPC/K8s 私网中。
     *
     * @param rawAddress   执行器上报的原始地址
     * @param allowPattern 白名单正则（整串匹配）；空表示不启用
     * @return 规范化后的地址（去首尾空白、去末尾斜杠）
     * @throws IllegalArgumentException 任一校验不通过时抛出，由全局异常处理器转为 400
     */
    public static String validateAndNormalize(String rawAddress, String allowPattern) {
        if (rawAddress == null || rawAddress.trim().isEmpty()) {
            throw new IllegalArgumentException("address required");
        }
        String key = rawAddress.trim() + "|" + (allowPattern == null ? "" : allowPattern.trim());
        long now = System.currentTimeMillis();

        CachedResult cached = CACHE.get(key);
        if (cached != null && cached.expiresAtMs() > now) {
            if (cached.error() != null) {
                throw cached.error();
            }
            return cached.normalized();
        }

        String normalized;
        try {
            normalized = doValidateAndNormalize(rawAddress, allowPattern);
        } catch (IllegalArgumentException e) {
            putCache(key, new CachedResult(now + CACHE_TTL_MS, null, e));
            throw e;
        }
        putCache(key, new CachedResult(now + CACHE_TTL_MS, normalized, null));
        return normalized;
    }

    /** 写缓存并按需清扫/淘汰，保证容量有界 */
    private static void putCache(String key, CachedResult result) {
        CACHE.put(key, result);
        if (CACHE.size() <= CACHE_MAX_ENTRIES) {
            return;
        }
        long now = System.currentTimeMillis();
        Iterator<Map.Entry<String, CachedResult>> it = CACHE.entrySet().iterator();
        while (it.hasNext()) {
            if (it.next().getValue().expiresAtMs() <= now) {
                it.remove();
            }
        }
        // 清扫后仍超限（地址数量真的大）：无差别淘汰一半，校验开销退化为一次性 DNS
        while (CACHE.size() > CACHE_MAX_ENTRIES) {
            int half = CACHE.size() / 2;
            it = CACHE.entrySet().iterator();
            int removed = 0;
            while (it.hasNext() && removed < half) {
                it.next();
                it.remove();
                removed++;
            }
            if (removed == 0) {
                break;
            }
        }
    }

    /** 实际校验链（无缓存版本），步骤见 {@link #validateAndNormalize} */
    private static String doValidateAndNormalize(String rawAddress, String allowPattern) {
        String address = OrbitProtocol.trimTrailingSlash(rawAddress.trim());
        URI uri;
        try {
            uri = new URI(address);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("invalid executor address: " + rawAddress);
        }
        String scheme = uri.getScheme();
        if (scheme == null || !("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("executor address must use http or https: " + rawAddress);
        }
        if (uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null) {
            throw new IllegalArgumentException("executor address must not contain user-info, query or fragment: " + rawAddress);
        }
        String host = uri.getHost();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("executor address must contain a host: " + rawAddress);
        }
        int port = uri.getPort();
        if (port != -1 && (port <= 0 || port > 65535)) {
            throw new IllegalArgumentException("executor address has an invalid port: " + rawAddress);
        }

        rejectReservedHost(host, rawAddress);
        rejectResolvedReservedHost(host, rawAddress);

        if (allowPattern != null && !allowPattern.trim().isEmpty()) {
            Pattern pattern;
            try {
                pattern = Pattern.compile(allowPattern.trim());
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(
                        "orbit.admin.executor-address-allow-pattern is not a valid regex: " + allowPattern);
            }
            if (!pattern.matcher(address).matches()) {
                throw new IllegalArgumentException("executor address rejected by allow-pattern: " + rawAddress);
            }
        }
        return address;
    }

    /**
     * 对域名做 DNS 解析，任一解析结果落在保留地址则拒绝。
     * 域名不可解析同样拒绝：注册一个调度中心连不上的地址没有意义。
     */
    private static void rejectResolvedReservedHost(String host, String rawAddress) {
        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress address : addresses) {
                if (isReserved(address)) {
                    throw new IllegalArgumentException(
                            "executor address resolves to a reserved/local address: " + rawAddress);
                }
            }
        } catch (java.net.UnknownHostException e) {
            throw new IllegalArgumentException("executor address host cannot be resolved: " + rawAddress);
        }
    }

    /** 判断 InetAddress 是否属于保留/特殊用途地址段（IPv4 精确到前两个字节） */
    private static boolean isReserved(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress()
                || address.isLinkLocalAddress() || address.isMulticastAddress()) {
            return true;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == 4) {
            int a = bytes[0] & 0xff;
            int b = bytes[1] & 0xff;
            // 0/8、127/8、169.254/16（云元数据地址所在链路本地网段）、组播/广播。
            return a == 0 || a == 127 || (a == 169 && b == 254) || a >= 224;
        }
        return false;
    }

    /** 拒绝 host 为保留地址字面量的注册（不做 DNS，直接匹配 IP 字面量） */
    private static void rejectReservedHost(String host, String rawAddress) {
        if (host.startsWith("[")) {
            String v6 = host.length() > 1 ? host.substring(1, host.length() - 1).toLowerCase() : "";
            if (v6.startsWith("fe8") || v6.startsWith("fe9") || v6.startsWith("fea") || v6.startsWith("feb")
                    || v6.startsWith("ff") || v6.equals("::") || v6.startsWith("::ffff:")) {
                throw new IllegalArgumentException("executor address must not be a reserved IPv6 address: " + rawAddress);
            }
            return;
        }
        java.util.regex.Matcher m = IPV4.matcher(host);
        if (!m.matches()) {
            return;
        }
        int[] o = new int[4];
        for (int i = 0; i < 4; i++) {
            o[i] = Integer.parseInt(m.group(i + 1));
            if (o[i] > 255) {
                throw new IllegalArgumentException("executor address has an invalid IPv4 literal: " + rawAddress);
            }
        }
        boolean reserved = o[0] == 0 || o[0] == 127
                || (o[0] == 169 && o[1] == 254) || o[0] >= 224;
        if (reserved) {
            throw new IllegalArgumentException("executor address must not be a reserved IPv4 address: " + rawAddress);
        }
    }
}

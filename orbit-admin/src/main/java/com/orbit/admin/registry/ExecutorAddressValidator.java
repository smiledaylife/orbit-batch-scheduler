package com.orbit.admin.registry;

import com.orbit.core.protocol.OrbitProtocol;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 执行器注册地址校验器。
 *
 * 注册地址最终会被调度中心作为 HTTP 出口访问，因此必须同时防护 IP 字面量和 DNS
 * 解析后的保留地址，避免通过恶意域名绕过 SSRF 检查。
 */
public final class ExecutorAddressValidator {

    private static final Pattern IPV4 = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

    private ExecutorAddressValidator() {
    }

    /**
     * 校验并规范化执行器地址。
     *
     * @param rawAddress 原始地址，例如 http://10.0.0.1:8081
     * @param allowPattern 白名单正则，为空时仍执行 DNS/保留地址检查
     * @return 去掉末尾斜杠后的规范化地址
     */
    public static String validateAndNormalize(String rawAddress, String allowPattern) {
        if (rawAddress == null || rawAddress.trim().isEmpty()) {
            throw new IllegalArgumentException("address required");
        }
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

    private static boolean isReserved(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress()
                || address.isLinkLocalAddress() || address.isSiteLocalAddress()
                || address.isMulticastAddress()) {
            return true;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == 4) {
            int a = bytes[0] & 0xff;
            int b = bytes[1] & 0xff;
            // RFC1918、link-local、CGNAT，以及 0/8、127/8、multicast/broadcast。
            return a == 0 || a == 127
                    || (a == 10)
                    || (a == 172 && b >= 16 && b <= 31)
                    || (a == 192 && b == 168)
                    || (a == 169 && b == 254)
                    || (a >= 224 && a <= 255);
        }
        return false;
    }

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
                || (o[0] == 10)
                || (o[0] == 172 && o[1] >= 16 && o[1] <= 31)
                || (o[0] == 192 && o[1] == 168)
                || (o[0] == 169 && o[1] == 254)
                || (o[0] >= 224 && o[0] <= 255);
        if (reserved) {
            throw new IllegalArgumentException("executor address must not be a reserved IPv4 address: " + rawAddress);
        }
    }
}

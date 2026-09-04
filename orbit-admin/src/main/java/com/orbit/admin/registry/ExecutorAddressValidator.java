package com.orbit.admin.registry;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * 执行器注册地址校验器。
 *
 * 背景：{@code POST /orbit/admin/registry} 的 {@code address} 字段完全由调用方给出，
 * 调度中心随后会向该地址发起 {@code POST /orbit/executor/run}，并在请求中携带 accessToken。
 * 若不校验，该接口即成为一个 SSRF 入口（例如注册云厂商元数据地址
 * {@code http://169.254.169.254/latest/meta-data}）。
 *
 * 校验规则：
 *
 *   1) 必须是合法 URI，且协议只能是 http / https；
 *   2) 必须含 host，端口（若显式给出）必须在 1..65535；
 *   3) host 为 IP 字面量时，拒绝链路本地（169.254.0.0/16、fe80::/10）、
 *      任意本地（0.0.0.0/8、::）、组播与广播地址；
 *   4) 若配置了 {@code orbit.admin.executor-address-allow-pattern}，
 *      地址还必须整体匹配该正则。
 *
 * 已知边界：
 *
 *   - 本类刻意不做 DNS 解析。原因是解析会给注册路径引入网络延迟与不确定性，
 *     且离线/测试环境（如 host 为 {@code a} 的用例）会因此失败。因此攻击者仍可能
 *     用一个解析到 169.254.169.254 的域名绕过第 3 条；
 *   - 回环地址（127.0.0.0/8、::1、localhost）刻意放行，以便单机 / docker-compose
 *     直接联调；
 *   - 上述两类绕过都需要在生产环境用第 4 条的白名单正则一并封堵。
 */
public final class ExecutorAddressValidator {

    /** IPv4 点分十进制字面量 */
    private static final Pattern IPV4 = Pattern.compile("^(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})\\.(\\d{1,3})$");

    private ExecutorAddressValidator() {
    }

    /**
     * 校验并规范化执行器地址。
     *
     * @param rawAddress   原始地址，例如 {@code http://10.0.0.1:8081}
     * @param allowPattern 白名单正则，为空表示不启用
     * @return 去掉末尾斜杠后的规范化地址
     * @throws IllegalArgumentException 地址不合法或不被允许
     */
    public static String validateAndNormalize(String rawAddress, String allowPattern) {
        if (rawAddress == null || rawAddress.trim().isEmpty()) {
            throw new IllegalArgumentException("address required");
        }
        String address = trimSlash(rawAddress.trim());

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

        String host = uri.getHost();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalArgumentException("executor address must contain a host: " + rawAddress);
        }

        int port = uri.getPort();
        if (port != -1 && (port <= 0 || port > 65535)) {
            throw new IllegalArgumentException("executor address has an invalid port: " + rawAddress);
        }

        rejectReservedHost(host, rawAddress);

        if (allowPattern != null && !allowPattern.trim().isEmpty()) {
            Pattern pattern;
            try {
                pattern = Pattern.compile(allowPattern.trim());
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException(
                        "orbit.admin.executor-address-allow-pattern is not a valid regex: " + allowPattern);
            }
            if (!pattern.matcher(address).matches()) {
                throw new IllegalArgumentException(
                        "executor address rejected by allow-pattern: " + rawAddress);
            }
        }
        return address;
    }

    /**
     * 拒绝 IP 字面量形式的保留地址（链路本地 / 任意本地 / 组播 / 广播）。
     * 主机名不在此处解析，见类注释「已知边界」。
     */
    private static void rejectReservedHost(String host, String rawAddress) {
        // IPv6 字面量在 URI.getHost() 中带方括号，例如 [::1] / [fe80::1]
        if (host.startsWith("[")) {
            String v6 = host.length() > 1 ? host.substring(1, host.length() - 1).toLowerCase() : "";
            // 链路本地 fe80::/10 与组播 ff00::/8
            if (v6.startsWith("fe8") || v6.startsWith("fe9") || v6.startsWith("fea") || v6.startsWith("feb")
                    || v6.startsWith("ff")) {
                throw new IllegalArgumentException("executor address must not be a reserved IPv6 address: " + rawAddress);
            }
            // 未指定地址 ::（对应 IPv4 的 0.0.0.0）
            if (v6.equals("::")) {
                throw new IllegalArgumentException("executor address must not be a reserved IPv6 address: " + rawAddress);
            }
            // IPv4 映射地址 ::ffff:a.b.c.d —— 剥出内嵌 IPv4 复用同一套规则，避免绕过
            if (v6.startsWith("::ffff:")) {
                rejectReservedHost(v6.substring("::ffff:".length()), rawAddress);
            }
            return;
        }

        java.util.regex.Matcher m = IPV4.matcher(host);
        if (!m.matches()) {
            return; // 主机名，交由白名单正则处理
        }
        int[] o = new int[4];
        for (int i = 0; i < 4; i++) {
            o[i] = Integer.parseInt(m.group(i + 1));
            if (o[i] > 255) {
                throw new IllegalArgumentException("executor address has an invalid IPv4 literal: " + rawAddress);
            }
        }
        boolean linkLocal = o[0] == 169 && o[1] == 254;      // 169.254.0.0/16，云元数据地址所在段
        boolean anyLocal = o[0] == 0;                        // 0.0.0.0/8
        boolean multicast = o[0] >= 224 && o[0] <= 239;      // 224.0.0.0/4
        boolean broadcast = o[0] == 255 && o[1] == 255 && o[2] == 255 && o[3] == 255;
        if (linkLocal || anyLocal || multicast || broadcast) {
            throw new IllegalArgumentException("executor address must not be a reserved IPv4 address: " + rawAddress);
        }
    }

    /**
     * 去掉末尾多余的斜杠（保留单独的 "/"）。
     */
    private static String trimSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }
}

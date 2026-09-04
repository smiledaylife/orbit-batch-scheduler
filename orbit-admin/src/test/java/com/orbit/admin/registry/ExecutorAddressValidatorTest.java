package com.orbit.admin.registry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 执行器注册地址校验器测试（SSRF 防护）。
 * 覆盖：协议白名单、地址规范化、保留地址拒绝、IPv4 映射的 IPv6 绕过、白名单正则。
 * 纯静态工具类，无需 Spring 上下文。
 */
class ExecutorAddressValidatorTest {

    private static final String NO_PATTERN = "";

    @Test
    void acceptHttpAndHttps() {
        assertEquals("http://10.0.0.1:8081",
                ExecutorAddressValidator.validateAndNormalize("http://10.0.0.1:8081", null));
        assertEquals("https://orbit-executor.orbit-system.svc.cluster.local:8081",
                ExecutorAddressValidator.validateAndNormalize(
                        "https://orbit-executor.orbit-system.svc.cluster.local:8081", NO_PATTERN));
    }

    @Test
    void normalizeTrimsWhitespaceAndTrailingSlash() {
        assertEquals("http://10.0.0.1:8081",
                ExecutorAddressValidator.validateAndNormalize("  http://10.0.0.1:8081/  ", null));
        assertEquals("http://orbit-executor:8081",
                ExecutorAddressValidator.validateAndNormalize("http://orbit-executor:8081/", null));
    }

    @Test
    void rejectBlankAddress() {
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize(null, null));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("   ", null));
    }

    @Test
    void rejectMissingOrNonHttpScheme() {
        // 无协议（Java URI 也无法把 "10.0.0.1:8081" 解析出 host）
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("10.0.0.1:8081", null));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("ftp://10.0.0.1:21", null));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("file:///etc/passwd", null));
    }

    @Test
    void rejectCloudMetadataAndReservedIpv4() {
        // 云厂商元数据地址，也是 SSRF 的经典目标
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://169.254.169.254/latest/meta-data", null));
        // 任意本地 0.0.0.0/8
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://0.0.0.0:8081", null));
        // 组播 224.0.0.0/4
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://224.0.0.1:8081", null));
        // 广播
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://255.255.255.255:8081", null));
        // 非法 IPv4 字面量
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://999.1.1.1:8081", null));
    }

    @Test
    void rejectReservedIpv6() {
        // 链路本地 fe80::/10
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[fe80::1]:8081", null));
        // 组播 ff00::/8
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[ff02::1]:8081", null));
        // 未指定地址 ::
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[::]:8081", null));
    }

    @Test
    void rejectIpv4MappedIpv6Bypass() {
        // 用 ::ffff:169.254.169.254 包装链路本地地址，不能绕过校验
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[::ffff:169.254.169.254]:8081", null));
    }

    @Test
    void allowPatternActsAsWhitelist() {
        String pattern = "^https?://orbit-executor[a-z0-9-]*\\.orbit-system\\.svc\\.cluster\\.local(:\\d+)?$";
        assertEquals("http://orbit-executor-1.orbit-system.svc.cluster.local:8081",
                ExecutorAddressValidator.validateAndNormalize(
                        "http://orbit-executor-1.orbit-system.svc.cluster.local:8081", pattern));
        // 合法但不在白名单内：即使不是保留地址也要拒绝
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://10.0.0.9:8081", pattern));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://evil.example.com:8081", pattern));
    }

    @Test
    void rejectMalformedAllowPattern() {
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://10.0.0.1:8081", "([unclosed"));
    }

    @Test
    void hostnameIsNotResolved() {
        // 离线/测试环境友好：主机名不做 DNS 解析，单标签主机名同样放行
        assertEquals("http://a", ExecutorAddressValidator.validateAndNormalize("http://a", null));
        assertEquals("http://localhost:8081",
                ExecutorAddressValidator.validateAndNormalize("http://localhost:8081", null));
    }
}

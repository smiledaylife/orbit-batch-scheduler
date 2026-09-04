package com.orbit.admin.registry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 执行器注册地址校验器单元测试（SSRF 防线）。
 * 覆盖：协议 / host / 端口合法性、保留 IPv4/IPv6 段拒绝、IPv4 映射 IPv6 绕过拦截、
 * 白名单正则命中与拒绝、非法正则的友好报错、末尾斜杠规范化。
 */
class ExecutorAddressValidatorTest {

    @Test
    void acceptsHttpAndHttps() {
        assertEquals("http://10.0.0.1:8081",
                ExecutorAddressValidator.validateAndNormalize("http://10.0.0.1:8081", ""));
        assertEquals("https://exec.example.com",
                ExecutorAddressValidator.validateAndNormalize("https://exec.example.com", ""));
    }

    @Test
    void normalizesTrailingSlash() {
        assertEquals("http://10.0.0.1:8081",
                ExecutorAddressValidator.validateAndNormalize("http://10.0.0.1:8081/", ""));
    }

    @Test
    void rejectsNonHttpScheme() {
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("ftp://10.0.0.1:8081", ""));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("file:///etc/passwd", ""));
    }

    @Test
    void rejectsMissingHost() {
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://", ""));
    }

    @Test
    void rejectsInvalidPort() {
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://10.0.0.1:99999", ""));
    }

    @Test
    void rejectsReservedIpv4() {
        // 云元数据地址（SSRF 典型目标）
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://169.254.169.254/latest", ""));
        // 任意本地 / 组播 / 广播
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://0.0.0.0:8081", ""));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://224.0.0.1:8081", ""));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://255.255.255.255:8081", ""));
        // 非法 IPv4 字面量（段值 > 255）
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://300.1.1.1:8081", ""));
    }

    @Test
    void rejectsReservedIpv6() {
        // 链路本地 fe80::/10
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[fe80::1]:8081", ""));
        // 未指定地址 ::
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[::]:8081", ""));
        // IPv4 映射地址 ::ffff:169.254.169.254 —— 不允许借道绕过 IPv4 规则
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://[::ffff:169.254.169.254]:8081", ""));
    }

    @Test
    void allowPatternControlsAcceptance() {
        String pattern = "^https?://10\\.0\\.0\\.[0-9]+:8081$";
        // 命中白名单
        assertEquals("http://10.0.0.7:8081",
                ExecutorAddressValidator.validateAndNormalize("http://10.0.0.7:8081", pattern));
        // 不在白名单内被拒绝（即使地址本身合法）
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://192.168.1.5:8081", pattern));
    }

    @Test
    void invalidAllowPatternFailsFast() {
        // 非法正则应在注册入口即时报错，而非静默放行
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("http://10.0.0.1:8081", "("));
    }

    @Test
    void rejectsNullAndBlank() {
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize(null, ""));
        assertThrows(IllegalArgumentException.class,
                () -> ExecutorAddressValidator.validateAndNormalize("   ", ""));
    }
}

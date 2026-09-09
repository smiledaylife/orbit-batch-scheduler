package com.orbit.core.protocol;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * 调度中心与执行器之间的 HTTP 协议约定。
 *
 * 两端各自独立部署，令牌头名称、地址规范化与令牌比对这些约定必须只有一份定义，
 * 否则任何一处改动都可能让两端静默不兼容。放在 orbit-core 是因为它是两端唯一共同依赖的模块。
 */
public final class OrbitProtocol {

    /** 双向鉴权令牌的 HTTP Header 名称 */
    public static final String TOKEN_HEADER = "X-Orbit-Token";

    /**
     * 常量时间字符串比对。
     *
     * 直接用 {@code String.equals} 会在第一个不同字节处返回，比对耗时与「猜对的前缀长度」相关，
     * 攻击者可据此逐字节猜测令牌。{@link MessageDigest#isEqual} 的耗时只与长度相关。
     *
     * @param a 待比对字符串
     * @param b 待比对字符串
     * @return 相等返回 true；任一为 null 返回 false
     */
    public static boolean constantTimeEquals(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * 去掉地址末尾多余的斜杠，使 {@code http://host:8080} 与 {@code http://host:8080/}
     * 拼接出的端点 URL 一致。
     *
     * @param s 原始地址，不可为 null
     * @return 规范化后的地址
     */
    public static String trimTrailingSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }

    private OrbitProtocol() {
    }
}

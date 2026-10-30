/*
 * AICode Begin
 * ConnectionInfoValidator：tb_connection_info 外部连接参数的安全校验。
 *
 * TableCheck / ConfigCheck 任务会用库表中录入的 JDBC URL / 主机 / 文件路径直连外部系统。
 * 这些值由管理端录入，属于"半可信"输入：
 *   - JDBC URL：PostgreSQL 驱动支持 socketFactory / sslfactory / loggerFile 等参数，
 *     可被用于任意类加载或文件写入，因此只允许 jdbc:postgresql:// 且禁止危险参数；
 *   - SSH 读取的文件路径：会被拼进 shell 命令，必须限制为安全字符集的绝对路径。
 */
package com.natpcp.natpcp.dsv2.util;

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * 连接信息安全校验工具。
 */
public final class ConnectionInfoValidator {

    private static final String JDBC_PG_PREFIX = "jdbc:postgresql://";

    /** 允许出现在 JDBC URL 中的字符（主机/端口/库名/常规参数） */
    private static final Pattern JDBC_URL_CHARS = Pattern.compile("[A-Za-z0-9._:/?&=,%+-]+");

    /** PostgreSQL 驱动中可导致类加载 / 文件写入 / 外连的危险参数 */
    private static final String[] JDBC_FORBIDDEN_PARAMS = {
            "socketfactory", "socketfactoryarg", "sslfactory", "sslfactoryarg", "sslhostnameverifier",
            "sslpasswordcallback", "authenticationpluginclassname", "loggerfile", "loggerlevel",
            "sslrootcert", "sslcert", "sslkey", "sslresponsetimeout"
    };

    /** 绝对路径，仅允许字母数字及 . _ - / */
    private static final Pattern SAFE_ABSOLUTE_PATH = Pattern.compile("/(?:[A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+");

    private static final Pattern PORT = Pattern.compile("\\d{1,5}");

    private ConnectionInfoValidator() {
    }

    /**
     * 校验并返回 PostgreSQL JDBC URL。
     *
     * @throws IllegalArgumentException URL 为空、非 PostgreSQL、含非法字符或危险参数
     */
    public static String requireSafeJdbcUrl(String url) {
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("JDBC URL 为空");
        }
        String trimmed = url.trim();
        String lower = trimmed.toLowerCase(Locale.ROOT);
        if (!lower.startsWith(JDBC_PG_PREFIX)) {
            throw new IllegalArgumentException("仅允许 PostgreSQL JDBC URL: " + trimmed);
        }
        if (!JDBC_URL_CHARS.matcher(trimmed).matches()) {
            throw new IllegalArgumentException("JDBC URL 含非法字符: " + trimmed);
        }
        int q = lower.indexOf('?');
        if (q >= 0) {
            for (String kv : lower.substring(q + 1).split("&")) {
                String key = kv.split("=", 2)[0].trim();
                for (String forbidden : JDBC_FORBIDDEN_PARAMS) {
                    if (forbidden.equals(key)) {
                        throw new IllegalArgumentException("JDBC URL 含禁止的参数: " + key);
                    }
                }
            }
        }
        return trimmed;
    }

    /**
     * 校验并返回远程配置文件的绝对路径（用于 SSH 读取）。
     *
     * @throws IllegalArgumentException 非绝对路径或含有 shell 元字符
     */
    public static String requireSafeAbsolutePath(String path) {
        if (path == null || !SAFE_ABSOLUTE_PATH.matcher(path.trim()).matches() || path.contains("/../")) {
            throw new IllegalArgumentException("非法的远程文件路径: " + path);
        }
        return path.trim();
    }

    /**
     * 校验并解析端口（1-65535）。
     */
    public static int requirePort(String port) {
        if (port == null || !PORT.matcher(port.trim()).matches()) {
            throw new IllegalArgumentException("非法端口: " + port);
        }
        int p = Integer.parseInt(port.trim());
        if (p < 1 || p > 65535) {
            throw new IllegalArgumentException("端口超出范围: " + port);
        }
        return p;
    }
}
/*
 * AICode End
 */

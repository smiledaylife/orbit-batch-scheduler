package com.orbit.scheduler.dialect;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 数据库方言自动解析器（三级探测，best-effort）：
 *
 * <ol>
 *   <li><b>显式配置</b>：orbit.scheduler.database.dialect = postgresql | gaussdb（最高优先级，
 *       用于探测不可靠场景兜底）</li>
 *   <li><b>连接 URL / 产品名</b>：jdbc:opengauss://、jdbc:gaussdb:// 或产品名含 "gauss" →
 *       GaussDB；jdbc:postgresql:// 或产品名含 "postgres" → 候选 PostgreSQL（继续第 3 级确认）</li>
 *   <li><b>内核特征探测</b>：openGauss 内核特有的 <code>SHOW sql_compatibility</code>
 *       可执行 ⇒ GaussDB 家族（覆盖"用 PostgreSQL 驱动连接 openGauss，产品名上报
 *       PostgreSQL"的场景）；原生 PostgreSQL 上该语句报错 ⇒ PostgreSQL</li>
 * </ol>
 *
 * <p>探测不到 PG 系特征时返回 {@link SchedulerDialect#OTHER}（MySQL / H2 等），
 * 框架不干预 Quartz delegate，保持既有行为，<b>向后兼容</b>。
 *
 * <p><b>优化</b>：Connection / Statement / ResultSet 全部使用 try-with-resources 管理，
 * 杜绝异常路径下资源泄漏；探测语句失败仅降级返回而不抛出。
 *
 * @author orbit
 */
public final class DialectResolver {

    private static final Logger log = LoggerFactory.getLogger(DialectResolver.class);

    private DialectResolver() {
    }

    /**
     * 解析数据库方言。
     *
     * @param dataSource 数据源（探测期间获取并释放一个连接）
     * @param hint       显式方言提示：postgresql / gaussdb / opengauss / auto / 空串
     * @return 解析出的方言；探测失败抛出异常（由调用方决定兜底策略）
     * @throws SQLException 数据库不可达或元数据读取失败
     */
    public static SchedulerDialect resolve(DataSource dataSource, String hint) throws SQLException {
        // ---- 1) 显式配置优先 ----
        if (hint != null && !hint.trim().isEmpty() && !"auto".equalsIgnoreCase(hint.trim())) {
            String h = hint.trim();
            if ("gaussdb".equalsIgnoreCase(h) || "opengauss".equalsIgnoreCase(h)) {
                return SchedulerDialect.GAUSSDB;
            }
            if ("postgresql".equalsIgnoreCase(h) || "postgres".equalsIgnoreCase(h)) {
                return SchedulerDialect.POSTGRESQL;
            }
            return SchedulerDialect.OTHER;
        }

        // ---- 2) URL / 产品名探测 ----
        try (Connection connection = dataSource.getConnection()) {
            return detectFromConnection(connection);
        }
    }

    private static SchedulerDialect detectFromConnection(Connection connection) throws SQLException {
        String url = "";
        String product = "";
        try {
            String u = connection.getMetaData().getURL();
            if (u != null) {
                url = u.toLowerCase();
            }
            String p = connection.getMetaData().getDatabaseProductName();
            if (p != null) {
                product = p.toLowerCase();
            }
        } catch (SQLException e) {
            // 部分兼容驱动不支持元数据查询，降级为 OTHER
            return SchedulerDialect.OTHER;
        }

        // openGauss 官方驱动（org.opengauss.Driver / com.huawei.gaussdb.jdbc）
        if (url.startsWith("jdbc:opengauss") || url.startsWith("jdbc:gaussdb")) {
            return SchedulerDialect.GAUSSDB;
        }
        // 产品名包含 gauss（openGauss 驱动上报 "openGauss" / "GaussDB"）
        if (product.contains("gauss")) {
            return SchedulerDialect.GAUSSDB;
        }
        // 非 PG 系：MySQL / H2 / Oracle 等，保持既有行为
        if (url.startsWith("jdbc:mysql") || product.contains("mysql")
                || url.startsWith("jdbc:h2") || product.contains("h2")
                || url.startsWith("jdbc:oracle") || product.contains("oracle")
                || url.startsWith("jdbc:mariadb") || product.contains("mariadb")) {
            return SchedulerDialect.OTHER;
        }

        // ---- 3) PG 系确认：PostgreSQL 驱动也可能连的是 openGauss，用内核特征区分 ----
        if (url.startsWith("jdbc:postgresql") || product.contains("postgres")) {
            String compatibility = probeCompatibilityMode(connection);
            return compatibility != null ? SchedulerDialect.GAUSSDB : SchedulerDialect.POSTGRESQL;
        }
        return SchedulerDialect.OTHER;
    }

    /**
     * 探测 openGauss 内核特征参数 sql_compatibility（原生 PostgreSQL 不支持该参数）。
     *
     * @return 参数值（仅 openGauss 内核返回）；非 openGauss 内核（原生 PostgreSQL 等）返回 null
     */
    public static String probeCompatibilityMode(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SHOW sql_compatibility")) {
            if (rs.next()) {
                return rs.getString(1);
            }
            return null;
        } catch (Exception e) {
            // 原生 PostgreSQL 不认识该参数 → 报错即认定为非 openGauss 内核
            return null;
        }
    }

    /**
     * 检查 Quartz 集群表是否已初始化（探测 QRTZ_LOCKS，best-effort）。
     *
     * @return true=表存在；false=不存在（提示执行 deploy/sql 对应脚本）
     */
    public static boolean quartzTablesPresent(Connection connection) {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SELECT COUNT(*) FROM QRTZ_LOCKS")) {
            rs.next(); // COUNT(*) 必有一行
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}

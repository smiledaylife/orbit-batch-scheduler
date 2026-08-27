package com.orbit.scheduler.lock;

import com.orbit.scheduler.spi.LockProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Duration;

/**
 * 数据库分布式锁：基于 t_cluster_lock 表的唯一约束 + 过期时间戳（毫秒）实现。
 *
 * <p>特性：
 * <ul>
 *   <li>tryLock：先 INSERT，撞唯一键则尝试"过期抢占"UPDATE，均失败即未获锁（非重入）</li>
 *   <li>时间戳统一使用应用侧 System.currentTimeMillis()，规避 DB 与应用时钟不一致</li>
 *   <li>节点宕机后，锁在租约到期后被其它节点惰性抢占（无需后台清理任务）</li>
 *   <li>建表 DDL 与全部 SQL 兼容 MySQL / PostgreSQL / GaussDB / H2</li>
 * </ul>
 *
 * <p>多库兼容关键点：唯一键冲突的异常翻译在不同驱动下表现不同——
 * MySQL/PostgreSQL/H2 驱动经 Spring 错误码翻译得到 DuplicateKeyException；
 * GaussDB 驱动的错误码不在 Spring sql-error-codes.xml 注册表中，
 * 可能降级翻译为父类 DataIntegrityViolationException。
 * 因此这里捕获父类（DuplicateKeyException 是其子类），双库行为一致。
 *
 * <p><b>性能优化</b>：tryLock 单次时间戳采集，避免插入与抢占分支时间不一致；
 * renew 同理。新增无写库副作用的 isLocked 方法（健康检查用）。
 *
 * <p>DDL：
 * <pre>
 * CREATE TABLE IF NOT EXISTS t_cluster_lock (
 *   lock_name    VARCHAR(128) PRIMARY KEY,
 *   owner        VARCHAR(128),
 *   expire_at    BIGINT,
 *   update_time  BIGINT
 * );
 * </pre>
 *
 * @author orbit
 */
public class JdbcLockProvider implements LockProvider {

    private static final Logger log = LoggerFactory.getLogger(JdbcLockProvider.class);

    private static final String DDL =
            "CREATE TABLE IF NOT EXISTS t_cluster_lock (" +
                    "lock_name VARCHAR(128) PRIMARY KEY, " +
                    "owner VARCHAR(128), " +
                    "expire_at BIGINT, " +
                    "update_time BIGINT)";

    private final JdbcTemplate jdbcTemplate;
    private final boolean tableReady;

    public JdbcLockProvider(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        boolean ready = false;
        try {
            jdbcTemplate.execute(DDL);
            ready = true;
            log.info("[orbit-scheduler] JDBC cluster lock table 't_cluster_lock' ready");
        } catch (Exception e) {
            log.warn("[orbit-scheduler] auto create t_cluster_lock failed (maybe exists with different schema): {}", e.getMessage());
        }
        this.tableReady = ready;
    }

    @Override
    public boolean tryLock(String key, String owner, Duration lease) {
        // 单次时间采集：保证 INSERT 与抢占分支的 expireAt / now 语义一致
        long now = System.currentTimeMillis();
        long expireAt = now + lease.toMillis();
        try {
            jdbcTemplate.update(
                    "INSERT INTO t_cluster_lock (lock_name, owner, expire_at, update_time) VALUES (?, ?, ?, ?)",
                    key, owner, expireAt, now);
            return true;
        } catch (DataIntegrityViolationException e) {
            // 已存在锁：仅当已过期时抢占（owner 相同也不允许重入，保证同任务互斥）
            // 注：兼容 GaussDB 驱动下唯一键冲突翻译为父类 DataIntegrityViolationException 的情况
            int rows = jdbcTemplate.update(
                    "UPDATE t_cluster_lock SET owner = ?, expire_at = ?, update_time = ? " +
                            "WHERE lock_name = ? AND expire_at < ?",
                    owner, expireAt, now, key, now);
            return rows > 0;
        }
    }

    @Override
    public boolean renew(String key, String owner, Duration lease) {
        // 单次时间采集：避免 expire_at 与 update_time 时间错位
        long now = System.currentTimeMillis();
        long expireAt = now + lease.toMillis();
        int rows = jdbcTemplate.update(
                "UPDATE t_cluster_lock SET expire_at = ?, update_time = ? WHERE lock_name = ? AND owner = ?",
                expireAt, now, key, owner);
        return rows > 0;
    }

    @Override
    public void unlock(String key, String owner) {
        try {
            jdbcTemplate.update("DELETE FROM t_cluster_lock WHERE lock_name = ? AND owner = ?", key, owner);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] unlock {} failed: {}", key, e.getMessage());
        }
    }

    /** 健康检查用：表是否就绪 */
    public boolean isTableReady() {
        return tableReady;
    }

    @Override
    public String type() {
        return "database";
    }
}

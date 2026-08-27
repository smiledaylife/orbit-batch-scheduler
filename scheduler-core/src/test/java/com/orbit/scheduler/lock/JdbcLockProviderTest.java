package com.orbit.scheduler.lock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * JDBC 分布式锁测试（H2 内存库，MySQL 模式）。
 */
class JdbcLockProviderTest {

    private JdbcLockProvider provider;
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void setUp() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:lock_test;MODE=MySQL;DB_CLOSE_DELAY=-1", "sa", "");
        jdbcTemplate = new JdbcTemplate(dataSource);
        provider = new JdbcLockProvider(jdbcTemplate);
        jdbcTemplate.update("DELETE FROM t_cluster_lock");
    }

    @AfterEach
    void tearDown() {
        jdbcTemplate.update("DELETE FROM t_cluster_lock");
    }

    @Test
    void shouldAcquireReleaseAndReacquire() {
        assertTrue(provider.tryLock("t1", "node-a", Duration.ofSeconds(60)));
        // 同/异节点二次加锁均失败（非重入）
        assertFalse(provider.tryLock("t1", "node-a", Duration.ofSeconds(60)));
        assertFalse(provider.tryLock("t1", "node-b", Duration.ofSeconds(60)));

        provider.unlock("t1", "node-a");
        // 释放后其它节点可获取
        assertTrue(provider.tryLock("t1", "node-b", Duration.ofSeconds(60)));
    }

    @Test
    void shouldNotUnlockOthersLock() {
        assertTrue(provider.tryLock("t1", "node-a", Duration.ofSeconds(60)));
        // 错误 owner 解锁无效
        provider.unlock("t1", "node-b");
        assertFalse(provider.tryLock("t1", "node-c", Duration.ofSeconds(60)));
    }

    @Test
    void shouldTakeOverExpiredLock() throws InterruptedException {
        assertTrue(provider.tryLock("t1", "node-a", Duration.ofMillis(50)));
        Thread.sleep(80);
        // 租约过期，其它节点可抢占
        assertTrue(provider.tryLock("t1", "node-b", Duration.ofSeconds(60)));
    }

    @Test
    void shouldRenewOnlyByOwner() {
        assertTrue(provider.tryLock("t1", "node-a", Duration.ofSeconds(60)));
        assertTrue(provider.renew("t1", "node-a", Duration.ofSeconds(60)));
        assertFalse(provider.renew("t1", "node-b", Duration.ofSeconds(60)));
    }

    @Test
    void shouldNotTakeOverUnexpiredLockEvenAfterManyAttempts() {
        assertTrue(provider.tryLock("t1", "node-a", Duration.ofSeconds(60)));
        for (int i = 0; i < 5; i++) {
            assertFalse(provider.tryLock("t1", "node-z" + i, Duration.ofSeconds(60)));
        }
    }
}

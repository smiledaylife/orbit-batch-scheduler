package com.orbit.scheduler.storage;

import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.dialect.DialectResolver;
import com.orbit.scheduler.dialect.SchedulerDialect;
import com.orbit.scheduler.lock.JdbcLockProvider;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PostgreSQL / GaussDB 方言兼容性集成测试。
 *
 * <p>使用 H2 的 PostgreSQL 兼容模式（MODE=PostgreSQL）模拟 PG 系内核，
 * 验证框架全部 DAO SQL（DDL 结构、BIGSERIAL 自增回填、LIMIT-OFFSET 分页、
 * 乐观锁 UPDATE、唯一约束冲突翻译、锁表租约语义）在 PG 方言下的正确性；
 * 并用 SEQUENCE 表结构验证 GaussDB 方言的 nextval 预取主键路径
 * （对应 deploy/sql/schema-gaussdb.sql 的 SEQUENCE 改写）。
 *
 * <p>注意：GaussDB 特有的"空串退化为 NULL"语义由 DAO 层归一化抹平
 * （blankStringNormalizedToNull 用例验证）。
 */
class PostgreSQLDialectCompatibilityTest {

    private JdbcTemplate jdbcTemplate;
    private JdbcTaskRepository taskRepository;
    private JdbcJobLogRepository logRepository;
    private JdbcLockProvider lockProvider;
    private DataSource dataSource;

    /** 与 deploy/sql/schema-postgresql.sql 业务表部分同构（BIGSERIAL/BOOLEAN/TIMESTAMP/TEXT） */
    private static final String[] PG_DDL = {
            "CREATE TABLE IF NOT EXISTS t_job_config (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    "task_name VARCHAR(128) NOT NULL, " +
                    "task_group VARCHAR(64) DEFAULT 'ORBIT', " +
                    "description VARCHAR(512), " +
                    "cron_expression VARCHAR(64), " +
                    "dispatch_type VARCHAR(16) NOT NULL DEFAULT 'LOCAL', " +
                    "http_service_name VARCHAR(128), " +
                    "http_path VARCHAR(256), " +
                    "timeout_seconds INT DEFAULT 300, " +
                    "params TEXT, " +
                    "enabled BOOLEAN NOT NULL DEFAULT TRUE, " +
                    "version INT DEFAULT 1, " +
                    "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                    "CONSTRAINT uk_job_config_task_name UNIQUE (task_name))",
            "CREATE TABLE IF NOT EXISTS t_job_log (" +
                    "id BIGSERIAL PRIMARY KEY, " +
                    "request_id VARCHAR(64), " +
                    "task_name VARCHAR(128) NOT NULL, " +
                    "task_group VARCHAR(64), " +
                    "dispatch_type VARCHAR(16), " +
                    "dispatch_node VARCHAR(128), " +
                    "worker_node VARCHAR(128), " +
                    "status VARCHAR(16) NOT NULL, " +
                    "start_time TIMESTAMP, " +
                    "end_time TIMESTAMP, " +
                    "cost_ms BIGINT, " +
                    "message TEXT)",
            "CREATE INDEX IF NOT EXISTS idx_job_log_task_start ON t_job_log (task_name, start_time)",
            "CREATE INDEX IF NOT EXISTS idx_job_log_status ON t_job_log (status)",
            "CREATE TABLE IF NOT EXISTS t_cluster_lock (" +
                    "lock_name VARCHAR(128) NOT NULL, " +
                    "owner VARCHAR(128), " +
                    "expire_at BIGINT, " +
                    "update_time BIGINT, " +
                    "PRIMARY KEY (lock_name))"
    };

    @BeforeEach
    void setUp() {
        dataSource = new DriverManagerDataSource(
                "jdbc:h2:mem:pg_compat;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DB_CLOSE_DELAY=-1", "sa", "");
        jdbcTemplate = new JdbcTemplate(dataSource);
        for (String ddl : PG_DDL) {
            jdbcTemplate.execute(ddl);
        }
        jdbcTemplate.update("DELETE FROM t_job_log");
        jdbcTemplate.update("DELETE FROM t_job_config");
        jdbcTemplate.update("DELETE FROM t_cluster_lock");
        taskRepository = new JdbcTaskRepository(jdbcTemplate);
        logRepository = new JdbcJobLogRepository(jdbcTemplate);
        lockProvider = new JdbcLockProvider(jdbcTemplate);
    }

    @AfterEach
    void tearDown() {
        jdbcTemplate.update("DELETE FROM t_job_log");
        jdbcTemplate.update("DELETE FROM t_job_config");
        jdbcTemplate.update("DELETE FROM t_cluster_lock");
    }

    private JobConfig newConfig(String name, String cron) {
        JobConfig c = new JobConfig();
        c.setTaskName(name);
        c.setTaskGroup("ORBIT");
        c.setDescription("pg-compat test task");
        c.setCronExpression(cron);
        c.setDispatchType(DispatchType.LOCAL);
        c.setTimeoutSeconds(120);
        c.setParamsJson("{\"k\":\"v\"}");
        c.setEnabled(true);
        return c;
    }

    @Test
    void taskConfigCrudAndPaging() {
        // INSERT：BIGSERIAL + getGeneratedKeys 主键回填
        JobConfig saved = taskRepository.save(newConfig("pgTaskA", "0 */5 * * * ?"));
        assertNotNull(saved.getId(), "BIGSERIAL generated key should be back-filled");
        assertEquals(1, saved.getVersion());

        JobConfig savedB = taskRepository.save(newConfig("pgTaskB", "0 0 1 * * ?"));
        assertNotNull(savedB.getId());

        // findByName
        JobConfig loaded = taskRepository.findByName("pgTaskA").orElse(null);
        assertNotNull(loaded);
        assertEquals("0 */5 * * * ?", loaded.getCronExpression());
        assertEquals(DispatchType.LOCAL, loaded.getDispatchType());
        assertTrue(loaded.isEnabled());
        assertEquals("{\"k\":\"v\"}", loaded.getParamsJson());

        // UPDATE（版本递增）
        loaded.setCronExpression("0 */10 * * * ?");
        JobConfig updated = taskRepository.save(loaded);
        assertEquals(2, updated.getVersion());
        assertEquals("0 */10 * * * ?", taskRepository.findByName("pgTaskA").get().getCronExpression());

        // LIMIT ? OFFSET ? 分页
        PageResult<JobConfig> page = taskRepository.page(null, 1, 10);
        assertEquals(2, page.getTotal());
        assertEquals(2, page.getItems().size());
        PageResult<JobConfig> page2 = taskRepository.page("pgTask", 2, 1);
        assertEquals(2, page2.getTotal());
        assertEquals(1, page2.getItems().size());

        // DELETE
        assertTrue(taskRepository.delete("pgTaskB"));
        assertFalse(taskRepository.delete("pgTaskB"));
    }

    @Test
    void optimisticLockUpdateAffectsZeroRowOnStaleVersion() {
        taskRepository.save(newConfig("lockTask", "0 0 2 * * ?"));
        // 复刻 DAO 乐观锁语句：stale version 更新应影响 0 行
        int rows = jdbcTemplate.update(
                "UPDATE t_job_config SET cron_expression = ?, version = version + 1, updated_at = ? " +
                        "WHERE task_name = ? AND version = ?",
                "0 0 3 * * ?", new java.sql.Timestamp(System.currentTimeMillis()), "lockTask", 999);
        assertEquals(0, rows, "stale version update must affect zero rows");
        // 正确 version 更新应影响 1 行
        int rows2 = jdbcTemplate.update(
                "UPDATE t_job_config SET cron_expression = ?, version = version + 1, updated_at = ? " +
                        "WHERE task_name = ? AND version = ?",
                "0 0 4 * * ?", new java.sql.Timestamp(System.currentTimeMillis()), "lockTask", 1);
        assertEquals(1, rows2);
    }

    @Test
    void blankStringNormalizedToNull() {
        // GaussDB 下 '' 会退化为 NULL —— DAO 归一化后三库行为一致
        JobConfig c = newConfig("blankTask", null);
        c.setDescription("");
        c.setHttpServiceName("   ");
        c.setHttpPath("");
        c.setParamsJson("");
        taskRepository.save(c);

        JobConfig loaded = taskRepository.findByName("blankTask").get();
        assertNull(loaded.getDescription(), "blank description should be normalized to NULL");
        assertNull(loaded.getHttpServiceName(), "blank http_service_name should be normalized to NULL");
        assertNull(loaded.getHttpPath(), "blank http_path should be normalized to NULL");
        assertNull(loaded.getParamsJson(), "blank params should be normalized to NULL");

        // 空白 task_name 应被拒绝（唯一键 + Quartz JobKey 来源）
        JobConfig bad = newConfig("  ", "0 0 1 * * ?");
        try {
            taskRepository.save(bad);
            throw new AssertionError("blank task_name must be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    void jobLogLifecycle() {
        JobLog start = new JobLog();
        start.setRequestId("req-001");
        start.setTaskName("pgTaskA");
        start.setTaskGroup("ORBIT");
        start.setDispatchType("LOCAL");
        start.setDispatchNode("pod-a");
        start.setWorkerNode("pod-a");
        start.setStatus("RUNNING");
        start.setStartTime(new Date());
        Long id = logRepository.appendStart(start);
        assertNotNull(id, "log id should be generated");

        logRepository.appendFinish(id, "SUCCESS", "pod-a", 128L, "done in pg-compat mode");

        PageResult<JobLog> page = logRepository.page("pgTaskA", 1, 10);
        assertEquals(1, page.getTotal());
        JobLog loaded = page.getItems().get(0);
        assertEquals("SUCCESS", loaded.getStatus());
        assertEquals(Long.valueOf(128L), loaded.getCostMs());
        assertNotNull(loaded.getEndTime());
        assertEquals("done in pg-compat mode", loaded.getMessage());
    }

    @Test
    void lockMutualExclusionAndExpiryTakeover() throws InterruptedException {
        assertTrue(lockProvider.tryLock("pg:job1", "node-a", Duration.ofSeconds(60)));
        assertFalse(lockProvider.tryLock("pg:job1", "node-b", Duration.ofSeconds(60)));
        assertTrue(lockProvider.renew("pg:job1", "node-a", Duration.ofSeconds(60)));
        assertFalse(lockProvider.renew("pg:job1", "node-b", Duration.ofSeconds(60)));
        lockProvider.unlock("pg:job1", "node-a");
        assertTrue(lockProvider.tryLock("pg:job1", "node-b", Duration.ofSeconds(60)));

        // 过期抢占
        assertTrue(lockProvider.tryLock("pg:job2", "node-a", Duration.ofMillis(50)));
        Thread.sleep(80);
        assertTrue(lockProvider.tryLock("pg:job2", "node-b", Duration.ofSeconds(60)));
    }

    @Test
    void dialectResolverBehavesOnNonPgFamilyDatasource() throws Exception {
        // H2 数据源：自动探测 → OTHER（不干预 Quartz delegate，保持既有行为）
        assertEquals(SchedulerDialect.OTHER, DialectResolver.resolve(dataSource, null));
        assertEquals(SchedulerDialect.OTHER, DialectResolver.resolve(dataSource, "auto"));
        // 显式 hint 优先于自动探测
        assertEquals(SchedulerDialect.GAUSSDB, DialectResolver.resolve(dataSource, "gaussdb"));
        assertEquals(SchedulerDialect.POSTGRESQL, DialectResolver.resolve(dataSource, "postgresql"));
        // Quartz 集群表未初始化时探测为 false
        assertFalse(DialectResolver.quartzTablesPresent(dataSource.getConnection()));
    }

    /**
     * GaussDB 方言主键路径：SEQUENCE 预取 + 显式 id 插入
     * （对应 schema-gaussdb.sql 将 BIGSERIAL 改写为 SEQUENCE + DEFAULT nextval；
     * 规避 INSERT 结果回填 RETURNING/getGeneratedKeys 的行为差异）。
     */
    @Test
    void gaussdbSequencePrefetchInsertPath() {
        // 复刻 schema-gaussdb.sql：同构表，主键换为 SEQUENCE + DEFAULT nextval
        jdbcTemplate.execute("DROP TABLE IF EXISTS t_job_log");
        jdbcTemplate.execute("DROP TABLE IF EXISTS t_job_config");
        jdbcTemplate.execute("DROP SEQUENCE IF EXISTS seq_job_log_id");
        jdbcTemplate.execute("DROP SEQUENCE IF EXISTS seq_job_config_id");
        jdbcTemplate.execute("CREATE SEQUENCE seq_job_config_id START WITH 1");
        jdbcTemplate.execute("CREATE SEQUENCE seq_job_log_id START WITH 1");
        jdbcTemplate.execute("CREATE TABLE t_job_config (" +
                "id BIGINT NOT NULL DEFAULT nextval('seq_job_config_id') PRIMARY KEY, " +
                "task_name VARCHAR(128) NOT NULL, " +
                "task_group VARCHAR(128) DEFAULT 'ORBIT', " +
                "description VARCHAR(1024), " +
                "cron_expression VARCHAR(64), " +
                "dispatch_type VARCHAR(16) NOT NULL DEFAULT 'LOCAL', " +
                "http_service_name VARCHAR(128), " +
                "http_path VARCHAR(256), " +
                "timeout_seconds INT DEFAULT 300, " +
                "params TEXT, " +
                "enabled BOOLEAN NOT NULL DEFAULT TRUE, " +
                "version INT DEFAULT 1, " +
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, " +
                "CONSTRAINT uk_job_config_task_name UNIQUE (task_name))");
        jdbcTemplate.execute("CREATE TABLE t_job_log (" +
                "id BIGINT NOT NULL DEFAULT nextval('seq_job_log_id') PRIMARY KEY, " +
                "request_id VARCHAR(64), " +
                "task_name VARCHAR(128) NOT NULL, " +
                "task_group VARCHAR(128), " +
                "dispatch_type VARCHAR(16), " +
                "dispatch_node VARCHAR(128), " +
                "worker_node VARCHAR(128), " +
                "status VARCHAR(16) NOT NULL, " +
                "start_time TIMESTAMP, " +
                "end_time TIMESTAMP, " +
                "cost_ms BIGINT, " +
                "message TEXT)");

        JdbcTaskRepository gaussTaskRepository = new JdbcTaskRepository(jdbcTemplate, SchedulerDialect.GAUSSDB);
        JdbcJobLogRepository gaussLogRepository = new JdbcJobLogRepository(jdbcTemplate, SchedulerDialect.GAUSSDB);

        // 任务配置：nextval 预取 + 显式 id 插入（不依赖生成键回填）
        JobConfig saved = gaussTaskRepository.save(newConfig("gaussTaskA", "0 */5 * * * ?"));
        assertNotNull(saved.getId(), "id should be prefetched from sequence");
        assertEquals(1, saved.getVersion());
        assertEquals(Integer.valueOf(1), jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM t_job_config WHERE task_name = 'gaussTaskA' AND id = " + saved.getId(),
                Integer.class));

        // 连续插入：序列单调递增
        JobConfig savedB = gaussTaskRepository.save(newConfig("gaussTaskB", "0 0 1 * * ?"));
        assertNotNull(savedB.getId());
        assertTrue(savedB.getId() > saved.getId(), "sequence ids must be increasing");

        // 空串归一化在 GaussDB 方言下同样生效
        JobConfig blank = newConfig("gaussBlank", null);
        blank.setDescription("");
        gaussTaskRepository.save(blank);
        assertNull(gaussTaskRepository.findByName("gaussBlank").get().getDescription(),
                "blank description should be normalized to NULL under gaussdb dialect");

        // 执行日志：同一预取路径
        JobLog start = new JobLog();
        start.setRequestId("req-gauss-001");
        start.setTaskName("gaussTaskA");
        start.setTaskGroup("ORBIT");
        start.setDispatchType("LOCAL");
        start.setDispatchNode("pod-a");
        start.setWorkerNode("pod-a");
        start.setStatus("RUNNING");
        start.setStartTime(new Date());
        Long logId = gaussLogRepository.appendStart(start);
        assertNotNull(logId, "log id should be prefetched from sequence");
        gaussLogRepository.appendFinish(logId, "SUCCESS", "pod-a", 66L, "via sequence prefetch");

        PageResult<JobLog> page = gaussLogRepository.page("gaussTaskA", 1, 10);
        assertEquals(1, page.getTotal());
        JobLog loaded = page.getItems().get(0);
        assertEquals(Long.valueOf(logId), loaded.getId());
        assertEquals("SUCCESS", loaded.getStatus());
        assertEquals("via sequence prefetch", loaded.getMessage());
    }
}

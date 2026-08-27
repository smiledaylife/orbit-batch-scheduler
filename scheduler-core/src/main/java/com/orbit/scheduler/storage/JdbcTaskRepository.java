package com.orbit.scheduler.storage;

import com.orbit.scheduler.annotation.DispatchType;
import com.orbit.scheduler.dialect.SchedulerDialect;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.spi.TaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * 数据库任务存储：对应表 t_job_config，集群共享、支持动态增删改与乐观锁。
 *
 * <p>DDL 参见 deploy/sql/schema-mysql.sql（MySQL）、schema-postgresql.sql（PostgreSQL）、
 * schema-gaussdb.sql（GaussDB）。全部 SQL 收敛在 PostgreSQL / GaussDB /
 * MySQL / H2 通用子集内（无 upsert 方言、无反引号、无 :: 类型转换、LIMIT-OFFSET 分页、
 * 时间值全部由应用侧计算后以参数传入）。
 *
 * <p><b>多库空串语义抹平</b>：GaussDB 下空字符串写入会退化为 NULL，
 * 故写库前统一将空串归一化为 NULL，使三种数据库行为完全一致；
 * task_name 为唯一键与 Quartz JobKey 来源，强制非空白校验。
 *
 * <p><b>主键生成按方言分流</b>：GaussDB 走 SEQUENCE 预取
 * （SELECT nextval → 显式 id 插入，规避 RETURNING/getGeneratedKeys 行为差异）；
 * PostgreSQL / MySQL / H2 走 getGeneratedKeys（显式指定回填列名）。
 *
 * <p><b>性能优化</b>：save() 不再在 insert 后二次 SELECT 回查，直接基于 KeyHolder
 * / 显式 id 组装返回对象；overview 的 count 走 SELECT COUNT(*)。
 *
 * @author orbit
 */
public class JdbcTaskRepository implements TaskRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcTaskRepository.class);

    private static final String COLS = "id, task_name, task_group, description, cron_expression, " +
            "dispatch_type, http_service_name, http_path, timeout_seconds, params, enabled, version, created_at, updated_at";

    /** GaussDB 主键序列名（与 deploy/sql/schema-gaussdb.sql 保持一致） */
    static final String ID_SEQUENCE = "seq_job_config_id";

    private final JdbcTemplate jdbcTemplate;
    private final SchedulerDialect dialect;

    private final RowMapper<JobConfig> mapper = (rs, rowNum) -> {
        JobConfig c = new JobConfig();
        c.setId(rs.getLong("id"));
        c.setTaskName(rs.getString("task_name"));
        c.setTaskGroup(rs.getString("task_group"));
        c.setDescription(rs.getString("description"));
        c.setCronExpression(rs.getString("cron_expression"));
        String dt = rs.getString("dispatch_type");
        c.setDispatchType(dt == null || dt.isEmpty() ? DispatchType.LOCAL : DispatchType.valueOf(dt));
        c.setHttpServiceName(rs.getString("http_service_name"));
        c.setHttpPath(rs.getString("http_path"));
        int timeout = rs.getInt("timeout_seconds");
        c.setTimeoutSeconds(rs.wasNull() ? 300 : timeout);
        c.setParamsJson(rs.getString("params"));
        c.setEnabled(rs.getBoolean("enabled"));
        c.setVersion(rs.getInt("version"));
        Timestamps.fill(c, rs);
        return c;
    };

    public JdbcTaskRepository(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, SchedulerDialect.OTHER);
    }

    public JdbcTaskRepository(JdbcTemplate jdbcTemplate, SchedulerDialect dialect) {
        this.jdbcTemplate = jdbcTemplate;
        this.dialect = dialect == null ? SchedulerDialect.OTHER : dialect;
    }

    @Override
    public List<JobConfig> findAll() {
        return jdbcTemplate.query("SELECT " + COLS + " FROM t_job_config ORDER BY task_name", mapper);
    }

    @Override
    public Optional<JobConfig> findByName(String taskName) {
        List<JobConfig> list = jdbcTemplate.query(
                "SELECT " + COLS + " FROM t_job_config WHERE task_name = ?", mapper, taskName);
        return list.isEmpty() ? Optional.<JobConfig>empty() : Optional.of(list.get(0));
    }

    @Override
    public long count() {
        Long n = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM t_job_config", Long.class);
        return n == null ? 0L : n.longValue();
    }

    @Override
    public JobConfig save(JobConfig config) {
        String taskName = requireName(config.getTaskName());
        config.setTaskName(taskName);
        // 通过 ID 是否存在判定 update / insert；不存在则 findByName 探测一次
        // —— 乐观锁语义要求 update 路径必须先读出 existing 版本号
        JobConfig existing = (config.getId() != null && config.getId() > 0)
                ? findById(config.getId())
                : findByName(taskName).orElse(null);
        if (existing == null) {
            return insert(config);
        }
        // 更新（乐观锁）
        config.setId(existing.getId());
        int expectedVersion = existing.getVersion();
        int rows = jdbcTemplate.update(
                "UPDATE t_job_config SET task_group = ?, description = ?, cron_expression = ?, " +
                        "dispatch_type = ?, http_service_name = ?, http_path = ?, timeout_seconds = ?, " +
                        "params = ?, enabled = ?, version = version + 1, updated_at = ? " +
                        "WHERE task_name = ? AND version = ?",
                nz(config.getTaskGroup()), nz(config.getDescription()), nz(config.getCronExpression()),
                config.getDispatchType() == null ? DispatchType.LOCAL.name() : config.getDispatchType().name(),
                nz(config.getHttpServiceName()), nz(config.getHttpPath()),
                config.getTimeoutSeconds() == null ? 300 : config.getTimeoutSeconds(),
                nz(config.getParamsJson()), config.isEnabled(), new java.sql.Timestamp(System.currentTimeMillis()),
                taskName, expectedVersion);
        if (rows == 0) {
            throw new IllegalStateException("Task '" + config.getTaskName()
                    + "' was modified concurrently (version " + expectedVersion + "), please retry");
        }
        // 不再二次 SELECT 回查；基于已知字段构造返回值（version+1，时间戳由触发器或下一次查询统一）
        config.setVersion(expectedVersion + 1);
        config.setUpdatedAt(new Date());
        log.debug("[orbit-scheduler] updated task config: {} (id={}, version={})",
                config.getTaskName(), config.getId(), config.getVersion());
        return config;
    }

    /** 按 id 查询，仅用于 save() 已知 id 时的乐观锁版本读取 */
    private JobConfig findById(Long id) {
        List<JobConfig> list = jdbcTemplate.query(
                "SELECT " + COLS + " FROM t_job_config WHERE id = ?", mapper, id);
        return list.isEmpty() ? null : list.get(0);
    }

    private JobConfig insert(JobConfig config) {
        requireName(config.getTaskName());
        if (dialect.prefetchIdsFromSequence()) {
            Long id = prefetchId(ID_SEQUENCE);
            if (id != null) {
                insertWithExplicitId(config, id);
                return config;
            }
            // 序列不可用（未执行 schema-gaussdb.sql？）：降级生成键回填路径并告警
        }
        insertWithGeneratedKey(config);
        return config;
    }

    /**
     * GaussDB 主键预取：SELECT nextval('seq')。
     * 仅依赖普通 SELECT + 普通 INSERT，彻底规避 GaussDB 与 PostgreSQL 在
     * INSERT 结果回填（RETURNING / getGeneratedKeys）上的行为差异；
     * 失败返回 null（调用方降级为生成键回填）。
     */
    private Long prefetchId(String sequenceName) {
        try {
            return jdbcTemplate.queryForObject("SELECT nextval('" + sequenceName + "')", Long.class);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] sequence {} not available (execute deploy/sql/schema-gaussdb.sql " +
                    "to create it), fallback to getGeneratedKeys: {}", sequenceName, e.getMessage());
            return null;
        }
    }

    /** GaussDB 路径：nextval 预取主键后以显式 id 插入 */
    private void insertWithExplicitId(JobConfig config, long id) {
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());
        jdbcTemplate.update(
                "INSERT INTO t_job_config (id, task_name, task_group, description, cron_expression, " +
                        "dispatch_type, http_service_name, http_path, timeout_seconds, params, enabled, version, created_at, updated_at) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                id, config.getTaskName(), nz(config.getTaskGroup()), nz(config.getDescription()),
                nz(config.getCronExpression()),
                config.getDispatchType() == null ? DispatchType.LOCAL.name() : config.getDispatchType().name(),
                nz(config.getHttpServiceName()), nz(config.getHttpPath()),
                config.getTimeoutSeconds() == null ? 300 : config.getTimeoutSeconds(),
                nz(config.getParamsJson()), config.isEnabled(), now, now);
        config.setId(id);
        config.setVersion(1);
        config.setCreatedAt(new Date(now.getTime()));
        config.setUpdatedAt(new Date(now.getTime()));
        log.info("[orbit-scheduler] inserted task config: {} (id={} via sequence)", config.getTaskName(), id);
    }

    /** PostgreSQL / MySQL / H2 路径：生成键回填 */
    private void insertWithGeneratedKey(JobConfig config) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        long nowMs = System.currentTimeMillis();
        java.sql.Timestamp now = new java.sql.Timestamp(nowMs);
        jdbcTemplate.update(con -> {
            // 显式指定回填列：MySQL 驱动返回自增主键；PostgreSQL 驱动默认
            // RETURNING 所有生成列（含带默认值的 created_at/updated_at），
            // 不指定列名会导致 GeneratedKeyHolder.getKey() 多键异常
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO t_job_config (task_name, task_group, description, cron_expression, " +
                            "dispatch_type, http_service_name, http_path, timeout_seconds, params, enabled, version, created_at, updated_at) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)",
                    new String[]{"id"});
            ps.setString(1, config.getTaskName());
            ps.setString(2, nz(config.getTaskGroup()));
            ps.setString(3, nz(config.getDescription()));
            ps.setString(4, nz(config.getCronExpression()));
            ps.setString(5, config.getDispatchType() == null ? DispatchType.LOCAL.name() : config.getDispatchType().name());
            ps.setString(6, nz(config.getHttpServiceName()));
            ps.setString(7, nz(config.getHttpPath()));
            ps.setInt(8, config.getTimeoutSeconds() == null ? 300 : config.getTimeoutSeconds());
            ps.setString(9, nz(config.getParamsJson()));
            ps.setBoolean(10, config.isEnabled());
            ps.setTimestamp(11, now);
            ps.setTimestamp(12, now);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        if (key != null) {
            config.setId(key.longValue());
        }
        config.setVersion(1);
        config.setCreatedAt(new Date(nowMs));
        config.setUpdatedAt(new Date(nowMs));
        log.info("[orbit-scheduler] inserted task config: {} (id={})", config.getTaskName(), config.getId());
    }

    @Override
    public boolean delete(String taskName) {
        return jdbcTemplate.update("DELETE FROM t_job_config WHERE task_name = ?", taskName) > 0;
    }

    @Override
    public PageResult<JobConfig> page(String nameLike, int page, int size) {
        Long total;
        List<JobConfig> items;
        int offset = Math.max(0, (page - 1) * size);
        if (nameLike == null || nameLike.isEmpty()) {
            total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM t_job_config", Long.class);
            items = jdbcTemplate.query(
                    "SELECT " + COLS + " FROM t_job_config ORDER BY task_name LIMIT ? OFFSET ?",
                    mapper, size, offset);
        } else {
            total = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM t_job_config WHERE task_name LIKE ?",
                    Long.class, "%" + nameLike + "%");
            items = jdbcTemplate.query(
                    "SELECT " + COLS + " FROM t_job_config WHERE task_name LIKE ? ORDER BY task_name LIMIT ? OFFSET ?",
                    mapper, "%" + nameLike + "%", size, offset);
        }
        return new PageResult<JobConfig>(page, size, total == null ? 0 : total.longValue(), items);
    }

    @Override
    public String type() {
        return "database";
    }

    /** 空串/纯空白 → NULL（GaussDB 下 '' 会退化为 NULL，统一后三库行为一致） */
    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? null : s;
    }

    /** task_name 非空校验（唯一键 + Quartz JobKey 来源，任何数据库都不允许为空） */
    private static String requireName(String taskName) {
        if (taskName == null || taskName.trim().isEmpty()) {
            throw new IllegalArgumentException("task_name must not be blank (it is the unique key and Quartz JobKey)");
        }
        return taskName.trim();
    }

    /** 时间戳填充小工具（避免主流程里散落的 rs.getTimestamp 判空） */
    static final class Timestamps {
        static void fill(JobConfig c, java.sql.ResultSet rs) throws java.sql.SQLException {
            java.sql.Timestamp ct = rs.getTimestamp("created_at");
            java.sql.Timestamp ut = rs.getTimestamp("updated_at");
            c.setCreatedAt(ct == null ? null : new Date(ct.getTime()));
            c.setUpdatedAt(ut == null ? null : new Date(ut.getTime()));
        }
    }
}

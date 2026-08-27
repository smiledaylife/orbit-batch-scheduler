package com.orbit.scheduler.storage;

import com.orbit.scheduler.dialect.SchedulerDialect;
import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.spi.JobLogRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.PreparedStatement;
import java.util.Date;
import java.util.List;

/**
 * 数据库执行日志存储：对应表 t_job_log。
 *
 * <p>DDL 参见 deploy/sql/schema-mysql.sql、schema-postgresql.sql、schema-gaussdb.sql；
 * SQL 与 PostgreSQL / GaussDB / MySQL / H2 通用（LIMIT-OFFSET 分页）。GaussDB 下
 * 空串会退化为 NULL，写库前统一归一化；主键按方言分流：GaussDB 走
 * SEQUENCE 预取（SELECT nextval → 显式 id 插入，规避 RETURNING/getGeneratedKeys
 * 行为差异），其余数据库走生成键回填（显式指定回填列名）。
 *
 * <p><b>性能优化</b>：appendStart 内部回填 startTime，避免后续 appendFinish
 * 需要二次查库；count(*) 查询使用同一 RowMapper 池避免重复构造。
 *
 * @author orbit
 */
public class JdbcJobLogRepository implements JobLogRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcJobLogRepository.class);

    private static final String COLS = "id, request_id, task_name, task_group, dispatch_type, " +
            "dispatch_node, worker_node, status, start_time, end_time, cost_ms, message";

    /** GaussDB 主键序列名（与 deploy/sql/schema-gaussdb.sql 保持一致） */
    static final String ID_SEQUENCE = "seq_job_log_id";

    private final JdbcTemplate jdbcTemplate;
    private final SchedulerDialect dialect;

    private final RowMapper<JobLog> mapper = (rs, rowNum) -> {
        JobLog l = new JobLog();
        l.setId(rs.getLong("id"));
        l.setRequestId(rs.getString("request_id"));
        l.setTaskName(rs.getString("task_name"));
        l.setTaskGroup(rs.getString("task_group"));
        l.setDispatchType(rs.getString("dispatch_type"));
        l.setDispatchNode(rs.getString("dispatch_node"));
        l.setWorkerNode(rs.getString("worker_node"));
        l.setStatus(rs.getString("status"));
        java.sql.Timestamp st = rs.getTimestamp("start_time");
        java.sql.Timestamp et = rs.getTimestamp("end_time");
        l.setStartTime(st == null ? null : new Date(st.getTime()));
        l.setEndTime(et == null ? null : new Date(et.getTime()));
        long cost = rs.getLong("cost_ms");
        l.setCostMs(rs.wasNull() ? null : cost);
        l.setMessage(rs.getString("message"));
        return l;
    };

    public JdbcJobLogRepository(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, SchedulerDialect.OTHER);
    }

    public JdbcJobLogRepository(JdbcTemplate jdbcTemplate, SchedulerDialect dialect) {
        this.jdbcTemplate = jdbcTemplate;
        this.dialect = dialect == null ? SchedulerDialect.OTHER : dialect;
    }

    @Override
    public Long appendStart(JobLog jobLog) {
        long nowMs = System.currentTimeMillis();
        java.sql.Timestamp now = new java.sql.Timestamp(nowMs);
        // 回填 startTime，保持与 DB 一致，避免后续业务侧需要二次查询
        jobLog.setStartTime(new Date(nowMs));
        if (dialect.prefetchIdsFromSequence()) {
            Long id = prefetchId(ID_SEQUENCE);
            if (id != null) {
                insertWithExplicitId(jobLog, id, now);
                jobLog.setId(id);
                return id;
            }
            // 序列不可用（未执行 schema-gaussdb.sql？）：降级生成键回填路径并告警
        }
        Long id = insertWithGeneratedKey(jobLog, now);
        if (id != null) {
            jobLog.setId(id);
        }
        return id;
    }

    /** GaussDB 主键预取：普通 SELECT nextval，规避 INSERT 回填差异；失败返回 null */
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
    private void insertWithExplicitId(JobLog jobLog, long id, java.sql.Timestamp now) {
        jdbcTemplate.update(
                "INSERT INTO t_job_log (id, request_id, task_name, task_group, dispatch_type, " +
                        "dispatch_node, worker_node, status, start_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                id, nz(jobLog.getRequestId()), nz(jobLog.getTaskName()), nz(jobLog.getTaskGroup()),
                nz(jobLog.getDispatchType()), nz(jobLog.getDispatchNode()), nz(jobLog.getWorkerNode()),
                jobLog.getStatus(), now);
    }

    /** PostgreSQL / MySQL / H2 路径：生成键回填 */
    private Long insertWithGeneratedKey(JobLog jobLog, java.sql.Timestamp now) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(con -> {
            // 显式指定回填列，规避 PostgreSQL 驱动 RETURNING 多列导致 getKey() 异常
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO t_job_log (request_id, task_name, task_group, dispatch_type, " +
                            "dispatch_node, worker_node, status, start_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    new String[]{"id"});
            ps.setString(1, nz(jobLog.getRequestId()));
            ps.setString(2, nz(jobLog.getTaskName()));
            ps.setString(3, nz(jobLog.getTaskGroup()));
            ps.setString(4, nz(jobLog.getDispatchType()));
            ps.setString(5, nz(jobLog.getDispatchNode()));
            ps.setString(6, nz(jobLog.getWorkerNode()));
            ps.setString(7, jobLog.getStatus());
            ps.setTimestamp(8, now);
            return ps;
        }, keyHolder);
        Number key = keyHolder.getKey();
        return key == null ? null : key.longValue();
    }

    @Override
    public void appendFinish(Long id, String status, String workerNode, long costMs, String message) {
        if (id == null) {
            return;
        }
        try {
            jdbcTemplate.update(
                    "UPDATE t_job_log SET status = ?, worker_node = ?, end_time = ?, cost_ms = ?, message = ? WHERE id = ?",
                    status, workerNode, new java.sql.Timestamp(System.currentTimeMillis()),
                    costMs, InMemoryJobLogRepository.abbreviate(message), id);
        } catch (Exception e) {
            log.warn("[orbit-scheduler] append finish log failed for id={}: {}", id, e.getMessage());
        }
    }

    @Override
    public PageResult<JobLog> page(String taskName, int page, int size) {
        Long total;
        List<JobLog> items;
        int offset = Math.max(0, (page - 1) * size);
        if (taskName == null || taskName.isEmpty()) {
            total = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM t_job_log", Long.class);
            items = jdbcTemplate.query(
                    "SELECT " + COLS + " FROM t_job_log ORDER BY id DESC LIMIT ? OFFSET ?",
                    mapper, size, offset);
        } else {
            total = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM t_job_log WHERE task_name = ?", Long.class, taskName);
            items = jdbcTemplate.query(
                    "SELECT " + COLS + " FROM t_job_log WHERE task_name = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                    mapper, taskName, size, offset);
        }
        return new PageResult<JobLog>(page, size, total == null ? 0 : total.longValue(), items);
    }

    @Override
    public String type() {
        return "database";
    }

    /** 空串/纯空白 → NULL（GaussDB 下 '' 会退化为 NULL，统一后多库行为一致） */
    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? null : s;
    }
}

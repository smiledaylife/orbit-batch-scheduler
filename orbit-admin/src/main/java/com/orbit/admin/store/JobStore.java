package com.orbit.admin.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.PageResult;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Repository;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 任务与日志持久化存储层（JDBC 实现）。
 * <p>兼容多种关系型数据库（PostgreSQL / openGauss / GaussDB / H2），采用 ANSI SQL 标准语法：
 * <ul>
 *   <li>任务定义表 {@code orbit_job}：支持并发乐观锁更新版本号控制（version）；</li>
 *   <li>调度日志表 {@code orbit_job_log}：支持全链路追踪日志记录与状态更新；</li>
 *   <li>提供高效分页查询、模糊检索与防超长字符串保护。</li>
 * </ul>
 */
@Repository
public class JobStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper = new ObjectMapper();

    /**
     * orbit_job 表记录与 JobInfo 实体对象的 RowMapper 映射器
     */
    private final RowMapper<JobInfo> jobMapper = (rs, i) -> {
        JobInfo j = new JobInfo();
        j.setId(rs.getLong("id"));
        j.setJobName(rs.getString("job_name"));
        j.setDescription(rs.getString("description"));
        j.setAppName(rs.getString("app_name"));
        j.setHandler(rs.getString("handler"));
        j.setCron(rs.getString("cron_expr"));
        j.setParams(parseMap(rs.getString("params")));
        int t = rs.getInt("timeout_seconds");
        j.setTimeoutSeconds(rs.wasNull() ? 300 : t);
        j.setRouteStrategy(rs.getString("route_strategy"));
        j.setEnabled(rs.getBoolean("enabled"));
        j.setVersion(rs.getInt("version"));
        Timestamp c = rs.getTimestamp("created_at");
        Timestamp u = rs.getTimestamp("updated_at");
        j.setCreatedAt(c == null ? null : new Date(c.getTime()));
        j.setUpdatedAt(u == null ? null : new Date(u.getTime()));
        return j;
    };

    /**
     * orbit_job_log 表记录与 JobLog 实体对象的 RowMapper 映射器
     */
    private final RowMapper<JobLog> logMapper = (rs, i) -> {
        JobLog l = new JobLog();
        l.setId(rs.getLong("id"));
        l.setLogId(rs.getString("log_id"));
        l.setJobId(rs.getLong("job_id"));
        l.setJobName(rs.getString("job_name"));
        l.setAppName(rs.getString("app_name"));
        l.setHandler(rs.getString("handler"));
        l.setExecutorAddress(rs.getString("executor_address"));
        l.setStatus(rs.getString("status"));
        l.setMessage(rs.getString("message"));
        l.setCostMs(rs.getLong("cost_ms"));
        Timestamp s = rs.getTimestamp("start_time");
        Timestamp e = rs.getTimestamp("end_time");
        l.setStartTime(s == null ? null : new Date(s.getTime()));
        l.setEndTime(e == null ? null : new Date(e.getTime()));
        return l;
    };

    public JobStore(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    /**
     * 查询所有任务列表（按任务名升序排列）
     *
     * @return 任务列表
     */
    public List<JobInfo> findAllJobs() {
        return jdbc.query("SELECT * FROM orbit_job ORDER BY job_name", jobMapper);
    }

    /**
     * 根据任务名称精确查询任务
     *
     * @param name 任务名称
     * @return 任务 Optional 包装
     */
    public Optional<JobInfo> findJobByName(String name) {
        List<JobInfo> list = jdbc.query("SELECT * FROM orbit_job WHERE job_name = ?", jobMapper, name);
        return list.isEmpty() ? Optional.<JobInfo>empty() : Optional.of(list.get(0));
    }

    /**
     * 根据任务主键 ID 查询任务
     *
     * @param id 任务主键 ID
     * @return 任务 Optional 包装
     */
    public Optional<JobInfo> findJobById(long id) {
        List<JobInfo> list = jdbc.query("SELECT * FROM orbit_job WHERE id = ?", jobMapper, id);
        return list.isEmpty() ? Optional.<JobInfo>empty() : Optional.of(list.get(0));
    }

    /**
     * 分页查询任务列表，支持按任务名进行模糊查询。
     *
     * @param nameLike 任务名模糊匹配关键字（可为空）
     * @param page     页码（从 1 开始）
     * @param size     每页大小（最小 1，最大 200）
     * @return 分页结果对象
     */
    public PageResult<JobInfo> pageJobs(String nameLike, int page, int size) {
        int p = Math.max(1, page);
        int s = Math.min(200, Math.max(1, size));
        int offset = (p - 1) * s;
        Long total;
        List<JobInfo> items;

        if (nameLike == null || nameLike.trim().isEmpty()) {
            total = jdbc.queryForObject("SELECT COUNT(*) FROM orbit_job", Long.class);
            items = jdbc.query("SELECT * FROM orbit_job ORDER BY job_name LIMIT ? OFFSET ?", jobMapper, s, offset);
        } else {
            String like = "%" + nameLike.trim() + "%";
            total = jdbc.queryForObject("SELECT COUNT(*) FROM orbit_job WHERE job_name LIKE ?", Long.class, like);
            items = jdbc.query("SELECT * FROM orbit_job WHERE job_name LIKE ? ORDER BY job_name LIMIT ? OFFSET ?",
                    jobMapper, like, s, offset);
        }
        return new PageResult<JobInfo>(p, s, total == null ? 0 : total, items);
    }

    /**
     * 保存或更新任务。
     * <p>若任务 ID 为 null 则执行 INSERT，并回填自增 ID 和版本号 1；
     * 若任务 ID 已存在则执行带版本号校验的 UPDATE（乐观锁），若影响行数为 0 则抛出并发冲突异常。
     *
     * @param job 待保存的任务对象
     * @return 保存成功后的任务对象
     */
    public JobInfo saveJob(JobInfo job) {
        Date now = new Date();

        // 入库前对可能超出列宽的字段做前置校验，避免触发 SQLException（列宽与 schema.sql 保持一致）
        String paramsJson = toJson(job.getParams());
        if (paramsJson != null && paramsJson.length() > 2000) {
            throw new IllegalArgumentException("params too long: serialized json length "
                    + paramsJson.length() + " exceeds limit 2000");
        }
        if (job.getDescription() != null && job.getDescription().length() > 256) {
            throw new IllegalArgumentException("description too long: length "
                    + job.getDescription().length() + " exceeds limit 256");
        }

        // 1. 新增操作
        if (job.getId() == null) {
            KeyHolder kh = new GeneratedKeyHolder();
            jdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement(
                        "INSERT INTO orbit_job (job_name, description, app_name, handler, cron_expr, params, " +
                                "timeout_seconds, route_strategy, enabled, version, created_at, updated_at) " +
                                "VALUES (?,?,?,?,?,?,?,?,?,1,?,?)",
                        new String[]{"id"});
                ps.setString(1, job.getJobName());
                ps.setString(2, blankToNull(job.getDescription()));
                ps.setString(3, job.getAppName());
                ps.setString(4, job.getHandler());
                ps.setString(5, blankToNull(job.getCron()));
                ps.setString(6, paramsJson);
                ps.setInt(7, job.getTimeoutSeconds() <= 0 ? 300 : job.getTimeoutSeconds());
                ps.setString(8, blankToNull(job.getRouteStrategy()) == null ? "ROUND" : job.getRouteStrategy());
                ps.setBoolean(9, job.isEnabled());
                ps.setTimestamp(10, new Timestamp(now.getTime()));
                ps.setTimestamp(11, new Timestamp(now.getTime()));
                return ps;
            }, kh);
            Number key = kh.getKey();
            if (key != null) {
                job.setId(key.longValue());
            }
            job.setVersion(1);
            job.setCreatedAt(now);
            job.setUpdatedAt(now);
            return job;
        }

        // 2. 更新操作（带乐观锁 version 比对）
        int rows = jdbc.update(
                "UPDATE orbit_job SET description=?, app_name=?, handler=?, cron_expr=?, params=?, " +
                        "timeout_seconds=?, route_strategy=?, enabled=?, version=version+1, updated_at=? " +
                        "WHERE id=? AND version=?",
                blankToNull(job.getDescription()), job.getAppName(), job.getHandler(),
                blankToNull(job.getCron()), paramsJson,
                job.getTimeoutSeconds() <= 0 ? 300 : job.getTimeoutSeconds(),
                blankToNull(job.getRouteStrategy()) == null ? "ROUND" : job.getRouteStrategy(),
                job.isEnabled(), new Timestamp(now.getTime()),
                job.getId(), job.getVersion());

        // 若更新受影响行数为 0，说明版本号已被其他请求抢先递增
        if (rows == 0) {
            throw new IllegalStateException("job concurrent update, please retry");
        }
        job.setVersion(job.getVersion() + 1);
        job.setUpdatedAt(now);
        return job;
    }

    /**
     * 根据任务名称物理删除任务记录
     *
     * @param name 任务名称
     * @return 是否成功删除
     */
    public boolean deleteJob(String name) {
        return jdbc.update("DELETE FROM orbit_job WHERE job_name = ?", name) > 0;
    }

    /**
     * 统计系统总任务数量
     *
     * @return 任务总数
     */
    public long countJobs() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM orbit_job", Long.class);
        return n == null ? 0 : n;
    }

    /**
     * 插入一条新的调度任务执行日志（初始为 RUNNING 状态）。
     *
     * @param log 日志实体
     */
    public void insertLog(JobLog log) {
        KeyHolder kh = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO orbit_job_log (log_id, job_id, job_name, app_name, handler, executor_address, " +
                            "status, message, cost_ms, start_time, end_time) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    new String[]{"id"});
            ps.setString(1, log.getLogId());
            ps.setObject(2, log.getJobId());
            ps.setString(3, log.getJobName());
            ps.setString(4, log.getAppName());
            ps.setString(5, log.getHandler());
            ps.setString(6, log.getExecutorAddress());
            ps.setString(7, log.getStatus());
            ps.setString(8, abbreviate(log.getMessage()));
            ps.setLong(9, log.getCostMs());
            ps.setTimestamp(10, log.getStartTime() == null ? null : new Timestamp(log.getStartTime().getTime()));
            ps.setTimestamp(11, log.getEndTime() == null ? null : new Timestamp(log.getEndTime().getTime()));
            return ps;
        }, kh);
        Number key = kh.getKey();
        if (key != null) {
            log.setId(key.longValue());
        }
    }

    /**
     * 更新指定日志记录的最终执行状态与结果。
     *
     * @param logId   日志追踪 ID
     * @param status  最终状态（SUCCESS / FAILED）
     * @param address 执行器节点地址
     * @param costMs  总耗时（毫秒）
     * @param message 响应或异常信息摘要
     */
    public void finishLog(String logId, String status, String address, long costMs, String message) {
        jdbc.update(
                "UPDATE orbit_job_log SET status=?, executor_address=?, cost_ms=?, message=?, end_time=? WHERE log_id=?",
                status, address, costMs, abbreviate(message), new Timestamp(System.currentTimeMillis()), logId);
    }

    /**
     * 分页查询调度日志列表。
     *
     * @param jobName 任务名称筛选（可为空）
     * @param page    页码
     * @param size    每页记录数
     * @return 分页结果集
     */
    public PageResult<JobLog> pageLogs(String jobName, int page, int size) {
        int p = Math.max(1, page);
        int s = Math.min(200, Math.max(1, size));
        int offset = (p - 1) * s;
        Long total;
        List<JobLog> items;

        if (jobName == null || jobName.trim().isEmpty()) {
            total = jdbc.queryForObject("SELECT COUNT(*) FROM orbit_job_log", Long.class);
            items = jdbc.query("SELECT * FROM orbit_job_log ORDER BY id DESC LIMIT ? OFFSET ?", logMapper, s, offset);
        } else {
            total = jdbc.queryForObject("SELECT COUNT(*) FROM orbit_job_log WHERE job_name = ?", Long.class, jobName);
            items = jdbc.query("SELECT * FROM orbit_job_log WHERE job_name = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                    logMapper, jobName, s, offset);
        }
        return new PageResult<JobLog>(p, s, total == null ? 0 : total, items);
    }

    /**
     * 将 JSON 字符串解析为 Map
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMap(String json) {
        if (json == null || json.trim().isEmpty()) {
            return new LinkedHashMap<String, Object>();
        }
        try {
            Map m = mapper.readValue(json, Map.class);
            return m == null ? new LinkedHashMap<String, Object>() : m;
        } catch (Exception e) {
            return new LinkedHashMap<String, Object>();
        }
    }

    /**
     * 将 Map 序列化为 JSON 字符串
     */
    private String toJson(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        try {
            return mapper.writeValueAsString(map);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 空白字符串转 null
     */
    private static String blankToNull(String s) {
        return s == null || s.trim().isEmpty() ? null : s.trim();
    }

    /**
     * 截断超长字符串（防止数据库字段超长溢出，最大保留 2000 字符）
     */
    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 2000 ? s : s.substring(0, 2000) + "...";
    }
}

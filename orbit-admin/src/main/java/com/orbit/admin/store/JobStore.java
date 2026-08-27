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
 * 任务与日志 JDBC 存储（H2 / MySQL / PostgreSQL 通用 SQL）。
 */
@Repository
public class JobStore {

    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper = new ObjectMapper();

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

    public List<JobInfo> findAllJobs() {
        return jdbc.query("SELECT * FROM orbit_job ORDER BY job_name", jobMapper);
    }

    public Optional<JobInfo> findJobByName(String name) {
        List<JobInfo> list = jdbc.query("SELECT * FROM orbit_job WHERE job_name = ?", jobMapper, name);
        return list.isEmpty() ? Optional.<JobInfo>empty() : Optional.of(list.get(0));
    }

    public Optional<JobInfo> findJobById(long id) {
        List<JobInfo> list = jdbc.query("SELECT * FROM orbit_job WHERE id = ?", jobMapper, id);
        return list.isEmpty() ? Optional.<JobInfo>empty() : Optional.of(list.get(0));
    }

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

    public JobInfo saveJob(JobInfo job) {
        Date now = new Date();
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
                ps.setString(6, toJson(job.getParams()));
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
        int rows = jdbc.update(
                "UPDATE orbit_job SET description=?, app_name=?, handler=?, cron_expr=?, params=?, " +
                        "timeout_seconds=?, route_strategy=?, enabled=?, version=version+1, updated_at=? " +
                        "WHERE id=? AND version=?",
                blankToNull(job.getDescription()), job.getAppName(), job.getHandler(),
                blankToNull(job.getCron()), toJson(job.getParams()),
                job.getTimeoutSeconds() <= 0 ? 300 : job.getTimeoutSeconds(),
                blankToNull(job.getRouteStrategy()) == null ? "ROUND" : job.getRouteStrategy(),
                job.isEnabled(), new Timestamp(now.getTime()),
                job.getId(), job.getVersion());
        if (rows == 0) {
            throw new IllegalStateException("job concurrent update, please retry");
        }
        job.setVersion(job.getVersion() + 1);
        job.setUpdatedAt(now);
        return job;
    }

    public boolean deleteJob(String name) {
        return jdbc.update("DELETE FROM orbit_job WHERE job_name = ?", name) > 0;
    }

    public long countJobs() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM orbit_job", Long.class);
        return n == null ? 0 : n;
    }

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

    public void finishLog(String logId, String status, String address, long costMs, String message) {
        jdbc.update(
                "UPDATE orbit_job_log SET status=?, executor_address=?, cost_ms=?, message=?, end_time=? WHERE log_id=?",
                status, address, costMs, abbreviate(message), new Timestamp(System.currentTimeMillis()), logId);
    }

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

    private static String blankToNull(String s) {
        return s == null || s.trim().isEmpty() ? null : s.trim();
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 2000 ? s : s.substring(0, 2000) + "...";
    }
}

package com.orbit.admin.store;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.admin.store.mapper.OrbitJobLogMapper;
import com.orbit.admin.store.mapper.OrbitJobMapper;
import com.orbit.admin.store.po.OrbitJobLogPO;
import com.orbit.admin.store.po.OrbitJobPO;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.PageResult;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 任务与日志持久化存储层（MyBatis-Plus 实现）。
 * 技术栈：Druid 连接池 + MyBatis-Plus（{@link com.baomidou.mybatisplus.core.mapper.BaseMapper}）。
 * 设计说明：
 * 
 *   - 对外暴露/返回的是 {@code orbit-core} 的协议模型（{@link JobInfo}/{@link JobLog}），
 *       持久层内部使用 {@code po} 包下的实体（{@link OrbitJobPO}/{@link OrbitJobLogPO}），
 *       二者在此处相互转换，保证共享协议模块不依赖任何 ORM 框架；
 *   - {@code orbit_job} 通过 {@code @Version} + 乐观锁插件实现并发更新控制；
 *   - {@code params} 以 JSON 字符串落库；日志 {@code message} 超长截断；
 *   - 分页依赖 {@link com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor}。
 * 
 */
@Repository
public class JobStore {

    /** params 列宽（与 schema.sql 一致） */
    private static final int PARAMS_MAX_LEN = 2000;
    /** description 列宽（与 schema.sql 一致） */
    private static final int DESC_MAX_LEN = 256;
    /** message 列宽（与 schema.sql 一致） */
    private static final int MESSAGE_MAX_LEN = 2000;
    /** 默认超时（秒） */
    private static final int DEFAULT_TIMEOUT_SECONDS = 300;
    /** 每页最大记录数 */
    private static final int MAX_PAGE_SIZE = 200;

    private final OrbitJobMapper jobMapper;
    private final OrbitJobLogMapper logMapper;
    private final ObjectMapper mapper = new ObjectMapper();

    public JobStore(OrbitJobMapper jobMapper, OrbitJobLogMapper logMapper) {
        this.jobMapper = jobMapper;
        this.logMapper = logMapper;
    }

    /**
     * 查询所有任务列表（按任务名升序）。
     *
     * @return 任务列表
     */
    public List<JobInfo> findAllJobs() {
        List<OrbitJobPO> pos = jobMapper.selectList(
                new LambdaQueryWrapper<OrbitJobPO>().orderByAsc(OrbitJobPO::getJobName));
        return toJobs(pos);
    }

    /**
     * 根据任务名称精确查询任务。
     *
     * @param name 任务名称
     * @return 任务 Optional 包装
     */
    public Optional<JobInfo> findJobByName(String name) {
        OrbitJobPO po = jobMapper.selectOne(
                new LambdaQueryWrapper<OrbitJobPO>().eq(OrbitJobPO::getJobName, name));
        return Optional.ofNullable(po).map(this::toJob);
    }

    /**
     * 根据任务主键 ID 查询任务。
     *
     * @param id 任务主键 ID
     * @return 任务 Optional 包装
     */
    public Optional<JobInfo> findJobById(long id) {
        return Optional.ofNullable(jobMapper.selectById(id)).map(this::toJob);
    }

    /**
     * 分页查询任务列表，支持按任务名模糊查询。
     *
     * @param nameLike 任务名模糊关键字（可为空）
     * @param page     页码（从 1 开始）
     * @param size     每页大小（最小 1，最大 200）
     * @return 分页结果对象
     */
    public PageResult<JobInfo> pageJobs(String nameLike, int page, int size) {
        int p = Math.max(1, page);
        int s = Math.min(MAX_PAGE_SIZE, Math.max(1, size));

        LambdaQueryWrapper<OrbitJobPO> qw = new LambdaQueryWrapper<>();
        if (nameLike != null && !nameLike.trim().isEmpty()) {
            qw.like(OrbitJobPO::getJobName, nameLike.trim());
        }
        qw.orderByAsc(OrbitJobPO::getJobName);

        IPage<OrbitJobPO> result = jobMapper.selectPage(new Page<OrbitJobPO>(p, s), qw);
        return new PageResult<JobInfo>(p, s, result.getTotal(), toJobs(result.getRecords()));
    }

    /**
     * 保存或更新任务。
     * ID 为 null 走 INSERT 并回填自增 ID、版本号 1；ID 已存在走带乐观锁的 UPDATE，
     * 若影响行数为 0 说明发生并发冲突，抛出异常。
     *
     * @param job 待保存的任务对象
     * @return 保存成功后的任务对象
     */
    public JobInfo saveJob(JobInfo job) {
        Date now = new Date();

        // 入库前对可能超出列宽的字段做前置校验，避免触发 SQLException（列宽与 schema.sql 保持一致）
        String paramsJson = toJson(job.getParams());
        if (paramsJson != null && paramsJson.length() > PARAMS_MAX_LEN) {
            throw new IllegalArgumentException("params too long: serialized json length "
                    + paramsJson.length() + " exceeds limit " + PARAMS_MAX_LEN);
        }
        if (job.getDescription() != null && job.getDescription().length() > DESC_MAX_LEN) {
            throw new IllegalArgumentException("description too long: length "
                    + job.getDescription().length() + " exceeds limit " + DESC_MAX_LEN);
        }

        // 1. 新增
        if (job.getId() == null) {
            OrbitJobPO po = new OrbitJobPO();
            po.setJobName(job.getJobName());
            po.setDescription(blankToNull(job.getDescription()));
            po.setAppName(job.getAppName());
            po.setHandler(job.getHandler());
            po.setCronExpr(blankToNull(job.getCron()));
            po.setParams(paramsJson);
            po.setTimeoutSeconds(job.getTimeoutSeconds() <= 0 ? DEFAULT_TIMEOUT_SECONDS : job.getTimeoutSeconds());
            po.setRouteStrategy(blankToNull(job.getRouteStrategy()) == null ? "ROUND" : job.getRouteStrategy());
            po.setEnabled(job.isEnabled());
            po.setVersion(1);
            po.setCreatedAt(now);
            po.setUpdatedAt(now);
            jobMapper.insert(po);

            job.setId(po.getId());
            job.setVersion(1);
            job.setCreatedAt(now);
            job.setUpdatedAt(now);
            return job;
        }

        // 2. 更新：走实体式更新（updateById），由 @Version 乐观锁插件自动补全
        //    「SET version = 旧版本 + 1」与「WHERE id = ? AND version = 旧版本」。
        //
        //    注意：这里不能改用 update(entity, updateWrapper) 再对同一批列做 .set(...)。
        //    MyBatis-Plus 的 update(et, ew) 会把「实体的 SET 片段」与「wrapper 的 SET 片段」拼接，
        //    同名列出现两次，数据库直接报 Duplicate column name；
        //    同时实体的主键条件与 wrapper 的 .eq(id) 叠加，WHERE 里 id 条件也会重复。
        //    description / cron_expr / params 三列在 OrbitJobPO 上标了 FieldStrategy.ALWAYS，
        //    因此传 null 也会被写进 SET，仍然可以把这几列清空为 NULL。
        if (job.getVersion() == null) {
            throw new IllegalStateException("job version is required for update, id=" + job.getId());
        }
        OrbitJobPO po = new OrbitJobPO();
        po.setId(job.getId());
        po.setDescription(blankToNull(job.getDescription()));
        po.setAppName(job.getAppName());
        po.setHandler(job.getHandler());
        po.setCronExpr(blankToNull(job.getCron()));
        po.setParams(paramsJson);
        po.setTimeoutSeconds(job.getTimeoutSeconds() <= 0 ? DEFAULT_TIMEOUT_SECONDS : job.getTimeoutSeconds());
        po.setRouteStrategy(blankToNull(job.getRouteStrategy()) == null ? "ROUND" : job.getRouteStrategy());
        po.setEnabled(job.isEnabled());
        po.setUpdatedAt(now);
        // 乐观锁版本号：插件据此拼 WHERE version=? 并在 SET 中自增
        po.setVersion(job.getVersion());

        int rows = jobMapper.updateById(po);
        if (rows == 0) {
            throw new IllegalStateException("job concurrent update, please retry");
        }
        job.setVersion(job.getVersion() + 1);
        job.setUpdatedAt(now);
        return job;
    }

    /**
     * 根据任务名称物理删除任务记录。
     *
     * @param name 任务名称
     * @return 是否成功删除
     */
    public boolean deleteJob(String name) {
        return jobMapper.delete(new LambdaQueryWrapper<OrbitJobPO>().eq(OrbitJobPO::getJobName, name)) > 0;
    }

    /**
     * 统计系统总任务数量。
     *
     * @return 任务总数
     */
    public long countJobs() {
        return jobMapper.selectCount(null);
    }

    /**
     * 插入一条新的调度执行日志（初始为 RUNNING 状态）。
     *
     * @param log 日志实体
     */
    public void insertLog(JobLog log) {
        OrbitJobLogPO po = new OrbitJobLogPO();
        po.setLogId(log.getLogId());
        po.setJobId(log.getJobId() == null ? null : log.getJobId());
        po.setJobName(log.getJobName());
        po.setAppName(log.getAppName());
        po.setHandler(log.getHandler());
        po.setExecutorAddress(log.getExecutorAddress());
        po.setStatus(log.getStatus());
        po.setMessage(abbreviate(log.getMessage()));
        po.setCostMs(log.getCostMs());
        po.setStartTime(log.getStartTime());
        po.setEndTime(log.getEndTime());
        logMapper.insert(po);
        if (po.getId() != null) {
            log.setId(po.getId());
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
        LambdaUpdateWrapper<OrbitJobLogPO> uw = new LambdaUpdateWrapper<OrbitJobLogPO>()
                .eq(OrbitJobLogPO::getLogId, logId)
                .set(OrbitJobLogPO::getStatus, status)
                .set(OrbitJobLogPO::getExecutorAddress, address)
                .set(OrbitJobLogPO::getCostMs, costMs)
                .set(OrbitJobLogPO::getMessage, abbreviate(message))
                .set(OrbitJobLogPO::getEndTime, new Date());
        logMapper.update(null, uw);
    }

    /**
     * 分页查询调度日志列表（按 ID 倒序，即最新在前）。
     *
     * @param jobName 任务名称筛选（可为空）
     * @param page    页码
     * @param size    每页记录数
     * @return 分页结果集
     */
    public PageResult<JobLog> pageLogs(String jobName, int page, int size) {
        int p = Math.max(1, page);
        int s = Math.min(MAX_PAGE_SIZE, Math.max(1, size));

        LambdaQueryWrapper<OrbitJobLogPO> qw = new LambdaQueryWrapper<>();
        if (jobName != null && !jobName.trim().isEmpty()) {
            qw.eq(OrbitJobLogPO::getJobName, jobName);
        }
        qw.orderByDesc(OrbitJobLogPO::getId);

        IPage<OrbitJobLogPO> result = logMapper.selectPage(new Page<OrbitJobLogPO>(p, s), qw);
        List<JobLog> items = new ArrayList<JobLog>();
        for (OrbitJobLogPO po : result.getRecords()) {
            items.add(toLog(po));
        }
        return new PageResult<JobLog>(p, s, result.getTotal(), items);
    }

    // ============================ PO <-> 模型 转换 ============================

    private List<JobInfo> toJobs(List<OrbitJobPO> pos) {
        List<JobInfo> list = new ArrayList<JobInfo>();
        for (OrbitJobPO po : pos) {
            list.add(toJob(po));
        }
        return list;
    }

    private JobInfo toJob(OrbitJobPO po) {
        JobInfo j = new JobInfo();
        j.setId(po.getId());
        j.setJobName(po.getJobName());
        j.setDescription(po.getDescription());
        j.setAppName(po.getAppName());
        j.setHandler(po.getHandler());
        j.setCron(po.getCronExpr());
        j.setParams(parseMap(po.getParams()));
        j.setTimeoutSeconds(po.getTimeoutSeconds() == null ? DEFAULT_TIMEOUT_SECONDS : po.getTimeoutSeconds());
        j.setRouteStrategy(po.getRouteStrategy() == null ? "ROUND" : po.getRouteStrategy());
        j.setEnabled(Boolean.TRUE.equals(po.getEnabled()));
        j.setVersion(po.getVersion() == null ? 0 : po.getVersion());
        j.setCreatedAt(po.getCreatedAt());
        j.setUpdatedAt(po.getUpdatedAt());
        return j;
    }

    private JobLog toLog(OrbitJobLogPO po) {
        JobLog l = new JobLog();
        l.setId(po.getId());
        l.setLogId(po.getLogId());
        l.setJobId(po.getJobId() == null ? 0L : po.getJobId());
        l.setJobName(po.getJobName());
        l.setAppName(po.getAppName());
        l.setHandler(po.getHandler());
        l.setExecutorAddress(po.getExecutorAddress());
        l.setStatus(po.getStatus());
        l.setMessage(po.getMessage());
        l.setCostMs(po.getCostMs() == null ? 0L : po.getCostMs());
        l.setStartTime(po.getStartTime());
        l.setEndTime(po.getEndTime());
        return l;
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
     * 截断超长字符串（防止数据库字段超长溢出）
     */
    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= MESSAGE_MAX_LEN ? s : s.substring(0, MESSAGE_MAX_LEN) + "...";
    }
}

package com.orbit.scheduler.spi;

import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;

/**
 * 任务执行日志存储 SPI：支持数据库（t_job_log）与内存环形队列两种实现。
 *
 * @author orbit
 */
public interface JobLogRepository {

    /** 记录一条 RUNNING 日志，返回日志 ID */
    Long appendStart(JobLog log);

    /** 回写最终状态（SUCCESS/FAILED/SKIPPED） */
    void appendFinish(Long id, String status, String workerNode, long costMs, String message);

    /** 分页查询（taskName 可空） */
    PageResult<JobLog> page(String taskName, int page, int size);

    /** 存储类型标识：database / memory */
    String type();
}

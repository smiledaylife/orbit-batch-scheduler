/*
 * AICode Begin
 * BatchMetadataCleanupTasklet：JobRepository 元数据清理 Tasklet。
 *
 * 删除保留期（默认 180 天）之前的 JobExecution / StepExecution / Context / Params 记录，
 * 按外键依赖顺序执行 DELETE，最后清理无任何 Execution 的孤儿 JobInstance。
 *
 * 关键约束：
 *   - 表名必须使用 spring.batch.jdbc.table-prefix（生产为 DSV2_BATCH_）拼接，
 *     pbat001db 中同库存在旧版 Spring Batch 4.x 的 BATCH_* 生产表，绝不能被本任务触碰；
 *   - 表前缀只允许 [A-Za-z0-9_]，防止配置被当作 SQL 片段注入；
 *   - CREATE_TIME 列为 timestamp without time zone，占位符必须传 java.sql.Timestamp。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.regex.Pattern;

/**
 * Spring Batch 元数据表清理（Tasklet）。
 */
@Slf4j
public class BatchMetadataCleanupTasklet implements Tasklet {

    /** 默认保留天数 */
    public static final int DEFAULT_RETENTION_DAYS = 180;

    private static final Pattern SAFE_PREFIX = Pattern.compile("[A-Za-z0-9_]+");

    private final JdbcTemplate batchJdbcTemplate;
    private final String tablePrefix;
    private final int retentionDays;

    public BatchMetadataCleanupTasklet(JdbcTemplate batchJdbcTemplate, String tablePrefix, int retentionDays) {
        if (tablePrefix == null || !SAFE_PREFIX.matcher(tablePrefix).matches()) {
            throw new IllegalArgumentException("非法的 Spring Batch 表前缀: " + tablePrefix);
        }
        if (retentionDays <= 0) {
            throw new IllegalArgumentException("保留天数必须为正数: " + retentionDays);
        }
        this.batchJdbcTemplate = batchJdbcTemplate;
        this.tablePrefix = tablePrefix;
        this.retentionDays = retentionDays;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        Timestamp cutoff = Timestamp.valueOf(LocalDateTime.now().minusDays(retentionDays));
        log.info("batchMetadataCleanup 清理 {}* 中 {} 之前的 JobRepository 元数据", tablePrefix, cutoff);

        String jobExec = tablePrefix + "JOB_EXECUTION";
        String jobExecParams = tablePrefix + "JOB_EXECUTION_PARAMS";
        String jobExecCtx = tablePrefix + "JOB_EXECUTION_CONTEXT";
        String stepExec = tablePrefix + "STEP_EXECUTION";
        String stepExecCtx = tablePrefix + "STEP_EXECUTION_CONTEXT";
        String jobInst = tablePrefix + "JOB_INSTANCE";

        // 按外键顺序删除：params/context -> step -> job_execution -> job_instance
        int params = batchJdbcTemplate.update(
                "DELETE FROM " + jobExecParams + " p USING " + jobExec + " e "
                        + "WHERE p.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID AND e.CREATE_TIME < ?", cutoff);
        int stepCtx = batchJdbcTemplate.update(
                "DELETE FROM " + stepExecCtx + " sc USING " + stepExec + " s, " + jobExec + " e "
                        + "WHERE sc.STEP_EXECUTION_ID = s.STEP_EXECUTION_ID "
                        + "AND s.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID AND e.CREATE_TIME < ?", cutoff);
        int jobCtx = batchJdbcTemplate.update(
                "DELETE FROM " + jobExecCtx + " c USING " + jobExec + " e "
                        + "WHERE c.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID AND e.CREATE_TIME < ?", cutoff);
        int step = batchJdbcTemplate.update(
                "DELETE FROM " + stepExec + " s USING " + jobExec + " e "
                        + "WHERE s.JOB_EXECUTION_ID = e.JOB_EXECUTION_ID AND e.CREATE_TIME < ?", cutoff);
        int exec = batchJdbcTemplate.update(
                "DELETE FROM " + jobExec + " WHERE CREATE_TIME < ?", cutoff);
        int inst = batchJdbcTemplate.update(
                "DELETE FROM " + jobInst + " i WHERE NOT EXISTS "
                        + "(SELECT 1 FROM " + jobExec + " e WHERE e.JOB_INSTANCE_ID = i.JOB_INSTANCE_ID)");
        log.info("batchMetadataCleanup 完成: params={}, stepCtx={}, jobCtx={}, step={}, exec={}, orphanInstance={}",
                params, stepCtx, jobCtx, step, exec, inst);
        return RepeatStatus.FINISHED;
    }
}
/*
 * AICode End
 */

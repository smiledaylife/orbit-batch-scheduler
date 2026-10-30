/*
 * AICode Begin
 * ProductionJobsConfig：7 个生产任务 + 1 个元数据清理任务 的 Spring Batch Job 定义。
 *
 * 背景：dsv2 的 7 个 @OrbitJob 定时任务全部纳入 Spring Batch 统一治理，
 *       获取：Job/Step 状态落库（ BATCH_*）、重启恢复、skip/retry 策略。
 *       本类仅保留 Job 装配（StartStep 常量 + Job 名称常量），
 *       Step 级装配已拆分至 ProductionStepsConfig。
 * 设计：
 *   - 每个任务一个独立 Job（保持调度单元与失败隔离）；
 *   - 所有 Job 仅由 @OrbitJob handler 经 JobLauncher 显式触发（spring.batch.job.enabled=false）；
 *   - Job 名称常量为对外契约（@Qualifier(ProductionJobsConfig.JOB_XXX)），不得随意变更。
 */
package com.natpcp.natpcp.dsv2.batch;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 生产任务 Spring Batch Job 配置（Job 定义，Step 见 ProductionStepsConfig）。
 */
@Configuration
public class ProductionJobsConfig {

    // ---------------- Job 名称常量（对外契约，Job Handler 按 @Qualifier 注入） ----------------
    public static final String JOB_UPDATE_REDIS_INFO = "updateRedisInfoJob";
    public static final String JOB_UPDATE_REDIS_FUNCTION_URLS = "updateRedisFunctionUrlsJob";
    public static final String JOB_CLEAR_NOTICE = "clearNoticeJob";
    public static final String JOB_VERSION_NOTICE = "versionNoticeJob";
    public static final String JOB_PVC_FILE_BACKUP = "pvcFileBackupJob";
    public static final String JOB_TABLE_CHECK = "tableCheckJob";
    public static final String JOB_CONFIG_CHECK = "configCheckJob";
    public static final String JOB_BATCH_METADATA_CLEANUP = "batchMetadataCleanupJob";

    // ==================== Job 定义 ====================

    /**
     * updateRedisInfoJob：4 步串联（转发规则增量 -> 规则表 chunk 同步 -> 配置版本 -> 大网关版本）。
     */
    @Bean(name = JOB_UPDATE_REDIS_INFO)
    public Job updateRedisInfoJobBean(JobRepository jobRepository,
                                      Step forwardRuleStep, Step updateRedisSyncStep,
                                      Step configVersionStep, Step ngVersionStep) {
        return new JobBuilder(JOB_UPDATE_REDIS_INFO, jobRepository)
                .start(forwardRuleStep)
                .next(updateRedisSyncStep)
                .next(configVersionStep)
                .next(ngVersionStep)
                .build();
    }

    @Bean(name = JOB_UPDATE_REDIS_FUNCTION_URLS)
    public Job updateRedisFunctionUrlsJobBean(JobRepository jobRepository, Step functionUrlsStep) {
        return new JobBuilder(JOB_UPDATE_REDIS_FUNCTION_URLS, jobRepository)
                .start(functionUrlsStep)
                .build();
    }

    @Bean(name = JOB_CLEAR_NOTICE)
    public Job clearNoticeJobBean(JobRepository jobRepository, Step clearNoticeStep) {
        return new JobBuilder(JOB_CLEAR_NOTICE, jobRepository)
                .start(clearNoticeStep)
                .build();
    }

    @Bean(name = JOB_VERSION_NOTICE)
    public Job versionNoticeJobBean(JobRepository jobRepository, Step versionNoticeStep) {
        return new JobBuilder(JOB_VERSION_NOTICE, jobRepository)
                .start(versionNoticeStep)
                .build();
    }

    @Bean(name = JOB_PVC_FILE_BACKUP)
    public Job pvcFileBackupJobBean(JobRepository jobRepository, Step pvcFileBackupStep) {
        return new JobBuilder(JOB_PVC_FILE_BACKUP, jobRepository)
                .start(pvcFileBackupStep)
                .build();
    }

    @Bean(name = JOB_TABLE_CHECK)
    public Job tableCheckJobBean(JobRepository jobRepository, Step tableCheckStep) {
        return new JobBuilder(JOB_TABLE_CHECK, jobRepository)
                .start(tableCheckStep)
                .build();
    }

    @Bean(name = JOB_CONFIG_CHECK)
    public Job configCheckJobBean(JobRepository jobRepository, Step configCheckStep) {
        return new JobBuilder(JOB_CONFIG_CHECK, jobRepository)
                .start(configCheckStep)
                .build();
    }

    @Bean(name = JOB_BATCH_METADATA_CLEANUP)
    public Job batchMetadataCleanupJobBean(JobRepository jobRepository, Step batchMetadataCleanupStep) {
        return new JobBuilder(JOB_BATCH_METADATA_CLEANUP, jobRepository)
                .start(batchMetadataCleanupStep)
                .build();
    }
}
/*
 * AICode End
 */

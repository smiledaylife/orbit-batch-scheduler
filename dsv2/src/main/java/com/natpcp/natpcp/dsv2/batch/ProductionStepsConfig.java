/*
 * AICode Begin
 * ProductionStepsConfig：生产任务 Spring Batch Step 定义（与 Job 定义分离）。
 *
 * 背景：从 ProductionJobsConfig 拆分而来，其职责聚焦 Step 级装配：
 *       Tasklet/Reader/Writer 实例化、chunk 切分、faultTolerant skip/retry。
 * 设计：
 *   - 各 Step 的 bean 名即方法名，Job 定义（ProductionJobsConfig）按参数名注入，
 *     完成配置类之间按类型/名称解耦；
 *   - updateRedisSyncStep 为 chunk（4）+ faultTolerant（retry 3 / skip 5），
 *     其余为单步 Tasklet；
 *   - updateRedisItemReader 必须保留 @StepScope：周期性触发时由 Spring Batch
 *     每次重建 Reader 实例，避免单例 Reader 迭代器耗尽后读到 0 条数据；
 *   - 事务管理器：所有 Step 统一使用容器内的 PlatformTransactionManager（即 BATCH_* 元数据
 *     所在数据源的 DataSourceTransactionManager）。各 Tasklet 的业务写入要么在同一数据源
 *     （tb_notice 等），要么是 Redis / Kafka / SSH 等非事务资源，因此不单独为业务库配置
 *     第二个事务管理器；若未来业务表迁至独立数据源，需为相应 Step 显式指定其事务管理器。
 */
package com.natpcp.natpcp.dsv2.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.batch.processor.UpdateRedisItemProcessor;
import com.natpcp.natpcp.dsv2.batch.reader.UpdateRedisItemReader;
import com.natpcp.natpcp.dsv2.batch.tasklet.BatchMetadataCleanupTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.ClearNoticeTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.ConfigCheckTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.ConfigVersionTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.ForwardRuleTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.FunctionUrlsTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.NgVersionTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.PvcFileBackupTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.TableCheckTasklet;
import com.natpcp.natpcp.dsv2.batch.tasklet.VersionNoticeTasklet;
import com.natpcp.natpcp.dsv2.batch.writer.UpdateRedisItemWriter;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.mapper.GenericMapper;
import com.natpcp.natpcp.dsv2.jobs.AdminJobClient;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.ConfigurationVersionService;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.service.PvcService;
import com.natpcp.natpcp.dsv2.service.impl.FunctionServiceImpl;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * 生产任务 Spring Batch Step 配置。
 */
@Slf4j
@Configuration
public class ProductionStepsConfig {

    /**
     * 清理 JobRepository 元数据表的 JdbcTemplate：必须使用批数据源（pbat001db）。
     */
    @Bean
    public JdbcTemplate batchJdbcTemplate(@Qualifier("batchDataSource") DataSource batchDataSource) {
        return new JdbcTemplate(batchDataSource);
    }

    /**
     * chunk 模式 Reader（@StepScope）：每次 Step 执行时由 Spring Batch 创建新的 Reader 实例，
     * 避免容器级单例 Reader 的迭代器在首次执行后被耗尽、后续执行读到 0 条数据的问题。
     * 注意：必须保留 @StepScope——updateRedisInfo 周期性触发，靠 scope 重建 iterator。
     */
    @Bean
    @StepScope
    public ItemReader<UpdateRedisSyncItem> updateRedisItemReader() {
        return new UpdateRedisItemReader();
    }

    @Bean
    public Step forwardRuleStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                GenericDataAccessService genericDataAccessService) {
        return new StepBuilder("forwardRuleStep", jobRepository)
                .tasklet(new ForwardRuleTasklet(genericDataAccessService), transactionManager)
                .build();
    }

    @Bean
    public Step updateRedisSyncStep(JobRepository jobRepository,
                                    PlatformTransactionManager transactionManager,
                                    GenericDataAccessService genericDataAccessService,
                                    GenericMapper genericMapper,
                                    RedisManager redisManager,
                                    ObjectMapper objectMapper) {
        return new StepBuilder("updateRedisSyncStep", jobRepository)
                .<UpdateRedisSyncItem, UpdateRedisSyncItem>chunk(4, transactionManager)
                .reader(updateRedisItemReader())
                .processor(new UpdateRedisItemProcessor(genericDataAccessService, genericMapper, redisManager, objectMapper))
                .writer(new UpdateRedisItemWriter(redisManager))
                // faultTolerant：单 key 查询/序列化失败重试 3 次；仍失败跳过该 key（不阻塞其它 key）
                .faultTolerant()
                .retryLimit(3)
                .retry(Exception.class)
                .skipLimit(5)
                .skip(Exception.class)
                // 被跳过的 key 意味着该规则表未同步到 Redis，必须显式告警而不是静默 COMPLETED
                .listener(new SkipListener<UpdateRedisSyncItem, UpdateRedisSyncItem>() {
                    @Override
                    public void onSkipInRead(Throwable t) {
                        log.error("updateRedisInfo 读取阶段跳过", t);
                    }

                    @Override
                    public void onSkipInProcess(UpdateRedisSyncItem item, Throwable t) {
                        log.error("updateRedisInfo 跳过 redisKey={}（DB 查询/序列化失败，该 key 本轮未同步）",
                                item.getRedisKey(), t);
                    }

                    @Override
                    public void onSkipInWrite(UpdateRedisSyncItem item, Throwable t) {
                        log.error("updateRedisInfo 跳过 redisKey={}（Redis 写入失败，该 key 本轮未同步）",
                                item.getRedisKey(), t);
                    }
                })
                .build();
    }

    @Bean
    public Step configVersionStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  GenericDataAccessService genericDataAccessService,
                                  GenericMapper genericMapper,
                                  RedisManager redisManager,
                                  ObjectMapper objectMapper) {
        return new StepBuilder("configVersionStep", jobRepository)
                .tasklet(new ConfigVersionTasklet(genericDataAccessService, genericMapper, redisManager, objectMapper),
                        transactionManager)
                .build();
    }

    @Bean
    public Step ngVersionStep(JobRepository jobRepository,
                              PlatformTransactionManager transactionManager,
                              ConfigurationVersionService configurationVersionService,
                              RedisManager redisManager,
                              ObjectMapper objectMapper,
                              RedisTemplate<String, String> redisTemplate) {
        return new StepBuilder("ngVersionStep", jobRepository)
                .tasklet(new NgVersionTasklet(configurationVersionService, redisManager, objectMapper, redisTemplate),
                        transactionManager)
                .build();
    }

    @Bean
    public Step functionUrlsStep(JobRepository jobRepository,
                                 PlatformTransactionManager transactionManager,
                                 FunctionServiceImpl functionService) {
        return new StepBuilder("functionUrlsStep", jobRepository)
                .tasklet(new FunctionUrlsTasklet(functionService), transactionManager)
                .build();
    }

    @Bean
    public Step clearNoticeStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                NoticeService noticeService) {
        return new StepBuilder("clearNoticeStep", jobRepository)
                .tasklet(new ClearNoticeTasklet(noticeService), transactionManager)
                .build();
    }

    @Bean
    public Step versionNoticeStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  RedisTemplate<String, String> redisTemplate,
                                  RedisManager redisManager,
                                  GenericDataAccessService genericDataAccessService,
                                  NoticeService noticeService,
                                  ConfigurableApplicationContext context) {
        return new StepBuilder("versionNoticeStep", jobRepository)
                .tasklet(new VersionNoticeTasklet(redisTemplate, redisManager,
                        genericDataAccessService, noticeService, context), transactionManager)
                .build();
    }

    @Bean
    public Step pvcFileBackupStep(JobRepository jobRepository,
                                  PlatformTransactionManager transactionManager,
                                  PvcService pvcService,
                                  KafkaProducer<String, String> kafkaProducer,
                                  @Value("${framework.dataCenter:H}") String dataCenter,
                                  @Value("${spring.kafka.topic:monitor_common_bus}") String monitorTopic) {
        return new StepBuilder("pvcFileBackupStep", jobRepository)
                .tasklet(new PvcFileBackupTasklet(pvcService, kafkaProducer, dataCenter, monitorTopic), transactionManager)
                .build();
    }

    @Bean
    public Step tableCheckStep(JobRepository jobRepository,
                               PlatformTransactionManager transactionManager,
                               ConnectionInfoMapper connectionInfoMapper,
                               NoticeService noticeService,
                               AdminJobClient adminJobClient) {
        return new StepBuilder("tableCheckStep", jobRepository)
                .tasklet(new TableCheckTasklet(connectionInfoMapper, noticeService, adminJobClient), transactionManager)
                .build();
    }

    @Bean
    public Step configCheckStep(JobRepository jobRepository,
                                PlatformTransactionManager transactionManager,
                                ConnectionInfoMapper connectionInfoMapper,
                                NoticeService noticeService,
                                @Value("${dsv2.ssh.strict-host-key-checking:no}") String strictHostKeyChecking) {
        return new StepBuilder("configCheckStep", jobRepository)
                .tasklet(new ConfigCheckTasklet(connectionInfoMapper, noticeService, strictHostKeyChecking),
                        transactionManager)
                .build();
    }

    /**
     * 元数据清理 Step：删除保留期（默认 180 天）之前的 JobRepository 元数据。
     * 表名使用 spring.batch.jdbc.table-prefix（生产为 DSV2_BATCH_）拼接，
     * 绝不能落到同库旧版 Spring Batch 4.x 的 BATCH_* 生产表上。
     */
    @Bean
    public Step batchMetadataCleanupStep(JobRepository jobRepository,
                                         @Qualifier("batchTransactionManager") PlatformTransactionManager batchTransactionManager,
                                         JdbcTemplate batchJdbcTemplate,
                                         BatchProperties batchProperties,
                                         @Value("${dsv2.batch.metadata.retention-days:180}") int retentionDays) {
        String tablePrefix = batchProperties.getJdbc().getTablePrefix();
        return new StepBuilder("batchMetadataCleanupStep", jobRepository)
                .tasklet(new BatchMetadataCleanupTasklet(batchJdbcTemplate, tablePrefix, retentionDays),
                        batchTransactionManager)
                .build();
    }
}
/*
 * AICode End
 */

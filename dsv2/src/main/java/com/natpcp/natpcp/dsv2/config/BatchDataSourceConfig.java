/*
 * AICode Begin
 * BatchDataSourceConfig：Spring Batch 专用数据源配置（Druid 版）。
 *
 * 背景：
 *   pbat001db 中已存在旧版 Spring Batch(4.x) 的 BATCH_* 元数据表（生产在用，共 3101 条执行记录），
 *   其表结构与 Spring Boot 3.5 / Spring Batch 5.2 官方 schema 不兼容（4.x 的
 *   batch_job_execution_params 为 type_cd/key_name/string_val 旧布局）。
 *   因此：
 *     1. JobRepository 使用独立数据源 pbat001db（@BatchDataSource，Boot 自动优先后台 JobRepository 使用）；
 *     2. 元数据表通过 table-prefix 隔离为 DSV2_BATCH_*（5.x 官方结构）；
 *     3. initialize-schema=never，DSV2_BATCH_* 表由 sql/batch_schema_dsv2_postgresql.sql 手工建一次。
 *
 * 实现要点（Spring Boot 官方多数据源范式 + Druid 连接池）：
 *   - 先用 DataSourceProperties 绑定 spring.datasource.batch.*（该 POJO 有 url/username/password 等
 *     setter，relaxed binding 正常）；再通过 initializeDataSourceBuilder().type(DruidDataSource.class)
 *     生成 DruidDataSource，该步骤负责属性映射（Druid 使用 setUrl/setUsername/setPassword）。
 *   - Druid 连接池参数通过第二个 @ConfigurationProperties 绑到 spring.datasource.druid.*
 *     （批数据源绑 spring.datasource.batch.druid.*）：initialSize/maxActive/minIdle/
 *     validationQuery/testWhileIdle 等。
 *   - 与体系内其他服务（natpcp/ds、natpcp/ots 等）统一使用 Druid，便于统一监控与运维。
 *
 * 关键点：
 *   - @BatchDataSource 注解是 Boot 自动配置识别"批数据源"的约定标记；
 *   - @BatchTransactionManager 使 Batch 事务使用批数据源自身的事务管理器；
 *   - 主数据源（pweb001db）不受影响，业务 Mapper 仍用默认 DataSource。
 */
package com.natpcp.natpcp.dsv2.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BatchDataSource;
import org.springframework.boot.autoconfigure.batch.BatchTransactionManager;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;

/**
 * Spring Batch 专用数据源（pbat001db）+ 主数据源（pweb001db）双数据源配置（Druid 版）。
 *
 * 注意：自定义 @BatchDataSource 后，Boot 的 DataSourceAutoConfiguration 因
 * @ConditionalOnMissingBean(DataSource) 不再自动创建默认数据源，因此必须显式
 * 定义 @Primary 主数据源（业务库 pweb001db），否则 MyBatis 会退化使用唯一的
 * batchDataSource（pbat001db）执行业务 SQL，导致业务表 relation does not exist。
 */
@Configuration
public class BatchDataSourceConfig {

    // ==================== 主数据源（业务库 pweb001db，MyBatis 使用） ====================

    /**
     * 主数据源连接属性：绑定 spring.datasource.*。
     */
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource")
    public DataSourceProperties primaryDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * 主数据源：由 DataSourceProperties 生成 DruidDataSource（连接池参数绑定 druid 子键）。
     */
    @Bean
    @Primary
    @ConfigurationProperties("spring.datasource.druid")
    public DruidDataSource primaryDataSource(
            @Qualifier("primaryDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(DruidDataSource.class).build();
    }

    /**
     * 主事务管理器：绑定主数据源（MyBatis / 业务 Service 事务）。
     */
    @Bean
    @Primary
    public PlatformTransactionManager primaryTransactionManager(
            @Qualifier("primaryDataSource") DataSource primaryDataSource) {
        return new DataSourceTransactionManager(primaryDataSource);
    }

    // ==================== 批数据源（pbat001db，JobRepository 使用） ====================

    /**
     * 批数据源连接属性：绑定 spring.datasource.batch.*。
     */
    @Bean
    @ConfigurationProperties("spring.datasource.batch")
    public DataSourceProperties batchDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * 批数据源：由 DataSourceProperties 生成 DruidDataSource，连接池参数绑定 druid 子键。
     */
    @Bean
    @BatchDataSource
    @ConfigurationProperties("spring.datasource.batch.druid")
    public DruidDataSource batchDataSource(
            @Qualifier("batchDataSourceProperties") DataSourceProperties properties) {
        return properties.initializeDataSourceBuilder().type(DruidDataSource.class).build();
    }

    /**
     * 批处理事务管理器：与批数据源绑定。
     */
    @Bean
    @BatchTransactionManager
    public PlatformTransactionManager batchTransactionManager(
            @Qualifier("batchDataSource") DataSource batchDataSource) {
        return new DataSourceTransactionManager(batchDataSource);
    }
}
/*
 * AICode End
 */
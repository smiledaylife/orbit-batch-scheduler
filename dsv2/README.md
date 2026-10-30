# dsv2 — plt-mgmt 定时任务执行器

将 plt-mgmt 的 7 个生效定时任务迁移为独立执行器：基于 orbit-executor SDK（`@OrbitJob`）注册到调度中心，
任务体全部纳入 Spring Batch 统一治理（状态落库、重启恢复、skip/retry）。

- 基线：JDK 21 · Spring Boot 3.5 · Spring Batch 5 · MyBatis-Plus · PostgreSQL · Redis Cluster · Kafka
- 约束：**零外部二方 jar**——plt-common / cpufp 相关类已以源码形式精简进本工程，只依赖开源库 + orbit-executor

## 1. 构建与运行

```bash
# 在仓库根目录（dsv2 已纳入根 reactor，CI 会跑全部模块测试）
mvn -B -ntp verify -pl dsv2 -am
java -jar dsv2/target/dsv2-1.0.0.jar
```

启动后应看到 `registered 8 handler(s)`，`GET /orbit/executor/handlers` 返回下表中的 8 个 handler。

## 2. 任务清单

| @OrbitJob handler | Spring Batch Job | Step / 说明 |
|---|---|---|
| `updateRedisInfo` | `updateRedisInfoJob` | forwardRule → updateRedisSync（chunk=4，retry 3 / skip 5）→ configVersion → ngVersion |
| `updateRedisFunctionUrls` | `updateRedisFunctionUrlsJob` | 全量覆写功能 URL 缓存（Tasklet） |
| `clearNotice` | `clearNoticeJob` | 清理历史通知 |
| `versionNotice` | `versionNoticeJob` | 实例 / 大网关配置版本与库内比对，不一致发通知 |
| `pvcFileBackup` | `pvcFileBackupJob` | PVC 文件备份，异常推送 Kafka 监控消息 |
| `TableCheck` | `tableCheckJob` | 表 / 分区表存在性巡检，最新分区表名回写调度中心任务参数 |
| `ConfigCheck` | `configCheckJob` | Redis / PostgreSQL / RocketMQ 配置项巡检 |
| `batchMetadataCleanup` | `batchMetadataCleanupJob` | 清理 `retention-days` 之前的 `DSV2_BATCH_*` 元数据 |

handler 类位于 `jobs/`，只负责桥接（`BatchJobBridge.run(job, handlerName, jobContext)`）；业务逻辑在 `batch/tasklet/`、`batch/{reader,processor,writer}/`。
任务参数通过 `JobParameters.jobParams`（JSON）传入，Tasklet 内用 `JobParamsHelper.getParamsMap` + `JsonUtil.convert` 还原为 BO。
`BatchJobBridge` 为每次触发追加 `runTime` 时间戳，保证每次 orbit 触发都是新的 JobInstance。

## 3. 配置

`src/main/resources/application.yml` 只含与环境无关的默认值。环境相关配置通过 **K8s 原生 envFrom** 注入
（ConfigMap → 普通配置，Secret → 凭据），示例见 `deploy/k8s/06-dsv2-executor.yaml`。
Spring Boot 宽松绑定：`spring.datasource.url` ← `SPRING_DATASOURCE_URL`，以此类推。**不使用 spring-cloud-kubernetes**，改配置后滚动重启 Pod。

必须由外部提供的键：

| 键 | 说明 |
|---|---|
| `spring.datasource.{url,username,password}` | 业务库 pweb001db（tb_notice / tb_connection_info / tb_configuration_version …） |
| `spring.datasource.batch.{url,username,password}` | 批元数据库 pbat001db（`DSV2_BATCH_*`） |
| `spring.data.redis.cluster.nodes` / `spring.data.redis.password` | gatewayRedis 集群 |
| `spring.kafka.bootstrap-servers` | 监控消息 topic |
| `jasypt.encryptor.password` | SM4 密钥，仅 Secret 注入 |
| `pvc.backup.path` / `plt.localFileDirRoot` | 备份与共享文件根目录 |
| `orbit.executor.{admin-addresses,app-name,access-token}` | 调度中心注册 |

dsv2 自定义键：`dsv2.batch.metadata.retention-days`（默认 180）、`dsv2.ssh.strict-host-key-checking`（默认 `no`，生产配好 known_hosts 后建议 `yes`）。

> 仓库任何文件都不得包含真实凭据。历史上 `PROGRESS.md` 曾写入过明文密码（已删除），相关凭据需要轮换并清理 Git 历史。

## 4. 关键设计决策

**双数据源**（`config/BatchDataSourceConfig`，Druid）
- `@Primary` 主数据源 → pweb001db，业务 Mapper 使用；`@BatchDataSource` → pbat001db，仅 JobRepository 使用。
- 必须显式声明 `@Primary` 主数据源：只定义 `@BatchDataSource` 时 Boot 不再自动创建默认数据源，MyBatis 会退化到批数据源执行业务 SQL。
- `@ConfigurationProperties` 先绑 `DataSourceProperties` 再 `initializeDataSourceBuilder()`，避免 `url`/`jdbcUrl` 属性名不匹配。

**元数据表前缀隔离**
- pbat001db 中已有生产在用的 Spring Batch 4.x `BATCH_*` 表（结构与 5.x 不兼容，不可动），因此 dsv2 使用 `spring.batch.jdbc.table-prefix: DSV2_BATCH_`，`initialize-schema: never`，表由 DBA 手工创建。
- `BatchMetadataCleanupTasklet` 按该前缀拼表名，绝不触碰旧表。
- `src/main/resources/sql/batch_schema_postgresql.sql` 是官方无前缀脚本，仅作参考；生产使用的 `DSV2_BATCH_` 前缀版本尚未入库，建表前请以该脚本为蓝本加前缀。

**JobRepository 隔离级别**：PostgreSQL 上默认 SERIALIZABLE 在多任务准点并发时触发 SSI 冲突，已设 `isolation-level-for-create: read_committed`（JobInstance 唯一性由唯一约束保证）。

**Step 事务管理器**：所有 Step 使用容器内 `PlatformTransactionManager`；Tasklet 的写入要么在同一数据源，要么是 Redis/Kafka/SSH 等非事务资源，不单独配置第二个事务管理器（详见 `ProductionStepsConfig` 头注释）。

**不能加 `@EnableBatchProcessing`**：会绕过 Boot 自动配置，使 `spring.batch.*` 属性失效。

**`updateRedisItemReader` 必须 `@StepScope`**：单例 Reader 的迭代器首次执行后耗尽，周期性触发会读到 0 条。

**bean 名冲突**：Job `@Bean` 用 `ProductionJobsConfig.JOB_*` 常量显式命名；`PvcFileBackupJob / TableCheckJob / ConfigCheckJob` 三个 handler 类显式 `@Component("xxxJobHandler")`。

**通知与 JSON**：告警统一走 `NoticeService.sendToAdminAndOps(title, content)`（0101 系统管理员 + 0104 运维员）；JSON 统一走 `util/JsonUtil`（Jackson，NON_NULL），工程内不再使用 fastjson。

**外部连接安全**：`ConfigCheck / TableCheck` 的 JDBC URL、SSH 路径、端口均来自库表，统一经 `util/ConnectionInfoValidator` 白名单校验；SSH / Redis / RestTemplate 均设超时。

**表名大小写**：`TableCheckTasklet.findTable` 先按 `DatabaseMetaData.storesLower/UpperCaseIdentifiers` 归一化表名查询，未命中再以原始写法兜底，避免 PostgreSQL（小写）/ Oracle（大写）与运维填参大小写不一致导致误报。

## 5. 调度中心契约

- `JobContext`：`getParams()` 为 `Map<String,Object>`（admin 创建任务时 params 填 JSON 对象）、`getHandler()`、`getLogId()` 等。
- TableCheck 回写：`AdminJobClient` `GET /orbit/admin/jobs/{name}` → `{data:{job,quartz}}`，合并 params 后 `PUT` 完整 JobInfo；支持多 admin 地址故障转移。
- 同时触发多个 Job 时 JobRepository 可能报 `could not serialize access`；生产 cron 请错开。

## 6. 已知待办

- 生产环境实跑验证 8 个任务（需 pweb001db / gatewayRedis / PVC 路径可用）。
- 将 `DSV2_BATCH_` 前缀建表脚本入库。
- `logback-common.xml` 当前未被加载（Spring Boot 只识别 `logback-spring.xml`），是否启用待定。
- 历史凭据轮换与 Git 历史清理（见 CHANGELOG 2026-10-30）。

变更历史见 [CHANGELOG.md](CHANGELOG.md)。

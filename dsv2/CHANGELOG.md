# dsv2 变更记录

## 2026-10-30 — 第三轮：遗留决策落地

- `TableCheckTasklet`：表名按数据库标识符存储习惯（`storesLowerCaseIdentifiers / storesUpperCaseIdentifiers`）归一化后查询，未命中再以原始写法兜底；新增 4 个测试。
- 移除 `application.yml` 中无对应依赖的 `spring.cloud.kubernetes.*` 配置；配置注入改为 K8s 原生 envFrom，新增 `deploy/k8s/06-dsv2-executor.yaml`（ConfigMap + Secret 示例 + Deployment）。
- `PROGRESS.md` 拆分为 `README.md`（构建、配置、设计决策）与本文件；过程性记录（本地路径、IDEA 设置、临时验证步骤）不再保留。
- 决策：`logback-common.xml` 暂不处理（保持未加载状态，待确认日志采集方式）。

## 2026-10-30 — 第二轮：可读性重构与依赖治理

- JSON 统一为 Jackson：新增 `util/JsonUtil`（toJson / fromJson / toMap / parseMap / convert）；`JobParamsHelper`、`ConfigCheckTasklet`、`TableCheckTasklet`、`VersionNoticeTasklet`、`PvcFileBackupTasklet` 及测试全部改用；移除 fastjson 1.2.84（EOL）。
- 通知发送去重：新增 `NoticeService.sendToAdminAndOps(title, content)`（0101 + 0104 角色），替换三个 Tasklet 各自复制的 insert ×2 代码。
- `ConfigCheckTasklet / TableCheckTasklet / VersionNoticeTasklet` 按职责拆分为小方法重写，业务语义与通知文案不变；`VersionNoticeTasklet` 库内版本只查一次。
- jsch：`com.jcraft:jsch 0.1.55` → `com.github.mwiede:jsch 2.28.8`（包名不变）。
- `ProductionStepsConfig` 补充事务管理器选型说明；清理残留注释。
- 测试修正：Postgres 配置巡检用例改用合法 JDBC URL；表存在用例补 `TABLE_NAME` 桩。
- 新增 `docs/ci/maven-failure-summary.patch.md`（CI 失败摘要 workflow 补丁，需维护者手工应用）。

## 2026-10-30 — 第一轮：代码审查阻断项修复

- 修复 `@MapperScan` 包名（原配置导致 Mapper 全部未注册）。
- `batchMetadataCleanup` 改为 `BatchMetadataCleanupTasklet`，表名按 `spring.batch.jdbc.table-prefix` 拼接（原硬编码会误删同库旧版生产表）。
- **安全**：清除 `PROGRESS.md` 中的明文凭据（数据库 / Redis 密码、SM4 密钥）——**相关凭据必须轮换，并清理 Git 历史**；`KafkaProducerConfig` 不再内置环境地址。
- `DynamicSqlProvider` 条件值改为 `#{}` 参数绑定（原字符串拼接存在 SQL 注入）。
- `objectRedisTemplate` 反序列化改为白名单 `BasicPolymorphicTypeValidator`。
- `ConfigCheck / TableCheck`：新增 `ConnectionInfoValidator`（JDBC URL / SSH 路径 / 端口白名单），Jedis try-with-resources，SSH / Redis 超时，StrictHostKeyChecking 可配置。
- `AdminJobClient`：RestTemplate 超时、多地址故障转移。
- `RedisManager.scanKeys` 不再吞异常；`VersionNoticeTasklet` 容忍重复 / 缺失版本记录；`PvcServiceImpl` 关闭 `Files.walk` 流并保留异常 cause。
- `application.yml` 补充 `spring.batch.*`（含 `isolation-level-for-create: read_committed`）。
- pom：orbit 版本对齐 1.0.0，移除不存在的 orbit-boot-support 与未使用的 hutool / pagehelper，jacoco 0.8.12；根 pom 将 dsv2 纳入 reactor。

## 2026-09-30 — 7 个生产任务纳入 Spring Batch

- 7 个 `@OrbitJob` handler 保留并桥接 Spring Batch Job；`updateRedisInfo` 改 chunk（faultTolerant retry 3 / skip 5），其余 Tasklet；新增 `batchMetadataCleanup`。
- 新增 `BatchJobBridge`、`JobParamsHelper`、`ProductionJobsConfig / ProductionStepsConfig`、reader / processor / writer、各 Tasklet。
- 双数据源：`@Primary` 主数据源 pweb001db + `@BatchDataSource` pbat001db；元数据表前缀 `DSV2_BATCH_` 隔离旧版 4.x `BATCH_*` 生产表。
- 端到端验证：8 个 Job 全部 COMPLETED，元数据落 pbat001db `dsv2_batch_*`，旧表行数未变。

## 2026-09-28 ~ 09-29 — 工程初始化

- 从 plt-mgmt 迁出 7 个任务到独立工程；包结构重排为 `com.natpcp.natpcp.dsv2.*`；删除 Soma 调度依赖与大量零引用代码（ExceptionCodeMsg 150 项 → 1 项、死 Mapper statement 等）。
- 启动验证：调度中心与执行器两侧均看到 7 个 handler 注册。

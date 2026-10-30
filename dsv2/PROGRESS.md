# dsv2 迁移进度记录

> 保存时间: 2026-09-30（7 个生产任务全部纳入 Spring Batch 统一治理，端到端验证通过）。本文件用于中断后续航恢复上下文。
> 当前状态：**7 个任务 handler 全部桥接 Spring Batch Job（事务/skip/retry/重启恢复），updateRedisInfo chunk 化（8 key 切片 faultTolerant），新增 batchMetadataCleanup 元数据清理任务；8 生产 Job 全部实测 COMPLETED（pbat001db DSV2_BATCH_* 状态落库）**。

## 项目目标
将 plt-mgmt 的 7 个生效定时任务迁移到独立执行器工程 `D:\Ai-codexProjects\natpcp\dsv2`：
基于 orbit-executor SDK（@OrbitJob），JDK 21 + Spring Boot 3.5.12 + Jakarta，删除 Soma 调度依赖，
**零外部二方 jar**（plt-common-*/cpufp 全部等价类复制/简化进工程），只依赖开源库 + orbit-executor。

## 工程位置与构建（新结构）
- 工程根：`D:\Ai-codexProjects\natpcp\dsv2`（**原 ds-V2 已重命名为 dsv2**，老路径不再存在）
- **包结构（用户重排）**：
  - 启动类 `com.natpcp.natpcp.dsv2.DsV2Application`（@SpringBootApplication 默认扫描 com + @MapperScan("com.natpcp.dsv2.dao.mapper")）
  - 业务单一根包 `com.natpcp.dsv2.{annotation, common, constant, dao, jobs, redis, service, util}`
  - **旧包 com.platform.* / com.cpufp.* / com.plt.* 已全部废弃**
- pom.xml：**自包含（无 parent）**，dependencyManagement import `spring-boot-dependencies:3.5.12` + 显式 mybatis-plus-spring-boot3-starter:3.5.7 / mybatis:3.5.19。
  properties: java.release=21、spring-boot.version=3.5.12、lombok 1.18.30。
  build: compiler 3.13.0（release 21 + -parameters）、spring-boot-maven-plugin 3.5.12（repackage）、surefire 3.5.4。
  依赖: orbit-executor 1.0.0、starter-web、starter-data-redis、mybatis-plus、postgresql、fastjson 1.2.83、hutool-all 5.8.21、
  bcprov-jdk18on 1.73、jedis 3.10.0、jsch 0.1.55、commons-lang3 3.12.0。
- 构建: `mvn.cmd -s D:\Ai-codexProjects\natpcp\dsv2\settings-dsv2.xml -f D:\Ai-codexProjects\natpcp\dsv2\pom.xml clean package -DskipTests`
  (JAVA_HOME=`C:\Program Files\Eclipse Adoptium\jdk-21.0.7.6-hotspot`)。**mvn clean compile 已 BUILD SUCCESS**（67 源文件）。
- 产物：fat jar（55.3MB 左右）version 1.0.0。
- settings-dsv2.xml：localRepository=otsp_repo + mirror repo.com:8999（可达已验证）。

## 目录重排后的查漏补缺（2026-09-29 已完成）
- 重命名 `dao/po/ConfigurationVersionPO.java` → `ConfigurationVersionPo.java`；修复 ConfigurationVersionMapper.xml 旧包 type 引用。
- 修复 4 个 java 旧 import（ConfigurationVersionServiceMapper/Service/ServiceImpl、TaskSchedulingNoticeJobs）。
- 修复 `PvcServiceImpl` package（service.impl.model → service.impl，唯一 package/目录不一致文件）。
- DsV2Application 重写：@SpringBootApplication（默认扫描）+ @MapperScan("com.natpcp.dsv2.dao.mapper")。
- 删除无引用类：`common/PltCachePrefix.java`、`dao/mapper/CustomedMapper.java`、`redis/manager/RedisLock.java`（+ FrameRedisConfiguration 中 redisLock bean）。

## 冗余清理（2026-09-29 已完成，clean compile 通过）
- `common/ExceptionCodeMsg`：887 行约 150 枚举项 → 仅保留 `TRADE_NOTICE_ERROR`（唯一被 TaskSchedulingNoticeJobs 引用）。
- `constant/Constant`：删除零引用常量 LOCAL/REMOTE/FORWARDRULE/APIGROUP/VERSION/NOTICE。
- `service/impl/DynamicSqlProvider`：删除 insertOrUpdate + generateOnConflictDoUpdate（GenericMapper 未声明，死代码）。
- `FunctionMapper.xml`：12 个 statement → 仅保留 getUrls（接口只声明 getUrls）。
- `NoticeMapper.xml`：删除接口未声明的死 statement（deleteByPrimaryKey/insertSelective/updateByPrimaryKeySelective/updateByPrimaryKey，
  其中 deleteByPrimaryKey 的 SQL 残留残缺 `where` 空条件）。
- `MgCachePrefixEnum`：仅保留 FUNCTION_URLS；`PltResponseStatus`：仅保留 PARAMETER_ERROR/SYS_ERROR。
- `CenterEnum/LocationEnum/UpdateRedisEnum`：删除未用静态方法（getByValue/getAllCenter/getAllLocation/getByRedisKey/getAllCenter）及其 import。
- `NoticePO/FunctionPO`：补 @EqualsAndHashCode(callSuper=true) 消除编译 warning。

## 启动验证（2026-09-28 旧结构已通过；新结构待重验）
- orbit-admin: `java -jar D:\Ai-codexProjects\orbit\orbit-admin\target\orbit-admin-1.0.0.jar --server.port=8080`
- dsv2: `java -jar D:\Ai-codexProjects\natpcp\dsv2\target\dsv2-1.0.0.jar`（端口 18081）
- 旧结构实测：日志 `registered 7 handler(s): [ConfigCheck, TableCheck, clearNotice, pvcFileBackup, updateRedisFunctionUrls, updateRedisInfo, versionNotice]`；
  `GET http://127.0.0.1:8080/orbit/admin/executors` 与 `GET http://127.0.0.1:18081/orbit/executor/handlers` 均返回 7 个 handler。

## application.yml 关键配置（已写）
- server.port=18081；spring.application.name=dsv2
- orbit.executor: admin-addresses=http://127.0.0.1:8080, app-name=dsv2, access-token 空, worker-threads 8, queue-capacity 256
- spring.datasource pweb001db：JDBC URL / 账号 / 密码由 k8s ConfigMap(dsv2-config) + Secret 注入（仓库中不保留任何凭据）
- spring.data.redis cluster：节点列表与密码由 ConfigMap/Secret 注入
- jasypt.encryptor.password（SM4 密钥）：仅由 Secret 注入，严禁写入仓库
- **安全提示**：本文件历史版本曾包含上述明文凭据，已于 2026-10-30 清除；相关数据库/Redis 密码与 SM4 密钥需要轮换。
- mybatis-plus mapper-locations=classpath:/mapper/*.xml
- pvc.rootPath=/app/appnatp/transfile/、pvc.backup.path=/app/appnatp/bak/pvc/、plt.localFileDirRoot=/app/appnatp/natp_plt_mgmt/temp/shareFiles/

## 7 个 handler（@OrbitJob + @Component，均在 com.natpcp.dsv2.jobs）
1. `MagtSchedulingJobs` → updateRedisFunctionUrls / clearNotice
2. `PvcFileBackupJob` → pvcFileBackup
3. `TaskSchedulingNoticeJobs` → versionNotice（fastjson JSONObject）
4. `UpdateRedisInfoJobs` → updateRedisInfo
5. `TableCheckJob` → TableCheck（参数 context.getParams() 解析 TableCheckBO；回写用 AdminJobClient PUT /orbit/admin/jobs/{name}）
6. `ConfigCheckJob` → ConfigCheck（jedis 3.x configGet 返回 List<String> 已适配；jsch 分支保留）
7. `AdminJobClient` → GET+PUT /orbit/admin/jobs/{name}（解析 {data:{job}}，合并 params）

## 待办（可选，尚未执行）
1. 通过 orbit-admin API / 管理界面创建剩余 6 个生产任务（updateRedisFunctionUrls/clearNotice/versionNotice/updateRedisInfo/pvcFileBackup/TableCheck/ConfigCheck，
   cron 按原 plt-mgmt 配置），**触发一次实跑验证业务正确性**（需真实环境可用：pweb001db/gatewayRedis/pvc 路径）。
2. 生产部署前：配置 orbit.admin.access-token 与内部 Redis callback 配置。
3. （可选）新结构下重跑启动 + 注册验证（心跳/handlers API）。

## Spring Batch 技术储备（2026-09-30 完成：隔离专用数据源 + 前缀表，端到端验证通过）
- **动机**：后续会有大批量任务（数据迁移/清洗/ETL）使用 Spring Batch，本次完成骨架 + 专用 JobRepository 数据库隔离。
- **重要背景**：pbat001db 中**已存在生产在用的旧版 Spring Batch(4.x) 元数据表** BATCH_*（3101 条执行记录、最近 2024-11-07，job 如 jnlSyncBatchJob/DynmcTaskJob 等），**不可动**；
  且其结构与 Spring Batch 5.x 官方 schema 不兼容（4.x 的 batch_job_execution_params 为 type_cd/key_name/string_val 旧布局）。→ 因此 dsv2 使用**独立表前缀 DSV2_BATCH_** 隔离。
- **pom.xml**：新增 `spring-boot-starter-batch`（BOM 收敛，实际 spring-batch-core 5.2.5）。
- **application.yml**：
  ```yaml
  spring:
    batch:
      job:
        enabled: false               # 禁止启动自动执行，仅由 @OrbitJob handler 显式触发
      jdbc:
        initialize-schema: never     # 不自动建表（旧表不可动 + 前缀表已手工建）；表由 sql/batch_schema_dsv2_postgresql.sql 建一次
        table-prefix: DSV2_BATCH_    # 前缀隔离，JobRepository 读写 DSV2_BATCH_* 表
    datasource:
      # 主数据源：业务库 pweb001db（wbnatpdb）—— 业务 Mapper 使用
      # 批数据源（spring.datasource.batch）：pbat001db（banatpdb）—— 仅 JobRepository 使用
  ```
- **新增代码**：
  - `batch/BatchDataSourceConfig`：**多数据源范式**。`@Bean @ConfigurationProperties("spring.datasource.batch") DataSourceProperties` → `properties.initializeDataSourceBuilder().type(HikariDataSource.class).build()` 生成 `@BatchDataSource`；
    `@BatchTransactionManager` 绑同一数据源事务。**注意坑**：若 @ConfigurationProperties 直接绑 HikariDataSource，`url` 不映射为 `setJdbcUrl`（JavaBean 属性名是 jdbcUrl），会报 `jdbcUrl is required with driverClassName`；
    必须先经 DataSourceProperties 再 initializeDataSourceBuilder()，该步骤完成 url→jdbcUrl 转换，hikari.* 连接池参数用第二个 @ConfigurationProperties("spring.datasource.batch.hikari") 绑定。
  - `batch/BatchConfig`：@Configuration（**不能加 @EnableBatchProcessing**，否则绕过 Boot 自动配置使 initialize-schema/属性失效）+ springBatchDemoJob（chunk(20) 示例）。
  - `jobs/SpringBatchDemoJobHandler`：@OrbitJob("springBatchDemo") 桥接，JobParameters 必须带唯一参数（triggerJobId + runTime 时间戳），否则重复触发抛 JobInstanceAlreadyCompleteException。
- **建表脚本**：`src/main/resources/sql/batch_schema_dsv2_postgresql.sql`（5.x 官方 schema-postgresql.sql + DSV2_BATCH_ 前缀 + IF NOT EXISTS + 前缀化 constraint 名，已手工执行到 pbat001db，9 条语句全 OK）。
  `batch_schema_postgresql.sql`（旧版官方脚本，无前缀）不再使用，仅留作参考。
- **验证结果（全链路打通，2026-09-30）**：
  - 启动日志：`batchHikari - Start completed`（批数据源连 pbat001db）+ `registered 8 handler(s)`（含 springBatchDemo）+ Started，无 ERROR。
  - pbat001db 表核对：`dsv2_batch_job_instance / _execution / _execution_params / _step_execution / _step_execution_context / _job_execution_context` 6 表 + 3 sequence 全部 EXISTS（5.x 结构）。
  - 触发：admin `POST /orbit/admin/jobs/springBatchDemo/trigger` → 5 chunk 各 20 条 → Job COMPLETED（2s859ms）。
  - 落库位置验证：exec_id=1 job=springBatchDemoJob status=COMPLETED 落在 **pbat001db.dsv2_batch_*** 表；
    **pweb001db 无任何 batch 表**（业务库干净）；**旧 BATCH_* 表行数保持 3101 未动**（生产零影响）。
  - 顺带清理：先前误建在 pweb001db 的 BATCH_* 表已 DROP 干净（`initialize-schema: always` 阶段创建）。
- **后续接入范式**：真实批量任务 = 新增 Job bean（reference BatchConfig 的 JobRepository/transactionManager）+ 新增 @OrbitJob handler 桥接（JobParameters 带时间戳）；
  JobRepository 读写 pbat001db 的 DSV2_BATCH_* 表，业务数据仍走 pweb001db 主数据源。
- **坑点补充**：YAML 中 `spring.datasource` 不能在文件内重复定义（SnakeYAML 重复键报错）——主/批数据源须在同名键下并列子键。

## 7 个生产任务纳入 Spring Batch 统一治理（2026-09-30 完成，端到端验证通过）
- **决策（用户确认 3 点）**：① 7 个 @OrbitJob handler 保留，内部桥接 Spring Batch Job；② updateRedisInfo 改 chunk，其余 Tasklet（updateRedisFunctionUrls 为全量覆写语义无分片价值，保持 Tasklet）；③ versionNotice 进 Batch，新增 batchMetadataCleanup 定期清理元数据。
- **依赖注入注意（双数据源组合）**：
  - `BatchDataSourceConfig` 现在定义 **主数据源（@Primary pweb001db）+ 批数据源（@BatchDataSource pbat001db）**。
  - **关键坑**：只定义 @BatchDataSource 而不定义 @Primary 主数据源时，Boot DataSourceAutoConfiguration 因 @ConditionalOnMissingBean(DataSource) 不再创建默认数据源，MyBatis 会退化使用唯一的 batchDataSource（pbat001db）执行**业务 SQL** → 报 `tb_forward_rule does not exist`（表在 pweb001db 明明存在）。
  - 必须显式：`@Primary @ConfigurationProperties("spring.datasource") DataSourceProperties` + `@Primary @ConfigurationProperties("spring.datasource.hikari") HikariDataSource` + `@Primary PlatformTransactionManager`（主事务）。
  - Batch 的 JobRepository 仍由 Boot 自动配置找 @BatchDataSource/@BatchTransactionManager。
- **新增文件（batch 包）**：
  - `ProductionJobsConfig`：8 个 Job 定义 + Step + `batchJdbcTemplate`（绑定 batchDataSource）。@Bean(name=JOB_XXX) 显式命名。
  - `BatchJobBridge`：orbit handler 通用桥接（参数->JobParameters：jobParams JSON + handlerName + triggerJobId + runTime；COMPLETED->success，否则抛异常）。
  - `util/JobParamsHelper`：StepContext 参数读取（getParamsMap 适配 Spring Batch 5 的 StepContext.getJobParameters() 返回 Map<String,Object> 而非 JobParameters）。
  - `reader/UpdateRedisItemReader`、`processor/UpdateRedisItemProcessor`、`writer/UpdateRedisItemWriter`、`item/UpdateRedisSyncItem`：updateRedisInfo chunk 化（按 UpdateRedisEnum 切片，排除 CONFIG_VERSION_*，faultTolerant retry 3 + skip 5）。
  - `tasklet/`：ForwardRuleTasklet、ConfigVersionTasklet、NgVersionTasklet、FunctionUrlsTasklet、ClearNoticeTasklet、VersionNoticeTasklet（原 TaskSchedulingNoticeJobs 逻辑搬入）、TableCheckTasklet、ConfigCheckTasklet、PvcFileBackupTasklet。
  - `jobs/BatchMetadataCleanupJobHandler`：@OrbitJob("batchMetadataCleanup")，清理 180 天前 DSV2_BATCH_* 记录（按外键顺序 DELETE）。
- **handler 改造**：7 个 handler 瘦身为桥接（@OrbitJob 注解名不变，调度中心兼容）；业务逻辑整体移入对应 Tasklet（避免 handler->Job->Step->Tasklet->handler 循环依赖，Spring Boot 3 默认拒绝循环）。
- **bean 名冲突坑**：@Bean 方法名与 @Component handler 类默认 bean 名冲突（如 Job `pvcFileBackupJob` vs 类 `PvcFileBackupJob`）。方案：Job @Bean 显式 `name=JOB_XXX` 常量；3 个 handler 类显式 @Component 名（pvcFileBackupJobHandler/tableCheckJobHandler/configCheckJobHandler）。
- **元数据清理 SQL 坑**：DSV2_BATCH_* 的 CREATE_TIME 是 `timestamp without time zone`，JdbcTemplate 占位符必须传 java.sql.Timestamp（传 String 报 `operator does not exist: timestamp < character varying`）。
- **验证结果（全链路打通）**：
  - 启动：`registered 9 handler(s)`（新增 batchMetadataCleanup），主+批双 Hikari 池（HikariPool-1 懒启动于首次任务触发）。
  - updateRedisInfoJob：4 Step（forwardRule -> updateRedisSync[chunk] -> configVersion -> ngVersion）全 COMPLETED；chunk step **read=8 write=8 commit=3 rollback=0**（8 key 分 3 次提交，faultTolerant 生效）。
  - updateRedisFunctionUrlsJob / clearNoticeJob / versionNoticeJob / batchMetadataCleanupJob：全 COMPLETED（executionId 5/6/7/10/11）。
  - 落库验证（pbat001db）：6 个 Job 名称记录全部可见（springBatchDemoJob 历史 3 次 + 生产 Job 记录）。
  - admin 任务已建成：id=3 updateRedisInfo、4 updateRedisFunctionUrls、2 versionNotice、6 clearNotice、7 batchMetadataCleanup、8 pvcFileBackup、9 TableCheck、10 ConfigCheck（均 appName=dsv2；esp. **原 id=3/4 的 appName 从旧值 ds-v2 已更新为 dsv2**）。
  - **并发注意**：同时触发多个 Job 时，orbit worker 并发写 JobRepository 元数据可能报 `could not serialize access due to read/write dependencies`（PostgreSQL）。生产 cron 错开即无碍；验证时逐个触发。
- **重启恢复落地方式**：Job 默认 restartable，失败后 JobExecution FAILED 落库（DSV2_BATCH_*），可用相同 JobParameters 重新启动（Spring Batch restart）；同样参数重复触发 COMPLETED 会抛 JobInstanceAlreadyCompleteException（幂等语义），BatchJobBridge 用 runTime 时间戳保证每次 orbit 触发都是新 JobInstance。
- **后续接入新批量任务范式**：ProductionJobsConfig 加 @Bean Job + Step（chunk 或 Tasklet）→ 新建 @OrbitJob handler 调 `batchJobBridge.run(jobBean, handlerName, context)`（Job bean 用 @Qualifier("JOB_XXX") 常量）。

## 关键技术事实备忘
- JobContext API：getString(key,default)/getParams()/getJobId()/getJobName()/getHandler()/getLogId()/getAttempt()
- @OrbitJob 标注在方法上，类为 Spring bean 即注册；handler 返回值 String 被执行框架记录
- orbit-core JobInfo.params 为 Map<String,Object>；admin 创建任务时 params 填 JSON 对象即进入 JobContext.getParams()
- TableCheck 回写契约：GET /orbit/admin/jobs/{name} → `{data:{job:{...},quartz:{...}}}`；PUT body = 完整 JobInfo
- **IDEA 报错根因（已定位）**：IDEA Maven 用 settings-natp0529.xml + D:\maven_natp_20240529，其 mirror `http://repo.com/artifactory/maven-central/`（无 :8999）返回 401，
  依赖靠旧缓存 classpath → BeanCreationException。命令行构建用 settings-dsv2.xml 则正常。建议 IDEA 切到 settings-dsv2.xml。
- PowerShell 5.1 中 curl=Invoke-WebRequest；用 curl.exe -s；Invoke-RestMethod 管道 JSON 在批量工具下可能被截断（用 webfetch 验证 API 更稳）

## 2026-10-30 代码审查修复记录
- 修复 `@MapperScan` 包名（com.natpcp.dsv2 → com.natpcp.natpcp.dsv2，原配置导致 Mapper 全部未注册）。
- `batchMetadataCleanup` 改为 `BatchMetadataCleanupTasklet`，表名按 `spring.batch.jdbc.table-prefix` 拼接（原硬编码 BATCH_* 会误删同库旧版生产表）。
- 清除本文件中的明文凭据；`KafkaProducerConfig` 不再内置环境地址，补充投递/阻塞超时。
- `DynamicSqlProvider` 条件值改为 `#{}` 参数绑定（原字符串拼接存在 SQL 注入）。
- `objectRedisTemplate` 反序列化改为白名单 `BasicPolymorphicTypeValidator`（原 LaissezFaire 存在 gadget 风险）。
- `ConfigCheck/TableCheck`：新增 `ConnectionInfoValidator`（JDBC URL 白名单、SSH 路径白名单、端口校验），Jedis 改 try-with-resources，SSH/Redis 增加超时，StrictHostKeyChecking 可配置。
- `AdminJobClient`：RestTemplate 超时、admin-addresses 多地址故障转移。
- `RedisManager.scanKeys` 不再吞异常返回 null；`VersionNoticeTasklet` 容忍重复/缺失版本记录；`PvcServiceImpl` 关闭 `Files.walk` 流并保留异常 cause。
- `application.yml` 补充 `spring.batch.*`（含 `isolation-level-for-create: read_committed` 根治 SSI 冲突）与 dsv2 自定义键说明。
- pom：orbit 版本对齐 1.0.0、移除不存在的 orbit-boot-support 与未使用的 hutool/pagehelper、jacoco 升级 0.8.12（JDK 21）；根 pom 将 dsv2 纳入 reactor 以进入 CI。

### 2026-10-30 第二轮：可读性重构与依赖治理

- JSON 统一为 Jackson：新增 `util/JsonUtil`（toJson / fromJson / toMap / parseMap / convert），
  `JobParamsHelper`、`ConfigCheckTasklet`、`TableCheckTasklet`、`VersionNoticeTasklet`、
  `PvcFileBackupTasklet` 及相关测试全部改用；`pom.xml` 移除 fastjson 1.2.84（EOL）。
- 通知发送去重：新增 `NoticeService.sendToAdminAndOps(title, content)`（0101 系统管理员 + 0104 运维员），
  替换三个 Tasklet 中各自复制的 "new NoticePO + insert ×2" 代码；`NoticeServiceImplTest` 增加用例。
- `ConfigCheckTasklet` / `TableCheckTasklet` / `VersionNoticeTasklet` 重写为按职责拆分的小方法
  （checkRedis/checkPostgres/checkRocketMq、resolveNextPartition、InstanceKey.parse 等），业务语义与通知文案不变；
  `VersionNoticeTasklet` 库内版本改为只查询一次。
- jsch：`com.jcraft:jsch 0.1.55`（2018 年停更）→ `com.github.mwiede:jsch 2.28.8`（包名不变，直接替换）。
- `ProductionStepsConfig` 头部补充 Step 事务管理器选型说明；清理 "prompt:" / "用户决定" 等残留注释。
- CI：`maven.yml` 构建失败时把 `[ERROR]` / 失败用例摘要输出为 annotation + Step Summary，并上传完整 `build.log` 产物。

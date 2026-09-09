# Orbit Batch Scheduler

轻量级 **云原生批量调度框架**，模型对齐 [XXL-JOB](https://www.xuxueli.com/xxl-job/)：

| 组件 | 说明 |
|------|------|
| **调度中心** `orbit-admin` | 任务 CRUD、Cron 触发、执行器注册、路由派发、执行日志 |
| **执行器** `orbit-executor` | 业务应用引入 SDK，`@OrbitJob` 注册 Handler，接收调度中心 HTTP 触发 |

```
┌─────────────┐  Cron/手动   ┌──────────────────┐  HTTP /run   ┌─────────────────┐
│  运维 / API  │────────────►│   orbit-admin    │─────────────►│  orbit-executor │
└─────────────┘              │  调度中心         │              │  业务 Pod × N   │
                             │  · 任务存储       │◄──heartbeat──│  @OrbitJob      │
                             │  · 执行器注册表   │              └─────────────────┘
                             └──────────────────┘
```

- **JDK 拓扑**：`orbit-admin` 调度中心需 **JDK 11+**（Quartz 2.5.x 起最低要求 JDK 11）；`orbit-executor`/`orbit-core` SDK 保持 **Java 8 字节码**，业务方可在 **JRE 8 + Spring Boot 2.7** 接入。admin 与 executor 仅通过 HTTP/JSON 通信、不共享 JVM，两者 JDK 大版本可不一致
- Spring Boot 2.7.18（JDK 8~19 均兼容）· Quartz 2.5.2（调度中心，JDK 11）
- 持久层：**Druid 1.2.8** 连接池 + **MyBatis 3.5.19** + **MyBatis-Plus 3.5.7**（仅 `orbit-admin` 使用；`orbit-core`/`orbit-executor` 不依赖 ORM）
- 执行器无状态，K8s 直接扩缩容；心跳超时自动摘除
- 默认 H2 文件库开箱即用（PostgreSQL 兼容模式），生产支持 PostgreSQL / GaussDB

---

## 1. 模块

```
orbit-batch-scheduler
├── orbit-core              # 共享协议（注册/触发/日志模型）
├── orbit-admin             # 调度中心（可独立部署）
├── orbit-executor          # 执行器 SDK（业务方依赖）
├── orbit-executor-sample   # 执行器示例
└── deploy/                 # docker-compose / k8s / SQL
```

### 依赖与版本管理

根 `pom.xml` **只是聚合器（aggregator）**，不作为任何模块的 `<parent>`，也不下发 `properties` /
`dependencyManagement` / 插件配置。每个模块 POM 自治：

- 各模块自行 `import` **`spring-boot-dependencies:2.7.18` BOM** 收敛 Spring / Jackson / JUnit / H2 等版本；
- 各模块自行声明编译级别、插件版本（compiler / surefire / spring-boot-maven-plugin 均显式带 `<version>`）；
- 模块间引用统一用 `${project.version}`，保证同版本号发布；
- 任一模块都可脱离本仓库单独构建（`cd orbit-core && mvn install`），聚合构建（reactor）也照常可用。

> ⚠️ 注意：BOM 以 `import` 方式引入时，**无法**像继承 `spring-boot-starter-parent` 那样用 `<properties>`
> （如 `quartz.version`）覆盖版本。因此 `orbit-admin` 在自己的 `dependencyManagement` 中**显式锁定**
> Quartz 2.5.2（Boot 2.7 BOM 内置 2.3.2）、MyBatis 3.5.19、MyBatis-Plus 3.5.7、Druid 1.2.8。

---

## 2. 本地 5 分钟跑通

```bash
# 构建（整个 reactor 需用 JDK 11+ 构建：admin 模块编译级别 11；
#       executor/core/sample 编译级别 8，产物为 Java 8 字节码，可运行在 JRE 8）
mvn clean package -DskipTests

# 若本机只有 JDK 8：admin 无法编译（Quartz 2.5 class 版本 55），只构建 SDK 模块即可
mvn clean package -DskipTests -pl orbit-core,orbit-executor,orbit-executor-sample

# 终端 1：调度中心 :8080（需 JRE 11+）
java -jar orbit-admin/target/orbit-admin-1.0.0.jar

# 终端 2：执行器 :8081（JRE 8/11 均可）
java -jar orbit-executor-sample/target/orbit-executor-sample-1.0.0.jar
```

执行器启动后会向 admin 注册 `demo-executor`。创建任务并触发：

```bash
# 查看在线执行器
curl http://localhost:8080/orbit/admin/executors

# 创建定时任务（handler = @OrbitJob 名称）
curl -X POST http://localhost:8080/orbit/admin/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "jobName": "dailyReportJob",
    "description": "日报",
    "appName": "demo-executor",
    "handler": "dailyReport",
    "cron": "0 */2 * * * ?",
    "params": {"bizDate": "yesterday"},
    "routeStrategy": "ROUND",
    "timeoutSeconds": 120,
    "enabled": true
  }'

# 立即触发一次（返回受理回执 accepted=true，不是执行结果）
curl -X POST http://localhost:8080/orbit/admin/jobs/dailyReportJob/trigger

# 查日志（执行器跑完后回传，日志才从 RUNNING 变成 SUCCESS/FAILED）
curl 'http://localhost:8080/orbit/admin/logs?jobName=dailyReportJob'
```

---

## 3. 业务应用接入执行器

```xml
<dependency>
  <groupId>com.orbit</groupId>
  <artifactId>orbit-executor</artifactId>
  <version>1.0.0</version>
</dependency>
```

```yaml
server:
  port: 8081
orbit:
  executor:
    app-name: order-service          # 与任务 appName 一致
    admin-addresses: http://orbit-admin:8080
    # address 留空：本地用本机 IP，K8s 用 POD_IP
    # port 无需配置，默认自动感知并继承应用自身的 server.port
    worker-threads: 8                # 单节点并发上限 + 超时强制中断（0 = 内联模式）
    queue-capacity: 256              # 排队上限，满则快速失败
    access-token: ""                 # 与 admin 一致时可开启
```

```java
@Component
public class OrderJobs {
    @OrbitJob("settleOrders")
    public String settle(JobContext ctx) {
        // 业务逻辑
        String bizDate = ctx.getString("bizDate");
        int batch = ctx.getInt("batch", 100);   // 类型化取参（getInt/getLong/getBoolean/getDouble）
        return "ok";
    }
}
```

方法签名：无参 / `JobContext` / `Map`。

> **任务执行线程池**：执行器内置有界工作线程池（默认 8 线程 + 256 排队，
> `orbit.executor.worker-threads` / `queue-capacity` 可调）：
> 限制单节点并发执行数、队列满时快速失败返回 `executor saturated`；
> 并按任务 `timeoutSeconds` **超时强制中断**（`interrupted=true`），
> 彻底消除「调度中心 HTTP 读超时放弃后，执行器任务永久僵尸运行」的问题。
> 设 `worker-threads: 0` 切换为内联模式：任务在请求线程内执行，无超时强制。

> 执行器 SDK（`orbit-executor` + `orbit-core`）以 **Java 8 字节码**发布，业务应用运行在 **JRE 8 及以上 + Spring Boot 2.7** 即可接入，
> 与调度中心使用 JDK 11 互不影响（两端仅经 HTTP/JSON 交互）。调度中心的 Quartz 依赖不会传递到业务侧。

---

## 4. 调度中心 API

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/orbit/admin/registry` | 执行器注册/心跳 |
| POST | `/orbit/admin/registry/remove` | 执行器下线 |
| GET | `/orbit/admin/jobs` | 任务分页（`page`/`size`/`nameLike`） |
| GET | `/orbit/admin/jobs/{name}` | 任务详情（含 Quartz 运行状态） |
| POST | `/orbit/admin/jobs` | 创建任务 |
| PUT | `/orbit/admin/jobs/{name}` | 更新 |
| DELETE | `/orbit/admin/jobs/{name}` | 删除 |
| POST | `/orbit/admin/jobs/{name}/pause` | 暂停（持久化 `enabled=false`，重启后保持） |
| POST | `/orbit/admin/jobs/{name}/resume` | 恢复（持久化 `enabled=true`） |
| POST | `/orbit/admin/jobs/{name}/trigger` | 立即触发一次（body 可传本次临时参数 JSON）。返回**受理回执**而非执行结果，结果查 `/logs` |
| GET | `/orbit/admin/logs` | 执行日志分页（`jobName`/`page`/`size`） |
| GET | `/orbit/admin/executors` | 在线执行器（`appName` 可选过滤） |
| POST | `/orbit/admin/callback` | 执行器**批量**回传执行结果（执行器调用；body 为结果数组，逐条按 `logId` 幂等收敛，响应返回实际收敛条数） |
| GET | `/orbit/admin/overview` | 总览（含触发通道指标：`dispatchActive` / `dispatchQueueSize` / `dispatchRejected` / `dispatchSkipped` / `dispatchOutstanding`） |
| POST | `/orbit/executor/run` | 执行器：受理调度触发（调度中心调用）。入队即返回 `accepted=true`，不等待任务执行 |
| GET | `/orbit/executor/handlers` | 执行器：查询本节点注册的 Handler 列表 |

任务字段：

| 字段 | 说明 | 长度上限（与建表脚本一致，超出返回 400） |
|------|------|------|
| `jobName` | 唯一名，需匹配 `[A-Za-z0-9_-.]{1,64}` | 64 |
| `appName` | 执行器应用名 | 64 |
| `handler` | `@OrbitJob` 名 | 128 |
| `cron` | Quartz Cron，空=仅手动 | 64 |
| `params` | JSON 参数（序列化后的 JSON 长度受限） | 2000 |
| `routeStrategy` | `ROUND` / `RANDOM` / `FIRST` | 16 |
| `timeoutSeconds` | 读超时（≤0 回落 300，超过 `max-timeout-seconds` 封顶） | — |
| `enabled` | 是否调度 | — |
| `description` | 任务描述 | 256 |

> 这些上限在**入参校验阶段**就检查，超限返回 400 并带上字段名；
> 若等到入库才失败，只会被全局异常处理器压成一句没有信息的 `internal error`。

> **创建/更新的原子性**：任务定义落库与 Quartz 编排是两步。若 Quartz 编排失败，
> 创建会回滚刚写入的任务行、更新会回滚到修改前的定义，
> 保证接口失败时数据库里既不会留下「看得见却永不触发」的幽灵任务，
> 也不会出现「接口说已保存、Quartz 仍按旧 cron 跑」的不一致。

---

### 4.1 触发-回传异步模型

触发与执行是解耦的，一次调度的完整链路：

```
Cron 到点 / 手动触发
      │
      ▼
[admin] 写 RUNNING 日志（生成 logId）
      │  POST /orbit/executor/run（读超时 trigger-timeout-seconds，默认 10s）
      ▼
[executor] 入队 → 立即回 accepted=true        ← 调度中心到此为止，日志保持 RUNNING
      │
      ▼  工作线程执行 @OrbitJob（到期由看门狗 cancel(true) 中断）
      │
      │  POST /orbit/admin/callback（一次最多 200 条；失败按 callback-retry-* 退避重试，重试耗尽整批退回队列）
      ▼
[admin] 逐条按 logId 把 RUNNING 收敛为 SUCCESS / FAILED
```

几个由此而来的性质：

- **调度中心为一次触发最多占用 `trigger-timeout-seconds`（默认 10 秒）**，与任务真实耗时无关。
  跑一天的任务也不会占住 Quartz 工作线程或触发线程；
- **日志有两段生命**：触发时写入 RUNNING，回传到达时才写终态。
  `orbit_job_log` 里 `status='RUNNING'` 表示「已触发、结果未回传」，不代表卡死；
- **回传是批量的**：执行器把结果先压进有界队列，发送线程一次取一条、再把队列里已积压的一起打包
  （上限 200 条）成一个请求。调度中心短暂不可用后恢复时，积压的结果一次补发完，不必逐条重连；
  单批失败按 `callback-retry-*` 退避重试，重试耗尽则整批退回队列而不是丢弃，靠队列容量做背压；
- **回传幂等**：`finishLogFromRunning` 的 `WHERE` 带 `status='RUNNING'`，
  所以执行器重试、重复回传、以及与孤儿回收的竞态都不会覆盖已写入的真实结果。
  重复回传同样返回成功，避免执行器把「已处理过」误判为失败而无限重试；
- **兜底**：执行器崩溃或回传彻底丢失时，日志由后台的僵尸回收
  （`log-reap-interval-ms`，阈值 = `max-timeout-seconds` + 5 分钟宽限）判为 FAILED。

> **升级注意（破坏性变更）**：`/orbit/executor/run` 的响应语义从「执行结果」变成了「受理回执」。
> 旧版执行器对新版调度中心会返回 `accepted=false` 的结果对象，被当成触发失败；
> 新版执行器对旧版调度中心则会让日志永远停在 RUNNING。**调度中心与执行器必须同版本升级。**
>
> 同批变更：令牌只走 `X-Orbit-Token` 请求头（调度中心侧也接受 `Authorization: Bearer`），
> 请求体里的 `accessToken` 字段已移除；`/orbit/admin/callback` 的请求体从单个对象变成数组。

---

## 5. 云原生部署

**默认（单副本）**：调度中心使用内存 JobStore（`job-store-type: memory`）+ H2 文件库，开箱即用。
此时**必须保持单副本** —— 多个副本各自持有一份内存 Quartz 调度器，
同一个 Cron 到点会被每个副本各触发一次，任务实际执行 N 次。执行器注册表已落库，与副本数无关。

**路由方式**：执行器心跳上报 `http://{POD_IP}:port`，调度中心直连 Pod IP 触发（与 XXL-JOB 一致），天然适配多副本。

**执行器注册（对齐 XXL-JOB）**：心跳写入共享表 `orbit_executor_registry`，任意 admin 副本都能读到全量在线节点。因此调度中心是**无状态 Deployment**，执行器只需 `admin-addresses: http://orbit-admin:8080`（普通 ClusterIP Service），**不需要** StatefulSet / Headless DNS 逐副本上报。

### 5.1 调度中心多副本（集群模式）

需要高可用 / 水平扩容时启用 Quartz JDBC JobStore 集群，由 `QRTZ_LOCKS` 行锁保证
**同一个 trigger 只被一个副本触发**（`SELECT * FROM QRTZ_LOCKS ... FOR UPDATE`），并附带故障自动接管。

四项前置条件，缺一不可：

| # | 条件 | 说明 |
|---|---|---|
| 1 | 建 11 张 `QRTZ_*` 表 | 执行 `deploy/sql/quartz-postgresql.sql`（PostgreSQL）或 `deploy/sql/quartz-gaussdb.sql`（openGauss / GaussDB）。脚本取自 Quartz v2.5.2 官方 DDL，`initialize-schema` 设为 `never`，Spring Boot 不会自动建表 |
| 2 | 真实数据库 | 默认的 H2 文件库无法跨副本共享，K8s 里还挂 `emptyDir`（Pod 重建即丢数据）。换 PostgreSQL / GaussDB |
| 3 | 启用 cluster profile | `--spring.profiles.active=cluster`（或环境变量 `SPRING_PROFILES_ACTIVE=cluster`），配置见 `orbit-admin/src/main/resources/application-cluster.yml` |
| 4 | 时钟 NTP 同步 | Quartz 集群靠 `QRTZ_SCHEDULER_STATE` 的时间戳判断副本存活，时钟漂移会误判失联并触发重复的故障恢复 |

```bash
# 1. 建 Quartz 表（11 张 QRTZ_*）
psql -U postgres -d orbit_admin -f deploy/sql/quartz-postgresql.sql
# 2. 建业务表（orbit_job / orbit_job_log）—— 或交给 spring.sql.init 自动执行
psql -U postgres -d orbit_admin -f deploy/sql/schema-postgresql.sql
# 3. 启动（可多副本）
java -jar orbit-admin.jar --spring.profiles.active=cluster
```

GaussDB 请把 `ORBIT_DB_URL` / `ORBIT_DB_DRIVER` 换成 openGauss 驱动，并将
`ORBIT_QUARTZ_DELEGATE` 设为 `org.quartz.impl.jdbcjobstore.GaussDBDelegate`
（M / Oracle 兼容模式则用 `StdJDBCDelegate`，DDL 改用官方 `tables_gauss_m_compatibility.sql`）。

> **无需额外引入连接池**：Spring Boot 在 `job-store-type=jdbc` 时会调用
> `schedulerFactoryBean.setDataSource(...)`，让 Quartz 直接复用 Spring 管理的 Druid 连接池，
> 不走 Quartz 自己的 `org.quartz.dataSource.*`，因此不需要 c3p0 / HikariCP。

> 执行器心跳落库，admin 多副本无需 StatefulSet。心跳打到任意副本即可。
> 多副本**调度**仍依赖 Quartz JDBC 集群（上面 1~4），与注册表无关。

```bash
# 镜像（构建阶段统一用 JDK 11；运行阶段 admin=JRE 11，executor=JRE 8）
docker build -t orbit-admin:1.0.0 \
  --build-arg MODULE=orbit-admin \
  --build-arg RUNTIME_IMAGE=eclipse-temurin:11-jre .
docker build -t orbit-executor-sample:1.0.0 \
  --build-arg MODULE=orbit-executor-sample .
# k8s 部署 orbit-admin 时请使用基于 JRE 11+ 的镜像（Quartz 2.5 要求）。

# 本地 compose
docker compose -f deploy/docker-compose.yml up -d

# K8s
kubectl apply -f deploy/k8s/
kubectl -n orbit-system scale deploy/orbit-executor --replicas=3
```

生产库支持 PostgreSQL / GaussDB（脚本见 `deploy/sql/`），配置示例：

**PostgreSQL：**

```yaml
spring:
  datasource:
    url: jdbc:postgresql://localhost:5432/orbit_admin
    driver-class-name: org.postgresql.Driver
    username: postgres
    password: your_password
  sql:
    init:
      mode: never   # 表已手工执行 deploy/sql/schema-postgresql.sql 初始化
```

**GaussDB / openGauss：**

```yaml
spring:
  datasource:
    url: jdbc:opengauss://localhost:5432/orbit_admin
    driver-class-name: org.opengauss.Driver
    username: gaussdb
    password: your_password
  sql:
    init:
      mode: never   # 表已手工执行 deploy/sql/schema-gaussdb.sql 初始化
```

**连接池 / ORM（`orbit-admin`）：**

- 连接池统一使用 **Druid**（`druid-spring-boot-starter`），连接池参数在 `spring.datasource.druid.*` 下配置
  （`initial-size` / `max-active` / `min-idle` / `max-wait` / 校验 `validation-query: SELECT 1` 等）。
- ORM 使用 **MyBatis 3.5.19 + MyBatis-Plus 3.5.7**：Mapper 位于 `com.orbit.admin.store.mapper`，
  实体（PO）位于 `com.orbit.admin.store.po`；分页用 `PaginationInnerInterceptor`（PostgreSQL 方言），
  并发更新用 `@Version` 乐观锁。`mybatis-plus.configuration.map-underscore-to-camel-case` 默认开启。
- `orbit-core` 只含通信协议 POJO，不依赖任何 ORM；PO ↔ 协议模型的转换在 `JobStore` 内完成。

---

## 6. 配置参考

**调度中心 `orbit.admin.*`**

| 项 | 默认 | 说明 |
|----|------|------|
| `access-token` | 空 | 非空时开启鉴权，与执行器双向校验；令牌取自 `X-Orbit-Token` 或 `Authorization: Bearer`，比对采用常量时间算法防时序侧信道 |
| `heartbeat-timeout-seconds` | 90 | 超时摘除执行器 |
| `evict-interval-ms` | 30000 | 后台扫描摘除失联节点的频率 |
| `timezone` | Asia/Shanghai | Cron 时区（非法值启动即失败） |
| `group` | ORBIT | Quartz Job/Trigger 分组名 |
| `connect-timeout-ms` | 3000 | 调执行器连接超时 |
| `max-timeout-seconds` | 3600 | 单任务执行超时上限：随触发下发给执行器做超时强制，同时是僵尸 RUNNING 回收阈值基准 |
| `trigger-timeout-seconds` | 10 | 触发请求的 HTTP 读超时。触发是「受理即返回」，只需覆盖网络往返与入队，不随 `max-timeout-seconds` 放大 |
| `registry-cache-ttl-ms` | 3000 | 注册表本地缓存 TTL：调度热路径免查库；写操作立即失效；0 = 关闭 |
| `log-retention-days` | 30 | 执行日志保留天数：后台分批删除更早日志；0 = 关闭 |
| `log-reap-interval-ms` | 60000 | 僵尸 RUNNING 日志回收频率（阈值 = max-timeout + 5 分钟宽限） |
| `log-cleanup-interval-ms` | 3600000 | 日志保留期清理频率 |
| `dispatch-threads` | 64 | 定时触发线程数。线程只在一次触发往返期间被占用，可远大于 `org.quartz.threadPool.threadCount`，两者独立 |
| `dispatch-queue-capacity` | 256 | 触发排队上限：满则新触发快速失败并写一条 FAILED 日志（`scheduler saturated`）；`0` = 不排队 |
| `dispatch-serial-per-job` | true | 同名任务串行：上一轮**执行结果未回传**时本次到点跳过（只计数，见 `/overview` 的 `dispatchSkipped`） |

> **注册表缓存说明**：TTL（默认 3s）远小于心跳超时（90s），多副本间写传播延迟上界即
> TTL；本进程写操作（注册/摘除/剔除）立即失效缓存；派发命中已下线节点由既有
> failover（不可达即摘除换节点）兑底。XXL-JOB 调度中心为纯内存注册表 + 30s DB
> 拉取，本实现 3s TTL 远比其新鲜。

**执行器 `orbit.executor.*`**

| 项 | 默认 | 说明 |
|----|------|------|
| `enabled` | true | 设为 false 关闭执行器自动装配 |
| `app-name` | orbit-executor | 应用名 |
| `admin-addresses` | http://127.0.0.1:8080 | 多地址逗号分隔 |
| `address` | 空 | 对外地址；空则 POD_IP/本机 IP + 端口 |
| `port` | 0（自动感知） | 默认自动继承 `server.port`，无需配置；仅端口映射需覆盖时指定 |
| `node-id` | 空 | 节点唯一标识；空则取 `POD_NAME`/主机名 |
| `heartbeat-interval-ms` | 20000 | 心跳间隔（保底不低于 5000） |
| `worker-threads` | 8 | 任务工作线程数：单节点并发上限 + 超时强制中断。下限为 1（误配 0/负数按 1 处理） |
| `queue-capacity` | 256 | 任务排队队列容量：满则新触发快速失败（executor saturated）；`0` = 不排队（超出 `worker-threads` 直接快速失败） |
| `max-job-wait-seconds` | 86400 | 请求未带 `timeoutSeconds` 时的兜底等待秒数（实际等待有 1 秒下限，误配 0/负数不会让任务瞬间「超时」） |
| `access-token` | 空 | 令牌，随 `X-Orbit-Token` 请求头发出（双向常量时间比对） |
| `callback-retry-times` | 3 | 结果回传失败的重试次数（不含首次）。回传失败会让日志停在 RUNNING 直到被回收 |
| `callback-retry-interval-ms` | 2000 | 回传重试的退避间隔 |
| `callback-queue-capacity` | 1000 | 待回传队列容量：满则丢弃最旧一条并打 ERROR |

---

## 7. 与 XXL-JOB 对照

| | XXL-JOB | Orbit |
|--|---------|-------|
| 调度中心 | xxl-job-admin | orbit-admin |
| 执行器 | xxl-job-core | orbit-executor |
| 任务注解 | `@XxlJob` | `@OrbitJob` |
| 注册 | 心跳写入 `xxl_job_registry`（MySQL） | 心跳写入 `orbit_executor_registry`（共享库，无状态 Deployment） |
| 触发 | HTTP，`ExecutorBizClient` 硬编码 3 秒超时 | HTTP `/orbit/executor/run`，超时 `trigger-timeout-seconds`（默认 10 秒） |
| 结果回传 | 执行器 `TriggerCallbackThread` 批量回调，失败落盘 `callbacklog/` 文件由重试线程补发 | 执行器 `CallbackClient`：有界队列 + 单请求最多 200 条 + 退避重试；重试耗尽整批退回队列（仅队列满才丢弃并打 ERROR），不落盘 |
| 阻塞策略 | 执行器侧 `SERIAL_EXECUTION` / `DISCARD_LATER` / `COVER_EARLY`，每 job 一条 `JobThread` + 无界队列 | 调度中心侧 `dispatch-serial-per-job` 串行；执行器侧共享有界线程池 + 有界队列（满则同步快速失败） |
| 触发线程池 | fast/slow 双池，1 分钟内慢触发 >10 次改投慢池 | 单一有界触发池 + `/overview` 水位指标 |
| 路由 | 轮询/随机/故障转移… | ROUND / RANDOM / FIRST（非法值创建/更新时拒绝） |
| 超时 | — | 双端对齐：admin 读超时 + 执行器强制中断（消除僵尸任务） |
| 存储 | MySQL | H2（默认）/ PostgreSQL / GaussDB |
| ORM/连接池 | — | MyBatis 3.5.19 + MyBatis-Plus 3.5.7 + Druid 1.2.8 |

设计刻意保持精简：无独立 Web 控制台 UI（用 REST API / 自行对接前端）、无 GLUE 模式、无子任务 DAG，满足「中心调度 + 业务侧执行」的云原生批量场景即可扩展。

---

## 8. 已知限制

刻意列出来，避免踩坑后才回头翻代码：

| # | 限制 | 影响与缓解 |
|---|------|-----------|
| 1 | **触发线程数仍是每副本的预算**：Quartz 工作线程微秒级返回，但每次触发仍要占用一条 `dispatch-threads` 线程，最长 `trigger-timeout-seconds` | 与任务耗时无关，默认 64 条 × 10 秒足以支撑很高的触发频率。真要打满说明执行器普遍响应慢，`/overview` 的 `dispatchActive` / `dispatchQueueSize` / `dispatchRejected` 可直接观测 |
| 2 | **手动触发不再返回执行结果**：`POST /jobs/{name}/trigger` 只返回受理回执 | 需要结果请轮询 `/orbit/admin/logs?jobName=...`，或对接 `/callback` 之后的日志 |
| 2b | **回传丢失会把成功的任务记成失败**：执行器跑完但回传重试耗尽（或执行器在完成与回传之间崩溃）时，日志会被孤儿回收判为 FAILED | 业务侧要保证幂等；`callback-retry-times` 调大、并保证执行器能访问 `admin-addresses` 中的至少一个地址 |
| 3 | **注册地址校验不解析 DNS**（见 `ExecutorAddressValidator` 类注释） | 攻击者仍可能用一个解析到 `169.254.169.254` 的域名绕过保留地址拦截。需要彻底封堵 SSRF 请配置 `orbit.admin.executor-address-allow-pattern` 白名单 |
| 4 | **执行器 SDK 面向 Spring Boot 2.7 + Servlet**：自动装配走 `META-INF/spring.factories`（Spring Boot 3 已不再读取该文件），`/orbit/executor/run` 也只在 Servlet Web 环境装配 | Spring Boot 3（`jakarta.*`）与 WebFlux 应用暂不能直接接入 |
| 5 | **同名任务串行守卫是进程内的**：`dispatch-serial-per-job` 的在途登记簿只在本副本内存里，且调度中心重启即清空 | 多副本部署时两个副本可能同时跑同一个任务；调度中心重启后、旧一轮结果回传前也可能重叠。跨副本不重叠依赖 Quartz 集群行锁（同一 trigger 只被一个副本触发），但它不保证「上一轮已结束」。任务不幂等时请自行加分布式锁 |
| 6 | **默认单副本内存 JobStore** | 多副本必须启用 cluster profile + 真实数据库，否则同一个 Cron 会被每个副本各触发一次（见第 5 节） |

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

# 立即执行一次
curl -X POST http://localhost:8080/orbit/admin/jobs/dailyReportJob/trigger

# 查日志
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
    access-token: ""                 # 与 admin 一致时可开启
```

```java
@Component
public class OrderJobs {
    @OrbitJob("settleOrders")
    public String settle(JobContext ctx) {
        // 业务逻辑
        return "ok";
    }
}
```

方法签名：无参 / `JobContext` / `Map`。

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
| POST | `/orbit/admin/jobs/{name}/trigger` | 立即执行（body 可传本次临时参数 JSON） |
| GET | `/orbit/admin/logs` | 执行日志分页（`jobName`/`page`/`size`） |
| GET | `/orbit/admin/executors` | 在线执行器（`appName` 可选过滤） |
| GET | `/orbit/admin/overview` | 总览 |
| POST | `/orbit/executor/run` | 执行器：接收调度触发（调度中心调用） |
| GET | `/orbit/executor/handlers` | 执行器：查询本节点注册的 Handler 列表 |

任务字段：

| 字段 | 说明 |
|------|------|
| `jobName` | 唯一名 |
| `appName` | 执行器应用名 |
| `handler` | `@OrbitJob` 名 |
| `cron` | Quartz Cron，空=仅手动 |
| `params` | JSON 参数 |
| `routeStrategy` | `ROUND` / `RANDOM` / `FIRST` |
| `timeoutSeconds` | 读超时 |
| `enabled` | 是否调度 |

---

## 5. 云原生部署

**默认（单副本）**：调度中心使用内存 JobStore（`job-store-type: memory`）+ H2 文件库，开箱即用。
此时**必须保持单副本** —— 多个副本各自持有一份 Quartz 调度器和一份内存执行器注册表，
同一个 Cron 到点会被每个副本各触发一次，任务实际执行 N 次。

**路由方式**：执行器心跳上报 `http://{POD_IP}:port`，调度中心直连 Pod IP 触发（与 XXL-JOB 一致），天然适配多副本。

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

> ⚠️ **还有一个容易踩的坑：执行器心跳寻址。**
> 执行器注册表在**内存**中，且执行器 `AdminClient` 会遍历 `orbit.executor.admin-addresses`
> 里逗号分隔的每个地址逐个上报。若只填普通 Service 名（如 `http://orbit-admin:8080`），
> 负载均衡会把每次心跳投给**随机一个**副本，各副本的注册表都是残缺的，
> 部分副本会持续报 `no online executor`。
> 多副本下必须把**每个 admin 副本的地址显式列出**，这要求用 StatefulSet + Headless Service
> 取得稳定 Pod DNS（`deploy/k8s/02-admin.yaml` 已附 Headless Service）。

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
| `access-token` | 空 | 与执行器双向校验 |
| `heartbeat-timeout-seconds` | 90 | 超时摘除执行器 |
| `evict-interval-ms` | 30000 | 后台扫描摘除失联节点的频率 |
| `timezone` | Asia/Shanghai | Cron 时区 |
| `group` | ORBIT | Quartz Job/Trigger 分组名 |
| `connect-timeout-ms` | 3000 | 调执行器连接超时 |
| `read-timeout-ms` | 300000 | 默认读超时 |

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
| `access-token` | 空 | 令牌 |

---

## 7. 与 XXL-JOB 对照

| | XXL-JOB | Orbit |
|--|---------|-------|
| 调度中心 | xxl-job-admin | orbit-admin |
| 执行器 | xxl-job-core | orbit-executor |
| 任务注解 | `@XxlJob` | `@OrbitJob` |
| 注册 | 执行器心跳 | 执行器心跳 |
| 触发 | HTTP | HTTP `/orbit/executor/run` |
| 路由 | 轮询/随机/故障转移… | ROUND / RANDOM / FIRST |
| 存储 | MySQL | H2（默认）/ PostgreSQL / GaussDB |
| ORM/连接池 | — | MyBatis 3.5.19 + MyBatis-Plus 3.5.7 + Druid 1.2.8 |

设计刻意保持精简：无独立 Web 控制台 UI（用 REST API / 自行对接前端）、无 GLUE 模式、无子任务 DAG，满足「中心调度 + 业务侧执行」的云原生批量场景即可扩展。

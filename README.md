# Orbit Batch Scheduler

基于 **JDK 1.8 + Spring Boot 2.7.18 + Quartz 2.5.2** 的分布式批量调度框架，面向云原生 Kubernetes 环境（ConfigMap + Service）设计。

[![JDK](https://img.shields.io/badge/JDK-1.8-blue)]() [![SpringBoot](https://img.shields.io/badge/Spring%20Boot-2.7.18-brightgreen)]() [![Quartz](https://img.shields.io/badge/Quartz-2.5.2-orange)]() [![K8s](https://img.shields.io/badge/Kubernetes-CloudNative-326CE5)]()

---

## 1. 核心特性

| 能力 | 说明 |
|------|------|
| **注解任务** | `@BatchTask` 标记方法即成为可调度任务，启动自动扫描注册 |
| **数据库存储** | 任务元数据存于 `t_job_config`，集群共享、动态增删改、乐观锁并发控制 |
| **本地存储** | `storage.type=memory` 内存注册表，零外部依赖，适合单机轻量场景 |
| **本地调度** | Quartz 触发 → 进程内反射执行（本节点无执行器时自动降级 HTTP 派发） |
| **HTTP 调度** | 经 K8s **Headless Service DNS** 解析全部 Pod 端点，轮询路由 + 故障转移 |
| **分布式锁** | 可插拔 SPI：**Redisson**（watchdog 续期）/ **数据库行锁**（过期抢占）/ 无锁 |
| **集群防重** | Quartz JDBC 集群（触发层防重）+ 分布式锁（任务层防重）双层保障 |
| **执行日志** | `t_job_log` 记录每次执行的调度节点、执行节点、状态、耗时，支持分页检索 |
| **多数据库兼容** | **PostgreSQL / GaussDB / MySQL** 同一构建产物直接运行：方言自动探测 + Quartz delegate 自动注入 + 主键策略分流 + 空串语义归一化 |
| **管理 API** | 任务 CRUD / 暂停恢复 / 立即触发 / 同步执行 / 日志查询 / 集群总览 |
| **云原生** | ConfigMap 外置配置、POD_NAME 节点标识、就绪/存活探针、优雅停机 |
| **健康检查** | Actuator `orbitScheduler` 指标：存储/锁类型、集群状态、端点数 |

## 2. 架构总览

```
                              ┌────────────────────────────────────────────────┐
                              │                 Kubernetes 集群                  │
                              │                                                │
   ┌──────────────┐  配置下发  │  ┌──────────────Pod A──────────────┐           │
   │  ConfigMap   ├───────────┼─►│  Orbit Scheduler (调度节点)      │           │
   │ (application │           │  │  ┌──────────────────────────┐    │           │
   │   .yml)      │           │  │  │ Quartz (JDBC集群/内存)    │    │           │
   └──────────────┘           │  │  └────────────┬─────────────┘    │           │
                              │  │               ▼ 触发              │           │
   ┌──────────────┐           │  │  ┌──────────────────────────┐    │           │
   │    Secret    │  凭据注入  │  │  │ JobManager 派发引擎      │    │           │
   │ (DB/Redis)   ├───────────┼─►│  │ ①分布式锁(Redis/DB)      │    │           │
   └──────────────┘           │  │  │ ②LOCAL: 本地反射执行     │    │           │
                              │  │  │ ③HTTP : 远程派发 ────────┼────┼───┐      │
                              │  │  └──────────────────────────┘    │   │      │
                              │  │        ▼           ▼              │   │      │
                              │  │  t_job_config   t_job_log        │   │      │
                              │  │  (任务元数据)    (执行日志)        │   │      │
                              │  └───────────────────────────────────┘   │      │
                              │                                          ▼      │
                              │  ┌──────────────Pod B──────────────┐  HTTP    │
                              │  │ Orbit Scheduler (执行节点)        │◄─┘      │
                              │  │ /api/scheduler/execute           │  Service │
                              │  │ (Headless DNS 解析全部 Pod)      │◄─────────┤
                              │  └───────────────────────────────────┘         │
                              │  ┌────────────────────────────────────┐        │
                              │  │ PostgreSQL/GaussDB(O)/MySQL        │        │
                              │  │ (集群状态/任务/日志/DB锁)          │        │
                              │  │ Redis(可选：分布式锁)              │        │
                              │  └────────────────────────────────────┘        │
                              └────────────────────────────────────────────────┘
```

**两层防重设计**

1. **触发层**：Quartz JDBC 集群模式（`isClustered=true`）保证同一触发时刻只有一个 Pod 的 Quartz 线程触发任务（数据库行锁 + 检入机制）。
2. **任务层**：`LockProvider` 分布式锁保证同一任务同一时刻全局只有一个执行体（覆盖 Quartz 内存模式、手动触发、API 同步执行等场景）。

**HTTP 派发路由**：默认通过普通 Kubernetes Service DNS（如 `orbit-scheduler`）访问服务 → Kubernetes Service/EndpointSlice 负责 Pod 负载均衡与故障摘除 → `POST /api/scheduler/execute` → 目标 Pod 本地执行 → 返回结果。框架仍支持显式 `headless-dns` 模式，在该模式下由客户端获取 Pod IP 并执行轮询/故障转移。

## 3. 模块结构

```
orbit-batch-scheduler
├── scheduler-core          # 核心框架（无强依赖侵入，可选依赖全部条件化装配）
│   └── com.orbit.scheduler
│       ├── annotation      # @BatchTask / DispatchType
│       ├── core            # TaskRegistry(扫描) / JobManager(派发引擎) / TaskContext
│       ├── quartz          # QuartzJobDispatcher(统一入口 Job) / GaussDBDelegate
│       ├── dialect         # SchedulerDialect / DialectResolver(三级自动探测)
│       ├── lock            # RedissonLockProvider / JdbcLockProvider / NoOpLockProvider
│       ├── storage         # Jdbc*/InMemory* 任务存储与执行日志
│       ├── discovery       # ServiceDns / HeadlessDns / Static 端点解析
│       ├── http            # HttpDispatchClient(轮询+故障转移)
│       ├── web             # JobController(管理API) / HttpDispatchController(远端执行)
│       ├── health          # Actuator 健康指标
│       ├── spi             # LockProvider / TaskRepository / JobLogRepository / ServiceEndpointResolver
│       └── support         # SchedulerProperties / NodeIdProvider
├── scheduler-starter       # 自动装配（一行依赖接入业务应用；含方言探测与 Quartz delegate 注入）
├── scheduler-sample        # 演示应用（standalone / local / postgresql / gaussdb / mysql 五模式）
└── deploy
    ├── k8s/                # namespace/configmap/secret/service/deployment/redis/ingress
    ├── sql/schema-postgresql.sql  # PostgreSQL：Quartz 11 表 + 业务表 + 种子（幂等）
    ├── sql/schema-gaussdb.sql     # GaussDB：同构表（主键 SEQUENCE 化）+ 差异抹平说明（幂等）
    ├── sql/schema-mysql.sql       # MySQL 8：同构表（兼容保留）
    └── docker-compose.yml  # PostgreSQL + Redis + App 本地一键拉起
```

## 4. 快速开始

### 4.1 构建

```bash
# JDK 8+ / Maven 3.6+
mvn clean package -DskipTests
# 产物：scheduler-sample/target/scheduler-sample-1.0.0.jar（可直接运行）
```

### 4.2 五种运行模式

| 模式 | 命令 | 依赖 | 适用 |
|------|------|------|------|
| standalone（默认） | `java -jar scheduler-sample-1.0.0.jar` | 无 | 本地体验、单机轻量调度 |
| local | `java -jar scheduler-sample-1.0.0.jar --spring.profiles.active=local` | 无（H2 内存库） | 体验数据库全链路 |
| **postgresql** | `java -jar scheduler-sample-1.0.0.jar --spring.profiles.active=postgresql` | PostgreSQL 10+（可选 Redis） | **生产形态（默认）** |
| **gaussdb** | `java -jar scheduler-sample-1.0.0.jar --spring.profiles.active=gaussdb` | GaussDB/openGauss（可选 Redis） | **生产形态（信创）** |
| mysql | `java -jar scheduler-sample-1.0.0.jar --spring.profiles.active=mysql` | MySQL 8（可选 Redis） | 生产形态（兼容保留） |

```bash
# PostgreSQL：首次启动前执行一次（幂等可重复）
psql -h <DB_HOST> -U postgres -d orbit_scheduler -f deploy/sql/schema-postgresql.sql
# GaussDB（建库：CREATE DATABASE orbit_scheduler DBCOMPATIBILITY='A' ENCODING 'UTF8';
#         脚本逐条自动提交执行，勿包事务；业务表主键为 SEQUENCE）
gsql -d orbit_scheduler -f deploy/sql/schema-gaussdb.sql
```

环境变量：`DB_HOST` / `DB_PORT` / `DB_NAME` / `DB_USER` / `DB_PASSWORD`。

### 4.3 PostgreSQL 与 GaussDB 双库兼容设计

**同一构建产物（同一 JAR/镜像）不改任何代码与配置结构，切换连接串即可在两种库上运行**；
数据库后端由三级探测自动识别，探测不可靠时可手动指定 `orbit.scheduler.database.dialect`。

已对照《PG 迁移 GaussDB 语法差异评估》清单逐项核对，框架 SQL 全部收敛在双库公共子集内，
涉及差异的写法全部按方言分流或前置抹平：

| 差异点 | PostgreSQL | GaussDB | 框架抹平手段 |
|--------|------------|---------|--------------|
| 空字符串语义 | `''` ≠ NULL | `''` ≡ NULL | 写库前统一"空串→NULL"归一化 + task_name 强制非空校验，三库行为一致 |
| 自增主键 | `BIGSERIAL` | SERIAL 需改写 | GaussDB DDL 改为 `SEQUENCE + DEFAULT nextval`（seq_job_config_id / seq_job_log_id） |
| INSERT 结果回填 | `RETURNING`/getGeneratedKeys 可用 | 行为存在差异 | GaussDB 方言走 `SELECT nextval` 预取 + 显式 id 插入，完全不依赖回填；序列缺失自动降级并告警 |
| Upsert 语法 | `ON CONFLICT DO UPDATE` | `MERGE INTO` | DAO 统一 UPDATE→INSERT 兜底，零方言依赖 |
| 生成列回填差异 | getGeneratedKeys 仅返回自增键 | 可能返回全部生成列 | 非 GaussDB 路径 `prepareStatement(sql, new String[]{"id"})` 显式指定回填列 |
| DATE 含时间部分 | DATE 仅日期 | DATE 含时间 | 时间列统一 TIMESTAMP；时间值全部由应用侧计算后参数传入，SQL 中无 CURRENT_DATE/DATE_TRUNC/AGE |
| VARCHAR(n) 计数 | n = 字符数 | n = 字节数（UTF-8 中文 3 字节） | GaussDB DDL 已放大含中文字段（description 1024、task_group 128、Quartz DESCRIPTION 750） |
| `::` 类型转换 | 原生支持 | 部分场景受限 | 全部使用标准 `CAST()` 或参数绑定，不使用 `::` |
| DDL 事务性 | 可回滚 | 隐式提交不可回滚 | 初始化脚本逐条自动提交 + 全量幂等（IF NOT EXISTS / NOT EXISTS 种子），勿包事务执行 |
| FILTER / DISTINCT ON / LATERAL / LISTEN-NOTIFY / JSONB 操作符 | 部分可用 | 不支持/行为不同 | 框架 SQL 均未使用（公共子集） |
| 唯一键冲突异常翻译 | `DuplicateKeyException` | 可能降级为父类 | 锁层捕获父类 `DataIntegrityViolationException`（子类兼容），双库一致 |
| Quartz Delegate | `PostgreSQLDelegate` | `GaussDBDelegate extends PostgreSQLDelegate` | 按方言自动注入，用户 yaml 显式配置优先 |
| 驱动 | `org.postgresql.Driver` | `org.opengauss.Driver` / 华为云 `com.huawei.gaussdb.jdbc.Driver` | `driver-class-name` 配置化，双驱动共存 |

其余 BYTEA / BOOLEAN / TIMESTAMP / TEXT / LIMIT-OFFSET / SELECT FOR UPDATE 双库语义一致；
启动时输出探测结果（方言/delegate/主键策略/空串语义），Quartz 表未初始化时告警。

方言探测顺序（`DialectResolver`）：

1. **显式配置**：`orbit.scheduler.database.dialect = postgresql | gaussdb`（最高优先级）
2. **连接 URL / 产品名**：`jdbc:opengauss://`、`jdbc:gaussdb://` 或产品名含 `gauss` → GaussDB；`jdbc:postgresql://` → 候选 PG
3. **内核特征**：`SHOW sql_compatibility` 可执行 ⇒ GaussDB 家族（覆盖用 PG 驱动连 openGauss 的场景）；否则 PostgreSQL

### 4.4 编写批量任务

```java
@Component
public class OrderTasks {

    /** 本地调度：凌晨 2 点生成报表 */
    @BatchTask(name = "dailyOrderReport", description = "生成前一日订单汇总报表",
               cron = "0 0 2 * * ?")
    public String dailyOrderReport(TaskContext ctx) {
        String bizDate = ctx.getString("bizDate", "yesterday");
        // ... 业务逻辑
        return "报表生成完成: " + bizDate;   // 返回值记入执行日志
    }

    /** HTTP 调度：经 Headless Service 派发到任意 Pod 执行 */
    @BatchTask(name = "remoteDataSync", cron = "0 */5 * * * ?",
               dispatchType = DispatchType.HTTP)
    public String remoteDataSync(Map<String, Object> params) {
        return "同步完成";
    }

    /** 手动任务：无 cron，仅通过 REST API 触发 */
    @BatchTask(name = "manualArchive", cron = "")
    public void manualArchive() { /* ... */ }
}
```

方法签名支持：无参 / `TaskContext` / `Map<String,Object>` 任意组合；参数由触发时传入与任务配置 `params` 合并而成。

### 4.4 管理接口（默认 `/api/scheduler`）

```bash
# 集群总览
curl http://localhost:8080/api/scheduler/overview

# 任务分页查询
curl "http://localhost:8080/api/scheduler/jobs?page=1&size=10&nameLike=sync"

# 动态创建任务（数据库存储模式，立即生效无需重启）
curl -X POST http://localhost:8080/api/scheduler/jobs \
  -H "Content-Type: application/json" \
  -d '{"taskName":"cleanTempFile","cronExpression":"0 0 3 * * ?",
       "dispatchType":"LOCAL","params":{"dir":"/tmp"},"enabled":true}'

# 修改 / 删除 / 暂停 / 恢复
curl -X PUT    http://localhost:8080/api/scheduler/jobs/cleanTempFile -H "Content-Type: application/json" -d '{"cronExpression":"0 0 4 * * ?","dispatchType":"LOCAL","enabled":true}'
curl -X DELETE http://localhost:8080/api/scheduler/jobs/cleanTempFile
curl -X POST   http://localhost:8080/api/scheduler/jobs/cleanTempFile/pause
curl -X POST   http://localhost:8080/api/scheduler/jobs/cleanTempFile/resume

# 立即触发（异步，走 Quartz 触发链路，受分布式锁保护）
curl -X POST http://localhost:8080/api/scheduler/jobs/cleanTempFile/trigger \
  -H "Content-Type: application/json" -d '{"force":true}'

# 同步执行（等待返回结果）
curl -X POST http://localhost:8080/api/scheduler/jobs/cleanTempFile/execute

# 执行日志分页
curl "http://localhost:8080/api/scheduler/logs?taskName=dailyOrderReport&page=1&size=20"

# 任务详情（含 Quartz 触发器状态 / 下次触发时间 / 本节点执行器有无）
curl http://localhost:8080/api/scheduler/jobs/dailyOrderReport
```

### 4.5 接入自己的业务应用

```xml
<dependency>
    <groupId>com.orbit</groupId>
    <artifactId>scheduler-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

```yaml
orbit:
  scheduler:
    group: ORBIT
    node-id: ${POD_NAME:}
    storage:
      type: database
    lock:
      type: auto
    http-dispatch:
      service-name: your-app
      port: 8080
spring:
  quartz:
    job-store-type: jdbc
    properties:
      org.quartz.scheduler.instanceId: AUTO
      org.quartz.jobStore.isClustered: true
```

## 5. Kubernetes 部署

```bash
# 1. 构建镜像（构建上下文 = 工程根目录）
docker build -t orbit-scheduler-sample:1.0.0 .

# 2. 初始化数据库（执行一次；按后端三选一）
psql  -h <DB_HOST> -U postgres -d orbit_scheduler -f deploy/sql/schema-postgresql.sql   # PostgreSQL
gsql   -d orbit_scheduler -f deploy/sql/schema-gaussdb.sql                            # GaussDB
mysql  -h <DB_HOST> -u root -p < deploy/sql/schema-mysql.sql                          # MySQL

# 3. 修改 deploy/k8s/02-secret.yaml 中的数据库凭据，然后部署
kubectl apply -f deploy/k8s/

# 4. 观察双副本集群
kubectl -n orbit-system get pods
kubectl -n orbit-system logs -f deployment/orbit-scheduler --tail=100
```

部署要点（见 `deploy/k8s/`）：

| 资源 | 作用 |
|------|------|
| `01-configmap.yaml` | 应用配置外置化，挂载 `/app/config/application.yml` |
| `02-secret.yaml` | DB 凭据（生产建议接入外部密钥系统） |
| `03-service.yaml` | ClusterIP 常规服务 + **Headless 服务**（clusterIP: None） |
| `04-deployment.yaml` | 2 副本、POD_NAME 节点标识、就绪/存活探针、优雅停机（45s） |
| `05-redis.yaml` | 演示用单实例 Redis（生产替换为高可用版） |

**扩缩容**：应用无状态（状态在数据库/Redis），直接 `kubectl scale deployment orbit-scheduler --replicas=N`；新 Pod 自动加入 Quartz 集群与 Headless DNS 解析结果。数据库后端切换（postgresql / gaussdb / mysql）只需改 ConfigMap 的 `spring.profiles.active` 与 Deployment 的 DB_HOST/DB_PORT。

**验证 HTTP 跨 Pod 派发**：观察日志可见 `dispatchNode`（触发 Pod）与 `workerNode`（执行 Pod）为不同节点：

```bash
curl "http://localhost:8080/api/scheduler/logs?taskName=remoteDataSync" | python3 -m json.tool | grep -E "dispatchNode|workerNode"
```

## 6. 配置参考（`orbit.scheduler.*`）

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `enabled` | `true` | 总开关 |
| `group` | `ORBIT` | Quartz 任务分组（框架管理边界） |
| `node-id` | POD_NAME→主机名 | 节点标识（锁 owner / 日志节点名） |
| `timezone` | `Asia/Shanghai` | Cron 时区 |
| `database.dialect` | `auto` | `postgresql` / `gaussdb` / `auto`（三级自动探测；决定 Quartz delegate 自动注入） |
| `annotation-scan` | `true` | 是否扫描 `@BatchTask` |
| `api-enabled` | `true` | 是否暴露管理 API（Web 环境） |
| `storage.type` | `auto` | `database` / `memory` / `auto`（有数据源则 DB） |
| `lock.enabled` | `true` | 是否启用任务级分布式锁 |
| `lock.type` | `auto` | `redis` / `database` / `none` / `auto`（Redis→DB→无锁） |
| `lock.lease` | `10m` | 锁租约（执行期间自动续期） |
| `lock.redis.address` | 空 | `host:port`，配置后启用 Redis 锁 |
| `http-dispatch.enabled` | `true` | 是否启用 HTTP 远程派发 |
| `http-dispatch.service-name` | 空 | Kubernetes Service/DNS 名 |
| `http-dispatch.discovery-mode` | `service-dns` | `service-dns`（普通 Service，推荐，由 K8s 负责负载均衡） / `headless-dns`（直接 Pod IP，由客户端选择） / `static` |
| `http-dispatch.port` | `8080` | 目标 Pod 端口 |
| `http-dispatch.path` | `/api/scheduler/execute` | 远端执行路径 |
| `http-dispatch.connect-timeout` | `3s` | 连接超时 |
| `http-dispatch.read-timeout` | `300s` | 读超时（兜底，任务级 `timeoutSeconds` 优先） |
| `http-dispatch.secret` | 空 | 派发令牌（`X-Scheduler-Token` 校验） |
| `http-dispatch.static-endpoints` | `[]` | `discovery-mode=static` 时使用的静态端点 |

> **远程派发路由语义**：推荐使用普通 Kubernetes `ClusterIP Service`。`service-dns` 模式只访问 Service 地址，Pod 级负载均衡和故障摘除交给 Kubernetes `Service/EndpointSlice`；Scheduler 不再做应用层 Pod 轮询。只有 `headless-dns` / `static` 模式才由 Scheduler 在多个实例端点之间选择。对于多实例端点，只有“任务不存在”或明确的网络连接失败才切换下一端点，普通业务执行失败不会自动重试，以避免重复执行。
| `log.storage` | `auto` | `database` / `memory` / `auto` |
| `log.memory-capacity` | `1000` | 内存日志环形队列容量 |

## 7. 数据库表

| 表 | 用途 |
|----|------|
| `QRTZ_*`（11 张） | Quartz 官方集群表（触发层防重、misfire 恢复） |
| `t_job_config` | 任务配置（cron/调度方式/参数/启停/乐观锁版本） |
| `t_job_log` | 执行日志（请求ID/调度节点/执行节点/状态/耗时/消息） |
| `t_cluster_lock` | 数据库分布式锁（毫秒时间戳租约，规避时钟不一致） |

## 8. 扩展点

全部核心能力面向接口，业务方可自定义替换（Spring Bean 覆盖即生效，`@ConditionalOnMissingBean`）：

| SPI | 内置实现 | 扩展场景示例 |
|-----|---------|-------------|
| `LockProvider` | Redisson / Jdbc / NoOp | Zookeeper / etcd 锁 |
| `TaskRepository` | Jdbc / InMemory | 配置中心（Nacos/Apollo）存储任务元数据 |
| `JobLogRepository` | Jdbc / InMemory | ES / Kafka 审计投递 |
| `ServiceEndpointResolver` | ServiceDns / HeadlessDns / Static | 对接其他注册中心、Service Mesh 或自定义服务发现 |

## 9. 设计说明

- **语义保证**：at-least-once。节点宕机时 Quartz `requestRecovery` 会对执行中任务发起恢复重触发；分布式锁租约超时后其它节点可接管 —— 任务实现方需保证幂等。
- **锁续期**：长任务执行期间看门狗线程按 `lease/3` 周期续期（Redisson 为内置 watchdog，30s 租约自动续）。
- **misfire 策略**：统一 `withMisfireHandlingInstructionDoNothing`（错过的触发不补偿），避免宕机恢复后触发风暴；如需补偿语义可在 `JobManager#scheduleOrUpdate` 调整。
- **注解与数据库配置的优先级**：注解仅在任务不存在时作为种子写入（`overwrite=true` 可强制回写）；数据库配置（运维侧改 cron）优先。
- **LOCAL 自动降级 HTTP**：本地无执行器的 LOCAL 任务自动走 HTTP 派发找其它节点执行 —— 注解任务天然获得跨节点执行能力。
- **管理 API 安全**：框架层提供 `http-dispatch.secret` 派发令牌；管理 API 建议通过 K8s NetworkPolicy / Ingress 鉴权收敛。

## 10. 常见问题

**Q: Quartz 2.5.2 与 Spring Boot 2.7.18 兼容吗？**
父 POM 已通过 `<quartz.version>2.5.2</quartz.version>` 覆盖 Boot BOM 默认的 2.3.2，公共 API（Job/Trigger/SchedulerFactoryBean）完全兼容，已通过构建与运行验证。

**Q: 数据库锁还是 Redis 锁？**
中小规模（<几十 QPS 触发）用数据库锁即可，省一个组件；高频触发或锁竞争激烈时用 Redis。`auto` 模式按"有 Redisson 用 Redis，其次数据库"自动决策。

**Q: 双副本部署后任务会重复执行吗？**
不会。Quartz JDBC 集群保证触发层唯一；即便手工触发/API 同步执行并发，分布式锁也会拒绝第二次派发（日志记为 SKIPPED）。

**Q: 为什么用 Headless Service 而不是普通 Service？**
普通 Service 的 ClusterIP/VIP DNS 解析只返回一个入口，负载均衡由 kube-proxy NAT 完成，无法拿到 Pod 列表做故障转移；Headless DNS 直接返回全部 Pod A 记录，框架可实现"端点级"轮询与故障转移，且天然跟随扩缩容。

**Q: 任务执行超过锁租约怎么办？**
看门狗会自动续期；若节点假死导致续期停止，租约到期后其它节点可接管（可能产生一次重复执行，at-least-once 语义，任务需幂等）。

**Q: GaussDB 不同兼容模式（A/B/C/PG）都支持吗？**
都可以。A 模式差异最大（空串退化 NULL、SERIAL 需改写等），框架已按差异清单逐项抹平；B/C/PG 模式行为更接近 PostgreSQL，同一套代码与表结构直接可用。建库 `DBCOMPATIBILITY='A'` 仅是贴合常见信创环境，非强制。

**Q: 在 GaussDB 上遇到驱动兼容问题怎么办？**
按服务端版本选择驱动：openGauss 5.x 对应 `org.opengauss:opengauss-jdbc:5.1.0-og`（父 POM 属性可改 6.x），华为云 GaussDB 也可替换官方 `gaussdbjdbc`（`com.huawei.gaussdb.jdbc.Driver`，URL 前缀 `jdbc:gaussdb://`）。若个别 GaussDB 版本对 boolean 绑定/锁语法有差异，继承覆写 `GaussDBDelegate` 对应方法即可，无需改动框架其他代码。

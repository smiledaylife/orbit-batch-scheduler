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

---

## 2. 本地 5 分钟跑通

```bash
# 构建（整个 reactor 需用 JDK 11+ 构建：admin 模块 release=11；
#       executor/core/sample 模块 release=8，产物为 Java 8 字节码且仅用 JDK 8 API，可运行在 JRE 8）
mvn clean package -DskipTests

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

> ⚠️ **高可用边界**：当前调度中心为**单实例**设计 —— 执行器注册表在内存、Quartz 默认 `job-store-type: memory`，
> K8s 示例中 `/app/data` 使用 `emptyDir`（Pod 重建后 H2 任务数据丢失）。
> 生产环境请：① 切换到 PostgreSQL / GaussDB（见下）；② 调度中心保持单副本，或自行扩展 Quartz JDBC JobStore 集群。

**路由方式**：执行器心跳上报 `http://{POD_IP}:port`，调度中心直连 Pod IP 触发（与 XXL-JOB 一致），天然适配多副本。

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

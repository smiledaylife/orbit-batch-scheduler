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

- **JDK 拓扑**：全模块统一 **JDK 21 + Spring Boot 3.5**（调度中心、执行器 SDK、协议层、示例应用同基线；新项目无历史包袱，`orbit-core` 也不再保留 Java 8 字节码双轨）。admin 与 executor 仅通过 HTTP/JSON 通信、不共享 JVM
- Spring Boot 3.5.12（要求 JDK 17+，本框架基线 **JDK 21**，建议 21.0.7+）· Quartz 2.5.2（调度中心）
- 持久层：**Druid 1.2.25** 连接池（`druid-spring-boot-3-starter`）+ **MyBatis 3.5.19** + **MyBatis-Plus 3.5.7**（`mybatis-plus-spring-boot3-starter`，仅 `orbit-admin` 使用；`orbit-core`/`orbit-executor` 不依赖 ORM）
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

根 `pom.xml` 既是聚合器也是**父 POM**：四个子模块通过 `<parent>` 继承，版本与插件配置全部在根 POM 统一下发，
子模块 POM 只保留差异部分（依赖清单与个别插件启用声明）：

- 根 POM 统一管理：`properties`（JDK/Boot 基线、第三方依赖版本、插件版本）+ `dependencyManagement`（import `spring-boot-dependencies:3.5.12` BOM，并显式锁定 Quartz 2.5.2 / MyBatis 3.5.19 / MyBatis-Plus 3.5.7 / Druid 1.2.25 / openGauss 5.0.1）+ `pluginManagement`（compiler `release=21` + `-parameters`、surefire、enforcer、spring-boot-maven-plugin）；
- 父 POM 直接声明的 `dependencyManagement` 条目优先级高于 import 的 BOM，版本覆盖只需在根 POM 写一次；
- 模块间引用统一用 `${project.version}`，保证同版本号发布；
- enforcer（JDK 21 基线校验）在根 POM 的 `<build><plugins>` 中声明，对所有模块生效。

> 构建需从根目录发起（继承模式下 reactor 负责解析父子关系）：
> `mvn clean package -DskipTests` 全量；`mvn package -pl orbit-admin -am` 单模块及其依赖；
> 需要单独构建某个模块时先 `mvn install -N` 安装父 POM。不用 `spring-boot-starter-parent`，
> Boot 3.2+ 必需的编译参数 `-parameters`（保留方法参数名，`@RequestParam`/`@PathVariable` 依赖）
> 已在根 POM 的 compiler 配置中统一下发，漏配会导致接口报 400。

### JDK 21 能力应用

| 特性 | 落点 | 说明 |
|------|------|------|
| 虚拟线程（JEP 444） | `spring.threads.virtual.enabled=true`（admin/sample 默认开启） | Tomcat 请求处理虚拟线程化，HTTP 入口在慢 IO 下不占平台线程；Quartz/Druid/@Scheduled 不受影响 |
| 虚拟线程（JEP 444） | `orbit.admin.dispatch-virtual-threads`（默认 false） | 定时触发通道改虚拟线程执行；有界并发/排队/拒绝语义不变；底层 HTTP 客户端已换用虚拟线程友好的 JDK HttpClient（NIO，无 synchronized 钉住问题） |
| 虚拟线程（JEP 444） | `orbit.executor.worker-virtual-threads`（默认 false） | 任务工作线程改虚拟线程，适合 IO 密集任务；synchronized 阻塞多的业务代码建议保持平台线程（JDK 21 下 synchronized 会钉住载体线程，JEP 491 于 JDK 24 才解除） |
| switch 模式匹配（JEP 441） | `ExecutorRegistry.route()` 路由分发；`JobService.validate()` 策略校验；`ExecutorClient.describeTriggerFailure()` 故障分类 | 路由策略按常量标签分发；故障分类按异常层级（含守卫模式 `when`）归一化关键词，且 `HttpConnectTimeoutException` 排在父类 `HttpTimeoutException` 之前展示优先级匹配 |
| instanceof 模式匹配 | `JobContext` 类型判定；`ExecutorClient` 异常解包链 | 判定与绑定一步完成 |
| record | `ExecutorRegistry.RegistrySnapshot`、`ExecutorAddressValidator.CachedResult`、`JobHandlerRegistry.Handler` | 只读内部值类，天然不可变；协议层 DTO 仍为可变 POJO（Jackson 反序列化需要字段默认值，变更收益不抵风险） |
| 文本块 | `OutstandingDispatches` 三段 Lua、`QuartzReconciler` 释放锁 Lua | 脚本原文直接可读、易比对 |
| SequencedCollection（JDK 21） | `route()` 的 `getFirst()`、handlers 截断的 `removeLast()` | 替代 `get(0)` / `remove(size()-1)` |
| `@Serial` | 全部 Serializable 模型 / PO | 序列化版本字段的标准化注解 |
| Locale.ROOT 修正 | 策略大写规范化两处 | 修复土耳其语等 locale 下 `"first"→"FİRST"` 导致合法策略被误拒的潜在缺陷 |

> 虚拟线程的并发上限语义：`dispatch-threads` / `worker-threads` 仍是在途并发的硬上限
> （线程池 + 有界队列 + 快速失败不变），虚拟线程只是线程实现更轻，不会放开背压。
> 两个开关默认关闭以保持与历史版本一致的线程模型，可按部署形态逐步开启。

---

## 2. 本地 5 分钟跑通

```bash
# 构建（全模块基于 Spring Boot 3.5 + JDK 21，需 JDK 21+ 构建）
mvn clean package -DskipTests

# 终端 1：调度中心 :8080
java -jar orbit-admin/target/orbit-admin-1.0.0.jar

# 终端 2：执行器 :8081
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
  threads:
    virtual:
      enabled: true               # JDK 21 虚拟线程处理 HTTP 请求（可选）
orbit:
  executor:
    app-name: order-service          # 与任务 appName 一致
    admin-addresses: http://orbit-admin:8080
    # address 留空：本地用本机 IP，K8s 用 POD_IP
    # port 无需配置，默认自动感知并继承应用自身的 server.port
    worker-threads: 8                # 单节点并发上限 + 超时强制中断，下限为 1
    queue-capacity: 256              # 排队上限，满则快速失败
    worker-virtual-threads: false    # 可选：工作线程改虚拟线程（IO 密集任务收益明显）
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
> `orbit.executor.worker-threads` / `queue-capacity` 可调，线程数下限 1）：
> 限制单节点并发执行数、队列满时快速失败返回 `executor saturated`；
> 并按任务 `timeoutSeconds` **超时强制中断**（`interrupted=true`），
> 彻底消除「调度中心 HTTP 读超时放弃后，执行器任务永久僵尸运行」的问题。
> 可选开启 `worker-virtual-threads: true` 把工作线程换成 JDK 21 虚拟线程：
> 有界并发、排队、超时中断语义完全不变，适合 HTTP/DB 等 IO 密集任务；
> 业务 handler 大量使用 synchronized 阻塞时建议保持默认平台线程。

> 执行器 SDK（`orbit-executor`）与调度中心同基线发布：业务应用需运行在 **JDK 21+ / Spring Boot 3.5.x** 接入
> （SDK 基于 `jakarta.*`，不再支持 Spring Boot 2.7 业务应用；两端仅经 HTTP/JSON 交互）。
> 调度中心的 Quartz / ORM 依赖不会传递到业务侧。

> **Redis 是可选依赖**：`orbit-executor` 的 `spring-boot-starter-data-redis` 声明为 `optional`，
> 业务应用不引入 Redis 也能完整运行 —— logId 幂等自动降级为 JVM 本地实现（仅保护本节点，TTL 上限 300 秒），
> 持久化回传退回纯 HTTP 回传；开启集群级能力（跨副本幂等 / Redis Stream 回传）时再自行补充 Redis 依赖与配置。
> 这也意味着不使用 Redis 的应用不会被被动拖入 Redis 健康探针（`/actuator/health` 不会因 Redis 不在而误报 DOWN）。

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
| POST | `/orbit/admin/jobs/{name}/trigger` | 立即触发一次（body 可传本次临时参数 JSON）。返回**受理回执**而非执行结果，结果查 `/logs`。与定时触发共用同名任务串行守卫（任务可用 `serialExecution` 覆盖全局）：上一轮未收敛则拒绝本次触发并返回原因 |
| GET | `/orbit/admin/logs` | 执行日志分页（`jobName`/`status`/`from`/`to`/`page`/`size`，`status` 非法值返回 400；时间格式 ISO 如 `2026-01-01T00:00:00`）。失败排查主入口：`/logs?status=FAILED` |
| GET | `/orbit/admin/executors` | 在线执行器（`appName` 可选过滤） |
| POST | `/orbit/admin/alerts/test` | 告警链路自检：异步投递一条 TEST 事件到已配置的告警处理器，返回告警通道指标 |
| POST | `/orbit/admin/callback` | 执行器**批量**回传执行结果（执行器调用；body 为结果数组，单批上限 2000 条、超限返回 400，逐条按 `logId` 幂等收敛，响应返回实际收敛条数） |
| GET | `/orbit/admin/overview` | 总览（含触发通道指标 `dispatchActive` / `dispatchQueueSize` / `dispatchRejected` / `dispatchSkipped` / `dispatchOutstanding`，以及告警通道指标 `alertFired` / `alertDelivered` / `alertDropped` / `alertFailed` / `alertQueueSize`） |
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
| `routeStrategy` | `ROUND` / `RANDOM` / `FIRST` / `CONSISTENT_HASH` | 16 |
| `timeoutSeconds` | 读超时（≤0 回落 300，超过 `max-timeout-seconds` 封顶） | — |
| `retryCount` | 失败重试次数（不含首次，`0` 不重试，范围 `[0,10]`），两层语义见下节 | — |
| `retryIntervalSeconds` | 失败重试间隔秒（`0` 立即重试，范围 `[0,3600]`，默认 10） | — |
| `serialExecution` | 任务级串行开关（每任务阻塞策略）：`null` 跟随全局 `dispatch-serial-per-job`，`true`/`false` 显式覆盖 | — |
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
  单批失败按 `callback-retry-*` 退避重试，重试耗尽则整批退回队列而不是丢弃，靠队列容量做背压。
  调度中心侧另有服务端守门：单批最多 2000 条，超限直接 400 整批拒绝，
  防止失控/恶意客户端把回传端点变成内存与数据库的压力源；
- **可选持久化回传（cluster 模式）**：执行器配置 `orbit.executor.durable-callback-enabled=true` 后，
  结果优先写入 Redis Stream（跨进程重启不丢），Admin 侧由消费组异步落库
  （`orbit.admin.durable-callback-enabled=true`）；Redis 不可用时自动降级回 HTTP 回传。
- **串行守卫的登记是派发前完成的**：定时触发在向执行器发起请求**之前**就把本次 logId 预登记进
  同名任务串行守卫，而不是拿到受理回执后再登记。否则执行器毫秒级跑完并回传时，
  回传可能与登记动作竞态，守卫被永久占用，任务从此不再被触发（且孤儿回收只扫 RUNNING 日志，
  无法兜底）。预登记让「回传可见」先于「回传可能发生」，从根上消除这个窗口；
  手动触发接入同一套守卫：上一轮未收敛时拒绝手动触发并返回明确原因，而不是并行执行；
- **failover 分级**：触发失败时按失败性质区分处理 ——
  明确的连接级失败（connection refused / connect timeout / no route 等）节点大概率已不存在，
  立即摘除并换下一个；模糊失败（connection reset / read timeout，请求可能已被执行器受理）
  换节点重试但**不摘除本节点**（是否下线交由心跳超时判断）；
  业务性拒绝（饱和、handler 不存在）换节点也不会成功，直接终止；
  同 logId 在执行器侧有本地幂等兜底（见下节），同节点重复入池会被拦截；
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

### 4.2 失败重试

任务字段 `retryCount`（次数，不含首次）与 `retryIntervalSeconds`（间隔秒）驱动两层互补的重试：

| 层级 | 触发条件 | 执行位置 | 日志形态 |
|------|---------|---------|---------|
| **执行级重试** | 任务真正执行了但失败/超时（handler 抛异常、看门狗超时中断） | 执行器节点内，`retryIntervalSeconds` 延迟后**同一 logId** 重跑 | 中间失败**不回传**，只回传最终结果；整条重试链共享同一条日志行，message 标注 `attempt i/N` |
| **触发级重试** | 派发本身失败（执行器全部不可达、无在线节点、同步拒绝等） | 调度中心内，`retryIntervalSeconds` 延迟后**新 logId** 重新走完整派发链路 | 每次尝试一条日志行；失败行 message 标注 `(trigger retry i/N scheduled in Xs)` |

要点：

- **两层互不叠加**：只有执行器受理失败才走触发级重试；一旦受理成功，后续失败由执行级重试兜住，
  调度中心不再重复重试（避免同一次失败被两层放大）；
- **总尝试上限 = retryCount + 1**，范围 `[0,10]`；`retryCount=0` 时行为与旧版一致（失败即终局）；
- 业务方法可经 `JobContext.getAttempt()` 区分首轮与重试轮次（如重试时降级、跳过已处理分片）；
- 触发级重试到点后按**库里最新任务定义**执行：等待期间任务被删除/停用则重试中止；
  重试同样接入同名任务串行守卫，上一轮已在途时主动让位；
- **重试不跨进程持久化**（轻量化取舍）：调度中心重启丢弃未到期的触发级重试；
  执行器重启丢弃未触发的执行级重试（此时回传不会到达，孤儿回收会把日志判为 FAILED）。
  需要强持久重试语义的任务，请依赖外部对账或把 retryIntervalSeconds 控制在单次部署窗口内；
- 派发通道饱和拒绝（`scheduler saturated`）**不做**触发级重试（重试会放大过载），只告警。

### 4.3 失败告警扩展点

调度中心在任务链路到达「最终失败」时异步投递告警事件，业务侧实现 `OrbitAlertHandler`
接口并注册为 Spring Bean 即完成接入，无需任何额外配置：

| 事件类型 | 含义 | 典型来源 |
|---------|------|---------|
| `EXECUTION_FAILED` | 任务执行最终失败（执行器侧执行级重试已耗尽） | 执行器回传 FAILED |
| `TRIGGER_FAILED` | 触发最终失败（触发级重试已耗尽，或派发通道饱和拒绝） | 调度中心派发失败 |
| `EXECUTION_LOST` | 执行结果丢失：日志悬挂 RUNNING 被孤儿回收 | 后台僵尸回收 |

接入示例（放在调度中心所在应用内，如 orbit-admin 或你的自定义 admin 扩展）：

```java
@Component
public class DingTalkAlertHandler implements OrbitAlertHandler {
    @Override
    public void onAlert(JobAlertEvent event) {
        // event.eventType() / jobName() / handler() / appName() / logId()
        // event.executorAddress() / costMs() / message() / eventTime()
        // 调钉钉/企微/邮件 webhook；建议自行做按 jobName 的节流去重
    }
}
```

投递契约（由 `AlertDispatcher` 保证）：

- **异步**：事件先入 256 容量的有界队列，专职分发线程消费，`fire()` 永不阻塞调度主链路；
- **隔离**：处理器抛出的任何异常都被消化并计入 `alertFailed`，不影响后续事件；
- **尽力而为**：队列满时新事件被丢弃并计数（fail-open）。告警是 at-most-once 语义，
  需要强一致请自行在实现内落盘重试；
- 业务侧未提供实现时，内置 `LoggingAlertHandler` 兜底（WARN 日志，可被日志采集器收集）；
- `/orbit/admin/alerts/test` 可随时验证链路连通性；`/orbit/admin/overview` 的
  `alertFired` / `alertDelivered` / `alertDropped` / `alertFailed` 暴露通道健康度。

> 告警事件中的任务上下文（jobName / appName / handler）由执行器回传时回填；
> 字段缺失时调度中心按 logId 反查日志行补齐，保证事件总能定位到任务。

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

> **已有部署升级（新增失败重试字段后）**：`orbit_job` 新增三列，旧库请手工执行
>（建表脚本已包含这三列，仅存量库需要）：
>
> ```sql
> ALTER TABLE orbit_job ADD COLUMN IF NOT EXISTS retry_count            INT DEFAULT 0;
> ALTER TABLE orbit_job ADD COLUMN IF NOT EXISTS retry_interval_seconds INT DEFAULT 10;
> ALTER TABLE orbit_job ADD COLUMN IF NOT EXISTS serial_execution       BOOLEAN;
> ```
>
> 未升级前新字段读侧落到安全默认值（不重试 / 10 秒 / 跟随全局串行），不影响存量任务运行。

GaussDB 请把 `ORBIT_DB_URL` / `ORBIT_DB_DRIVER` 换成 openGauss 驱动，并将
`ORBIT_QUARTZ_DELEGATE` 设为 `org.quartz.impl.jdbcjobstore.GaussDBDelegate`
（M / Oracle 兼容模式则用 `StdJDBCDelegate`，DDL 改用官方 `tables_gauss_m_compatibility.sql`）。

> **无需额外引入连接池**：Spring Boot 在 `job-store-type=jdbc` 时会调用
> `schedulerFactoryBean.setDataSource(...)`，让 Quartz 直接复用 Spring 管理的 Druid 连接池，
> 不走 Quartz 自己的 `org.quartz.dataSource.*`，因此不需要 c3p0 / HikariCP。

> 执行器心跳落库，admin 多副本无需 StatefulSet。心跳打到任意副本即可。
> 多副本**调度**仍依赖 Quartz JDBC 集群（上面 1~4），与注册表无关。

```bash
# 镜像（构建阶段统一用 JDK 21；运行阶段统一 JRE 21）
docker build -t orbit-admin:1.0.0 \
  --build-arg MODULE=orbit-admin \
  --build-arg RUNTIME_IMAGE=eclipse-temurin:21-jre .
docker build -t orbit-executor-sample:1.0.0 \
  --build-arg MODULE=orbit-executor-sample .
# k8s 部署时请使用基于 JRE 21 的镜像。

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

- 连接池统一使用 **Druid**（`druid-spring-boot-3-starter`，Spring Boot 3 专用 artifact；旧的 `druid-spring-boot-starter` 在 Boot 3 下不生效），连接池参数在 `spring.datasource.druid.*` 下配置
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
| `heartbeat-timeout-seconds` | 90 | 超时摘除执行器（下限 5 秒：误配 0 或负数会让每轮扫描清空整张注册表） |
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
| `dispatch-serial-per-job` | true | 同名任务串行：上一轮**执行结果未回传**时本次到点跳过（只计数，见 `/overview` 的 `dispatchSkipped`）。cluster 模式下升级为 Redis Lease 跨副本互斥 |
| `dispatch-virtual-threads` | false | 触发通道改用 JDK 21 虚拟线程：有界并发/排队/拒绝语义不变，仅线程实现更轻；底层 HTTP 客户端已为虚拟线程友好实现，高频 Cron 场景可开启 |
| `executor-address-allow-pattern` | 空 | 执行器注册地址白名单（Java 正则，需整串匹配）。空 = 不启用，仅做基础校验（http/https、必须有 host、禁保留地址含 DNS 解析后复查）。生产建议显式配置，见第 8 节 SSRF 说明 |
| `execution-lease-enabled` | false | 集群级执行 Lease（Redis）：同一 Job 跨副本同时只允许一个有效派发占用；cluster profile 强制开启 |
| `execution-lease-ttl-ms` | 120000 | Lease 持有时间，应明显大于 Redis/网络瞬时抖动 |
| `execution-lease-renew-interval-ms` | 30000 | Lease 续租周期，必须小于 TTL 的三分之一 |
| `durable-callback-enabled` | false | Redis Stream 持久化 callback：执行器先写 Stream、Admin 消费落库，避免单纯 HTTP 回传丢结果；cluster profile 强制开启 |
| `callback-stream-key` | orbit:callback:stream | 执行器写入、Admin 消费的 Stream key（两端需一致） |
| `callback-stream-group` | orbit-admin | Admin 侧 Stream Consumer Group 名称 |
| `quartz-reconcile-interval-ms` | 60000 | DB 与 Quartz 状态对账周期（删除孤儿 Job + 重注册缺失任务） |
| `quartz-reconcile-initial-delay-ms` | 15000 | 首次对账延迟，给应用启动和 Quartz 初始化预留时间 |

> **注册表缓存说明**：TTL（默认 3s）远小于心跳超时（90s），多副本间写传播延迟上界即
> TTL；本进程写操作（注册/摘除/剔除）立即失效缓存；派发命中已下线节点由
> failover（不可达即摘除换节点）兜底。XXL-JOB 调度中心为纯内存注册表 + 30s DB
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
| `worker-virtual-threads` | false | 工作线程改用 JDK 21 虚拟线程：并发上限/排队/超时中断语义不变，适合 IO 密集任务；synchronized 阻塞多的业务代码建议保持默认（JDK 21 下会钉住载体线程） |
| `max-job-wait-seconds` | 86400 | 请求未带 `timeoutSeconds` 时的兜底等待秒数（实际等待有 1 秒下限，误配 0/负数不会让任务瞬间「超时」） |
| `access-token` | 空 | 令牌，随 `X-Orbit-Token` 请求头发出（双向常量时间比对） |
| `callback-retry-times` | 3 | 结果回传失败的重试次数（不含首次）。回传失败会让日志停在 RUNNING 直到被回收 |
| `callback-retry-interval-ms` | 2000 | 回传重试的退避间隔 |
| `callback-queue-capacity` | 1000 | 待回传队列容量（保留兼容配置；实际使用无界队列避免主动丢结果） |
| `execution-idempotency-enabled` | false | 执行幂等：同 logId 在 TTL 内最多进入一次执行线程池（防 HTTP 超时重试重复执行）。有 Redis 时跨副本生效，Redis 不可用直接拒绝触发；无 Redis 时自动降级为 JVM 本地实现（仅保护本节点，TTL 上限 300 秒，见 `ExecutionIdempotency` 类注释） |
| `execution-idempotency-ttl-seconds` | 86400 | 幂等 key 保留时间，应覆盖最大任务耗时及回传重试窗口；本地兜底模式按 300 秒封顶 |
| `durable-callback-enabled` | false | 结果先写 Redis Stream（跨进程重启不丢），写入失败自动降级 HTTP 回传；与 admin 侧同名开关配合使用 |
| `callback-stream-key` | orbit:callback:stream | 执行器写入的 Stream key，与 admin 侧一致 |

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
| 阻塞策略 | 执行器侧 `SERIAL_EXECUTION` / `DISCARD_LATER` / `COVER_EARLY`，每 job 一条 `JobThread` + 无界队列 | 调度中心侧 `dispatch-serial-per-job` 串行（任务可用 `serialExecution` 按任务覆盖）；执行器侧共享有界线程池 + 有界队列（满则同步快速失败） |
| 触发线程池 | fast/slow 双池，1 分钟内慢触发 >10 次改投慢池 | 单一有界触发池 + `/overview` 水位指标 |
| 路由 | 轮询/随机/故障转移/一致性哈希… | ROUND / RANDOM / FIRST / CONSISTENT_HASH（160 虚拟节点 + FNV-1a + splitmix64，以任务名为哈希键；非法值创建/更新时拒绝） |
| 失败重试 | `failRetryCount`：执行器 `JobThread` 内同 logId 重试 | 两层互补：执行级（同 logId 重跑，中间失败不回传）+ 触发级（派发失败后新 logId 重派，每次尝试独立日志行）；重试间隔可配、不跨进程持久化（见 §4.2） |
| 失败告警 | 邮件 `JobAlarmer` 接口 | `OrbitAlertHandler` SPI：实现 Bean 即接入钉钉/企微/webhook 等，异步有界投递、异常隔离、内置 WARN 日志兜底（见 §4.3） |
| 超时 | — | 双端对齐：admin 读超时 + 执行器强制中断（消除僵尸任务） |
| 存储 | MySQL | H2（默认）/ PostgreSQL / GaussDB |
| ORM/连接池 | — | MyBatis 3.5.19 + MyBatis-Plus 3.5.7 + Druid 1.2.25 |

设计刻意保持精简：无独立 Web 控制台 UI（用 REST API / 自行对接前端）、无 GLUE 模式、无子任务 DAG，满足「中心调度 + 业务侧执行」的云原生批量场景即可扩展。

**尚未实现且暂时裁剪的 XXL-JOB 能力**（如需可作后续迭代）：

- **分片广播（SHARDING_BROADCAST）**：需要把触发从「选一个节点」改为「向全部节点各发一份并携带 shardIndex/shardTotal」，与串行守卫（按 jobName 占槽）、执行级重试、孤儿回收的按行回收模型都有交叉改造；轻量化阶段建议按业务分片键拆成多个任务（配 CONSISTENT_HASH 路由）达成类似的水平拆分效果；
- **子任务 DAG**：依赖编排超出「中心调度 + 业务侧执行」的定位，建议用工作流引擎或业务侧串联。

---

## 8. 已知限制

刻意列出来，避免踩坑后才回头翻代码：

| # | 限制 | 影响与缓解 |
|---|------|-----------|
| 1 | **触发线程数仍是每副本的预算**：Quartz 工作线程微秒级返回，但每次触发仍要占用一条 `dispatch-threads` 线程，最长 `trigger-timeout-seconds` | 与任务耗时无关，默认 64 条 × 10 秒足以支撑很高的触发频率。真要打满说明执行器普遍响应慢，`/overview` 的 `dispatchActive` / `dispatchQueueSize` / `dispatchRejected` 可直接观测 |
| 2 | **手动触发不再返回执行结果**：`POST /jobs/{name}/trigger` 只返回受理回执 | 需要结果请轮询 `/orbit/admin/logs?jobName=...`，或对接 `/callback` 之后的日志 |
| 2b | **回传丢失会把成功的任务记成失败**：执行器跑完但回传重试耗尽（或执行器在完成与回传之间崩溃）时，日志会被孤儿回收判为 FAILED | 业务侧要保证幂等；`callback-retry-times` 调大、并保证执行器能访问 `admin-addresses` 中的至少一个地址 |
| 3 | **注册地址校验的 DNS 解析与 TOCTOU 窗口**（见 `ExecutorAddressValidator` 类注释） | 注册时会拒绝解析到保留地址（如 `169.254.169.254`）的域名；校验结果（含拒绝）缓存 60 秒以消除心跳路径的每轮 DNS 解析，代价是域名重新指向最多延迟一个缓存周期被发现；彻底封堵请配置 `orbit.admin.executor-address-allow-pattern` 白名单 |
| 4 | **执行器 SDK 面向 Spring Boot 3.5.x + Servlet 容器（`jakarta.*`）**：自动装配走 `META-INF/spring/...AutoConfiguration.imports`，`/orbit/executor/run` 只在 Servlet Web 环境装配 | Spring Boot 2.7 业务应用请使用旧版本 SDK；WebFlux 应用暂不能直接接入。另注意 Boot 3.4+ 起 Redis 配置前缀为 `spring.data.redis.*` |
| 5 | **同名任务串行守卫的默认实现是进程内的**：`dispatch-serial-per-job` 未开启 Redis Lease 时，在途登记簿只在本副本内存里，且调度中心重启即清空 | cluster 模式请开启 `orbit.admin.execution-lease-enabled=true`（Redis Lease，跨副本互斥 + 自动续租）；单机模式下多副本不重叠依赖 Quartz 集群行锁（同一 trigger 只被一个副本触发），但它不保证「上一轮已结束」。任务不幂等时请开启 Lease 或自行加分布式锁 |
| 5b | **执行幂等的本地兜底只保护本节点**：无 Redis 时同 logId 去重仅在单进程内生效；failover 遇模糊错误（connection reset / read timeout）换节点重试时，跨节点双执行在未开启 Redis 幂等的部署下理论上仍可能 | 任务不幂等时请补充 Redis 依赖并开启 `orbit.executor.execution-idempotency-enabled=true`（跨副本 SET NX + TTL），或依赖业务侧自身的幂等设计 |
| 6 | **默认单副本内存 JobStore** | 多副本必须启用 cluster profile + 真实数据库，否则同一个 Cron 会被每个副本各触发一次（见第 5 节） |
| 7 | **失败重试不跨进程持久化**：触发级重试队列在调度中心内存里，执行级重试链在执行器进程内 | 任一侧重启都会丢弃未到期的重试：调度中心重启丢触发级重试（该任务等到下一轮 Cron）；执行器重启丢执行级重试且回传不会到达，日志由孤儿回收判为 FAILED。对重启敏感的任务把 `retryIntervalSeconds` 控制在部署窗口内，或依赖下一轮 Cron 自然接续 |
| 8 | **告警是 at-most-once 的尽力而为语义**：事件入 256 容量有界队列，队列满直接丢弃；处理器异常只计数不重试 | 告警绝不阻塞调度主链路，代价是极端拥塞时可能少报。接入强一致告警渠道（如值班系统）请自行在 `OrbitAlertHandler` 实现内落盘重试；`/overview` 的 `alertDropped` 持续增长说明渠道处理能力不足 |

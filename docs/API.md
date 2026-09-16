# Orbit Batch Scheduler 接口文档

> 适用版本：**1.0.0**（Spring Boot 3.5.12 / JDK 21）。
> 本文是**逐端点的字段级契约**：参数、约束、响应结构、错误码与幂等语义。
> 架构与部署说明见 [README.md](../README.md)（§4 触发-回传模型、§6 配置参考）；
> 机器可读规范见 [docs/openapi.yaml](openapi.yaml)（OpenAPI 3.0.3，可导入 Swagger UI / Postman / 代码生成器）。

目录：

- [1. 接口总览](#1-接口总览)
- [2. 通用约定](#2-通用约定)
- [3. 调度中心 API：执行器通信侧](#3-调度中心-api执行器通信侧)
- [4. 调度中心 API：任务管理侧](#4-调度中心-api任务管理侧)
- [5. 调度中心 API：观测与运维](#5-调度中心-api观测与运维)
- [6. 执行器 API](#6-执行器-api)
- [7. 数据模型字典](#7-数据模型字典)
- [8. 字段长度与校验规则](#8-字段长度与校验规则)
- [9. 错误信息目录](#9-错误信息目录)
- [10. 端到端调用示例](#10-端到端调用示例)
- [11. OpenAPI 规范的使用方式](#11-openapi-规范的使用方式)
- [12. 兼容性与变更约定](#12-兼容性与变更约定)

---

## 1. 接口总览

系统有两个 HTTP API 面：**调度中心**（`orbit-admin`，默认 `:8080`，前缀 `/orbit/admin`）与
**执行器**（业务应用内嵌 `orbit-executor` SDK，前缀 `/orbit/executor`）。两端仅通过 HTTP/JSON 通信，不共享 JVM。

| # | 方法 | 路径 | 部署在 | 调用方 | 鉴权 | 幂等 |
|---|------|------|--------|--------|------|------|
| 1 | POST | `/orbit/admin/registry` | 调度中心 | 执行器（启动 + 心跳） | ✅ | 是（upsert） |
| 2 | POST | `/orbit/admin/registry/remove` | 调度中心 | 执行器（优雅停机） | ✅ | 是 |
| 3 | POST | `/orbit/admin/callback` | 调度中心 | 执行器（结果回传） | ✅ | 是（按 `logId`） |
| 4 | GET | `/orbit/admin/jobs` | 调度中心 | 运维 / 前端 | ✅ | 读 |
| 5 | GET | `/orbit/admin/jobs/{name}` | 调度中心 | 运维 / 前端 | ✅ | 读 |
| 6 | POST | `/orbit/admin/jobs` | 调度中心 | 运维 / 前端 | ✅ | 否（重名 400） |
| 7 | PUT | `/orbit/admin/jobs/{name}` | 调度中心 | 运维 / 前端 | ✅ | 是（按 `name` 覆盖写，乐观锁保护） |
| 8 | DELETE | `/orbit/admin/jobs/{name}` | 调度中心 | 运维 / 前端 | ✅ | 否（不存在 400） |
| 9 | POST | `/orbit/admin/jobs/{name}/pause` | 调度中心 | 运维 / 前端 | ✅ | 是 |
| 10 | POST | `/orbit/admin/jobs/{name}/resume` | 调度中心 | 运维 / 前端 | ✅ | 是 |
| 11 | POST | `/orbit/admin/jobs/{name}/trigger` | 调度中心 | 运维 / 前端 | ✅ | 否（每次新 `logId`） |
| 12 | GET | `/orbit/admin/logs` | 调度中心 | 运维 / 前端 | ✅ | 读 |
| 13 | GET | `/orbit/admin/executors` | 调度中心 | 运维 / 前端 | ✅ | 读 |
| 14 | GET | `/orbit/admin/overview` | 调度中心 | 运维 / 监控 | ✅ | 读 |
| 15 | POST | `/orbit/admin/alerts/test` | 调度中心 | 运维 | ✅ | 否（每次投递一条 TEST 事件） |
| 16 | POST | `/orbit/executor/run` | 执行器 | 调度中心 | ✅ | 是（按 `logId`） |
| 17 | GET | `/orbit/executor/handlers` | 执行器 | 运维 / 调度中心排障 | ✅ | 读 |

「鉴权 ✅」= 受 `X-Orbit-Token` 令牌保护，见 [§2.2](#22-鉴权)。**未配置令牌时全部放行**（开发态默认）。

另有 Spring Boot Actuator 端点（**不在令牌保护范围内，便于 K8s 探针免鉴权访问**）：
`GET /actuator/health`、`/actuator/health/liveness`、`/actuator/health/readiness`、`/actuator/info`。

---

## 2. 通用约定

### 2.1 基地址与内容类型

| 项 | 约定 |
|----|------|
| 调度中心基地址 | `http://<admin-host>:8080`（无 `server.servlet.context-path`，路径即 `/orbit/admin/...`） |
| 执行器基地址 | `http://<executor-address>`，即注册上报的 `address`（如 `http://10.0.1.5:8081`） |
| Content-Type | `application/json`（请求与响应均为 `application/json;charset=UTF-8`） |
| 请求体 | 仅 `POST`/`PUT` 带 JSON body；`POST /orbit/admin/jobs/{name}/trigger` 的 body 可缺省 |
| 跨域 | **未配置 CORS**。浏览器直连需经网关/反向代理转发，或由前端同源部署 |
| HTTP 版本 | 无特殊要求（服务端为 Servlet 容器 + JDK HttpClient 客户端） |

> **未知字段被静默忽略**：Spring Boot 默认关闭 `FAIL_ON_UNKNOWN_PROPERTIES`，
> 因此请求体里拼错的字段名不会报错、也不会生效（例：把 `routeStrategy` 写成 `routeStategy` 会按默认值 `ROUND` 落库）。
> 严格校验请在调用方侧做（或改用 `spring.jackson.deserialization.fail-on-unknown-properties=true`）。
> 同理，**空 body 或非法 JSON** 不被专门映射为 400，会落到兜底异常处理器返回 `500 internal error`。

### 2.2 鉴权

双向共享令牌，请求头名称由 `OrbitProtocol.TOKEN_HEADER` 固定为 **`X-Orbit-Token`**：

| 方向 | 校验点 | 令牌配置项 | 失败响应 |
|------|--------|-----------|----------|
| 运维 / 执行器 → 调度中心 | `AdminAuthInterceptor` 拦截 `/orbit/admin/**` **全部**端点 | `orbit.admin.access-token` | `401` + `{"code":401,"success":false,"msg":"invalid access token","data":null}` |
| 调度中心 → 执行器 | `ExecutorController.checkToken()`（仅 `/run`、`/handlers`） | `orbit.executor.access-token` | `403` + `{"code":403,"success":false,"msg":"invalid access token","data":null}` |

要点：

- 调度中心侧支持两种写法（任选其一，优先前者优先）：`X-Orbit-Token: <token>` 或 `Authorization: Bearer <token>`；
  **执行器侧只识别 `X-Orbit-Token`**，不支持 `Bearer`。
- 令牌比对为**常量时间**（`MessageDigest.isEqual`），抵御逐字节猜定时令牌的时序侧信道。
- 令牌为空（默认）= **不鉴权**，启动时打印醒目告警；生产环境两端**必须**配置，且通常配成同一个值。
- `cluster` profile 下 `orbit.admin.access-token` 为空会导致启动失败（配置强校验）。
- 令牌只在请求头传递，**不读取请求体、不支持 query 参数**。

### 2.3 统一响应包装

调度中心 `/orbit/admin/**` 全部端点返回 `ApiResult<T>`（执行器 `/orbit/executor/**` **不包装**，见 §6）：

```json
{
  "code": 200,
  "success": true,
  "msg": "OK",
  "data": { }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| `code` | int | 业务码，与 HTTP 状态码保持一致（200 / 400 / 401 / 403 / 500） |
| `success` | boolean | 是否成功。**`success=true` 不代表任务执行成功**，只代表接口调用成功（触发端点返回的是受理回执） |
| `msg` | string | 成功固定 `"OK"`；失败为可直接展示的原因（见 §9） |
| `data` | any | 业务数据，可为 `null` |

### 2.4 分页约定

分页查询响应中的 `data` 为 `PageResult<T>`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `page` | int | 当前页码，从 1 开始 |
| `size` | int | 每页大小 |
| `total` | long | 满足条件的总记录数 |
| `items` | array | 当前页数据 |

请求参数钳位规则（**不报错**，静默归一）：`page < 1 → 1`；`size < 1 → 1`；`size > 200 → 200`（服务端硬上限 `MAX_PAGE_SIZE=200`）。

- `GET /orbit/admin/jobs` 按 `job_name` **升序**；
- `GET /orbit/admin/logs` 按日志主键 `id` **降序**（等价于最新在前）。

### 2.5 时间与格式

| 场景 | 格式 |
|------|------|
| 响应中的时间字段（`createdAt` / `updatedAt` / `startTime` / `endTime` / `lastHeartbeat`） | ISO-8601 字符串，如 `"2026-09-16T09:06:00.000+00:00"`（Spring Boot 默认关闭 `WRITE_DATES_AS_TIMESTAMPS`；默认按 UTC 偏移输出，可用 `spring.jackson.time-zone` / `date-format` 调整） |
| `GET /orbit/admin/logs` 的 `from` / `to` | **ISO 本地日期时间**，`yyyy-MM-dd'T'HH:mm[:ss][.SSS]`，如 `2026-01-01T00:00:00`；**不接受带时区或 `Z` 后缀**（会 400）；按服务端 JVM 默认时区解释，与入库口径一致 |
| `GET /orbit/admin/jobs/{name}` 的 `quartz.nextFireTime` | **epoch 毫秒数字**（`long`），无下次触发时为 `null` |
| 布尔 / 整数 | 由 Jackson 宽松转换：`"3"` 可解析为 `3`；字段传 `null` 等同缺省，按模型默认值回填（不会报 400） |

---

## 3. 调度中心 API：执行器通信侧

这三个端点由执行器 SDK 自动调用，业务方一般无需手工请求；此处完整列出，便于自建执行器、写运维脚本或排查注册链路。

### 3.1 `POST /orbit/admin/registry` — 执行器注册 / 心跳

```
POST /orbit/admin/registry
X-Orbit-Token: <token>        # 配置了 orbit.admin.access-token 时必带
Content-Type: application/json
```

请求体（`RegistryRequest`）：

| 字段 | 类型 | 必填 | 约束 / 说明 |
|------|------|------|-------------|
| `appName` | string | ✅ | 非空，≤64；与任务定义里的 `appName` 严格对应 |
| `address` | string | ✅ | 非空，≤256；执行器可访问基地址。经 `ExecutorAddressValidator` 校验：仅 `http`/`https`、不得含 user-info/query/fragment、必须有 host、端口合法、拒绝保留地址（`127/8`、`0/8`、`169.254/16` 云元数据、链路本地 IPv6 等，**域名 DNS 解析后复查**），可选受 `orbit.admin.executor-address-allow-pattern` 白名单正则整串匹配。首尾空白与末尾 `/` 被规范化去除 |
| `nodeId` | string | — | ≤128；K8s 通常传 `POD_NAME`，本地为主机名 |
| `handlers` | string[] | — | 本节点已注册的 `@OrbitJob` 名称；序列化为 JSON 后超过 2000 字符会**从尾部截断条目**并打 WARN（不报错） |

行为：按 `(appName, address)` **upsert**——已存在则刷新 `last_heartbeat`（并更新 nodeId/handlers），否则插入；多副本并发首注册的唯一键冲突自动转为更新。写库成功后立即失效本进程的注册表缓存（默认 TTL 3s）。

响应：`ApiResult<Void>`，`data` 为 `null`。

```json
{"code": 200, "success": true, "msg": "OK", "data": null}
```

错误：`400`（`appName required` / `address required` / `executor address ...` / `appName too long: length N exceeds limit 64` 等，见 §9）。
失联判定：`last_heartbeat` 超过 `orbit.admin.heartbeat-timeout-seconds`（默认 90s）的节点被后台任务摘除（扫描频率 `evict-interval-ms`，默认 30s），执行器心跳周期为 `orbit.executor.heartbeat-interval-ms`（默认 20s，下限 5s）。

### 3.2 `POST /orbit/admin/registry/remove` — 执行器主动下线

请求体同上，**只读取 `appName` 与 `address`**（`nodeId`/`handlers` 忽略）。执行器优雅停机时调用，立刻摘除本节点，避免心跳超时前仍被派发。

```bash
curl -X POST http://localhost:8080/orbit/admin/registry/remove \
  -H 'X-Orbit-Token: <token>' -H 'Content-Type: application/json' \
  -d '{"appName":"demo-executor","address":"http://10.0.1.5:8081"}'
```

行为：按 `(appName, address)` 删除，`address` 仅做 trim + 去尾斜杠（**不做** SSRF 校验）；
节点不存在时也返回成功（幂等，`data=null`）。

### 3.3 `POST /orbit/admin/callback` — 执行器批量回传执行结果

触发是「受理即返回」的，任务真实成败**只能**经此端点送达：它是 `orbit_job_log` 从 `RUNNING` 走向 `SUCCESS`/`FAILED` 的唯一正常路径（兜底路径为僵尸日志回收）。

请求体：`TriggerResult[]`（**顶层是数组**，见 §7.4 字段说明）

```bash
curl -X POST http://localhost:8080/orbit/admin/callback \
  -H 'X-Orbit-Token: <token>' -H 'Content-Type: application/json' \
  -d '[{
    "logId": "9f2c1e0a7b3d4e5f6a7b8c9d0e1f2a3b",
    "jobId": 12,
    "success": true,
    "message": "report done, bizDate=yesterday, orders=9213",
    "costMs": 743,
    "workerNode": "orbit-executor-7d9f8b6c5-x2kqz",
    "accepted": false,
    "jobName": "dailyReportJob",
    "appName": "demo-executor",
    "handler": "dailyReport"
  }]'
```

| 约束 | 说明 |
|------|------|
| 单批上限 | **2000 条**（`JobService.MAX_CALLBACK_BATCH`），超限整批 `400 callback batch too large: N (max 2000); split the batch and retry`。执行器侧单批实际最多 200 条 |
| 逐条幂等 | 存储层条件更新 `WHERE log_id=? AND status='RUNNING'`；重复回传、与孤儿回收竞态都**不会覆盖已写入的结果**，仍返回 200（避免执行器误判失败而无限重试） |
| 缺 `logId` 的条目 | 被忽略并记 WARN，不影响同批其余结果 |
| 空数组 / `null` | 合法，`data=0` |
| `accepted` 字段 | 回传方向应为 `false`（最终结果）；`true` 表示「触发回执」，仅在 `success=false` 时被调度中心当作同步失败信号使用 |

响应 `data` = **本批真正完成 `RUNNING → 终态` 转换的条数**：

```json
{"code": 200, "success": true, "msg": "OK", "data": 1}
```

> 回传的 `jobName`/`appName`/`handler` 用于构造告警事件，缺失时调度中心按 `logId` 反查日志行补齐。
> 开启 Redis Stream 持久化回传（`orbit.executor.durable-callback-enabled=true` + `orbit.admin.durable-callback-enabled=true`）时，
> 结果优先写 Stream（字段 `logId`/`jobId`/`success`/`accepted`/`costMs`/`workerNode`/`message`）由 admin 侧消费组异步落库，
> **本端点作为 Redis 不可用时的降级通道仍然保持开放**。

---

## 4. 调度中心 API：任务管理侧

### 4.1 `GET /orbit/admin/jobs` — 任务分页列表

| Query 参数 | 类型 | 默认 | 说明 |
|-----------|------|------|------|
| `page` | int | `1` | 页码，`<1` 归一为 1 |
| `size` | int | `10` | 每页大小，钳位 `[1,200]` |
| `nameLike` | string | — | 任务名模糊匹配（SQL `LIKE %x%`，自动 trim；空串视为不过滤） |

```bash
curl 'http://localhost:8080/orbit/admin/jobs?page=1&size=20&nameLike=daily' -H 'X-Orbit-Token: <token>'
```

响应：`ApiResult<PageResult<JobInfo>>`（按 `job_name` 升序）。

```json
{
  "code": 200, "success": true, "msg": "OK",
  "data": {
    "page": 1, "size": 20, "total": 1,
    "items": [{
      "id": 12, "jobName": "dailyReportJob", "description": "日报",
      "appName": "demo-executor", "handler": "dailyReport", "cron": "0 */2 * * * ?",
      "params": {"bizDate": "yesterday"}, "timeoutSeconds": 120, "routeStrategy": "ROUND",
      "retryCount": 0, "retryIntervalSeconds": 10, "serialExecution": null,
      "enabled": true, "version": 2,
      "createdAt": "2026-09-16T09:06:00.000+00:00",
      "updatedAt": "2026-09-16T09:40:12.000+00:00"
    }]
  }
}
```

### 4.2 `GET /orbit/admin/jobs/{name}` — 任务详情 + Quartz 运行态

| Path 参数 | 说明 |
|-----------|------|
| `name` | 任务名（`jobName`），非正则合法但需与库中记录完全一致 |

响应 `data` 为**两个键**的对象：`job`（`JobInfo`）与 `quartz`（调度器实时状态）：

| `quartz` 字段 | 类型 | 说明 |
|---------------|------|------|
| `jobExists` | boolean | Quartz 中是否存在该 JobDetail（任务暂停/无有效 cron 时为 `false`） |
| `triggerState` | string | `NONE` / `WAITING` / `ACQUIRED` / `RUNNING` / `BLOCKED` / `PAUSED` / `PAUSED_BLOCKED` / `ERROR` |
| `nextFireTime` | long \| null | 下次触发时间，**epoch 毫秒**；`NONE` 时为 `null` |
| `cron` | string | 当前生效的 Cron 表达式（仅 CronTrigger 存在时返回） |

```json
{
  "code": 200, "success": true, "msg": "OK",
  "data": {
    "job": {"id": 12, "jobName": "dailyReportJob", "enabled": true, "cron": "0 */2 * * * ?"},
    "quartz": {"jobExists": true, "triggerState": "WAITING", "nextFireTime": 1789615560000, "cron": "0 */2 * * * ?"}
  }
}
```

> 上面 `job` 为节省篇幅做了省略，实际返回完整 `JobInfo`。
> 注意：`data.job` 不存在时**不会**走到这里——直接 `400 job not found: <name>`。
> Quartz 查询异常不会导致接口失败，而是以 `{"error": "<原因>"}` 出现在 `quartz` 里。

### 4.3 `POST /orbit/admin/jobs` — 创建任务

请求体：`JobInfo`，字段与约束见 §7.1 / §8。

```bash
curl -X POST http://localhost:8080/orbit/admin/jobs \
  -H 'X-Orbit-Token: <token>' -H 'Content-Type: application/json' \
  -d '{
    "jobName": "dailyReportJob",
    "description": "日报",
    "appName": "demo-executor",
    "handler": "dailyReport",
    "cron": "0 */2 * * * ?",
    "params": {"bizDate": "yesterday", "batch": 200},
    "routeStrategy": "ROUND",
    "timeoutSeconds": 120,
    "retryCount": 2,
    "retryIntervalSeconds": 30,
    "serialExecution": null,
    "enabled": true
  }'
```

响应：`ApiResult<JobInfo>`，`data` 为**落库后的完整定义**（`id` 已回填、`version=1`、`createdAt/updatedAt` 为服务端时间）。
缺省字段按模型默认值落库：`routeStrategy=ROUND`、`timeoutSeconds=300`、`retryCount=0`、`retryIntervalSeconds=10`、`enabled=true`、`params={}`。

| 服务端行为 | 说明 |
|-----------|------|
| 名称唯一 | 重名 → `400 job already exists: <name>`；并发创建的唯一键冲突也归一为同一句 |
| 默认值回填 | `timeoutSeconds<=0` → 300；超过 `orbit.admin.max-timeout-seconds`（默认 3600）→ **封顶**并打 WARN |
| 策略规范化 | `routeStrategy` 去空白 + 转大写（`Locale.ROOT`）后校验；非法值直接拒绝，不静默按 `ROUND` 处理 |
| 原子性 | 先落库再编排 Quartz；**Quartz 编排失败会回滚刚写入的任务行**，接口失败 == 什么都没发生 |
| 仅手动任务 | `cron` 为空/空白 → 不注册 Quartz Trigger，只能手动触发 |
| 请求体禁忌 | **不要携带 `id`/`version`/`createdAt`/`updatedAt`**：`id` 非空会让存储层走「更新」分支，导致 500 |

错误：`400`（校验与重名，见 §9）、`500 internal error`（Quartz/存储层异常，完整堆栈在服务端日志）。

### 4.4 `PUT /orbit/admin/jobs/{name}` — 更新任务（全量覆盖）

```bash
curl -X PUT http://localhost:8080/orbit/admin/jobs/dailyReportJob \
  -H 'X-Orbit-Token: <token>' -H 'Content-Type: application/json' \
  -d '{
    "description": "日报 v2", "appName": "demo-executor", "handler": "dailyReport",
    "cron": "0 0/5 * * * ?", "params": {"bizDate": "today"},
    "routeStrategy": "FIRST", "timeoutSeconds": 180,
    "retryCount": 1, "retryIntervalSeconds": 15,
    "serialExecution": true, "enabled": true
  }'
```

| 语义要点 | 说明 |
|---------|------|
| **不是 PATCH** | 以请求体逐字段覆盖：`body` 里缺失的字段取 `JobInfo` 的**默认值**——`enabled` 缺失即被写为 `true`（等于顺带恢复调度）、`params` 缺失即清空为 `{}`、`timeoutSeconds`/`retryCount`/`retryIntervalSeconds` 回到 300/0/10。更新前请先 `GET` 再改再 `PUT` |
| `jobName` | 以路径变量 `{name}` 为准，body 内的 `jobName` 被忽略（**不支持改名**，改名请「新建 + 删除」） |
| `id` / `version` | 由库中当前记录决定，body 内的值被忽略；乐观锁由服务端内部处理 |
| 校验 | 与创建同套规则，但**不校验 `jobName` 格式**（沿用路径名） |
| 原子性 | Quartz 编排失败会**回滚到更新前的定义**，避免出现「接口说已保存、Quartz 仍按旧 cron 跑」 |
| 并发冲突 | 存储层乐观锁更新 0 行时抛 `IllegalStateException`，接口返回 `500 internal error`（服务端日志含 `job concurrent update, please retry`），调用方应重新 GET 后再 PUT |
| 热更新 | 更新成功即重排 Quartz Trigger（`rescheduleJob`，Trigger 缺失时自动补建） |

响应：`ApiResult<JobInfo>`（更新后的定义，`version` 递增）。

### 4.5 `DELETE /orbit/admin/jobs/{name}` — 删除任务

```bash
curl -X DELETE http://localhost:8080/orbit/admin/jobs/dailyReportJob -H 'X-Orbit-Token: <token>'
```

以数据库为唯一事实来源：**先删库、再清理 Quartz**；Quartz 清理失败只记日志不影响接口成功（重启后 `init()` 不会再装载它）。
不删除历史执行日志。任务不存在 → `400 job not found: <name>`（**不是 404**）。响应 `data=null`。

### 4.6 `POST /orbit/admin/jobs/{name}/pause` — 暂停

`enabled=false` 持久化 + 从 Quartz 删除调度计划。之所以「删」而不是「pause trigger」：内存 JobStore 重启即清空，
只有把状态写进库里，暂停才能在调度中心重启后保持。任务不存在 → `400`。响应 `data=null`。

### 4.7 `POST /orbit/admin/jobs/{name}/resume` — 恢复

`enabled=true` 持久化 + 重新注册 Cron Trigger（`cron` 为空/非法时只更新状态、不注册，`triggerState` 仍为 `NONE`）。
任务不存在 → `400`。响应 `data=null`。

### 4.8 `POST /orbit/admin/jobs/{name}/trigger` — 立即触发一次

| 位置 | 名称 | 类型 | 说明 |
|------|------|------|------|
| Path | `name` | string | 任务名 |
| Body | —（可缺省） | object | 本次触发的**临时参数**，与任务定义里的 `params` 合并，**同名键以本处为准** |

```bash
# 不带临时参数
curl -X POST http://localhost:8080/orbit/admin/jobs/dailyReportJob/trigger -H 'X-Orbit-Token: <token>'

# 带临时参数（覆盖任务里的 bizDate）
curl -X POST http://localhost:8080/orbit/admin/jobs/dailyReportJob/trigger \
  -H 'X-Orbit-Token: <token>' -H 'Content-Type: application/json' \
  -d '{"bizDate":"2026-09-15","batch":500}'
```

响应 `data` 为 **`TriggerResult` 受理回执**，不是执行结果：

```json
{
  "code": 200, "success": true, "msg": "OK",
  "data": {
    "logId": "9f2c1e0a7b3d4e5f6a7b8c9d0e1f2a3b",
    "jobId": 12,
    "success": true,
    "message": "accepted, queued on executor orbit-executor-7d9f8b6c5-x2kqz",
    "costMs": 0,
    "workerNode": "orbit-executor-7d9f8b6c5-x2kqz",
    "accepted": true,
    "jobName": null, "appName": null, "handler": null
  }
}
```

判读规则（**必须看 `data` 里的两个布尔位**）：

| `data.accepted` | `data.success` | 含义 | 后续动作 |
|------------------|----------------|------|----------|
| `true` | `true` | 已被某执行器受理并入队，日志停在 `RUNNING` | 用 `logId` / `jobName` 轮询 `GET /orbit/admin/logs` 看终态 |
| `false` | `false` | 本次触发**同步失败**（无在线节点、执行器饱和、全部不可达、被串行守卫拒绝等） | 看 `data.message` 定位原因；日志已直接写 `FAILED` |

其他特性：

- **不等待执行结果**：整条链路的 HTTP 读超时为 `orbit.admin.trigger-timeout-seconds`（默认 10s），与任务真实耗时无关；
- **串行守卫共用**：与 Cron 触发同一套「同名任务串行」判定（全局 `orbit.admin.dispatch-serial-per-job`，任务级 `serialExecution` 覆盖）。上一轮结果未回传时返回：
  `data.success=false`，`data.message="manual trigger rejected: previous run of job '<name>' is still awaiting callback (orbit.admin.dispatch-serial-per-job=true)"`；
- **暂停中的任务同样可以手动触发**（`trigger` 不检查 `enabled`），便于补数；
- 派发失败按任务 `retryCount`/`retryIntervalSeconds` 走**触发级重试**（每次尝试一条新日志行，`message` 标注 `(trigger retry i/N scheduled in Xs)`）；
- 任务不存在 → `400 job not found: <name>`。

### 4.9 触发链路上的 HTTP 调用（调度中心 → 执行器）

为完整性：调度中心手动/定时触发时会对选中节点发起

```
POST http://<executor-address>/orbit/executor/run
X-Orbit-Token: <orbit.admin.access-token>
```

请求体为 `TriggerRequest`（见 §7.3），响应为 `TriggerResult`。路由策略从在线节点中选点，
失败按原因分级 failover：连接级失败（`connection refused` / `connect timed out` / `unknownhost` / `no route` / `network unreachable`）
立即摘除该节点并换下一个；模糊失败（`read timed out` / `connection reset`）换节点但**不摘除**；
业务性拒绝（饱和、handler 不存在）直接终止。详见 README §4.1。

---

## 5. 调度中心 API：观测与运维

### 5.1 `GET /orbit/admin/logs` — 执行日志分页

| Query | 类型 | 默认 | 说明 |
|-------|------|------|------|
| `jobName` | string | — | 精确匹配（非模糊） |
| `status` | string | — | `RUNNING` / `SUCCESS` / `FAILED`，大小写不敏感、自动 trim；非法值 → `400 status must be one of RUNNING/SUCCESS/FAILED: <v>` |
| `from` | ISO datetime | — | `start_time >= from`（含） |
| `to` | ISO datetime | — | `start_time <= to`（含） |
| `page` / `size` | int | `1` / `10` | 同 §2.4 |

```bash
# 失败排查主入口
curl 'http://localhost:8080/orbit/admin/logs?status=FAILED&size=20' -H 'X-Orbit-Token: <token>'

# 按任务 + 时间范围
curl 'http://localhost:8080/orbit/admin/logs?jobName=dailyReportJob&from=2026-09-16T00:00:00&to=2026-09-16T12:00:00' \
  -H 'X-Orbit-Token: <token>'
```

响应 `data.items[]` 为 `JobLog`（见 §7.2），按 `id` 降序：

```json
{
  "id": 1024,
  "logId": "9f2c1e0a7b3d4e5f6a7b8c9d0e1f2a3b",
  "jobId": 12,
  "jobName": "dailyReportJob",
  "appName": "demo-executor",
  "handler": "dailyReport",
  "executorAddress": "http://10.0.1.5:8081",
  "status": "SUCCESS",
  "message": "report done, bizDate=2026-09-15, orders=9213",
  "costMs": 743,
  "startTime": "2026-09-16T09:40:00.000+00:00",
  "endTime": "2026-09-16T09:40:00.743+00:00"
}
```

`message` 超过 2000 字符会被截断为 1997 字符 + `...`（`ColumnLimits.abbreviate`）。
状态语义：`RUNNING` = 「已触发、结果尚未回传」，**不代表卡死**；超过 `max-timeout-seconds + 5 分钟` 的悬挂 `RUNNING`
会被后台僵尸回收判为 `FAILED`（`message` 标注 `EXECUTION_LOST`，并投递同名告警）。
日志保留 `orbit.admin.log-retention-days`（默认 30 天，`0` = 不清理）。

> 暂无「按 `logId` 查单条日志」的端点；请用 `jobName` + 时间范围过滤后在 `items` 中定位 `logId`。

### 5.2 `GET /orbit/admin/executors` — 在线执行器列表

| Query | 说明 |
|-------|------|
| `appName` | 可选过滤；空白/缺省 = 返回全部在线节点 |

响应 `data` 为 `ExecutorNode[]`，**只含未失联节点**（`last_heartbeat >= now - heartbeat-timeout-seconds`），按 `address` 升序：

```json
{
  "code": 200, "success": true, "msg": "OK",
  "data": [{
    "appName": "demo-executor",
    "address": "http://10.0.1.5:8081",
    "nodeId": "orbit-executor-7d9f8b6c5-x2kqz",
    "handlers": ["dailyReport", "dataSync", "manualClean", "flaky", "alwaysFail"],
    "lastHeartbeat": "2026-09-16T09:40:02.000+00:00",
    "online": true
  }]
}
```

> 结果经注册表本地缓存（TTL `orbit.admin.registry-cache-ttl-ms`，默认 3000ms）提供，
> 因此该视图最多滞后一个 TTL；本进程的注册/摘除操作会立即失效缓存。列表里 `online` 恒为 `true`——离线节点已被过滤而非标记。

### 5.3 `GET /orbit/admin/overview` — 运行大盘

响应 `data` 为指标字典（键顺序固定）：

| 键 | 类型 | 来源 | 说明 |
|----|------|------|------|
| `jobCount` | int | `JobService.overview()` | 库中任务总数 |
| `executorOnline` | int | | 在线执行器节点数 |
| `scheduledCount` | int | Quartz | 已装载到调度器的 Job 数（查询异常时为 `-1`） |
| `quartzClustered` | boolean | Quartz | 是否使用集群 JobStore（默认内存 JobStore 为 `false`） |
| `timezone` | string | 配置 | Cron 时区（`orbit.admin.timezone`） |
| `dispatchThreads` | int | `DispatchExecutor` | 触发通道线程数上限 |
| `dispatchActive` | int | | 正在执行的触发数（在途） |
| `dispatchQueueSize` | int | | 触发排队数 |
| `dispatchQueueCapacity` | int | | 排队上限（`dispatch-queue-capacity`） |
| `dispatchRejected` | long | | 因队列满被快速失败的累计次数（伴随一条 `FAILED` 日志 `scheduler saturated`） |
| `dispatchSkipped` | long | | 被同名任务串行守卫跳过的累计次数 |
| `dispatchOutstanding` | int | | 当前在途（结果未回传）的执行数 |
| `alertHandler` | string | `AlertDispatcher` | 生效的告警处理器类简单名（默认 `LoggingAlertHandler`） |
| `alertFired` / `alertDelivered` / `alertDropped` / `alertFailed` | long | | 告警事件投递 / 送达 / 队列满丢弃 / 处理器异常计数 |
| `alertQueueSize` | int | | 告警队列当前积压（容量 256） |
| `callbackConsumerDelivered` / `callbackStreamLen` / `callbackStreamPending` / `callbackStreamConsumers` / `callbackStreamLastDeliveredId` | long/string | `CallbackStreamConsumer` | **仅 `orbit.admin.durable-callback-enabled=true`（cluster）时出现**：Redis Stream 回传通道的消费指标 |

```bash
curl -s http://localhost:8080/orbit/admin/overview -H 'X-Orbit-Token: <token>' | jq .data
```

告警：`dispatchRejected` 持续增长 → 扩容执行器或调大 `dispatch-threads`/`dispatch-queue-capacity`；
`alertDropped` 持续增长 → 告警渠道处理太慢（README §8 第 8 条）。

### 5.4 `POST /orbit/admin/alerts/test` — 告警链路自检

向已配置的 `OrbitAlertHandler` **异步**投递一条 `eventType=TEST` 的事件，用于验证「事件 → 分发器 → 处理器」链路连通。

```bash
curl -X POST http://localhost:8080/orbit/admin/alerts/test -H 'X-Orbit-Token: <token>'
```

响应 `data` = 当前告警指标 + `dispatched` 标记（**只表示已受理入队**，不代表已送达渠道）：

```json
{
  "code": 200, "success": true, "msg": "OK",
  "data": {
    "alertHandler": "LoggingAlertHandler",
    "alertFired": 1, "alertDelivered": 0, "alertDropped": 0, "alertFailed": 0,
    "alertQueueSize": 1,
    "dispatched": true
  }
}
```

事件载荷为固定测试值：`jobName/handler/appName = "(test)"`，`message = "alert channel test from /orbit/admin/alerts/test"`。

---

## 6. 执行器 API

执行器端点由 `orbit-executor` SDK 自动装配（Servlet Web 环境），部署在**业务应用自己的端口**上（默认示例 `:8081`）。
**响应不使用 `ApiResult` 包装**，直接返回裸 JSON 对象。

### 6.1 `POST /orbit/executor/run` — 受理一次任务触发

请求体（`TriggerRequest`，由调度中心构造，字段见 §7.3）：

```json
{
  "jobId": 12, "jobName": "dailyReportJob", "appName": "demo-executor",
  "handler": "dailyReport", "logId": "9f2c1e0a7b3d4e5f6a7b8c9d0e1f2a3b",
  "params": {"bizDate": "yesterday", "batch": 200},
  "timeoutSeconds": 120, "retryCount": 2, "retryIntervalSeconds": 30
}
```

**受理即返回**（入队不等执行）：

```json
{
  "logId": "9f2c1e0a7b3d4e5f6a7b8c9d0e1f2a3b",
  "jobId": 12,
  "success": true,
  "message": "accepted, queued on executor orbit-executor-7d9f8b6c5-x2kqz",
  "costMs": 0,
  "workerNode": "orbit-executor-7d9f8b6c5-x2kqz",
  "accepted": true,
  "jobName": null, "appName": null, "handler": null
}
```

入口处理顺序与各分支响应（**除令牌错误外一律 HTTP 200**，成败看 body 字段）：

| 顺序 | 条件 | HTTP | body 关键值 |
|------|------|------|-------------|
| 1 | 配置了 `access-token` 且 `X-Orbit-Token` 不匹配 | `403` | `{"code":403,"success":false,"msg":"invalid access token","data":null}`（此响应是 `ApiResult` 形态） |
| 2 | `handler` 为空/空白 | `200` | `accepted=false`、`success=false`、`message="handler required"` |
| 3 | 本节点未注册该 handler | `200` | `accepted=false`、`success=false`、`message="handler not found on this executor: <handler>"` |
| 4 | `logId` 已受理过（重复触发 / HTTP 超时重试） | `200` | `accepted=true`、`success=true`、`message="duplicate request ignored: logId already accepted"`（**不再次入池**） |
| 5 | 工作线程池与队列已满 | `200` | `accepted=false`、`success=false`、`message="executor saturated: job queue full (workers=<n>, queueCapacity=<m>); consider scaling executor replicas or raising orbit.executor.queue-capacity"` |
| 6 | 受理成功 | `200` | `accepted=true`、`success=true`、`costMs=0` |

幂等细节：`logId` 是一次调度执行的全局幂等主键。占位在进入线程池**之前**预约，
第 5/6 步之外的同步失败（如线程池抛 `RejectedExecutionException`）会释放占位以允许后续重试；
未开启 Redis 时为 JVM 本地 Caffeine（仅保护本节点，TTL 上限 300s，容量 65536），
开启 `orbit.executor.execution-idempotency-enabled=true` 后为跨副本 `SET NX + TTL`（Redis 不可用时**直接拒绝触发**而非放行）。

真实执行结果通过 `POST /orbit/admin/callback` 回传（§3.3），本接口不会返回业务成败。

### 6.2 `GET /orbit/executor/handlers` — 查询本节点 Handler 与身份信息

```bash
curl http://localhost:8081/orbit/executor/handlers -H 'X-Orbit-Token: <token>'
```

响应（裸对象，无 `ApiResult` 包装）：

```json
{
  "appName": "demo-executor",
  "address": "http://10.0.1.5:8081",
  "nodeId": "orbit-executor-7d9f8b6c5-x2kqz",
  "handlers": ["dailyReport", "dataSync", "manualClean", "flaky", "alwaysFail"]
}
```

| 字段 | 说明 |
|------|------|
| `appName` | `orbit.executor.app-name` |
| `address` | 实际注册给调度中心的地址（显式配置 > `POD_IP` > 本机 IP > `127.0.0.1`，端口默认继承 `server.port`） |
| `nodeId` | 未配置 `node-id` 时取 `POD_NAME` / 主机名 |
| `handlers` | 本节点 `@OrbitJob` 名称列表 |

用途：确认「任务里的 `handler` 是否真的部署在目标执行器上」、核对 `address` 推导是否正确（K8s 网络问题的第一现场）。
令牌校验失败同样返回 `403`。

---

## 7. 数据模型字典

### 7.1 `JobInfo` — 任务定义

| 字段 | 类型 | 写 | 读 | 默认 | 约束 / 说明 |
|------|------|-----|-----|------|-------------|
| `id` | long | ✏️ 忽略 | ✅ | — | 主键；**请求体不要带**（见 §4.3） |
| `jobName` | string | ✅（仅创建） | ✅ | — | 唯一；`[A-Za-z0-9_-.]{1,64}`；不可改名 |
| `description` | string | ✅ | ✅ | `null` | ≤256；空白串按 `NULL` 存 |
| `appName` | string | ✅ | ✅ | — | **必填**，≤64；须与执行器 `orbit.executor.app-name` 一致 |
| `handler` | string | ✅ | ✅ | — | **必填**，≤128；`@OrbitJob` 名 |
| `cron` | string | ✅ | ✅ | `null` | ≤64；Quartz 6/7 段 Cron，须过 `CronExpression.isValidExpression`；空 = 仅手动 |
| `params` | object | ✅ | ✅ | `{}` | 任意 JSON 对象；序列化后 ≤2000；触发时透传给 `JobContext` |
| `timeoutSeconds` | int | ✅ | ✅ | `300` | ≤0 回落 300；超过 `orbit.admin.max-timeout-seconds` 被封顶；既是执行器超时中断依据，也影响孤儿回收阈值 |
| `routeStrategy` | string | ✅ | ✅ | `ROUND` | `ROUND` / `RANDOM` / `FIRST` / `CONSISTENT_HASH`；大小写不敏感、入库为大写；列宽 16 |
| `retryCount` | int | ✅ | ✅ | `0` | `[0,10]`（不含首次）；两层重试语义见 README §4.2 |
| `retryIntervalSeconds` | int | ✅ | ✅ | `10` | `[0,3600]`；`0` = 立即重试 |
| `serialExecution` | boolean | ✅ | ✅ | `null` | `null` 跟随全局 `dispatch-serial-per-job`；`true`/`false` 任务级覆盖 |
| `enabled` | boolean | ✅ | ✅ | `true` | `false` = 不注册 Quartz（暂停态持久化） |
| `version` | int | ✏️ 忽略 | ✅ | — | 乐观锁版本，每次更新 +1 |
| `createdAt` / `updatedAt` | date | ✏️ 忽略 | ✅ | — | 服务端写入 |

### 7.2 `JobLog` — 执行日志

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | long | 自增主键（列表按它降序） |
| `logId` | string | 32 位无连字符 UUID，**单次调度全链路唯一**，也是回传幂等键（库上唯一索引） |
| `jobId` / `jobName` / `appName` / `handler` | — | 触发时刻的任务快照 |
| `executorAddress` | string | 实际派发的目标地址（受理成功后立刻写入；未派发成功时可能为 `null`） |
| `status` | string | `RUNNING` / `SUCCESS` / `FAILED`（状态机：`RUNNING → SUCCESS|FAILED`，终态不可再改） |
| `message` | string | 执行结果文本 / 失败原因，≤2000 字符（超长尾部 `...` 截断）；重试轮次在此标注 `attempt i/N` |
| `costMs` | long | 执行器本地计时，覆盖该 `logId` 的全部重试轮次 |
| `startTime` / `endTime` | date | 触发时刻 / 收敛终态时刻（`RUNNING` 时 `endTime` 为 `null`） |

### 7.3 `TriggerRequest` — 调度中心 → 执行器的触发载荷

| 字段 | 类型 | 说明 |
|------|------|------|
| `jobId` | long | 任务 ID（缺 ID 时为 `0`） |
| `jobName` / `appName` / `handler` | string | 任务与执行器标识 |
| `logId` | string | 幂等主键 + 全链路追踪 ID |
| `params` | object | 任务静态参数与本次临时参数的合并结果（后者覆盖前者） |
| `timeoutSeconds` | int | 执行器超时强制中断窗口（看门狗 `cancel(true)`） |
| `retryCount` / `retryIntervalSeconds` | int | 执行级重试参数（同一 `logId` 内重跑，中间失败不回传） |

### 7.4 `TriggerResult` — 一个实体、两种方向

| 字段 | 类型 | 触发回执（`/run` 响应） | 执行结果（`/callback` 请求体） |
|------|------|------------------------|-------------------------------|
| `logId` | string | 原样带回 | **必填**（定位日志行） |
| `jobId` | long | 原样带回 | 原样带回 |
| `accepted` | boolean | `true` = 已受理入队 | 恒 `false` |
| `success` | boolean | 受理是否成功 | **业务真实成败** |
| `message` | string | 受理说明 / 失败原因 | 结果文本 / 异常摘要 |
| `costMs` | long | 恒 `0` | 实际耗时（含全部重试轮次） |
| `workerNode` | string | 承接节点标识（缺省时调度中心用地址回填） | 承接节点标识 |
| `jobName` / `appName` / `handler` | string | 通常为 `null` | 由新版 SDK 回填，供告警事件直接取用 |

### 7.5 枚举常量

| 名称 | 取值 | 出现位置 |
|------|------|----------|
| `RouteStrategy` | `ROUND`（轮询，默认） / `RANDOM`（等概率） / `FIRST`（地址序首节点） / `CONSISTENT_HASH`（任务名为哈希键的粘性路由，160 虚拟节点） | `JobInfo.routeStrategy`；`orbit_job.route_strategy`；非法值创建/更新时 400 |
| `JobLogStatus` | `RUNNING` / `SUCCESS` / `FAILED` | `JobLog.status`；`GET /logs?status=` |
| 告警事件类型 `JobAlertEvent` | `EXECUTION_FAILED`（执行最终失败） / `TRIGGER_FAILED`（触发最终失败或派发饱和） / `EXECUTION_LOST`（结果丢失，被僵尸回收） | 仅出现在 `OrbitAlertHandler` 实现内，不经 REST 暴露 |
| Quartz `triggerState` | `NONE` / `WAITING` / `ACQUIRED` / `RUNNING` / `BLOCKED` / `PAUSED` / `PAUSED_BLOCKED` / `ERROR` | `GET /jobs/{name}` 的 `quartz.triggerState` |

### 7.6 HTTP 状态码与错误包络

| HTTP | `body.code` | 触发条件 | 响应体形态 |
|------|-------------|----------|-----------|
| `200` | 200 | 成功（**含受理型失败**：触发被拒、handler 不存在等业务结果仍走 200，看 `data.success`/`data.accepted`） | `ApiResult`（执行器端为裸对象） |
| `400` | 400 | 入参校验失败、类型不匹配、资源不存在（`job not found`）、回调批次超限 | `ApiResult.fail(400, msg)` |
| `401` | 401 | 调度中心令牌缺失/错误（`orbit.admin.access-token` 已配置时） | 固定 JSON（与 `ApiResult` 同构） |
| `403` | 403 | 执行器令牌错误（`orbit.executor.access-token` 已配置时） | `ApiResult.fail(403, msg)` |
| `500` | 500 | 未预期异常、非法 JSON/空 body、乐观锁冲突、存储层故障 | `ApiResult.fail(500, "internal error")`（**不回显内部细节**，堆栈只在服务端日志） |

---

## 8. 字段长度与校验规则

上限的唯一事实来源是 `ColumnLimits`，与 `orbit-admin/src/main/resources/schema.sql`、`deploy/sql/schema-*.sql` 一一对应；
超限在**入参校验阶段**返回 400 并带字段名（不等到入库报 SQL 异常）。

| 端点 / 模型 | 字段 | 上限 | 超限响应 |
|------------|------|------|----------|
| `POST`/`PUT /orbit/admin/jobs` | `jobName` | 64 + 正则 `[A-Za-z0-9_-.]{1,64}` | `400 jobName must match [A-Za-z0-9_-.]{1,64}`（仅创建时校验正则） |
| | `description` | 256 | `400 description too long: length N exceeds limit 256` |
| | `appName` | 64（非空） | `400 appName required` / `... exceeds limit 64` |
| | `handler` | 128（非空） | `400 handler required` / `... exceeds limit 128` |
| | `cron` | 64 + 合法性 | `400 invalid cron: <expr>` / `400 cron too long: ...` |
| | `params` | 序列化后 2000 | `400 params too long: length N exceeds limit 2000` |
| | `routeStrategy` | 枚举 + 16 | `400 routeStrategy must be one of ROUND/RANDOM/FIRST/CONSISTENT_HASH: <v>` |
| | `retryCount` | `[0,10]` | `400 retryCount must be within [0,10]` |
| | `retryIntervalSeconds` | `[0,3600]` | `400 retryIntervalSeconds must be within [0,3600]` |
| | `timeoutSeconds` | `≤ max-timeout-seconds` | 不报错，**封顶 + WARN**（`≤0` 回落 300） |
| `POST /orbit/admin/registry` | `appName` / `address` / `nodeId` | 64 / 256 / 128 | `400 <field> too long: length N exceeds limit M` |
| | `handlers` | JSON 化 2000 | 不报错，**从尾部截断条目** + WARN |
| `POST /orbit/admin/callback` | 批次条数 | 2000 | `400 callback batch too large: N (max 2000); split the batch and retry` |
| `GET /orbit/admin/logs` | `size` | 200 | 不报错，静默钳位 |
| 日志写入 | `message` | 2000 | 不报错，截断为 `…`（保留 1997 字符 + `...`） |

---

## 9. 错误信息目录

`msg` 为服务端返回的**原文**（`%s` 处为实际值）。列出的分类有助于把「自己传错了」与「服务端/链路异常」分开处理。

### 9.1 参数与资源（`400`，调度中心）

| `msg` | 触发端点 | 处置 |
|-------|----------|------|
| `jobName must match [A-Za-z0-9_-.]{1,64}` | 创建 | 改名为字母/数字/`_`/`-`/`.` |
| `job already exists: %s` | 创建 | 先查后改用 `PUT`；并发抢占属正常 |
| `job not found: %s` | 详情 / 更新 / 删除 / 暂停 / 恢复 / 触发 | **注意是 400 而非 404** |
| `appName required` | 创建 / 更新 / 注册 | 必填 |
| `handler required` | 创建 / 更新 | 必填 |
| `invalid cron: %s` | 创建 / 更新 | 用 Quartz 6/7 段语法（秒 分 时 日 月 周 [年]） |
| `routeStrategy must be one of ROUND/RANDOM/FIRST/CONSISTENT_HASH: %s` | 创建 / 更新 | 拼写检查 |
| `retryCount must be within [0,10]` / `retryIntervalSeconds must be within [0,3600]` | 创建 / 更新 | 收敛配置 |
| `%s too long: length %d exceeds limit %d` | 创建 / 更新 / 注册 | 见 §8 |
| `status must be one of RUNNING/SUCCESS/FAILED: %s` | 日志查询 | 大小写不敏感，值必须属于集合 |
| `invalid value for '%s': %s` | 日志查询（`page`/`size`/`from`/`to`） | 分页须为整数；时间须 ISO 本地日期时间（不带时区/`Z`） |
| `address required` / `invalid executor address: %s` / `executor address must use http or https: %s` / `executor address must not contain user-info, query or fragment: %s` / `executor address must contain a host: %s` / `executor address has an invalid port: %s` / `executor address host cannot be resolved: %s` / `executor address must not be a reserved address: %s` / `executor address must not be a reserved IPv6 address: %s` / `executor address rejected by allow-pattern: %s` | 注册 | 检查 `orbit.executor.address` 推导结果与 `orbit.admin.executor-address-allow-pattern`；校验结果（含拒绝）缓存 60s，改配置后需等一个缓存周期或重启 |
| `callback batch too large: %d (max 2000); split the batch and retry` | 回传 | 拆批重发（逐条幂等，重发安全） |

### 9.2 鉴权（`401` / `403`）

| HTTP | `msg` | 触发 |
|------|-------|------|
| `401` | `invalid access token` | 调度中心：未带 / 带错 `X-Orbit-Token` 与 `Authorization: Bearer`，或服务端未配置却期望？（**服务端未配置令牌时全部放行**） |
| `403` | `invalid access token` | 执行器：`orbit.executor.access-token` 非空且 `X-Orbit-Token` 不匹配；同时写审计 WARN |

### 9.3 受理与执行失败（`200`，看 `data.message`）

| `message` | 产生位置 | 含义 |
|-----------|----------|------|
| `no online executor for appName=%s` | 调度中心派发 | 该 `appName` 无在线节点：核对执行器已启动、`app-name` 与任务一致、注册未超时 |
| `executor saturated: job queue full (workers=%d, queueCapacity=%d); consider scaling executor replicas or raising orbit.executor.queue-capacity` | 执行器受理 | 单节点并发+队列打满；扩容副本或调大队列 |
| `scheduler saturated: trigger queue full (threads=%d, queueCapacity=%d)` | 调度中心触发通道 | 触发通道打满，**不做触发级重试**，只写 `FAILED` 日志 + 告警 |
| `manual trigger rejected: previous run of job '%s' is still awaiting callback (orbit.admin.dispatch-serial-per-job=true)` | 手动触发 | 同名任务上一轮结果未回传；等收敛后重试，或任务级 `serialExecution=false` |
| `handler not found on this executor: %s` | 执行器受理 | 部署缺该 `@OrbitJob`，用 `GET /orbit/executor/handlers` 核对 |
| `duplicate request ignored: logId already accepted` | 执行器受理 | `logId` 幂等命中（超时重试的常见现象），非错误 |
| `connection refused: ...` / `connect timed out: ...` / `read timed out: ...` / `unknownhost: ...` / `network unreachable: ...` | 调度中心派发（`ExecutorClient` 归一化） | 网络/端口/服务发现问题；前两类会触发摘除 + failover，`read timed out` 属「结果不明」只换节点不摘节点 |
| `empty response from executor` | 调度中心派发 | 执行器返回 204/空体 |
| `dispatch failed: %s` | 调度中心派发 | 派发过程抛异常（路由、合并参数、落库环节），日志已被兜底收敛为 `FAILED` |
| `%s (trigger retry %d/%d scheduled in %ds)` | 触发级重试中 | 本次尝试失败但已安排重试 |
| `%s (trigger failed after %d/%d attempt(s))` | 触发级重试耗尽 | 终局失败，已投递 `TRIGGER_FAILED` 告警 |
| `execution timed out on executor after %ds (timeoutSeconds=%d, attempt %d/%d)` | 执行器回传 | 看门狗超时中断（`interrupted`） |
| `execution cancelled on executor (attempt %d/%d)` / `executor interrupted while running job (attempt %d/%d)` | 执行器回传 | 执行被取消/中断 |
| `%s (attempt %d/%d)` | 执行器回传 | handler 抛异常，重试链中 |
| `%s (attempt %d/%d final; ...)` | 执行器回传 | 执行级重试耗尽后的终局失败（`EXECUTION_FAILED` 告警） |

### 9.4 服务端（`500`）

| `msg` | 说明 |
|-------|------|
| `internal error` | 兜底异常处理器输出。**故意不回显 `e.getMessage()`**（可能含 SQL 片段、表名、驱动类名）；完整堆栈在调度中心日志。常见根因：非法/空 JSON body、存储层乐观锁冲突（日志含 `job concurrent update, please retry`）、Quartz 编排失败（日志含 `schedule failed: ...`）、DB 不可用 |

---

## 10. 端到端调用示例

以下脚本假设调度中心在 `localhost:8080`、执行器样例已启动（`demo-executor`，`:8081`），未启用令牌。
设置令牌后，把 `-H 'X-Orbit-Token: <token>'` 补到每条 curl 上即可。

```bash
ADMIN=http://localhost:8080
TOK='X-Orbit-Token: <token>'

# 1) 确认执行器在线、handler 已注册
curl -s $ADMIN/orbit/admin/executors -H "$TOK" | jq '.data[] | {appName, address, handlers}'
curl -s $ADMIN/orbit/executor/handlers -H "$TOK" >/dev/null   # 直连执行器请看 §10.2

# 2) 创建任务（cron 为空 = 仅手动）
curl -s -X POST $ADMIN/orbit/admin/jobs -H "$TOK" -H 'Content-Type: application/json' -d '{
  "jobName":"dailyReportJob","description":"日报","appName":"demo-executor","handler":"dailyReport",
  "cron":"0 */2 * * * ?","params":{"bizDate":"yesterday"},
  "routeStrategy":"ROUND","timeoutSeconds":120,"retryCount":2,"retryIntervalSeconds":30,"enabled":true
}' | jq .data

# 3) 详情 + Quartz 计划
curl -s $ADMIN/orbit/admin/jobs/dailyReportJob -H "$TOK" | jq '.data.quartz'

# 4) 立即触发一次（拿受理回执 + logId）
LOG=$(curl -s -X POST $ADMIN/orbit/admin/jobs/dailyReportJob/trigger -H "$TOK" \
        -H 'Content-Type: application/json' -d '{"bizDate":"2026-09-15"}' | jq -r '.data.logId')

# 5) 轮询终态（执行器跑完后日志才从 RUNNING 变 SUCCESS/FAILED）
for i in 1 2 3 4 5; do
  S=$(curl -s "$ADMIN/orbit/admin/logs?size=1&jobName=dailyReportJob" -H "$TOK" | jq -r '.data.items[0].status')
  echo "attempt $i logId=$LOG status=$S"; [ "$S" != "RUNNING" ] && break; sleep 2
done

# 6) 暂停 / 恢复 / 删除（幂等，重复调用安全）
curl -s -X POST $ADMIN/orbit/admin/jobs/dailyReportJob/pause   -H "$TOK" | jq -c .
curl -s -X POST $ADMIN/orbit/admin/jobs/dailyReportJob/resume   -H "$TOK" | jq -c .
curl -s -X DELETE $ADMIN/orbit/admin/jobs/dailyReportJob        -H "$TOK" | jq -c .

# 7) 观测大盘 + 告警链路自检
curl -s $ADMIN/orbit/admin/overview -H "$TOK" | jq .data
curl -s -X POST $ADMIN/orbit/admin/alerts/test -H "$TOK" | jq .data
```

### 10.1 失败排查最短路径

```bash
# 最近失败（含 message 里的根因）
curl -s "$ADMIN/orbit/admin/logs?status=FAILED&size=10" -H "$TOK" \
  | jq -r '.data.items[] | "\(.startTime)  \(.jobName)  \(.message)"'

# 长时间 RUNNING：结果丢失/执行器未回传，等僵尸回收或检查执行器
curl -s "$ADMIN/orbit/admin/logs?status=RUNNING&size=10" -H "$TOK" | jq '.data.items[] | {logId, jobName, startTime}'
```

### 10.2 直连执行器（排障视角）

```bash
EX=http://10.0.1.5:8081
curl -s $EX/orbit/executor/handlers -H "$TOK" | jq .

# 手工模拟一次触发（等价于调度中心发出的请求；logId 自拟，注意幂等）
curl -s -X POST $EX/orbit/executor/run -H "$TOK" -H 'Content-Type: application/json' -d '{
  "jobId":0,"jobName":"manual","appName":"demo-executor","handler":"dailyReport",
  "logId":"manual-'$(date +%s)'","params":{"bizDate":"today"},
  "timeoutSeconds":60,"retryCount":0,"retryIntervalSeconds":0
}' | jq .
```

### 10.3 Python 最小客户端

```python
import requests

ADMIN = "http://localhost:8080"
HEADERS = {"X-Orbit-Token": "<token>"}          # 未配置令牌时删掉这行

def create_job(job: dict) -> dict:
    r = requests.post(f"{ADMIN}/orbit/admin/jobs", json=job, headers=HEADERS, timeout=10)
    body = r.json()
    if not body["success"]:
        raise RuntimeError(f"{r.status_code}: {body['msg']}")
    return body["data"]

def trigger_and_wait(job_name: str, timeout: float = 60.0) -> dict:
    import time
    acc = requests.post(f"{ADMIN}/orbit/admin/jobs/{job_name}/trigger",
                        json={}, headers=HEADERS, timeout=10).json()["data"]
    if not acc["accepted"]:
        raise RuntimeError(f"dispatch rejected: {acc['message']}")
    deadline = time.time() + timeout
    while time.time() < deadline:
        log = requests.get(f"{ADMIN}/orbit/admin/logs",
                           params={"jobName": job_name, "size": 1},
                           headers=HEADERS, timeout=10).json()["data"]["items"][0]
        if log["status"] != "RUNNING":
            return log
        time.sleep(1)
    raise TimeoutError(f"job {job_name} still RUNNING after {timeout}s")
```

---

## 11. OpenAPI 规范的使用方式

[docs/openapi.yaml](openapi.yaml) 是与本文同步维护的 **OpenAPI 3.0.3** 规范，覆盖调度中心 15 个端点与执行器 2 个端点，
含全部 schema、枚举、长度约束与各端点的错误响应。它不依赖任何运行时组件（项目刻意不引入 springdoc，保持零 UI 依赖）。

```bash
# 本地预览（任选其一）
npx @redocly/cli preview-docs docs/openapi.yaml
npx swagger-project/swagger-cli validate docs/openapi.yaml

# 生成静态 HTML 文档
npx @redocly/cli build-docs docs/openapi.yaml -o /tmp/orbit-api.html

# 生成客户端代码（Java / Python / TS）
npx @openapitools/openapi-generator-cli generate \
  -i docs/openapi.yaml -g python -o /tmp/orbit-client
```

导入 Postman：`Import → Upload Files → docs/openapi.yaml`（自动带上 `X-Orbit-Token` 的 securityScheme）。

若后续希望由代码自动生成规范（避免与实现漂移），可加 `springdoc-openapi-starter-webmvc-ui`
（Boot 3.x 对应 artifact），它会自动扫描 `AdminApiController` / `ExecutorController`；
届时建议让 `docs/openapi.yaml` 由 `/v3/api-docs` 导出生成，而非手工维护。

---

## 12. 兼容性与变更约定

- **版本策略**：接口随框架同版本号发布（无 URI 版本号，如 `/v1`）。协议 DTO 集中在 `orbit-core`，
  两端唯一共同依赖，字段只允许**新增（带默认值）与放宽约束**，不得改名、删字段或改变语义；
  枚举（`RouteStrategy`、`JobLogStatus`）取值即线协议，新增值须两端同步升级后才可使用。
- **两端版本对齐**：调度中心与执行器 SDK 必须同基线（当前 **JDK 21 + Spring Boot 3.5.x**）。
  老执行器不会因新增字段而崩溃（未知字段被忽略），但新增的**行为**（如回传回填 `jobName`）需要执行器侧升级才生效——
  例如 §3.3 的告警字段补齐逻辑正是为兼容未回填字段的旧版执行器而存在。
- **鉴权约定**：`X-Orbit-Token` 头名定义在 `OrbitProtocol`，两端共享同一份定义；改头名 = 破坏兼容，需同 major 版本演进。
- **错误语义稳定**：`400`（可自助修复）、`401`/`403`（令牌）、`500`（内部错误，`msg` 固定 `"internal error"`）三类不会回显堆栈；
  受理型失败走 `200 + data.accepted=false`，客户端不应只看 HTTP 状态判定成败。
- **已知接口限制**（README §8 的接口侧摘要）：
  1. 无按 `logId` 查询单条日志的端点；
  2. `trigger` 只返回受理回执，结果需轮询 `/logs`；
  3. `PUT /jobs/{name}` 为全量覆盖且不支持改名；
  4. 无批量创建/批量触发端点（脚本侧循环调用即可）；
  5. 无 CORS 配置，浏览器需经代理；
  6. 未提供独立的 Web 控制台 UI，管理界面请基于本文端点自行对接；
  7. 认证之外无授权/角色模型：能访问调度中心端口即拥有全部管理权限，请务必配置令牌并在网络层限制来源。

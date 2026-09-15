package com.orbit.admin.web;

import com.orbit.admin.alert.AlertDispatcher;
import com.orbit.admin.alert.JobAlertEvent;
import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.DispatchExecutor;
import com.orbit.admin.registry.ExecutorRegistry;
import com.orbit.admin.service.JobService;
import com.orbit.core.model.ApiResult;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.JobLog;
import com.orbit.core.model.PageResult;
import com.orbit.core.model.RegistryRequest;
import com.orbit.core.model.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度中心对外统一 RESTful API 控制器。
 * 端点涵盖：
 *
 *   - 执行器通信侧：
 *
 *       - {@code POST /orbit/admin/registry}：执行器注册与心跳上报
 *       - {@code POST /orbit/admin/registry/remove}：执行器主动下线
 *
 *   - 运维与管理控制侧：
 *
 *       - {@code /orbit/admin/jobs/**}：任务增删改查、暂停、恢复、即时手动触发
 *       - {@code /orbit/admin/logs}：任务调度执行日志分页查询（支持状态/时间范围过滤）
 *       - {@code /orbit/admin/executors}：在线执行器节点列表查询
 *       - {@code /orbit/admin/overview}：调度中心运行大盘统计数据
 *       - {@code /orbit/admin/alerts/test}：告警扩展点连通性自检
 *
 */
@RestController
@RequestMapping("/orbit/admin")
public class AdminApiController {

    private static final Logger log = LoggerFactory.getLogger(AdminApiController.class);

    private final JobService jobService;
    private final ExecutorRegistry registry;
    private final AdminProperties properties;
    private final DispatchExecutor dispatchExecutor;
    private final AlertDispatcher alertDispatcher;

    /**
     * @param jobService       任务与日志服务
     * @param registry         执行器注册表，供 /executors 查询
     * @param properties       调度中心配置，供 /overview 暴露水位
     * @param dispatchExecutor 触发线程池，供 /overview 暴露水位
     * @param alertDispatcher  告警分发器，供 /overview 暴露告警指标
     */
    public AdminApiController(JobService jobService, ExecutorRegistry registry, AdminProperties properties,
                              DispatchExecutor dispatchExecutor, AlertDispatcher alertDispatcher) {
        this.jobService = jobService;
        this.registry = registry;
        this.properties = properties;
        this.dispatchExecutor = dispatchExecutor;
        this.alertDispatcher = alertDispatcher;
    }

    // ==========================================
    // 1. 执行器注册与心跳管理
    // ==========================================

    /**
     * 接收执行器的心跳上报或初次注册请求。
     *
     * @param req   注册请求数据
     * @return 成功响应
     */
    @PostMapping("/registry")
    public ApiResult<Void> registry(@RequestBody RegistryRequest req) {
        registry.register(req);
        return ApiResult.ok();
    }

    /**
     * 接收执行器主动下线注销通知。
     *
     * @param req   下线请求数据
     * @return 成功响应
     */
    @PostMapping("/registry/remove")
    public ApiResult<Void> registryRemove(@RequestBody RegistryRequest req) {
        registry.remove(req.getAppName(), req.getAddress());
        return ApiResult.ok();
    }

    /**
     * 接收执行器回传的任务执行结果，把对应的 RUNNING 日志收敛到终态。
     *
     * 触发是异步的（执行器受理即回执），任务的真实成败只能经此端点送达，
     * 因此这里是调度日志从 RUNNING 走向 SUCCESS/FAILED 的唯一正常路径
     * （另一条是孤儿回收，用于执行器崩溃或回传丢失的兜底）。
     *
     * 执行器会把积压的结果打包成一个请求发送，因此请求体是一个结果数组；
     * 单条被忽略不影响同批其余结果。
     *
     * 幂等：存储层只允许 RUNNING -> 终态 的一次转换，所以执行器重试、重复回传
     * 以及与孤儿回收的竞态都不会覆盖已写入的真实结果。重复回传同样返回成功，
     * 避免执行器把「已处理过」误判为失败而无限重试。
     *
     * @param results 执行器回传的一批最终结果（accepted=false）
     * @return data 为本批中真正完成状态转换的条数
     */
    @PostMapping("/callback")
    public ApiResult<Integer> callback(@RequestBody List<TriggerResult> results) {
        return ApiResult.ok(jobService.handleCallbacks(results));
    }

    // ==========================================
    // 2. 任务元数据 CRUD 与调度控制
    // ==========================================

    /**
     * 分页查询任务列表。
     *
     * @param page     页码（默认 1）
     * @param size     每页大小（默认 10）
     * @param nameLike 任务名称模糊匹配
     * @return 任务分页数据
     */
    @GetMapping("/jobs")
    public ApiResult<PageResult<JobInfo>> pageJobs(@RequestParam(defaultValue = "1") int page,
                                                   @RequestParam(defaultValue = "10") int size,
                                                   @RequestParam(required = false) String nameLike) {
        return ApiResult.ok(jobService.page(nameLike, page, size));
    }

    /**
     * 查询任务详情及 Quartz 运行期状态。
     *
     * @param name 任务名称
     * @return 任务详情与 Quartz 信息
     */
    @GetMapping("/jobs/{name}")
    public ApiResult<Map<String, Object>> detail(@PathVariable("name") String name) {
        JobInfo job = jobService.get(name);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("job", job);
        data.put("quartz", jobService.quartzInfo(name));
        return ApiResult.ok(data);
    }

    /**
     * 创建定时任务。
     *
     * @param job 任务元数据
     * @return 创建成功的任务
     */
    @PostMapping("/jobs")
    public ApiResult<JobInfo> create(@RequestBody JobInfo job) {
        return ApiResult.ok(jobService.create(job));
    }

    /**
     * 修改任务信息，并热更新 Quartz 调度。
     *
     * @param name 任务名称
     * @param job  待更新的数据
     * @return 更新后的任务
     */
    @PutMapping("/jobs/{name}")
    public ApiResult<JobInfo> update(@PathVariable("name") String name, @RequestBody JobInfo job) {
        return ApiResult.ok(jobService.update(name, job));
    }

    /**
     * 删除任务。
     *
     * @param name 任务名称
     * @return 成功响应
     */
    @DeleteMapping("/jobs/{name}")
    public ApiResult<Void> delete(@PathVariable("name") String name) {
        jobService.delete(name);
        return ApiResult.ok();
    }

    /**
     * 暂停任务的定时触发。
     *
     * @param name 任务名称
     * @return 成功响应
     */
    @PostMapping("/jobs/{name}/pause")
    public ApiResult<Void> pause(@PathVariable("name") String name) {
        jobService.pause(name);
        return ApiResult.ok();
    }

    /**
     * 恢复任务的定时触发。
     *
     * @param name 任务名称
     * @return 成功响应
     */
    @PostMapping("/jobs/{name}/resume")
    public ApiResult<Void> resume(@PathVariable("name") String name) {
        jobService.resume(name);
        return ApiResult.ok();
    }

    /**
     * 手动立即触发一次任务执行。
     *
     * @param name   任务名称
     * @param params 本次单次执行的临时入参（可为空）
     * @return 任务执行结果
     */
    @PostMapping("/jobs/{name}/trigger")
    public ApiResult<TriggerResult> trigger(@PathVariable("name") String name,
                                            @RequestBody(required = false) Map<String, Object> params) {
        return ApiResult.ok(jobService.triggerNow(name, params));
    }

    // ==========================================
    // 3. 执行日志、在线执行器与运维总览
    // ==========================================

    /**
     * 分页查询调度日志，支持任务名、状态与时间范围过滤。
     *
     * 时间参数用 {@code LocalDateTime}（ISO 格式，如 2026-01-01T00:00:00）：
     * Spring 6 下 {@code ISO.DATE_TIME} 对 {@code java.util.Date} 的解析要求毫秒与时区，
     * 裸秒格式会直接 500；LocalDateTime 裸秒可解析，按 JVM 默认时区转 Date 后与
     * start_time（同为默认时区写库）口径一致。
     *
     * @param jobName 任务名称过滤
     * @param status  状态过滤（RUNNING / SUCCESS / FAILED，非法值拋 400）
     * @param from    起始时间过滤（含，ISO 格式如 2026-01-01T00:00:00）
     * @param to      截止时间过滤（含，ISO 格式）
     * @param page    页码
     * @param size    每页大小
     * @return 日志分页数据
     */
    @GetMapping("/logs")
    public ApiResult<PageResult<JobLog>> logs(@RequestParam(required = false) String jobName,
                                              @RequestParam(required = false) String status,
                                              @RequestParam(required = false)
                                              @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) java.time.LocalDateTime from,
                                              @RequestParam(required = false)
                                              @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) java.time.LocalDateTime to,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int size) {
        Date fromDate = from == null ? null : java.util.Date.from(from.atZone(java.time.ZoneId.systemDefault()).toInstant());
        Date toDate = to == null ? null : java.util.Date.from(to.atZone(java.time.ZoneId.systemDefault()).toInstant());
        return ApiResult.ok(jobService.pageLogs(jobName, status, fromDate, toDate, page, size));
    }

    /**
     * 告警扩展点连通性自检：向已配置的告警处理器异步发送一条 TEST 事件。
     *
     * 用于接入告警渠道后验证链路（事件 -> 分发器 -> 处理器）是否通畅：
     * 事件异步投递，本接口只返回受理结果，实际效果（日志/钉钉/邮件等）由
     * 处理器实现决定，通常在调用后秒级可见。
     *
     * @return 受理回执（含当前告警指标）
     */
    @PostMapping("/alerts/test")
    public ApiResult<Map<String, Object>> alertTest() {
        alertDispatcher.fire(new JobAlertEvent("TEST", "(test)", "(test)", "(test)", null,
                null, 0, "alert channel test from /orbit/admin/alerts/test", new Date()));
        Map<String, Object> data = new LinkedHashMap<String, Object>(alertDispatcher.metrics());
        data.put("dispatched", true);
        return ApiResult.ok(data);
    }

    /**
     * 查询在线执行器列表。
     *
     * @param appName 应用名（为空则查全部在线节点）
     * @return 在线执行器节点列表
     */
    @GetMapping("/executors")
    public ApiResult<List<ExecutorNode>> executors(@RequestParam(required = false) String appName) {
        if (appName == null || appName.trim().isEmpty()) {
            return ApiResult.ok(registry.listAll());
        }
        return ApiResult.ok(registry.listByApp(appName));
    }

    /**
     * 查询调度中心监控总览统计数据，含派发通道的实时指标
     * （在跑数 / 排队数 / 累计拒绝数 / 累计跳过数）与告警通道指标
     * （已投递 / 丢弃 / 失败计数）。
     *
     * @return 统计指标集合
     */
    @GetMapping("/overview")
    public ApiResult<Map<String, Object>> overview() {
        Map<String, Object> data = new LinkedHashMap<String, Object>(jobService.overview());
        data.putAll(dispatchExecutor.metrics());
        data.putAll(alertDispatcher.metrics());
        return ApiResult.ok(data);
    }

    // ==========================================
    // 4. 统一异常处理与安全校验
    // ==========================================

    /**
     * 捕获非法参数异常（400）
     */
    @ExceptionHandler(IllegalArgumentException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiResult<Void> badRequest(IllegalArgumentException e) {
        return ApiResult.fail(400, e.getMessage());
    }

    /**
     * 捕获请求参数类型不匹配（400）：如 status 拼错、时间格式不符合 ISO 格式。
     * 不拦的话会落到全局 500，调用方无法区分「自己传错了」与「服务端坏了」。
     */
    @ExceptionHandler(org.springframework.web.method.annotation.MethodArgumentTypeMismatchException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiResult<Void> badType(org.springframework.web.method.annotation.MethodArgumentTypeMismatchException e) {
        String name = e.getName() == null ? "param" : e.getName();
        return ApiResult.fail(400, "invalid value for '" + name + "': " + e.getValue());
    }

    /**
     * 捕获全局未处理异常（500）。
     *
     * 不把 {@code e.getMessage()} 回给调用方：其中可能包含 SQL 片段、表名、
     * 驱动类名等内部信息。完整堆栈只写服务端日志。
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ApiResult<Void> error(Exception e) {
        log.error("[orbit-admin] api error", e);
        return ApiResult.fail(500, "internal error");
    }

}

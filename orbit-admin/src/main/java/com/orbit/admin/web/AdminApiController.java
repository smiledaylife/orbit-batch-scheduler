package com.orbit.admin.web;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.dispatch.ExecutorClient;
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
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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
 *       - {@code /orbit/admin/logs}：任务调度执行日志分页查询
 *       - {@code /orbit/admin/executors}：在线执行器节点列表查询
 *       - {@code /orbit/admin/overview}：调度中心运行大盘统计数据
 *     
 */
@RestController
@RequestMapping("/orbit/admin")
public class AdminApiController {

    private static final Logger log = LoggerFactory.getLogger(AdminApiController.class);

    private final JobService jobService;
    private final ExecutorRegistry registry;
    private final AdminProperties properties;

    public AdminApiController(JobService jobService, ExecutorRegistry registry, AdminProperties properties) {
        this.jobService = jobService;
        this.registry = registry;
        this.properties = properties;
    }

    // ==========================================
    // 1. 执行器注册与心跳管理
    // ==========================================

    /**
     * 接收执行器的心跳上报或初次注册请求。
     *
     * @param req   注册请求数据
     * @param token HTTP Header 中的安全令牌
     * @return 成功响应
     */
    @PostMapping("/registry")
    public ApiResult<Void> registry(@RequestBody RegistryRequest req,
                                    @RequestHeader(value = ExecutorClient.TOKEN_HEADER, required = false) String token) {
        // 校验安全令牌
        checkToken(token, req.getAccessToken());
        // 注册或刷新节点
        registry.register(req);
        return ApiResult.ok();
    }

    /**
     * 接收执行器主动下线注销通知。
     *
     * @param req   下线请求数据
     * @param token HTTP Header 中的安全令牌
     * @return 成功响应
     */
    @PostMapping("/registry/remove")
    public ApiResult<Void> registryRemove(@RequestBody RegistryRequest req,
                                          @RequestHeader(value = ExecutorClient.TOKEN_HEADER, required = false) String token) {
        checkToken(token, req.getAccessToken());
        registry.remove(req.getAppName(), req.getAddress());
        return ApiResult.ok();
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
     * 分页查询调度日志。
     *
     * @param jobName 任务名称过滤
     * @param page    页码
     * @param size    每页大小
     * @return 日志分页数据
     */
    @GetMapping("/logs")
    public ApiResult<PageResult<JobLog>> logs(@RequestParam(required = false) String jobName,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int size) {
        return ApiResult.ok(jobService.pageLogs(jobName, page, size));
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
     * 查询调度中心监控总览统计数据。
     *
     * @return 统计指标集合
     */
    @GetMapping("/overview")
    public ApiResult<Map<String, Object>> overview() {
        return ApiResult.ok(jobService.overview());
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
     * 捕获全局未处理异常（500）。
     * <p>
     * 不再把 {@code e.getMessage()} 回给调用方：其中可能包含 SQL 片段、表名、
     * 驱动类名等内部信息。完整堆栈只写服务端日志。
     */
    @ExceptionHandler(Exception.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ApiResult<Void> error(Exception e) {
        log.error("[orbit-admin] api error", e);
        return ApiResult.fail(500, "internal error");
    }

    /**
     * 校验安全令牌。
     *
     * @param headerToken 请求头中的 Token
     * @param bodyToken   请求体中的 Token
     */
    private void checkToken(String headerToken, String bodyToken) {
        String expect = properties.getAccessToken();
        if (expect == null || expect.isEmpty()) {
            return;
        }
        String actual = headerToken != null && !headerToken.isEmpty() ? headerToken : bodyToken;
        // 常量时间比对（MessageDigest.isEqual）：抵御时序侧信道逐字节猜测令牌
        if (actual == null || !MessageDigest.isEqual(
                expect.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("invalid access token");
        }
    }
}

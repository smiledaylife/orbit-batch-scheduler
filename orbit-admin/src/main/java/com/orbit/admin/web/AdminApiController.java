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
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度中心 REST API。
 *
 * <pre>
 * 执行器侧：
 *   POST /orbit/admin/registry     注册/心跳
 *   POST /orbit/admin/registry/remove  下线
 *
 * 管理侧：
 *   /orbit/admin/jobs/**   任务 CRUD / 触发
 *   /orbit/admin/logs      执行日志
 *   /orbit/admin/executors 在线执行器
 *   /orbit/admin/overview  总览
 * </pre>
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

    // -------- 执行器注册 --------

    @PostMapping("/registry")
    public ApiResult<Void> registry(@RequestBody RegistryRequest req,
                                    @RequestHeader(value = ExecutorClient.TOKEN_HEADER, required = false) String token) {
        checkToken(token, req.getAccessToken());
        registry.register(req);
        return ApiResult.ok();
    }

    @PostMapping("/registry/remove")
    public ApiResult<Void> registryRemove(@RequestBody RegistryRequest req,
                                          @RequestHeader(value = ExecutorClient.TOKEN_HEADER, required = false) String token) {
        checkToken(token, req.getAccessToken());
        registry.remove(req.getAppName(), req.getAddress());
        return ApiResult.ok();
    }

    // -------- 任务管理 --------

    @GetMapping("/jobs")
    public ApiResult<PageResult<JobInfo>> pageJobs(@RequestParam(defaultValue = "1") int page,
                                                   @RequestParam(defaultValue = "10") int size,
                                                   @RequestParam(required = false) String nameLike) {
        return ApiResult.ok(jobService.page(nameLike, page, size));
    }

    @GetMapping("/jobs/{name}")
    public ApiResult<Map<String, Object>> detail(@PathVariable("name") String name) {
        JobInfo job = jobService.get(name);
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("job", job);
        data.put("quartz", jobService.quartzInfo(name));
        return ApiResult.ok(data);
    }

    @PostMapping("/jobs")
    public ApiResult<JobInfo> create(@RequestBody JobInfo job) {
        return ApiResult.ok(jobService.create(job));
    }

    @PutMapping("/jobs/{name}")
    public ApiResult<JobInfo> update(@PathVariable("name") String name, @RequestBody JobInfo job) {
        return ApiResult.ok(jobService.update(name, job));
    }

    @DeleteMapping("/jobs/{name}")
    public ApiResult<Void> delete(@PathVariable("name") String name) {
        jobService.delete(name);
        return ApiResult.ok();
    }

    @PostMapping("/jobs/{name}/pause")
    public ApiResult<Void> pause(@PathVariable("name") String name) {
        jobService.pause(name);
        return ApiResult.ok();
    }

    @PostMapping("/jobs/{name}/resume")
    public ApiResult<Void> resume(@PathVariable("name") String name) {
        jobService.resume(name);
        return ApiResult.ok();
    }

    @PostMapping("/jobs/{name}/trigger")
    public ApiResult<TriggerResult> trigger(@PathVariable("name") String name,
                                            @RequestBody(required = false) Map<String, Object> params) {
        return ApiResult.ok(jobService.triggerNow(name, params));
    }

    // -------- 日志 / 执行器 / 总览 --------

    @GetMapping("/logs")
    public ApiResult<PageResult<JobLog>> logs(@RequestParam(required = false) String jobName,
                                              @RequestParam(defaultValue = "1") int page,
                                              @RequestParam(defaultValue = "10") int size) {
        return ApiResult.ok(jobService.pageLogs(jobName, page, size));
    }

    @GetMapping("/executors")
    public ApiResult<List<ExecutorNode>> executors(@RequestParam(required = false) String appName) {
        if (appName == null || appName.trim().isEmpty()) {
            return ApiResult.ok(registry.listAll());
        }
        return ApiResult.ok(registry.listByApp(appName));
    }

    @GetMapping("/overview")
    public ApiResult<Map<String, Object>> overview() {
        return ApiResult.ok(jobService.overview());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ApiResult<Void> badRequest(IllegalArgumentException e) {
        return ApiResult.fail(400, e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ApiResult<Void> error(Exception e) {
        log.error("[orbit-admin] api error", e);
        return ApiResult.fail(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }

    private void checkToken(String headerToken, String bodyToken) {
        String expect = properties.getAccessToken();
        if (expect == null || expect.isEmpty()) {
            return;
        }
        String actual = headerToken != null && !headerToken.isEmpty() ? headerToken : bodyToken;
        if (!expect.equals(actual)) {
            throw new IllegalArgumentException("invalid access token");
        }
    }
}

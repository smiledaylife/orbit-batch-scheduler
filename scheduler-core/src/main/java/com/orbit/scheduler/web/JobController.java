package com.orbit.scheduler.web;

import com.orbit.scheduler.core.DispatchSummary;
import com.orbit.scheduler.core.JobManager;
import com.orbit.scheduler.core.TaskRegistry;
import com.orbit.scheduler.model.ApiResult;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.http.HttpDispatchClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度管理 REST API（前缀 /api/scheduler）。
 *
 * <p>生产环境建议通过 K8s NetworkPolicy / Ingress 鉴权限制访问。
 *
 * <p><b>性能优化</b>：
 * <ul>
 *   <li>{@code detail} 接口直接调用 {@code findByName} 替代 page(name,1,1)+filter 二次过滤，
 *       避免无意义的 LIMIT 1 + 流式过滤</li>
 *   <li>{@code guessSelfIp} 缓存到字段，避免每请求一次 InetAddress.getLocalHost 调用</li>
 * </ul>
 *
 * @author orbit
 */
@RestController
@RequestMapping("/api/scheduler")
public class JobController {

    private static final Logger log = LoggerFactory.getLogger(JobController.class);

    private final JobManager jobManager;
    private final TaskRegistry taskRegistry;
    private final HttpDispatchClient httpDispatchClient;
    /** 自身 IP 缓存（节点级不变，启动时解析一次） */
    private final String cachedSelfIp;

    public JobController(JobManager jobManager, TaskRegistry taskRegistry, HttpDispatchClient httpDispatchClient) {
        this.jobManager = jobManager;
        this.taskRegistry = taskRegistry;
        this.httpDispatchClient = httpDispatchClient;
        this.cachedSelfIp = resolveSelfIpOnce();
    }

    // ---------------- 任务配置 CRUD ----------------

    @GetMapping("/jobs")
    public ApiResult<PageResult<JobConfig>> pageJobs(@RequestParam(defaultValue = "1") int page,
                                                     @RequestParam(defaultValue = "10") int size,
                                                     @RequestParam(required = false) String nameLike) {
        return ApiResult.ok(jobManager.pageJobs(nameLike, normalize(page), normalize(size)));
    }

    @GetMapping("/jobs/{name}")
    public ApiResult<Map<String, Object>> detail(@PathVariable("name") String name) {
        // 性能优化：直接 findByName 替代 page(name,1,1) 的模糊匹配 + 二次过滤
        JobConfig cfg = jobManager.findOneJob(name).orElse(null);
        if (cfg == null) {
            return ApiResult.notFound("task '" + name + "' not found");
        }
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("config", cfg);
        data.put("quartz", jobManager.getQuartzInfo(name));
        data.put("localExecutorPresent", taskRegistry.hasTask(name));
        return ApiResult.ok(data);
    }

    @PostMapping("/jobs")
    public ApiResult<JobConfig> create(@RequestBody JobConfig config) {
        return ApiResult.ok(jobManager.createJob(config));
    }

    @PutMapping("/jobs/{name}")
    public ApiResult<JobConfig> update(@PathVariable("name") String name, @RequestBody JobConfig config) {
        return ApiResult.ok(jobManager.updateJob(name, config));
    }

    @DeleteMapping("/jobs/{name}")
    public ApiResult<Void> delete(@PathVariable("name") String name) {
        jobManager.deleteJob(name);
        return ApiResult.ok();
    }

    // ---------------- 调度控制 ----------------

    @PostMapping("/jobs/{name}/pause")
    public ApiResult<Void> pause(@PathVariable("name") String name) {
        jobManager.pauseJob(name);
        return ApiResult.ok();
    }

    @PostMapping("/jobs/{name}/resume")
    public ApiResult<Void> resume(@PathVariable("name") String name) {
        jobManager.resumeJob(name);
        return ApiResult.ok();
    }

    /** 立即异步触发（走完整 Quartz 触发链路，与定时触发一致，受分布式锁保护） */
    @PostMapping("/jobs/{name}/trigger")
    public ApiResult<Void> trigger(@PathVariable("name") String name,
                                   @RequestBody(required = false) Map<String, Object> params) {
        jobManager.triggerNow(name, params);
        return ApiResult.ok();
    }

    /** 同步执行（当前节点直接派发并等待结果，受分布式锁保护） */
    @PostMapping("/jobs/{name}/execute")
    public ApiResult<DispatchSummary> execute(@PathVariable("name") String name,
                                              @RequestBody(required = false) Map<String, Object> params) {
        return ApiResult.ok(jobManager.executeSync(name, params));
    }

    // ---------------- 日志 / 集群 ----------------

    @GetMapping("/logs")
    public ApiResult<PageResult<JobLog>> pageLogs(@RequestParam(required = false) String taskName,
                                                  @RequestParam(defaultValue = "1") int page,
                                                  @RequestParam(defaultValue = "10") int size) {
        return ApiResult.ok(jobManager.pageLogs(taskName, normalize(page), normalize(size)));
    }

    @GetMapping("/nodes")
    public ApiResult<List<Map<String, Object>>> nodes() {
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        if (httpDispatchClient != null) {
            List<ServiceEndpoint> endpoints = httpDispatchClient.listEndpoints(null);
            String self = jobManager.getNodeId();
            String selfIp = cachedSelfIp;
            for (ServiceEndpoint ep : endpoints) {
                Map<String, Object> node = new LinkedHashMap<String, Object>();
                node.put("endpoint", ep.getUrl());
                node.put("self", ep.getUrl().contains(selfIp));
                list.add(node);
            }
            Map<String, Object> selfNode = new LinkedHashMap<String, Object>();
            selfNode.put("nodeId", self);
            selfNode.put("self", true);
            list.add(0, selfNode);
        }
        return ApiResult.ok(list);
    }

    @GetMapping("/overview")
    public ApiResult<Map<String, Object>> overview() {
        return ApiResult.ok(jobManager.overview());
    }

    // ---------------- 异常处理 ----------------

    @ExceptionHandler(IllegalArgumentException.class)
    public ApiResult<Void> badRequest(IllegalArgumentException e) {
        log.warn("[orbit-scheduler] bad request: {}", e.getMessage());
        return ApiResult.badRequest(e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ApiResult<Void> serverError(Exception e) {
        log.error("[orbit-scheduler] api error", e);
        return ApiResult.serverError(e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
    }

    private int normalize(int v) {
        return v <= 0 ? 10 : Math.min(v, 200);
    }

    /** 启动期一次性解析本机 IP，避免每请求一次 InetAddress.getLocalHost */
    private static String resolveSelfIpOnce() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "0.0.0.0";
        }
    }
}

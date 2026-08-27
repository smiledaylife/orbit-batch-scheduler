package com.orbit.scheduler.web;

import com.orbit.scheduler.core.DispatchSummary;
import com.orbit.scheduler.core.JobManager;
import com.orbit.scheduler.core.TaskRegistry;
import com.orbit.scheduler.http.HttpDispatchClient;
import com.orbit.scheduler.http.RemoteServiceClient;
import com.orbit.scheduler.model.ApiResult;
import com.orbit.scheduler.model.JobConfig;
import com.orbit.scheduler.model.JobLog;
import com.orbit.scheduler.model.PageResult;
import com.orbit.scheduler.model.RemoteServiceDefinition;
import com.orbit.scheduler.model.ServiceEndpoint;
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
 * <p>跨服务批量调度相关接口：
 * <ul>
 *   <li>{@code GET /remote-services} —— 已注册的外部业务服务</li>
 *   <li>{@code POST /remote-services} —— 运行时注册远程服务</li>
 *   <li>创建任务时 {@code dispatchType=REMOTE|WORKFLOW} 即可编排外部服务</li>
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
    private final RemoteServiceClient remoteServiceClient;
    /** 自身 IP 缓存（节点级不变，启动时解析一次） */
    private final String cachedSelfIp;

    public JobController(JobManager jobManager, TaskRegistry taskRegistry,
                         HttpDispatchClient httpDispatchClient) {
        this(jobManager, taskRegistry, httpDispatchClient, null);
    }

    public JobController(JobManager jobManager, TaskRegistry taskRegistry,
                         HttpDispatchClient httpDispatchClient,
                         RemoteServiceClient remoteServiceClient) {
        this.jobManager = jobManager;
        this.taskRegistry = taskRegistry;
        this.httpDispatchClient = httpDispatchClient;
        this.remoteServiceClient = remoteServiceClient;
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

    // ---------------- 远程服务注册（跨服务批量调度） ----------------

    @GetMapping("/remote-services")
    public ApiResult<Map<String, Object>> remoteServices() {
        if (remoteServiceClient == null) {
            return ApiResult.ok(emptyRemoteOverview());
        }
        return ApiResult.ok(remoteServiceClient.overview());
    }

    @GetMapping("/remote-services/{name}")
    public ApiResult<Map<String, Object>> remoteServiceDetail(@PathVariable("name") String name) {
        if (remoteServiceClient == null) {
            return ApiResult.notFound("RemoteServiceClient not available");
        }
        RemoteServiceDefinition def = remoteServiceClient.getRegistry().get(name);
        if (def == null) {
            return ApiResult.notFound("remote service '" + name + "' not registered");
        }
        Map<String, Object> data = new LinkedHashMap<String, Object>();
        data.put("definition", def);
        data.put("endpoints", remoteServiceClient.getRegistry()
                .resolveEndpoints(name, def.getPort() > 0 ? def.getPort() : 8080));
        return ApiResult.ok(data);
    }

    /**
     * 运行时注册/更新远程业务服务。
     * <pre>
     * POST /api/scheduler/remote-services
     * {
     *   "name": "order-service",
     *   "serviceName": "order-service",
     *   "port": 8080,
     *   "pathPrefix": "/api",
     *   "baseUrl": "",
     *   "defaultMethod": "POST"
     * }
     * </pre>
     */
    @PostMapping("/remote-services")
    public ApiResult<RemoteServiceDefinition> registerRemoteService(@RequestBody RemoteServiceDefinition def) {
        if (remoteServiceClient == null) {
            return ApiResult.serverError("RemoteServiceClient not available (spring-web missing?)");
        }
        if (def.getName() == null || def.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("remote service name is required");
        }
        remoteServiceClient.getRegistry().register(def);
        return ApiResult.ok(remoteServiceClient.getRegistry().get(def.getName()));
    }

    @DeleteMapping("/remote-services/{name}")
    public ApiResult<Void> unregisterRemoteService(@PathVariable("name") String name) {
        if (remoteServiceClient == null) {
            return ApiResult.serverError("RemoteServiceClient not available");
        }
        remoteServiceClient.getRegistry().unregister(name);
        return ApiResult.ok();
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

    private static Map<String, Object> emptyRemoteOverview() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("count", 0);
        m.put("services", new ArrayList<Object>());
        return m;
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

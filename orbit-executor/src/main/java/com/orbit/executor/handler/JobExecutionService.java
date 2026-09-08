package com.orbit.executor.handler;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.JobContext;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务执行服务：把「接收触发」与「执行业务方法」解耦，并提供三项生产级保障。
 * <p>
 * 核心能力（当 {@code orbit.executor.worker-threads > 0}，默认 8）：
 * <ol>
 *   <li><b>有界并发</b>：任务在专职线程池（orbit-job-worker-N）执行，单节点同时运行的任务数
 *       被限制为 worker-threads；超出部分进入有界队列排队，队列满则快速失败
 *       （executor saturated），保护业务应用不被触发风暴打爆。
 *       {@code queue-capacity} 设为 0 表示「不排队」：超过 worker-threads 的触发直接快速失败；</li>
 *   <li><b>超时强制</b>：按任务 {@code timeoutSeconds} 到期后 {@code future.cancel(true)}
 *       中断任务线程，使调度中心 HTTP 读超时放弃后，执行器上的任务不会继续<b>僵尸运行</b>、
 *       白占线程与资源。注意中断是尽力而为（best-effort）：
 *       响应 {@code InterruptedException} 的业务代码会被立即中止，CPU 密集死循环无法被打断；</li>
 *   <li><b>优雅停机</b>：应用关闭时先拒绝新任务、等待在跑任务收尾（最长 10 秒），超时再中断，
 *       避免硬杀导致业务半途而废。</li>
 * </ol>
 * <p>
 * 超时计时口径与调度中心一致：从<b>触发请求到达本节点</b>起算（含排队等待时间），
 * 与调度中心 HTTP 读超时同时开始，两边判定天然对齐。
 * <p>
 * 设为 {@code worker-threads: 0} 切换为内联模式（在 Web 请求线程内执行，无超时强制）。
 * 两种模式的 HTTP/JSON 协议一致：成功/失败都以同步 {@link TriggerResult} 返回。
 */
public class JobExecutionService implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionService.class);

    /** 优雅停机时等待在跑任务收尾的最长时间（秒） */
    private static final int SHUTDOWN_GRACE_SECONDS = 10;

    /** 单次任务等待下限（毫秒）：防止超时配置被误配成 0/负数导致所有任务瞬间「超时」 */
    private static final long MIN_WAIT_MS = 1000L;

    private final ExecutorProperties properties;

    /** 任务执行线程池；null 表示内联模式 */
    private final ThreadPoolExecutor pool;

    /** 生效的排队容量（负值归零后保存，仅用于日志与饱和提示，避免展示 -1 这类无意义值） */
    private final int queueCapacity;

    public JobExecutionService(ExecutorProperties properties) {
        this.properties = properties;
        int threads = properties.getWorkerThreads();
        if (threads > 0) {
            int queue = Math.max(0, properties.getQueueCapacity());
            this.queueCapacity = queue;
            // queue-capacity <= 0 语义为「不排队」，必须换成 SynchronousQueue 直接交付：
            // JDK 的 LinkedBlockingQueue 构造器要求 capacity > 0，传 0（或负数）会在
            // Bean 创建阶段抛 IllegalArgumentException，业务应用直接启动失败，
            // 而报错信息里只有 "capacity must be greater than zero"，极难定位到是本配置项。
            BlockingQueue<Runnable> workQueue = queue > 0
                    ? new LinkedBlockingQueue<Runnable>(queue)
                    : new SynchronousQueue<Runnable>();
            this.pool = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
                    workQueue, newJobThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy());
            // 空闲时允许回收核心线程，避免常驻占用
            this.pool.allowCoreThreadTimeOut(true);
            log.info("[orbit-executor] job worker pool initialized: workers={}, queueCapacity={} "
                    + "(timeout enforcement enabled)", threads,
                    queue > 0 ? String.valueOf(queue) : "0 (no queueing, hand-off only)");
        } else {
            this.pool = null;
            this.queueCapacity = 0;
            log.info("[orbit-executor] job worker pool disabled (worker-threads=0), "
                    + "jobs run inline on request threads without timeout enforcement");
        }
    }

    /**
     * 执行指定触发请求对应的 JobHandler。
     * 调用方（Web 控制器线程）将阻塞直至执行完成、超时或被拒绝（同步协议）；
     * 业务方法运行在专职工作线程，受超时强制与并发上限约束。
     *
     * @param request   触发请求
     * @param registry  JobHandler 注册表
     * @param workerNode 本节点标识（用于结果回填）
     * @return 执行结果
     */
    public TriggerResult execute(TriggerRequest request, JobHandlerRegistry registry, String workerNode) {
        String handler = request.getHandler();
        long start = System.currentTimeMillis();

        if (pool == null) {
            // 内联模式：直接在调用方线程内执行
            return invokeAndWrap(request, registry, workerNode, handler, start);
        }

        long waitMs = resolveWaitMs(request.getTimeoutSeconds());
        FutureTask<TriggerResult> task = new FutureTask<TriggerResult>(
                () -> invokeAndWrap(request, registry, workerNode, handler, start));
        try {
            pool.execute(task);
        } catch (RejectedExecutionException saturated) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode,
                    System.currentTimeMillis() - start,
                    "executor saturated: job queue full (workers=" + properties.getWorkerThreads()
                            + ", queueCapacity=" + queueCapacity
                            + "); consider scaling executor replicas or raising orbit.executor.queue-capacity");
        }
        Future<TriggerResult> future = task;

        try {
            return future.get(waitMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException te) {
            // 到期强制中断：防止调度中心放弃后任务在本地僵尸运行
            boolean interrupted = future.cancel(true);
            long cost = System.currentTimeMillis() - start;
            String msg = "execution timed out on executor after " + (cost / 1000) + "s (timeoutSeconds="
                    + request.getTimeoutSeconds() + ", interrupted=" + interrupted + ")";
            log.warn("[orbit-executor] handler '{}' logId={} {}", handler, request.getLogId(), msg);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost, msg);
        } catch (InterruptedException ie) {
            // 本线程（Web 容器请求线程）被中断：停机场景，取消任务并恢复中断标记
            future.cancel(true);
            Thread.currentThread().interrupt();
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode,
                    System.currentTimeMillis() - start, "executor interrupted while waiting for job completion");
        } catch (ExecutionException ee) {
            // invokeAndWrap 内部已兜底异常，理论上不可达；防御性处理
            Throwable c = ee.getCause() == null ? ee : ee.getCause();
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode,
                    System.currentTimeMillis() - start,
                    c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage());
        }
    }

    /**
     * 同步调用 handler 并包装为 TriggerResult（内联与线程池模式共用）。
     */
    private TriggerResult invokeAndWrap(TriggerRequest request, JobHandlerRegistry registry,
                                        String workerNode, String handler, long start) {
        try {
            JobContext ctx = new JobContext(request.getJobId(), request.getJobName(), handler,
                    request.getLogId(), request.getParams());
            Object ret = registry.invoke(handler, ctx);
            long cost = System.currentTimeMillis() - start;
            String msg = ret == null ? "OK" : String.valueOf(ret);
            log.info("[orbit-executor] run handler={} job={} logId={} {}ms",
                    handler, request.getJobName(), request.getLogId(), cost);
            return TriggerResult.ok(request.getLogId(), request.getJobId(), workerNode, cost, msg);
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            log.error("[orbit-executor] handler '{}' failed", handler, e);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    /**
     * 计算本次执行的等待上限（毫秒）：timeoutSeconds&gt;0 用之；否则用兜底最大值。
     * <p>
     * 结果保证不小于 {@link #MIN_WAIT_MS}：{@code orbit.executor.max-job-wait-seconds}
     * 被误配成 0 或负数时 {@code future.get(<=0)} 会立即抛 TimeoutException ——
     * 表现为「所有任务都在 0ms 超时失败」，和「任务真的跑不完」几乎无法区分。
     */
    private long resolveWaitMs(int timeoutSeconds) {
        int seconds = timeoutSeconds > 0 ? timeoutSeconds : properties.getMaxJobWaitSeconds();
        long ms = seconds * 1000L;
        return ms < MIN_WAIT_MS ? MIN_WAIT_MS : ms;
    }

    /**
     * 任务工作线程工厂：独立命名，便于线程 dump 定位；守护线程（不阻止 JVM 退出，
     * 停机收尾由 destroy() 负责）。
     */
    private static ThreadFactory newJobThreadFactory() {
        final AtomicInteger seq = new AtomicInteger(0);
        return r -> {
            Thread t = new Thread(r, "orbit-job-worker-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }

    /**
     * Spring 容器销毁回调：优雅停机。
     * 先停止接收新任务，给在跑任务最多 10 秒收尾，超时后强制中断，避免硬杀业务。
     */
    @Override
    public void destroy() {
        if (pool == null) {
            return;
        }
        pool.shutdown();
        try {
            if (!pool.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                log.warn("[orbit-executor] job worker pool still busy after {}s, forcing interrupt",
                        SHUTDOWN_GRACE_SECONDS);
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

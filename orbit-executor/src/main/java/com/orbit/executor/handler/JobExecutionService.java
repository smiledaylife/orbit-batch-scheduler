package com.orbit.executor.handler;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.JobContext;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
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
 *       （executor saturated），保护业务应用不被触发风暴打爆；</li>
 *   <li><b>超时强制</b>：按任务 {@code timeoutSeconds} 到期后 {@code future.cancel(true)}
 *       中断任务线程。旧实现中调度中心 HTTP 读超时放弃后，执行器上的任务会<b>永久僵尸运行</b>，
 *       继续占用线程与资源——本组件消除该问题。注意中断是尽力而为（best-effort）：
 *       响应 {@code InterruptedException} 的业务代码会被立即中止，CPU 密集死循环无法被打断；</li>
 *   <li><b>优雅停机</b>：应用关闭时先拒绝新任务、等待在跑任务收尾（最长 10 秒），超时再中断，
 *       避免硬杀导致业务半途而废。</li>
 * </ol>
 * <p>
 * 超时计时口径与调度中心一致：从<b>触发请求到达本节点</b>起算（含排队等待时间），
 * 与调度中心 HTTP 读超时同时开始，两边判定天然对齐。
 * <p>
 * 兼容性：设为 {@code worker-threads: 0} 可退回旧行为（在 Web 请求线程内联执行，无超时强制）。
 * HTTP/JSON 协议完全不变：成功/失败仍以同步 {@link TriggerResult} 返回。
 */
public class JobExecutionService implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionService.class);

    /** 优雅停机时等待在跑任务收尾的最长时间（秒） */
    private static final int SHUTDOWN_GRACE_SECONDS = 10;

    private final ExecutorProperties properties;

    /** 任务执行线程池；null 表示内联（旧版）模式 */
    private final ThreadPoolExecutor pool;

    public JobExecutionService(ExecutorProperties properties) {
        this.properties = properties;
        int threads = properties.getWorkerThreads();
        if (threads > 0) {
            int queue = Math.max(0, properties.getQueueCapacity());
            this.pool = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<Runnable>(queue), newJobThreadFactory(),
                    new ThreadPoolExecutor.AbortPolicy());
            // 空闲时允许回收核心线程，避免常驻占用
            this.pool.allowCoreThreadTimeOut(true);
            log.info("[orbit-executor] job worker pool initialized: workers={}, queueCapacity={} "
                    + "(timeout enforcement enabled)", threads, queue);
        } else {
            this.pool = null;
            log.info("[orbit-executor] job worker pool disabled (worker-threads=0), "
                    + "jobs run inline on request threads without timeout enforcement");
        }
    }

    /**
     * 执行指定触发请求对应的 JobHandler。
     * 调用方（Web 控制器线程）将阻塞直至执行完成、超时或被拒绝——与旧版同步协议一致；
     * 区别在于业务方法运行在专职工作线程，且受超时强制与并发上限约束。
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
            // 内联模式：保持与旧版完全一致的行为
            return invokeAndWrap(request, registry, workerNode, handler, start);
        }

        long waitMs = resolveWaitMs(request.getTimeoutSeconds());
        Future<TriggerResult> future;
        FutureTask<TriggerResult> task = new FutureTask<TriggerResult>(
                () -> invokeAndWrap(request, registry, workerNode, handler, start));
        try {
            pool.execute(task);
        } catch (RejectedExecutionException saturated) {
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode,
                    System.currentTimeMillis() - start,
                    "executor saturated: job queue full (workers=" + properties.getWorkerThreads()
                            + ", queueCapacity=" + properties.getQueueCapacity()
                            + "); consider scaling executor replicas or raising orbit.executor.queue-capacity");
        }
        future = task;

        long deadline = start + waitMs;
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
     */
    private long resolveWaitMs(int timeoutSeconds) {
        int seconds = timeoutSeconds > 0 ? timeoutSeconds : properties.getMaxJobWaitSeconds();
        long ms = seconds * 1000L;
        return ms <= 0 ? properties.getMaxJobWaitSeconds() * 1000L : ms;
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

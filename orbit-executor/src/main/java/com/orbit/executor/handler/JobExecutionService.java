package com.orbit.executor.handler;

import com.orbit.core.model.TriggerRequest;
import com.orbit.core.model.TriggerResult;
import com.orbit.executor.JobContext;
import com.orbit.executor.client.CallbackClient;
import com.orbit.executor.config.ExecutorProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 任务执行服务：受理触发、异步执行、结果回传。
 *
 * 调用契约是异步的：{@link #submit} 在任务入队后立即返回「已受理」回执，
 * 业务方法在工作线程池里跑，跑完由 {@link CallbackClient} 把最终结果推回调度中心。
 * 因此调用方（Web 请求线程）被占用的时间只有一次入队操作，与任务真实耗时无关。
 *
 * 四项保障：
 *
 * 1. 有界并发：任务在专职线程池（orbit-job-worker-N）执行，单节点同时运行的任务数
 *    被限制为 worker-threads（下限 1）；超出部分进入有界队列，队列满则同步返回失败
 *    （executor saturated），让调度中心立刻把这条日志判失败，而不是无声积压。
 *    queue-capacity 设为 0 表示「不排队」；
 * 2. 超时强制：按任务 timeoutSeconds 到期后由看门狗 cancel(true) 中断工作线程，
 *    并把超时作为失败结果回传。看门狗是全节点共享的一条调度线程，
 *    不为每次执行额外开线程。中断是尽力而为：响应 InterruptedException 的业务代码
 *    会被立即中止，CPU 密集死循环无法被打断；
 * 3. 结果必达：执行结果无论成功、失败还是超时都会回传；只有回传本身重试耗尽才丢失，
 *    此时调度中心的孤儿回收会兜底把日志判失败；
 * 4. 优雅停机：先拒绝新任务、等待在跑任务收尾（最长 10 秒），超时再中断，
 *    最后把回传队列里剩余的结果尽量发完。
 *
 * 超时计时口径：从触发请求到达本节点起算，含排队等待时间。
 */
public class JobExecutionService implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(JobExecutionService.class);

    /** 优雅停机时等待在跑任务收尾的最长时间（秒） */
    private static final int SHUTDOWN_GRACE_SECONDS = 10;

    /** 停机时等待回传队列发空的最长时间（秒） */
    private static final int CALLBACK_GRACE_SECONDS = 5;

    /** 单次任务等待下限（毫秒）：防止超时配置被误配成 0/负数导致所有任务瞬间「超时」 */
    private static final long MIN_WAIT_MS = 1000L;

    private final ExecutorProperties properties;
    private final CallbackClient callbackClient;

    /** 任务执行线程池 */
    private final ThreadPoolExecutor pool;

    /** 超时看门狗：全节点共享一条线程，负责在到期时中断对应的工作线程 */
    private final ScheduledExecutorService watchdog;

    /** 生效的排队容量（负值归零后保存，仅用于日志与饱和提示，避免展示 -1 这类无意义值） */
    private final int queueCapacity;

    /** 生效的工作线程数（下限 1） */
    private final int workerThreads;

    public JobExecutionService(ExecutorProperties properties, CallbackClient callbackClient) {
        this.properties = properties;
        this.callbackClient = callbackClient;
        this.workerThreads = Math.max(1, properties.getWorkerThreads());
        int queue = Math.max(0, properties.getQueueCapacity());
        this.queueCapacity = queue;

        // queue-capacity <= 0 语义为「不排队」，必须换成 SynchronousQueue 直接交付：
        // JDK 的 LinkedBlockingQueue 构造器要求 capacity > 0，传 0（或负数）会在
        // Bean 创建阶段抛 IllegalArgumentException，业务应用直接启动失败，
        // 而报错信息里只有 "capacity must be greater than zero"，极难定位到是本配置项。
        BlockingQueue<Runnable> workQueue = queue > 0
                ? new LinkedBlockingQueue<Runnable>(queue)
                : new SynchronousQueue<Runnable>();
        this.pool = new ThreadPoolExecutor(workerThreads, workerThreads, 60L, TimeUnit.SECONDS,
                workQueue, newJobThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        // 空闲时允许回收核心线程，避免常驻占用
        this.pool.allowCoreThreadTimeOut(true);

        this.watchdog = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "orbit-job-watchdog");
                t.setDaemon(true);
                return t;
            }
        });

        log.info("[orbit-executor] job worker pool initialized: workers={}, queueCapacity={} "
                        + "(async execution, result delivered by callback)",
                workerThreads, queue > 0 ? String.valueOf(queue) : "0 (no queueing, hand-off only)");
    }

    /**
     * 受理一次任务触发：入队后立即返回，不等待业务方法执行。
     *
     * @param request    触发请求
     * @param registry   JobHandler 注册表
     * @param workerNode 本节点标识（用于结果回填）
     * @return 受理回执（accepted=true）；线程池饱和时返回同步失败结果（accepted=false）
     */
    public TriggerResult submit(TriggerRequest request, JobHandlerRegistry registry, String workerNode) {
        final String handler = request.getHandler();
        final long start = System.currentTimeMillis();
        final long waitMs = resolveWaitMs(request.getTimeoutSeconds());

        Runnable work = new Runnable() {
            @Override
            public void run() {
                runWithTimeout(request, registry, workerNode, handler, start, waitMs);
            }
        };

        try {
            pool.execute(work);
        } catch (RejectedExecutionException saturated) {
            // 同步失败：调度中心收到后立刻把这条日志判失败，不会留在 RUNNING 等回传
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode,
                    System.currentTimeMillis() - start,
                    "executor saturated: job queue full (workers=" + workerThreads
                            + ", queueCapacity=" + queueCapacity
                            + "); consider scaling executor replicas or raising orbit.executor.queue-capacity");
        }
        return TriggerResult.accepted(request.getLogId(), request.getJobId(), workerNode,
                "accepted, queued on executor " + workerNode);
    }

    /**
     * 在工作线程内执行 handler，到期由看门狗中断，结束后把结果交给回传客户端。
     *
     * 超时用「共享看门狗 + FutureTask.cancel(true)」实现：工作线程自己跑 task.run()，
     * 看门狗到期时 cancel 会中断它。相比为每次执行再开一条线程，全节点只需一条看门狗线程。
     */
    private void runWithTimeout(TriggerRequest request, JobHandlerRegistry registry, String workerNode,
                                String handler, long start, long waitMs) {
        final AtomicBoolean timedOut = new AtomicBoolean(false);
        final FutureTask<TriggerResult> task = new FutureTask<TriggerResult>(
                () -> invokeAndWrap(request, registry, workerNode, handler, start));

        ScheduledFuture<?> deadline = watchdog.schedule(new Runnable() {
            @Override
            public void run() {
                // cancel 仅在任务仍处于 NEW 状态时返回 true；返回 false 说明任务
                // 刚好在到期前跑完，此时应保留真实结果，不能改判为超时。
                if (task.cancel(true)) {
                    timedOut.set(true);
                }
            }
        }, waitMs, TimeUnit.MILLISECONDS);

        TriggerResult result;
        try {
            task.run();
            result = outcome(task, timedOut.get(), request, workerNode, handler, start);
        } finally {
            deadline.cancel(false);
        }

        try {
            callbackClient.send(result);
        } catch (Exception e) {
            // 回传客户端本身不抛异常，这里只兜住极端情况，避免弄死工作线程
            log.error("[orbit-executor] failed to hand result to callback client, logId={}",
                    request.getLogId(), e);
        }
    }

    /**
     * 取出执行结果：超时优先判定，其次取 FutureTask 的结果或异常。
     */
    private TriggerResult outcome(FutureTask<TriggerResult> task, boolean timedOut, TriggerRequest request,
                                  String workerNode, String handler, long start) {
        long cost = System.currentTimeMillis() - start;
        if (timedOut) {
            String msg = "execution timed out on executor after " + (cost / 1000) + "s (timeoutSeconds="
                    + request.getTimeoutSeconds() + ")";
            log.warn("[orbit-executor] handler '{}' logId={} {}", handler, request.getLogId(), msg);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost, msg);
        }
        try {
            return task.get();
        } catch (CancellationException ce) {
            // 停机 shutdownNow 或看门狗与完成竞态：按失败回传，避免日志永久 RUNNING
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    "execution cancelled on executor");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    "executor interrupted while running job");
        } catch (ExecutionException ee) {
            // invokeAndWrap 内部已兜底异常，理论上不可达；防御性处理
            Throwable c = ee.getCause() == null ? ee : ee.getCause();
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage());
        }
    }

    /**
     * 同步调用 handler 并包装为 TriggerResult。
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
     *
     * 结果保证不小于 {@link #MIN_WAIT_MS}：{@code orbit.executor.max-job-wait-seconds}
     * 被误配成 0 或负数时看门狗会立刻触发 ——
     * 表现为「所有任务都在 0ms 超时失败」，和「任务真的跑不完」几乎无法区分。
     */
    private long resolveWaitMs(int timeoutSeconds) {
        int seconds = timeoutSeconds > 0 ? timeoutSeconds : properties.getMaxJobWaitSeconds();
        long ms = seconds * 1000L;
        return ms < MIN_WAIT_MS ? MIN_WAIT_MS : ms;
    }

    /**
     * 当前线程池水位，供测试与诊断读取。
     *
     * @return [工作线程数, 活跃数, 队列积压]
     */
    public int[] stats() {
        return new int[]{workerThreads, pool.getActiveCount(), pool.getQueue().size()};
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
     * 先停止接收新任务，给在跑任务最多 10 秒收尾，超时后强制中断；
     * 最后给回传队列 5 秒把剩余结果发完，减少「任务跑完但结果没送达」的窗口。
     */
    @Override
    public void destroy() {
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
        watchdog.shutdownNow();
        callbackClient.shutdown(CALLBACK_GRACE_SECONDS);
    }
}

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
 * 失败重试（执行级）：任务定义 retryCount > 0 时，执行失败/超时的任务在
 * 本节点内按 retryIntervalSeconds 延迟重跑 —— 同一 logId、同一日志行，
 * 中间失败不回传，只回传最终结果（成功或重试耗尽后的失败），
 * 对齐 XXL-JOB JobThread 的执行侧重试语义；业务方法可经 JobContext.attempt
 * 区分首轮与重试轮次。重试链路仅存在于本进程内存（执行器重启后链路丢失，
 * 调度中心孤儿回收兜底），重试再入队饱和时直接回传失败。
 *
 * 超时计时口径：从触发请求到达本节点起算，含排队等待时间；每轮重试各自享有完整超时。
 *
 * 线程模型：默认平台线程；开启 orbit.executor.worker-virtual-threads 后改用 JDK 21 虚拟线程
 * （有界并发、排队、饱和拒绝与超时中断语义完全不变，仅线程实现不同）。
 * 虚拟线程适合 HTTP/DB 等 IO 密集任务；synchronized 阻塞多的业务代码在 JDK 21 下会钉住
 * 载体线程，此时保持平台线程更稳妥（见 ExecutorProperties#workerVirtualThreads 注释）。
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

    /**
     * 执行级重试定时器：全节点共享一条线程，负责把失败任务的下一轮重试
     * 按延迟重新投递进工作线程池。只做时间触发，不执行任务。
     */
    private final ScheduledExecutorService retryTimer;

    /** 生效的排队容量（负值归零后保存，仅用于日志与饱和提示，避免展示 -1 这类无意义值） */
    private final int queueCapacity;

    /** 生效的工作线程数（下限 1） */
    private final int workerThreads;

    /**
     * 构造执行线程池与超时看门狗。
     *
     * 线程数下限 1、队列容量下限 0（0 表示不排队，改用 {@code SynchronousQueue} 直接交付）：
     * 二者误配成 0 或负数时退化为保守行为，而不是让业务应用启动失败。
     *
     * @param properties     执行器配置，提供线程数与队列容量
     * @param callbackClient 结果回传客户端，任务终态经它推回调度中心
     */
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
                workQueue, newJobThreadFactory(properties.isWorkerVirtualThreads()), new ThreadPoolExecutor.AbortPolicy());
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

        this.retryTimer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "orbit-executor-retry");
                t.setDaemon(true);
                return t;
            }
        });

        log.info("[orbit-executor] job worker pool initialized: workers={}, queueCapacity={}, threadType={} "
                        + "(async execution, result delivered by callback)",
                workerThreads, queue > 0 ? String.valueOf(queue) : "0 (no queueing, hand-off only)",
                properties.isWorkerVirtualThreads() ? "virtual" : "platform");
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
        return submit(request, registry, workerNode, 1);
    }

    /**
     * 受理一次任务触发（完整版本，支持执行轮次）。
     *
     * 首次受理由 {@code ExecutorController} 调用（attempt = 1）；
     * 执行级重试由本类内部调度（attempt 递增），不走这里。
     *
     * @param request    触发请求
     * @param registry   JobHandler 注册表
     * @param workerNode 本节点标识（用于结果回填）
     * @param attempt    执行轮次（1 起始）
     * @return 受理回执（accepted=true）；线程池饱和时返回同步失败结果（accepted=false）
     */
    public TriggerResult submit(TriggerRequest request, JobHandlerRegistry registry, String workerNode,
                                int attempt) {
        final String handler = request.getHandler();
        final long start = System.currentTimeMillis();
        final long waitMs = resolveWaitMs(request.getTimeoutSeconds());

        Runnable work = new Runnable() {
            @Override
            public void run() {
                runWithTimeout(request, registry, workerNode, handler, start, waitMs, attempt);
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
     * 在工作线程内执行 handler，到期由看门狗中断；失败且仍有重试额度时安排下一轮，
     * 否则把最终结果交给回传客户端。
     *
     * 超时用「共享看门狗 + FutureTask.cancel(true)」实现：工作线程自己跑 task.run()，
     * 看门狗到期时 cancel 会中断它。相比为每次执行再开一条线程，全节点只需一条看门狗线程。
     *
     * 重试语义：中间失败不回传（调度日志保持 RUNNING，串行守卫不释放），
     * 只在任务成功或重试耗尽时回传一次 —— 调度中心视角下整个重试链是同一条日志。
     * costMs 从首轮入队起累计，覆盖全部轮次。
     */
    private void runWithTimeout(TriggerRequest request, JobHandlerRegistry registry, String workerNode,
                                String handler, long start, long waitMs, int attempt) {
        final AtomicBoolean timedOut = new AtomicBoolean(false);
        final FutureTask<TriggerResult> task = new FutureTask<TriggerResult>(
                () -> invokeAndWrap(request, registry, workerNode, handler, start, attempt));

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
            result = outcome(task, timedOut.get(), request, workerNode, handler, start, attempt);
        } finally {
            deadline.cancel(false);
        }

        // 失败且仍有重试额度：不回传，安排下一轮（见类注释「失败重试（执行级）」）
        if (!result.isSuccess() && attempt <= request.getRetryCount()) {
            if (scheduleRetry(request, registry, workerNode, attempt + 1, result)) {
                return;
            }
            // 重试无法安排（饱和/停机）：落到底，按最终失败回传
            result = withRetryTrail(result, request.getRetryCount() + 1,
                    "retry attempt " + (attempt + 1) + " could not be queued");
        }

        try {
            callbackClient.send(withJobContext(result, request));
        } catch (Exception e) {
            // 回传客户端本身不抛异常，这里只兜住极端情况，避免弄死工作线程
            log.error("[orbit-executor] failed to hand result to callback client, logId={}",
                    request.getLogId(), e);
        }
    }

    /**
     * 安排下一轮执行级重试：延迟 retryIntervalSeconds 后重新投递进工作线程池。
     *
     * @return true 表示已安排成功；false 表示重试无法安排（工作池饱和或正在停机），
     *         调用方应立即把当前失败结果按终局回传
     */
    private boolean scheduleRetry(TriggerRequest request, JobHandlerRegistry registry, String workerNode,
                                  int nextAttempt, TriggerResult lastResult) {
        int intervalSec = Math.max(0, request.getRetryIntervalSeconds());
        long start = System.currentTimeMillis() - lastResult.getCostMs();
        try {
            retryTimer.schedule(() -> {
                try {
                    pool.execute(() -> runWithTimeout(request, registry, workerNode,
                            request.getHandler(), start, resolveWaitMs(request.getTimeoutSeconds()), nextAttempt));
                } catch (RejectedExecutionException saturated) {
                    log.warn("[orbit-executor] retry attempt {} of logId={} rejected by worker pool",
                            nextAttempt, request.getLogId());
                }
            }, intervalSec, TimeUnit.SECONDS);
            log.warn("[orbit-executor] handler '{}' logId={} attempt {}/{} failed ({}), retry in {}s",
                    request.getHandler(), request.getLogId(), nextAttempt - 1, request.getRetryCount() + 1,
                    lastResult.getMessage(), intervalSec);
            return true;
        } catch (RejectedExecutionException shutdown) {
            // retryTimer 已关闭（应用停机）：重试链路中断，交由调用方按终局回传
            log.warn("[orbit-executor] retry for logId={} abandoned: executor is shutting down",
                    request.getLogId());
            return false;
        }
    }

    /**
     * 给结果附加重试轨迹说明（重试链异常中断时的终局信息）。
     */
    private static TriggerResult withRetryTrail(TriggerResult result, int maxAttempts, String trail) {
        String base = result.getMessage() == null ? "" : result.getMessage();
        result.setMessage(base + " (attempt " + maxAttempts + "/" + maxAttempts + " final; " + trail + ")");
        return result;
    }

    /**
     * 回传结果回填任务上下文（jobName / appName / handler）：
     * 调度中心据此构造告警事件，无需反查数据库；字段缺失时调度中心会自行兜底。
     */
    private static TriggerResult withJobContext(TriggerResult result, TriggerRequest request) {
        result.setJobName(request.getJobName());
        result.setAppName(request.getAppName());
        result.setHandler(request.getHandler());
        return result;
    }

    /**
     * 取出执行结果：超时优先判定，其次取 FutureTask 的结果或异常；
     * 终局结果会在 message 中标注执行轮次（attempt i/N）。
     */
    private TriggerResult outcome(FutureTask<TriggerResult> task, boolean timedOut, TriggerRequest request,
                                  String workerNode, String handler, long start, int attempt) {
        long cost = System.currentTimeMillis() - start;
        int maxAttempts = request.getRetryCount() + 1;
        if (timedOut) {
            String msg = "execution timed out on executor after " + (cost / 1000) + "s (timeoutSeconds="
                    + request.getTimeoutSeconds() + ", attempt " + attempt + "/" + maxAttempts + ")";
            log.warn("[orbit-executor] handler '{}' logId={} {}", handler, request.getLogId(), msg);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost, msg);
        }
        try {
            TriggerResult r = task.get();
            return attempt > 1 ? withAttemptNote(r, attempt, maxAttempts) : r;
        } catch (CancellationException ce) {
            // 停机 shutdownNow 或看门狗与完成竞态：按失败回传，避免日志永久 RUNNING
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    "execution cancelled on executor (attempt " + attempt + "/" + maxAttempts + ")");
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    "executor interrupted while running job (attempt " + attempt + "/" + maxAttempts + ")");
        } catch (ExecutionException ee) {
            // invokeAndWrap 内部已兜底异常，理论上不可达；防御性处理
            Throwable c = ee.getCause() == null ? ee : ee.getCause();
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    (c.getMessage() == null ? c.getClass().getSimpleName() : c.getMessage())
                            + " (attempt " + attempt + "/" + maxAttempts + ")");
        }
    }

    /** 成功结果的重试轮次标注：仅重试轮次才追加，首轮保持原始 message */
    private static TriggerResult withAttemptNote(TriggerResult result, int attempt, int maxAttempts) {
        String base = result.getMessage() == null ? "OK" : result.getMessage();
        result.setMessage(base + " (attempt " + attempt + "/" + maxAttempts + ")");
        return result;
    }

    /**
     * 同步调用 handler 并包装为 TriggerResult。
     */
    private TriggerResult invokeAndWrap(TriggerRequest request, JobHandlerRegistry registry,
                                        String workerNode, String handler, long start, int attempt) {
        try {
            JobContext ctx = new JobContext(request.getJobId(), request.getJobName(), handler,
                    request.getLogId(), request.getParams(), attempt);
            Object ret = registry.invoke(handler, ctx);
            long cost = System.currentTimeMillis() - start;
            String msg = ret == null ? "OK" : String.valueOf(ret);
            log.info("[orbit-executor] run handler={} job={} logId={} attempt={} {}ms",
                    handler, request.getJobName(), request.getLogId(), attempt, cost);
            return TriggerResult.ok(request.getLogId(), request.getJobId(), workerNode, cost, msg);
        } catch (Exception e) {
            long cost = System.currentTimeMillis() - start;
            log.error("[orbit-executor] handler '{}' failed (attempt {})", handler, attempt, e);
            return TriggerResult.fail(request.getLogId(), request.getJobId(), workerNode, cost,
                    e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
        }
    }

    /**
     * 计算本次执行的等待上限（毫秒）：{@code timeoutSeconds > 0} 用之；否则用兜底最大值。
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
     *
     * JDK 21 虚拟线程模式下由 {@link Thread#ofVirtual()} 提供工厂：虚拟线程恒为守护态，
     * 命名规则（orbit-job-worker-N）与平台线程保持一致。
     *
     * @param virtual true 使用虚拟线程（JDK 21），false 使用传统平台线程
     */
    private static ThreadFactory newJobThreadFactory(boolean virtual) {
        if (virtual) {
            return Thread.ofVirtual().name("orbit-job-worker-", 0).factory();
        }
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
        // 先关重试定时器再等回传：未触发的重试直接放弃，
        // 正在执行的轮次仍会按终局回传（scheduleRetry 已无法安排时也会回传失败）。
        retryTimer.shutdownNow();
        watchdog.shutdownNow();
        callbackClient.shutdown(CALLBACK_GRACE_SECONDS);
    }
}

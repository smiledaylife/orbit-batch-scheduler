package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.service.JobService;
import com.orbit.core.model.JobInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定时派发的异步通道：把「Cron 到点」与「HTTP 派发执行器」解耦。
 *
 * 派发是对执行器的同步阻塞 HTTP 调用，最长会占住调用线程 timeoutSeconds 秒
 * （上限见 orbit.admin.max-timeout-seconds）。若直接在 Quartz 工作线程上执行，
 * org.quartz.threadPool.threadCount 个长任务同时运行就会占满 Quartz 线程池，
 * 其它任务的 Cron 到点也发不出去。本组件让 Quartz 工作线程在微秒级内返回，
 * 阻塞发生在专职派发线程上。
 *
 * 三项约束：
 *
 * 1. 有界并发：派发线程数由 orbit.admin.dispatch-threads 决定（默认 64）。
 *    这些线程绝大部分时间阻塞在 socket read 上，因此可以远大于 Quartz 的 threadCount，
 *    两者相互独立。超出线程数的触发进入有界队列，队列满则快速失败并写一条 FAILED 日志
 *    （scheduler saturated），保护调度中心不被触发风暴打爆；
 * 2. 同名任务串行：orbit.admin.dispatch-serial-per-job（默认 true）开启时，
 *    上一轮尚未结束的任务本次到点直接跳过。这一职责由本组件承担，
 *    因此 OrbitQuartzJob 本身不需要 @DisallowConcurrentExecution；
 * 3. 优雅停机：关闭时先停止接收新任务，给在跑的派发最多 30 秒收尾，
 *    超时再中断，避免硬杀导致调度日志停在 RUNNING。
 *
 * 手动触发（POST /jobs/{name}/trigger）不走本组件，仍是同步的 —— 调用方需要拿到执行结果。
 */
@Component
public class DispatchExecutor implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DispatchExecutor.class);

    /** 优雅停机时等待在跑派发收尾的最长时间（秒） */
    private static final int SHUTDOWN_GRACE_SECONDS = 30;

    private final JobService jobService;
    private final AdminProperties properties;

    /** 派发线程池 */
    private final ThreadPoolExecutor pool;

    /** 生效的排队容量（负值归零后保存） */
    private final int queueCapacity;

    /** 同名任务串行守卫：key = jobName，存在即表示该任务有一轮派发尚未结束 */
    private final ConcurrentHashMap<String, Boolean> inFlight = new ConcurrentHashMap<String, Boolean>();

    /** 队列满被拒绝的累计次数 */
    private final AtomicLong rejectedCount = new AtomicLong();

    /** 因上一轮未结束而被跳过的累计次数 */
    private final AtomicLong skippedCount = new AtomicLong();

    public DispatchExecutor(JobService jobService, AdminProperties properties) {
        this.jobService = jobService;
        this.properties = properties;

        int threads = Math.max(1, properties.getDispatchThreads());
        this.queueCapacity = Math.max(0, properties.getDispatchQueueCapacity());

        // queue-capacity <= 0 表示「不排队」，必须换成 SynchronousQueue 直接交付：
        // LinkedBlockingQueue 的构造器要求 capacity > 0，传 0 会在 Bean 创建阶段就抛异常。
        BlockingQueue<Runnable> workQueue = queueCapacity > 0
                ? new LinkedBlockingQueue<Runnable>(queueCapacity)
                : new SynchronousQueue<Runnable>();

        this.pool = new ThreadPoolExecutor(threads, threads, 60L, TimeUnit.SECONDS,
                workQueue, newThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        // 空闲时回收核心线程，避免常驻占用
        this.pool.allowCoreThreadTimeOut(true);

        log.info("[orbit-admin] dispatch pool initialized: threads={}, queueCapacity={}, serialPerJob={}",
                threads, queueCapacity, properties.isDispatchSerialPerJob());
    }

    /**
     * 提交一次定时派发。本方法立即返回，实际派发在线程池中执行。
     *
     * @param job 任务定义（Cron 到点时从数据库读到的最新状态）
     */
    public void submit(JobInfo job) {
        final String jobName = job.getJobName();
        final boolean guard = properties.isDispatchSerialPerJob();

        if (guard && inFlight.putIfAbsent(jobName, Boolean.TRUE) != null) {
            skippedCount.incrementAndGet();
            log.debug("[orbit-admin] skip fire, previous run of {} still in flight", jobName);
            return;
        }

        try {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        jobService.dispatch(job, null);
                    } catch (Exception e) {
                        // dispatch 内部已兜住绝大多数异常，这里兜住剩余的
                        // （例如 jobStore.insertLog 在数据库不可用时抛出），
                        // 否则异常会杀死工作线程并打到 stderr。
                        log.error("[orbit-admin] dispatch failed for job {}", jobName, e);
                    } finally {
                        // 必须放在 finally：dispatch 内部的 jobStore.insertLog 在它的
                        // try 之外，数据库不可用时会抛出，此处不清守卫会让该任务永久停在「在跑」。
                        if (guard) {
                            inFlight.remove(jobName);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException saturated) {
            if (guard) {
                inFlight.remove(jobName);
            }
            rejectedCount.incrementAndGet();
            String reason = "scheduler saturated: dispatch queue full (threads=" + pool.getMaximumPoolSize()
                    + ", queueCapacity=" + queueCapacity
                    + "); consider raising orbit.admin.dispatch-threads or orbit.admin.dispatch-queue-capacity";
            log.warn("[orbit-admin] job {} rejected: {}", jobName, reason);
            recordRejection(job, reason);
        }
    }

    /**
     * 当前派发通道的运行指标，供 /orbit/admin/overview 暴露。
     *
     * @return 指标字典
     */
    public Map<String, Object> metrics() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("dispatchThreads", pool.getMaximumPoolSize());
        m.put("dispatchActive", pool.getActiveCount());
        m.put("dispatchQueueSize", pool.getQueue().size());
        m.put("dispatchQueueCapacity", queueCapacity);
        m.put("dispatchRejected", rejectedCount.get());
        m.put("dispatchSkipped", skippedCount.get());
        return m;
    }

    /**
     * 被拒绝的触发同样要落一条 FAILED 调度日志，否则 Cron 到点却没有任何记录，
     * 与「任务根本没被触发」无法区分。自身异常只记录不外抛。
     *
     * @param job    任务定义
     * @param reason 拒绝原因
     */
    private void recordRejection(JobInfo job, String reason) {
        try {
            jobService.recordRejectedDispatch(job, reason);
        } catch (Exception e) {
            log.error("[orbit-admin] failed to record rejected dispatch for job {}: {}",
                    job.getJobName(), e.getMessage(), e);
        }
    }

    /**
     * 派发线程工厂：独立命名，便于线程 dump 定位；守护线程，停机收尾由 destroy() 负责。
     */
    private static ThreadFactory newThreadFactory() {
        final AtomicInteger seq = new AtomicInteger(0);
        return new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "orbit-dispatch-" + seq.incrementAndGet());
                t.setDaemon(true);
                return t;
            }
        };
    }

    /**
     * Spring 容器销毁回调：优雅停机。
     */
    @Override
    public void destroy() {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(SHUTDOWN_GRACE_SECONDS, TimeUnit.SECONDS)) {
                log.warn("[orbit-admin] dispatch pool still busy after {}s, forcing interrupt",
                        SHUTDOWN_GRACE_SECONDS);
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

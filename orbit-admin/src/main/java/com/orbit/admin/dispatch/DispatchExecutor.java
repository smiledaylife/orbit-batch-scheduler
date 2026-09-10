package com.orbit.admin.dispatch;

import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.service.JobService;
import com.orbit.core.model.JobInfo;
import com.orbit.core.model.TriggerResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 定时触发通道：把「Cron 到点」与「向执行器发起触发」解耦。
 *
 * 触发本身是一次短 HTTP 调用（读超时见 orbit.admin.trigger-timeout-seconds，默认 10 秒），
 * 执行器受理后立即回执，任务真正的执行结果由执行器异步回传。因此本通道占用线程的时间
 * 只有一次触发往返，与任务耗时无关 —— 这正是 Quartz 工作线程可以立即返回的前提。
 *
 * 三项约束：
 *
 * 1. 有界并发：触发线程数由 orbit.admin.dispatch-threads 决定（默认 64），
 *    与 org.quartz.threadPool.threadCount 相互独立。超出线程数的触发进入有界队列，
 *    队列满则快速失败并写一条 FAILED 日志（scheduler saturated），
 *    保护调度中心不被触发风暴打爆；
 * 2. 同名任务串行：orbit.admin.dispatch-serial-per-job（默认 true）开启时，
 *    上一轮**执行**尚未收敛（结果未回传）的任务本次到点直接跳过。
 *    在途判定由 {@link OutstandingDispatches} 承担，因此 OrbitQuartzJob 本身
 *    不需要 @DisallowConcurrentExecution；
 * 3. 优雅停机：关闭时先停止接收新任务，给在跑的触发最多 30 秒收尾，
 *    超时再中断，避免硬杀导致调度日志停在 RUNNING。
 *
 * 手动触发（POST /jobs/{name}/trigger）不走本组件，仍是同步的 —— 调用方需要立刻知道派发结果。
 */
@Component
public class DispatchExecutor implements DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(DispatchExecutor.class);

    /** 优雅停机时等待在跑触发收尾的最长时间（秒） */
    private static final int SHUTDOWN_GRACE_SECONDS = 30;

    private final JobService jobService;
    private final AdminProperties properties;
    private final OutstandingDispatches outstanding;

    /** 触发线程池 */
    private final ThreadPoolExecutor pool;

    /** 生效的排队容量（负值归零后保存） */
    private final int queueCapacity;

    /** 队列满被拒绝的累计次数 */
    private final AtomicLong rejectedCount = new AtomicLong();

    /**
     * 构造触发线程池。线程数与排队容量都取下限保护：误配 0 或负数会分别退化为单线程和不排队，
     * 而不是让 Bean 创建失败或产生无界队列。
     *
     * @param jobService  任务服务，实际执行派发
     * @param properties  调度中心配置，提供 dispatch-threads 与 dispatch-queue-capacity
     * @param outstanding 在途执行登记簿，用于同名任务串行守卫
     */
    public DispatchExecutor(JobService jobService, AdminProperties properties,
                            OutstandingDispatches outstanding) {
        this.jobService = jobService;
        this.properties = properties;
        this.outstanding = outstanding;

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

        log.info("[orbit-admin] trigger pool initialized: threads={}, queueCapacity={}, serialPerJob={}",
                threads, queueCapacity, properties.isDispatchSerialPerJob());
    }

    /**
     * 提交一次定时触发。本方法立即返回，实际触发在线程池中执行。
     *
     * @param job 任务定义（Cron 到点时从数据库读到的最新状态）
     */
    public void submit(JobInfo job) {
        final String jobName = job.getJobName();
        final boolean guard = properties.isDispatchSerialPerJob();

        if (guard && !outstanding.tryAcquire(jobName)) {
            // 只计数 + DEBUG 日志，不写调度日志：一个 1 秒 Cron 配 1 小时 Handler
            // 会每秒产生一条「被跳过」记录，足以刷爆日志表。
            log.debug("[orbit-admin] skip fire, previous run of {} still awaiting callback", jobName);
            return;
        }

        try {
            pool.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        triggerAndTrack(job, jobName, guard);
                    } catch (Exception e) {
                        // jobStore.insertLog 在 dispatch() 的 try 之外，数据库不可用时会抛出。
                        // 不在这里兜住，异常会杀死工作线程并打到 stderr。
                        log.error("[orbit-admin] dispatch failed for job {}", jobName, e);
                        if (guard) {
                            outstanding.releaseByJob(jobName);
                        }
                    }
                }
            });
        } catch (RejectedExecutionException saturated) {
            if (guard) {
                outstanding.releaseByJob(jobName);
            }
            rejectedCount.incrementAndGet();
            String reason = "scheduler saturated: trigger queue full (threads=" + pool.getMaximumPoolSize()
                    + ", queueCapacity=" + queueCapacity
                    + "); consider raising orbit.admin.dispatch-threads or orbit.admin.dispatch-queue-capacity";
            log.warn("[orbit-admin] job {} rejected: {}", jobName, reason);
            recordRejection(job, reason);
        }
    }

    /**
     * 发起触发并登记在途状态。
     *
     * 受理成功时日志保持 RUNNING，槽位留到回传或孤儿回收时释放；
     * 触发同步失败（执行器不可达、饱和等）时日志已经是终态，槽位必须立刻释放，
     * 否则该任务会永久无法再被触发。
     */
    private void triggerAndTrack(JobInfo job, String jobName, boolean guard) {
        TriggerResult result = jobService.dispatch(job, null);
        if (!guard) {
            return;
        }
        if (result != null && result.isAccepted()) {
            outstanding.bind(jobName, result.getLogId());
        } else {
            outstanding.releaseByJob(jobName);
        }
    }

    /**
     * 当前触发通道的运行指标，供 /orbit/admin/overview 暴露。
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
        m.put("dispatchSkipped", outstanding.skipped());
        m.put("dispatchOutstanding", outstanding.outstanding());
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
     * 触发线程工厂：独立命名，便于线程 dump 定位；守护线程，停机收尾由 destroy() 负责。
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
                log.warn("[orbit-admin] trigger pool still busy after {}s, forcing interrupt",
                        SHUTDOWN_GRACE_SECONDS);
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}

package com.orbit.admin.dispatch;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 在途执行登记簿：记录「哪些任务已经触发出去、但还没收到执行结果」。
 *
 * 触发是异步的（执行器受理即回执），所以「上一轮是否结束」不能靠触发调用是否返回来判断，
 * 只能靠执行结果有没有回传。本组件就是这个判断的唯一依据：
 *
 * 1. 派发前 {@link #tryAcquire} 占用任务槽位并**同步预登记 logId**，占不到说明上一轮还在跑，
 *    本次到点跳过；
 * 2. 收到回传、或孤儿回收把日志收敛到终态时 {@link #release} 按 logId 释放槽位；
 * 3. 派发同步失败（未拿到受理回执）时 {@link #releaseByJob} 按任务名兜底清理。
 *
 * 为什么 logId 必须在触发请求发出前就登记（而不是受理后再 bind）：
 * 执行器可能毫秒级跑完任务并回传，回传请求与「派发线程拿到受理回执之后的登记动作」
 * 之间存在真实竞态 —— 若登记晚于回传，release 会因查不到映射而空过，
 * 槽位被永久占用（该任务从此不再被 Cron 触发，且孤儿回收只扫 RUNNING 日志，无法兜底）。
 * 预登记让「回传可见」先于「回传可能发生」，从根上消除这个窗口。
 *
 * 一个任务同时最多只允许一个在途 logId，因此任务槽位用「任务名 -> logId」单值映射即可，
 * 不需要计数。释放时按 logId 反查任务名，只清理确实由该 logId 占用的槽位。
 *
 * 该登记簿是进程内的：多副本部署时各副本互不可见，跨副本不重叠依赖 Quartz 集群的行锁
 * （同一 trigger 只会被一个副本触发），而行锁并不保证「上一轮已结束」。
 * 任务不幂等时需要业务侧自行加分布式锁。
 */
@Component
public class OutstandingDispatches {

    /** 任务名 -> 在途 logId；存在即表示该任务有一轮执行尚未收敛 */
    private final ConcurrentHashMap<String, String> logIdByJob = new ConcurrentHashMap<String, String>();

    /** 在途 logId -> 任务名，供回传时反查 */
    private final ConcurrentHashMap<String, String> jobByLogId = new ConcurrentHashMap<String, String>();

    /** 因上一轮未结束而被跳过的累计次数 */
    private final AtomicLong skippedCount = new AtomicLong();

    /**
     * 尝试为一次触发占用任务槽位，并同步登记 logId 的双向映射。
     *
     * 登记必须发生在触发请求发出之前：执行器最快可能在请求发出后毫秒级回传，
     * 只有先登记，回传线程的 {@link #release} 才一定找得到映射。
     *
     * @param jobName 任务名
     * @param logId   本次派发的调度日志 ID（由调用方在派发前生成）
     * @return true 表示占用成功、可以派发；false 表示上一轮仍在途，本次应跳过
     */
    public boolean tryAcquire(String jobName, String logId) {
        if (jobName == null) {
            return true;
        }
        if (logIdByJob.putIfAbsent(jobName, logId) != null) {
            skippedCount.incrementAndGet();
            return false;
        }
        jobByLogId.put(logId, jobName);
        return true;
    }

    /**
     * 按 logId 释放在途槽位。重复调用、或对未登记的 logId 调用都是安全的空操作。
     *
     * @param logId 调度日志 ID
     * @return 是否真的释放了一个槽位
     */
    public boolean release(String logId) {
        if (logId == null) {
            return false;
        }
        String jobName = jobByLogId.remove(logId);
        if (jobName == null) {
            return false;
        }
        // 带值删除：只在该槽位仍由这个 logId 占用时清理
        logIdByJob.remove(jobName, logId);
        return true;
    }

    /**
     * 按任务名释放在途槽位。用于派发同步失败（未拿到受理回执、不会有回传到达）时的兜底清理。
     *
     * @param jobName 任务名
     */
    public void releaseByJob(String jobName) {
        if (jobName == null) {
            return;
        }
        String logId = logIdByJob.remove(jobName);
        if (logId != null) {
            jobByLogId.remove(logId, jobName);
        }
    }

    /**
     * 当前在途执行的任务数。
     *
     * @return 在途任务数
     */
    public int outstanding() {
        return logIdByJob.size();
    }

    /**
     * 累计跳过次数。
     *
     * @return 跳过次数
     */
    public long skipped() {
        return skippedCount.get();
    }
}

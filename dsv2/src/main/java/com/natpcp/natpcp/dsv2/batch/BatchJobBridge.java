/*
 * AICode Begin
 * BatchJobBridge：@OrbitJob handler 与 Spring Batch Job 的通用桥接器。
 *
 * 职责：
 *   1. 将 orbit 下发参数（JobContext.getParams()）序列化为 jobParams JSON 放入 JobParameters；
 *   2. 将 orbit 任务名/触发参数写入 JobParameters（handlerName / triggerJobId / runTime）；
 *   3. 调用 JobLauncher.run 同步执行，返回执行结果（COMPLETED -> "success"，否则抛异常）；
 *   4. 参数唯一性：JobParameters 携带 runTime 时间戳，保证每次 orbit 触发都是新的 JobInstance
 *      （Spring Batch Job 幂等语义），同时保留 restartable（失败后可查 / 可手工重启）。
 */
package com.natpcp.natpcp.dsv2.batch;

import com.orbit.executor.JobContext;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.CannotSerializeTransactionException;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * orbit handler -> Spring Batch Job 桥接器。
 */
@Slf4j
@Component
public class BatchJobBridge {

    /** PostgreSQL 可串行化（SSI）冲突重试的最大尝试次数（首次 + 重试） */
    private static final int MAX_SSI_RETRY_ATTEMPTS = 3;

    /** SSI 冲突重试的初始退避毫秒数（每次翻倍：200 -> 400 -> ...） */
    private static final long SSI_RETRY_BASE_BACKOFF_MS = 200L;

    @Autowired
    private JobLauncher jobLauncher;

    /**
     * 执行指定 Job（同步）。
     *
     * @param job      目标 Job
     * @param jobName  任务名（写入 JobParameters.handlerName，供 Tasklet 回写等场景使用）
     * @param context  orbit 任务上下文
     * @return "success"（COMPLETED）或抛出异常（FAILED，含元数据 jobExecutionId）
     */
    public String run(Job job, String jobName, JobContext context) throws Exception {
        return run(job, jobName, context, context.getParams());
    }

    /**
     * 执行指定 Job（同步，允许覆盖参数）。
     *
     * <p>SSI 冲突自愈：Spring Batch JobRepository 默认以 SERIALIZABLE 隔离级别写元数据，
     * 多个任务准点并发启动时，PostgreSQL 可能以
     * {@code could not serialize access due to read/write dependencies among transactions}
     * 取消其中一个事务（官方建议 retry）。本方捕获
     * {@link CannotSerializeTransactionException} 后指数退避重试；每次重试都用新的
     * runTime 时间戳，保证 JobInstance 唯一（不产生重复/脏元数据），最大尝试
     * {@link #MAX_SSI_RETRY_ATTEMPTS} 次后仍失败则向上抛出。</p>
     *
     * @param job      目标 Job
     * @param jobName  任务名（写入 JobParameters.handlerName，供 Tasklet 回写等场景使用）
     * @param context  orbit 任务上下文
     * @param params   覆盖参数（序列化进 JobParameters.jobParams）
     * @return "success"（COMPLETED）或抛出异常（FAILED，含元数据 jobExecutionId）
     */
    public String run(Job job, String jobName, JobContext context, Map<String, Object> params) throws Exception {
        int attempt = 1;
        while (true) {
            JobParameters jobParameters = buildJobParameters(jobName, context, params);
            try {
                JobExecution execution = jobLauncher.run(job, jobParameters);
                BatchStatus status = execution.getStatus();
                log.info("[{}] batch job 执行完成, executionId={}, status={}", jobName, execution.getId(), status);
                if (status == BatchStatus.COMPLETED) {
                    return "success";
                }
                throw new RuntimeException(jobName + " 执行失败, status=" + status
                        + ", executionId=" + execution.getId()
                        + (execution.getExitStatus() == null ? "" : ", exit=" + execution.getExitStatus().getExitDescription()));
            } catch (CannotSerializeTransactionException e) {
                if (attempt >= MAX_SSI_RETRY_ATTEMPTS) {
                    log.warn("[{}] JobRepository 元数据写入连续 {} 次遇到 PostgreSQL 可串行化冲突(SSI)，放弃重试并抛出",
                            jobName, attempt);
                    throw e;
                }
                long backoff = SSI_RETRY_BASE_BACKOFF_MS * (1L << (attempt - 1));
                log.warn("[{}] JobRepository 元数据写入遇到 PostgreSQL 可串行化冲突(SSI)，{}ms 后第 {} 次重试",
                        jobName, backoff, attempt + 1);
                Thread.sleep(backoff);
                attempt++;
            }
        }
    }

    /** 组装 JobParameters：handlerName/triggerJobId/runTime（唯一性）/jobParams */
    private JobParameters buildJobParameters(String jobName, JobContext context, Map<String, Object> params) {
        return new JobParametersBuilder()
                .addString("handlerName", jobName)
                .addString("triggerJobId", String.valueOf(context.getJobId()))
                .addLong("runTime", System.currentTimeMillis())
                .addString(JobParamsHelper.KEY_JOB_PARAMS, JobParamsHelper.toJson(params))
                .toJobParameters();
    }
}
/*
 * AICode End
 */

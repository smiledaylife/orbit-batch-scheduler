/*
 * AICode Begin
 * JobParamsHelper：Spring Batch Job 执行上下文参数读取工具。
 *
 * orbit -> Spring Batch 桥接时，orbit 下发的业务参数（JobContext.getParams()）
 * 会被序列化为 JSON 字符串放入 JobParameters（key = "jobParams"），
 * Tasklet/Step 通过本工具统一读取，避免各组件重复序列化逻辑。
 */
package com.natpcp.natpcp.dsv2.batch.util;

import com.natpcp.natpcp.dsv2.util.JsonUtil;
import org.springframework.batch.core.JobParameters;

import java.util.Collections;
import java.util.Map;

/**
 * Job 参数读取工具。
 */
public final class JobParamsHelper {

    public static final String KEY_JOB_PARAMS = "jobParams";

    private JobParamsHelper() {
    }

    /**
     * 从 JobParameters 中读取 orbit 下发的业务参数 Map（无参任务 / 非法 JSON 返回空 Map）。
     */
    public static Map<String, Object> getParams(JobParameters jobParameters) {
        if (jobParameters == null) {
            return Collections.emptyMap();
        }
        return JsonUtil.toMap(jobParameters.getString(KEY_JOB_PARAMS));
    }

    /**
     * 从 Spring Batch 5 StepContext 的 Map 形式 JobParameters 中读取业务参数。
     * StepContext.getJobParameters() 返回 Map&lt;String,Object&gt;，键为标准键 jobParams。
     */
    public static Map<String, Object> getParamsMap(Map<String, Object> jobParameters) {
        if (jobParameters == null || jobParameters.isEmpty()) {
            return Collections.emptyMap();
        }
        Object value = jobParameters.get(KEY_JOB_PARAMS);
        return value == null ? Collections.emptyMap() : JsonUtil.toMap(value.toString());
    }

    /**
     * 将业务参数 Map 序列化为 JobParameters 字符串值（空参数固定为 "{}"）。
     */
    public static String toJson(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return "{}";
        }
        return JsonUtil.toJson(params);
    }
}
/*
 * AICode End
 */

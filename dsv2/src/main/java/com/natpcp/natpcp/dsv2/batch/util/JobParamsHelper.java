/*
 * AICode Begin
 * JobParamsHelper：Spring Batch Job 执行上下文参数读取工具。
 *
 * orbit -> Spring Batch 桥接时，orbit 下发的业务参数（JobContext.getParams()）
 * 会被序列化为 JSON 字符串放入 JobParameters（key = "jobParams"），
 * Tasklet/Step 通过本工具统一读取，避免各组件重复序列化逻辑。
 */
package com.natpcp.natpcp.dsv2.batch.util;

import com.alibaba.fastjson.JSON;
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
     * 从 JobParameters 中读取 orbit 下发的业务参数 Map（无参任务返回空 Map）。
     */
    public static Map<String, Object> getParams(JobParameters jobParameters) {
        if (jobParameters == null) {
            return Collections.emptyMap();
        }
        String json = jobParameters.getString(KEY_JOB_PARAMS);
        if (json == null || json.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Map<String, Object> map = JSON.parseObject(json);
            return map == null ? Collections.emptyMap() : map;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * 从 Spring Batch 5 StepContext 的 Map 形式 JobParameters 中读取业务参数。
     * StepContext.getJobParameters() 返回 Map<String,Object>；钥匙为标准键 jobParams。
     */
    public static Map<String, Object> getParamsMap(Map<String, Object> jobParameters) {
        if (jobParameters == null || jobParameters.isEmpty()) {
            return Collections.emptyMap();
        }
        Object value = jobParameters.get(KEY_JOB_PARAMS);
        if (value == null) {
            return Collections.emptyMap();
        }
        String json = value.toString();
        if (json.trim().isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            Map<String, Object> map = JSON.parseObject(json);
            return map == null ? Collections.emptyMap() : map;
        } catch (Exception e) {
            return Collections.emptyMap();
        }
    }

    /**
     * 将业务参数 Map 序列化为 JobParameters。
     */
    public static String toJson(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return "{}";
        }
        return JSON.toJSONString(params);
    }
}
/*
 * AICode End
 */

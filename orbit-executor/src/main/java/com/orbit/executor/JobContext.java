package com.orbit.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务执行上下文（由调度中心透传）。
 */
public class JobContext {

    private final long jobId;
    private final String jobName;
    private final String handler;
    private final String logId;
    private final Map<String, Object> params;

    public JobContext(long jobId, String jobName, String handler, String logId, Map<String, Object> params) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.handler = handler;
        this.logId = logId;
        this.params = params == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new HashMap<String, Object>(params));
    }

    public long getJobId() { return jobId; }
    public String getJobName() { return jobName; }
    public String getHandler() { return handler; }
    public String getLogId() { return logId; }
    public Map<String, Object> getParams() { return params; }

    public String getString(String key) {
        Object v = params.get(key);
        return v == null ? null : String.valueOf(v);
    }

    public String getString(String key, String defaultValue) {
        String v = getString(key);
        return v == null ? defaultValue : v;
    }
}

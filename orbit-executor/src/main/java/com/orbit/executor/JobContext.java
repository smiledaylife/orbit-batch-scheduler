package com.orbit.executor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 任务执行上下文对象。
 * 调度中心在派发任务时，会将任务元数据（任务 ID、任务名称、日志追踪 ID）
 * 以及合并后的动态参数封装进此对象，并作为参数传递给带有 {@link com.orbit.executor.annotation.OrbitJob} 注解的业务执行函数。
 */
public class JobContext {

    /**
     * 任务主键 ID（对应调度中心 orbit_job 表主键）
     */
    private final long jobId;

    /**
     * 任务唯一业务名称
     */
    private final String jobName;

    /**
     * 当前调用的 JobHandler 名称
     */
    private final String handler;

    /**
     * 本次任务执行调度的全链路唯一追踪日志 ID（UUID 生成）
     */
    private final String logId;

    /**
     * 任务执行时传入的键值对参数集合（不可修改视图）
     */
    private final Map<String, Object> params;

    /**
     * 构造任务执行上下文
     *
     * @param jobId   任务 ID
     * @param jobName 任务名称
     * @param handler Handler 名称
     * @param logId   日志跟踪 ID
     * @param params  入参字典
     */
    public JobContext(long jobId, String jobName, String handler, String logId, Map<String, Object> params) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.handler = handler;
        this.logId = logId;
        this.params = params == null
                ? Collections.<String, Object>emptyMap()
                : Collections.unmodifiableMap(new HashMap<String, Object>(params));
    }

    public long getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getHandler() {
        return handler;
    }

    public String getLogId() {
        return logId;
    }

    public Map<String, Object> getParams() {
        return params;
    }

    /**
     * 获取指定参数键的字符串形式值
     *
     * @param key 参数键
     * @return 字符串值，若键不存在或值为 null 则返回 null
     */
    public String getString(String key) {
        Object v = params.get(key);
        return v == null ? null : String.valueOf(v);
    }

    /**
     * 获取指定参数键的字符串形式值，若不存在或为 null 则返回默认值
     *
     * @param key          参数键
     * @param defaultValue 默认值
     * @return 字符串值或默认值
     */
    public String getString(String key, String defaultValue) {
        String v = getString(key);
        return v == null ? defaultValue : v;
    }

    /**
     * 获取指定参数键的 int 形式值。
     * 数值以 {@link Number}（JSON 数字）或可解析字符串传入均可。
     *
     * @param key          参数键
     * @param defaultValue 键不存在、为 null 或无法解析时的默认值
     * @return int 值
     */
    public int getInt(String key, int defaultValue) {
        Number n = asNumber(key);
        return n == null ? defaultValue : n.intValue();
    }

    /**
     * 获取指定参数键的 long 形式值。
     * 数值以 {@link Number}（JSON 数字）或可解析字符串传入均可。
     *
     * @param key          参数键
     * @param defaultValue 键不存在、为 null 或无法解析时的默认值
     * @return long 值
     */
    public long getLong(String key, long defaultValue) {
        Number n = asNumber(key);
        return n == null ? defaultValue : n.longValue();
    }

    /**
     * 获取指定参数键的 double 形式值。
     * 数值以 {@link Number}（JSON 数字）或可解析字符串传入均可。
     *
     * @param key          参数键
     * @param defaultValue 键不存在、为 null 或无法解析时的默认值
     * @return double 值
     */
    public double getDouble(String key, double defaultValue) {
        Number n = asNumber(key);
        return n == null ? defaultValue : n.doubleValue();
    }

    /**
     * 获取指定参数键的 boolean 形式值。
     * 兼容 Boolean 值与 "true"/"false"（忽略大小写）字符串。
     *
     * @param key          参数键
     * @param defaultValue 键不存在、为 null 或无法解析时的默认值
     * @return boolean 值
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Object v = params.get(key);
        if (v == null) {
            return defaultValue;
        }
        if (v instanceof Boolean) {
            return (Boolean) v;
        }
        String s = String.valueOf(v).trim();
        if ("true".equalsIgnoreCase(s)) {
            return true;
        }
        if ("false".equalsIgnoreCase(s)) {
            return false;
        }
        return defaultValue;
    }

    /**
     * 将参数值规约为 Number：Number 直接返回，字符串尝试解析，失败返回 null。
     */
    private Number asNumber(String key) {
        Object v = params.get(key);
        if (v == null) {
            return null;
        }
        if (v instanceof Number) {
            return (Number) v;
        }
        try {
            return Long.parseLong(String.valueOf(v).trim());
        } catch (NumberFormatException ignore) {
            try {
                return Double.parseDouble(String.valueOf(v).trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }
}

package com.orbit.scheduler.core;

import java.io.Serializable;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

/**
 * 任务执行上下文：一次派发执行中传递给 {@code @BatchTask} 方法的运行时信息。
 *
 * @author orbit
 */
public class TaskContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 任务名 */
    private final String taskName;
    /** 任务分组 */
    private final String taskGroup;
    /** 任务参数（来自 t_job_config.params JSON 或触发时传入） */
    private final Map<String, Object> params;
    /** 本次触发时间 */
    private final Date fireTime;
    /** 发起调度的节点 ID（可能是远端节点） */
    private final String dispatchNode;
    /** 实际执行节点 ID（即当前节点） */
    private final String workerNode;
    /** 本次执行追踪 ID */
    private final String requestId;

    public TaskContext(String taskName, String taskGroup, Map<String, Object> params,
                       Date fireTime, String dispatchNode, String workerNode, String requestId) {
        this.taskName = taskName;
        this.taskGroup = taskGroup;
        this.params = params == null ? Collections.<String, Object>emptyMap() : params;
        this.fireTime = fireTime;
        this.dispatchNode = dispatchNode;
        this.workerNode = workerNode;
        this.requestId = requestId;
    }

    /** 获取字符串参数，不存在时返回默认值 */
    public String getString(String key, String defaultValue) {
        Object v = params.get(key);
        return v == null ? defaultValue : String.valueOf(v);
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public Object get(String key) {
        return params.get(key);
    }

    public String getTaskName() { return taskName; }

    public String getTaskGroup() { return taskGroup; }

    public Map<String, Object> getParams() { return params; }

    public Date getFireTime() { return fireTime; }

    public String getDispatchNode() { return dispatchNode; }

    public String getWorkerNode() { return workerNode; }

    public String getRequestId() { return requestId; }
}

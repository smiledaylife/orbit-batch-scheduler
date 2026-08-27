package com.orbit.scheduler.core;

import com.orbit.scheduler.annotation.DispatchType;

/**
 * 注解扫描得到的任务元信息（只读快照）。
 *
 * @author orbit
 */
public class TaskDefinition {

    private final String name;
    private final String description;
    private final String cron;
    private final DispatchType dispatchType;
    private final String httpService;
    private final String httpPath;
    private final String httpMethod;
    private final boolean overwrite;
    private final String beanName;
    private final String methodName;

    public TaskDefinition(String name, String description, String cron, DispatchType dispatchType,
                          boolean overwrite, String beanName, String methodName) {
        this(name, description, cron, dispatchType, null, null, null, overwrite, beanName, methodName);
    }

    public TaskDefinition(String name, String description, String cron, DispatchType dispatchType,
                          String httpService, String httpPath, String httpMethod,
                          boolean overwrite, String beanName, String methodName) {
        this.name = name;
        this.description = description;
        this.cron = cron;
        this.dispatchType = dispatchType;
        this.httpService = httpService;
        this.httpPath = httpPath;
        this.httpMethod = httpMethod;
        this.overwrite = overwrite;
        this.beanName = beanName;
        this.methodName = methodName;
    }

    public String getName() { return name; }

    public String getDescription() { return description; }

    public String getCron() { return cron; }

    public DispatchType getDispatchType() { return dispatchType; }

    public String getHttpService() { return httpService; }

    public String getHttpPath() { return httpPath; }

    public String getHttpMethod() { return httpMethod; }

    public boolean isOverwrite() { return overwrite; }

    public String getBeanName() { return beanName; }

    public String getMethodName() { return methodName; }

    @Override
    public String toString() {
        return "TaskDefinition{name='" + name + '\'' + ", cron='" + cron + '\'' +
                ", dispatchType=" + dispatchType + ", bean=" + beanName + "#" + methodName + '}';
    }
}

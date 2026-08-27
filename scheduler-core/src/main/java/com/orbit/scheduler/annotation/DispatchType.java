package com.orbit.scheduler.annotation;

/**
 * 任务调度方式。
 *
 * <p>LOCAL：在本节点进程内直接执行（本节点不存在执行器时自动降级为 HTTP 远程派发）。
 * <p>HTTP：通过 K8s Service（Headless DNS）解析出的实例端点，远程派发到目标 Pod 执行。
 */
public enum DispatchType {

    /** 本地调度：进程内反射调用 {@code @BatchTask} 标记的方法 */
    LOCAL,

    /** HTTP 调度：经 Service 端点远程派发（云原生模式） */
    HTTP
}

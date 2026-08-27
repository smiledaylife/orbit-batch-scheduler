package com.orbit.scheduler.annotation;

/**
 * 任务调度方式。
 *
 * <p>LOCAL：在本节点进程内直接执行（本节点不存在执行器时自动降级为 HTTP 远程派发）。
 * <p>HTTP：通过 K8s Service 解析出的<strong>同框架</strong>实例端点，远程派发到目标 Pod 的
 * {@code /api/scheduler/execute} 执行（目标服务需接入 orbit-scheduler）。
 * <p>REMOTE：调用<strong>外部业务服务</strong>的任意 HTTP 接口（目标服务无需接入本框架），
 * 适用于"调度中心编排、业务在其他微服务执行"的批量场景。
 * <p>WORKFLOW：按编排定义串行/并行调度多个子步骤（LOCAL / HTTP / REMOTE 可混用），
 * 实现跨服务批量流水线。
 */
public enum DispatchType {

    /** 本地调度：进程内反射调用 {@code @BatchTask} 标记的方法 */
    LOCAL,

    /** HTTP 调度：经 Service 端点远程派发到同框架节点（云原生模式） */
    HTTP,

    /**
     * 远程服务调度：调用其他微服务的业务 HTTP 接口。
     * 目标由 {@code httpServiceName}（远程服务注册名）+ {@code httpPath} + {@code httpMethod} 决定。
     */
    REMOTE,

    /**
     * 工作流编排：按 {@code workflowDef} 定义的步骤列表依次/并行调度多个任务或远程接口。
     */
    WORKFLOW
}

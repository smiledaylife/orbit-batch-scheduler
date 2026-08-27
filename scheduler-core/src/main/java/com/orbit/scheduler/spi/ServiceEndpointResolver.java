package com.orbit.scheduler.spi;

import com.orbit.scheduler.model.ServiceEndpoint;

import java.util.List;

/**
 * 服务端点解析 SPI：HTTP 调度的路由目标来源。
 *
 * <p>内置实现：
 * <ul>
 *   <li>{@code ServiceDnsServiceEndpointResolver} —— 默认解析普通 K8s Service DNS，
 *       由 ClusterIP/EndpointSlice 负责 Pod 负载均衡</li>
 *   <li>{@code HeadlessDnsServiceEndpointResolver} —— 显式模式下解析 Headless Service，
 *       获取 Pod IP 列表</li>
 *   <li>{@code StaticServiceEndpointResolver} —— 静态端点列表</li>
 * </ul>
 *
 * @author orbit
 */
public interface ServiceEndpointResolver {

    /**
     * 解析服务的全部可用端点。
     *
     * @param serviceName 服务名（普通 Service DNS 或 Headless Service DNS；空则使用全局默认服务名）
     * @return 端点列表（可能为空，调用方需处理）
     */
    List<ServiceEndpoint> resolve(String serviceName);

    /**
     * 是否由服务端/平台负责负载均衡。
     *
     * <p>普通 Kubernetes ClusterIP Service 返回 true：调用方只需要访问 Service，
     * 不应在业务层对返回结果做 Pod 级轮询或故障转移。Headless/Static 返回 false，
     * 调用方可以在多个实例端点之间做显式路由。</p>
     */
    default boolean isLoadBalanced() {
        return false;
    }
}

package com.orbit.scheduler.discovery;

import com.orbit.scheduler.model.ServiceEndpoint;
import com.orbit.scheduler.spi.ServiceEndpointResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 静态端点解析器：适用于非 K8s 环境（如传统虚机集群 / 本地联调）。
 * 端点列表来自配置 orbit.scheduler.http-dispatch.static-endpoints。
 *
 * <p><b>优化</b>：构造期对外暴露的 List 通过 {@link Collections#unmodifiableList(List)}
 * 包装，避免调用方误改端点列表影响后续请求；同时返回单例空列表避免重复构造。
 *
 * @author orbit
 */
public class StaticServiceEndpointResolver implements ServiceEndpointResolver {

    private static final Logger log = LoggerFactory.getLogger(StaticServiceEndpointResolver.class);

    private static final List<ServiceEndpoint> EMPTY = Collections.<ServiceEndpoint>emptyList();

    private final List<ServiceEndpoint> endpoints;

    public StaticServiceEndpointResolver(List<String> urls) {
        List<ServiceEndpoint> tmp = new ArrayList<ServiceEndpoint>();
        if (urls != null) {
            for (String url : urls) {
                if (url != null && !url.trim().isEmpty()) {
                    tmp.add(new ServiceEndpoint(url.trim()));
                }
            }
        }
        this.endpoints = tmp.isEmpty() ? EMPTY : Collections.unmodifiableList(tmp);
        log.info("[orbit-scheduler] static endpoint resolver initialized with {} endpoint(s): {}",
                endpoints.size(), endpoints);
    }

    @Override
    public List<ServiceEndpoint> resolve(String serviceName) {
        return endpoints;
    }
}


package com.orbit.scheduler.discovery;

import com.orbit.scheduler.model.RemoteServiceDefinition;
import com.orbit.scheduler.model.ServiceEndpoint;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 远程服务注册表测试。
 */
class RemoteServiceRegistryTest {

    @Test
    void registerAndResolveStaticEndpoints() {
        RemoteServiceRegistry registry = new RemoteServiceRegistry();
        RemoteServiceDefinition def = new RemoteServiceDefinition();
        def.setName("order-service");
        def.setStaticEndpoints(Arrays.asList("http://10.0.0.1:8080", "http://10.0.0.2:8080/"));
        registry.register(def);

        assertTrue(registry.contains("order-service"));
        List<ServiceEndpoint> endpoints = registry.resolveEndpoints("order-service", 8080);
        assertEquals(2, endpoints.size());
        assertEquals("http://10.0.0.1:8080", endpoints.get(0).getUrl());
        assertEquals("http://10.0.0.2:8080", endpoints.get(1).getUrl());
    }

    @Test
    void resolveBaseUrl() {
        RemoteServiceRegistry registry = new RemoteServiceRegistry();
        RemoteServiceDefinition def = new RemoteServiceDefinition();
        def.setName("inv");
        def.setBaseUrl("http://inventory:9090/");
        registry.register(def);

        List<ServiceEndpoint> endpoints = registry.resolveEndpoints("inv", 8080);
        assertEquals(1, endpoints.size());
        assertEquals("http://inventory:9090", endpoints.get(0).getUrl());
    }

    @Test
    void resolveAbsoluteUrlAsServiceName() {
        RemoteServiceRegistry registry = new RemoteServiceRegistry();
        List<ServiceEndpoint> endpoints = registry.resolveEndpoints("http://example.com:8080", 8080);
        assertEquals(1, endpoints.size());
        assertEquals("http://example.com:8080", endpoints.get(0).getUrl());
    }

    @Test
    void unregister() {
        RemoteServiceRegistry registry = new RemoteServiceRegistry();
        RemoteServiceDefinition def = new RemoteServiceDefinition();
        def.setName("tmp");
        def.setBaseUrl("http://tmp:1");
        registry.register(def);
        registry.unregister("tmp");
        assertFalse(registry.contains("tmp"));
    }
}

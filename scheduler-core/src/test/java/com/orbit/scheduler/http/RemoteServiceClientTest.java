package com.orbit.scheduler.http;

import com.orbit.scheduler.model.ServiceEndpoint;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * RemoteServiceClient 工具方法测试。
 */
class RemoteServiceClientTest {

    @Test
    void buildUrlJoinsBaseAndPath() {
        assertEquals("http://svc:8080/api/batch",
                RemoteServiceClient.buildUrl("http://svc:8080", "/api/batch"));
        assertEquals("http://svc:8080/api/batch",
                RemoteServiceClient.buildUrl("http://svc:8080/", "api/batch"));
        assertEquals("http://svc:8080",
                RemoteServiceClient.buildUrl("http://svc:8080/", ""));
        assertEquals("http://other/x",
                RemoteServiceClient.buildUrl("http://svc:8080", "http://other/x"));
    }

    @Test
    void serviceEndpointEquality() {
        ServiceEndpoint a = new ServiceEndpoint("http://a:1");
        ServiceEndpoint b = new ServiceEndpoint("http://a:1");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }
}

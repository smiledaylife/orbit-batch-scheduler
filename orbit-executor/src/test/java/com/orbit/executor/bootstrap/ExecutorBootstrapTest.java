package com.orbit.executor.bootstrap;

import com.orbit.executor.client.AdminClient;
import com.orbit.executor.config.ExecutorProperties;
import com.orbit.executor.handler.JobHandlerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.server.WebServer;
import org.springframework.mock.env.MockEnvironment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link ExecutorBootstrap} 注册地址与端口解析测试。
 * <p>
 * 用例一律按真实启动顺序驱动：{@code WebServerInitializedEvent} 先于 {@link ExecutorBootstrap#start()}。
 * Spring Boot 2.3+ 由 {@code WebServerStartStopLifecycle}（{@code SmartLifecycle}，
 * phase = {@code Integer.MAX_VALUE - 1}）启动容器并发布该事件，
 * 本类 phase 为 {@code Integer.MAX_VALUE}，排在它之后启动。
 */
class ExecutorBootstrapTest {

    private ExecutorBootstrap bootstrap;

    @AfterEach
    void tearDown() {
        if (bootstrap != null) {
            bootstrap.stop();
        }
    }

    /**
     * admin-addresses 置空：{@code AdminClient} 在地址列表为空时直接短路返回，
     * 保证本测试类不产生任何真实 HTTP 调用。
     */
    private static ExecutorProperties offlineProps() {
        ExecutorProperties props = new ExecutorProperties();
        props.setAppName("test-executor");
        props.setAdminAddresses("");
        return props;
    }

    private ExecutorBootstrap newBootstrap(ExecutorProperties props, String serverPort) {
        ExecutorBootstrap b = new ExecutorBootstrap(props, new JobHandlerRegistry(), new AdminClient(props));
        MockEnvironment env = new MockEnvironment();
        if (serverPort != null) {
            env.setProperty("server.port", serverPort);
        }
        b.setEnvironment(env);
        return b;
    }

    private static WebServerInitializedEvent webServerEvent(int port) {
        WebServerInitializedEvent event = mock(WebServerInitializedEvent.class);
        WebServer webServer = mock(WebServer.class);
        when(event.getWebServer()).thenReturn(webServer);
        when(webServer.getPort()).thenReturn(port);
        return event;
    }

    /**
     * {@code server.port=0} 时 Environment 里读不到有效端口，注册地址的端口只能来自容器事件。
     * 用 0 而不是具体端口，才能把「端口来自事件」与「端口来自 server.port」两种来源区分开。
     */
    @Test
    void randomPortIsTakenFromWebServerInitializedEvent() {
        bootstrap = newBootstrap(offlineProps(), "0");

        bootstrap.onApplicationEvent(webServerEvent(54321));
        bootstrap.start();

        assertTrue(bootstrap.getResolvedAddress().endsWith(":54321"),
                "address must use the port reported by the web server: " + bootstrap.getResolvedAddress());
    }

    /**
     * 显式配置 {@code orbit.executor.address} 时以配置为准，
     * 容器实际监听端口是多少都不改写（端口映射 / Service 域名场景依赖这一点）。
     */
    @Test
    void explicitAddressWinsOverDetectedPort() {
        ExecutorProperties props = offlineProps();
        props.setAddress("http://executor.svc.cluster.local:9000");
        bootstrap = newBootstrap(props, "8081");

        bootstrap.onApplicationEvent(webServerEvent(8081));
        bootstrap.start();

        assertEquals("http://executor.svc.cluster.local:9000", bootstrap.getResolvedAddress());
    }

    /**
     * 显式配置 {@code orbit.executor.port} 的优先级高于容器上报端口，
     * 用于容器内监听端口与对外端口不一致的场景。
     */
    @Test
    void explicitPortOverridesWebServerPort() {
        ExecutorProperties props = offlineProps();
        props.setPort(19000);
        bootstrap = newBootstrap(props, "8081");

        bootstrap.onApplicationEvent(webServerEvent(8081));
        bootstrap.start();

        assertTrue(bootstrap.getResolvedAddress().endsWith(":19000"),
                "explicit orbit.executor.port must win: " + bootstrap.getResolvedAddress());
    }

    /**
     * 没有容器事件（例如非 Servlet 环境）时，退回读取 {@code server.port}。
     */
    @Test
    void fallsBackToServerPortWithoutWebServerEvent() {
        bootstrap = newBootstrap(offlineProps(), "8090");

        bootstrap.start();

        assertTrue(bootstrap.getResolvedAddress().endsWith(":8090"),
                "address must fall back to server.port: " + bootstrap.getResolvedAddress());
    }
}

package com.orbit.executor;

import com.orbit.executor.annotation.OrbitJob;
import org.junit.jupiter.api.Test;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.boot.web.server.WebServer;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.stereotype.Component;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 执行器核心组件单元测试：
 * 
 *   - {@link JobHandlerRegistry} 注解扫描与动态反射调用测试
 *   - {@link ExecutorBootstrap} 自动感知 server.port 端口逻辑测试
 * 
 */
class JobHandlerRegistryTest {

    /**
     * 模拟测试用的示例 JobHandler 组件
     */
    @Component
    static class Sample {
        @OrbitJob("hello")
        public String hello(JobContext ctx) {
            return "hi-" + ctx.getString("name", "x");
        }

        @OrbitJob("mapJob")
        public String mapJob(Map<String, Object> params) {
            return String.valueOf(params.get("k"));
        }
    }

    /**
     * 验证 Spring 容器中 @OrbitJob 注解方法的扫描、解析和反射调用
     */
    @Test
    void scanAndInvoke() {
        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(Sample.class, JobHandlerRegistry.class);
        ctx.refresh();

        JobHandlerRegistry registry = ctx.getBean(JobHandlerRegistry.class);
        // 验证两个 Handler 均已成功注册
        assertTrue(registry.has("hello"));
        assertTrue(registry.has("mapJob"));
        assertEquals(2, registry.listNames().size());

        // 验证 JobContext 入参方式的调用与参数提取
        JobContext jc = new JobContext(1, "j", "hello", "log1",
                java.util.Collections.<String, Object>singletonMap("name", "orbit"));
        assertEquals("hi-orbit", registry.invoke("hello", jc));

        // 验证 Map 入参方式的调用
        JobContext jc2 = new JobContext(1, "j", "mapJob", "log2",
                java.util.Collections.<String, Object>singletonMap("k", "v"));
        assertEquals("v", registry.invoke("mapJob", jc2));
        ctx.close();
    }

    /**
     * 验证 ExecutorBootstrap 自动感知 server.port 且不需要显式配置端口
     */
    @Test
    void bootstrapAutoDetectServerPort() {
        ExecutorProperties props = new ExecutorProperties();
        // port 默认为 0，不显式配置
        assertEquals(0, props.getPort());

        JobHandlerRegistry registry = new JobHandlerRegistry();
        AdminClient adminClient = new AdminClient(props);
        ExecutorBootstrap bootstrap = new ExecutorBootstrap(props, registry, adminClient);

        // 模拟 Spring Environment 配置了 server.port=8088
        MockEnvironment env = new MockEnvironment();
        env.setProperty("server.port", "8088");
        bootstrap.setEnvironment(env);

        // 模拟 Web 容器启动就绪事件 (例如实际绑定 8088)
        WebServerInitializedEvent event = mock(WebServerInitializedEvent.class);
        WebServer webServer = mock(WebServer.class);
        when(event.getWebServer()).thenReturn(webServer);
        when(webServer.getPort()).thenReturn(8088);
        bootstrap.onApplicationEvent(event);

        bootstrap.start();
        String address = bootstrap.getResolvedAddress();
        assertTrue(address.endsWith(":8088"), "Address should resolve with server.port 8088: " + address);
        bootstrap.stop();
    }
}

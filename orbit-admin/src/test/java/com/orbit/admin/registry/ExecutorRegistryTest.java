package com.orbit.admin.registry;

import com.baomidou.mybatisplus.test.autoconfigure.MybatisPlusTest;
import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.config.MybatisPlusConfig;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RegistryRequest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.jdbc.Sql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 执行器共享库注册表测试。
 */
@MybatisPlusTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({MybatisPlusConfig.class, ExecutorRegistry.class, ExecutorRegistryTest.Cfg.class})
@Sql(scripts = "/schema.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
class ExecutorRegistryTest {

    @TestConfiguration
    static class Cfg {
        @Bean
        AdminProperties adminProperties() {
            AdminProperties p = new AdminProperties();
            p.setHeartbeatTimeoutSeconds(90);
            // 关闭注册表缓存：保证每个断言读到的都是最新落库状态
            p.setRegistryCacheTtlMs(0);
            return p;
        }
    }

    @Autowired
    private ExecutorRegistry registry;

    @Test
    void registerAndRouteRoundRobin() {
        registry.register(req("demo", "http://10.0.0.1:8081"));
        registry.register(req("demo", "http://10.0.0.2:8081"));

        assertEquals(2, registry.listByApp("demo").size());

        Set<String> hit = new HashSet<String>();
        for (int i = 0; i < 4; i++) {
            ExecutorNode n = registry.route("demo", "ROUND");
            assertNotNull(n);
            hit.add(n.getAddress());
        }
        assertEquals(2, hit.size());
    }

    @Test
    void routeEmptyReturnsNull() {
        assertNull(registry.route("missing", "ROUND"));
    }

    @Test
    void remove() {
        registry.register(req("demo", "http://10.0.0.8:1"));
        registry.remove("demo", "http://10.0.0.8:1");
        assertEquals(0, registry.listByApp("demo").size());
    }

    /**
     * 上报字段超出列宽时必须给出可读的错误（字段名 + 上限），
     * 而不是每轮心跳都以一句被截断的 SQL 驱动报错失败。
     */
    @Test
    void rejectsOversizedFields() {
        StringBuilder app = new StringBuilder();
        for (int i = 0; i < 65; i++) {
            app.append('a');   // app_name VARCHAR(64)
        }
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> registry.register(req(app.toString(), "http://10.0.0.9:1")));
        assertTrue(ex.getMessage().contains("appName too long"), ex.getMessage());

        StringBuilder node = new StringBuilder();
        for (int i = 0; i < 129; i++) {
            node.append('n');   // node_id VARCHAR(128)
        }
        RegistryRequest longNode = req("demo", "http://10.0.0.9:1");
        longNode.setNodeId(node.toString());
        IllegalArgumentException nodeEx = assertThrows(IllegalArgumentException.class,
                () -> registry.register(longNode));
        assertTrue(nodeEx.getMessage().contains("nodeId too long"), nodeEx.getMessage());
    }

    private static RegistryRequest req(String app, String addr) {
        RegistryRequest r = new RegistryRequest();
        r.setAppName(app);
        r.setAddress(addr);
        r.setNodeId("n1");
        r.setHandlers(Arrays.asList("h1"));
        return r;
    }
}

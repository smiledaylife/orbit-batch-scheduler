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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 执行器注册表「本地缓存」行为测试：
 * 验证 TTL 缓存生效（读路径不重复查库由行为等价性保证），
 * 以及写操作（注册 / 摘除）对缓存的<b>立即失效</b>——这是缓存正确性的关键。
 */
@MybatisPlusTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import({MybatisPlusConfig.class, ExecutorRegistry.class, ExecutorRegistryCacheTest.Cfg.class})
@Sql(scripts = "/schema.sql", executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD)
class ExecutorRegistryCacheTest {

    /** 缓存 TTL 设置为远大于测试执行时长：若没有失效机制，后续读将命中陈旧快照 */
    @TestConfiguration
    static class Cfg {
        @Bean
        AdminProperties adminProperties() {
            AdminProperties p = new AdminProperties();
            p.setHeartbeatTimeoutSeconds(90);
            p.setRegistryCacheTtlMs(600_000L);
            return p;
        }
    }

    @Autowired
    private ExecutorRegistry registry;

    @Test
    void registerInvalidatesCacheImmediately() {
        registry.register(req("cache-app", "http://10.9.0.1:8081"));
        assertEquals(1, registry.listByApp("cache-app").size());

        // 第二个节点注册后，不经过 TTL 等待，缓存应立即反映新节点
        registry.register(req("cache-app", "http://10.9.0.2:8081"));
        assertEquals(2, registry.listByApp("cache-app").size());
    }

    @Test
    void removeInvalidatesCacheImmediately() {
        registry.register(req("cache-app", "http://10.9.0.3:8081"));
        registry.register(req("cache-app", "http://10.9.0.4:8081"));
        assertEquals(2, registry.listByApp("cache-app").size());

        // 摘除后缓存应立即失效，读到 1 个节点（若无失效机制，TTL 内仍会读到 2 个）
        registry.remove("cache-app", "http://10.9.0.4:8081");
        assertEquals(1, registry.listByApp("cache-app").size());
    }

    @Test
    void routeOnCandidatesPicksFromList() {
        registry.register(req("route-app", "http://10.9.1.1:8081"));
        registry.register(req("route-app", "http://10.9.1.2:8081"));
        List<ExecutorNode> candidates = registry.listByApp("route-app");

        // 在已查出的候选列表上选点（dispatch 单次查询优化所依赖的 API）
        ExecutorNode chosen = registry.route(candidates, "route-app", "FIRST");
        assertNotNull(chosen);
        assertTrue(candidates.contains(chosen));

        ExecutorNode rr = registry.route(candidates, "route-app", "ROUND");
        assertNotNull(rr);
        assertTrue(candidates.contains(rr));

        // 传入空列表安全返回 null
        assertEquals(null, registry.route(new ArrayList<ExecutorNode>(), "route-app", "ROUND"));
    }

    @Test
    void cachedListIsDefensiveCopy() {
        registry.register(req("cache-app", "http://10.9.0.5:8081"));
        List<ExecutorNode> first = registry.listByApp("cache-app");
        // 调用方修改返回列表不应污染缓存
        first.clear();
        assertEquals(1, registry.listByApp("cache-app").size());
    }

    private static RegistryRequest req(String app, String addr) {
        RegistryRequest r = new RegistryRequest();
        r.setAppName(app);
        r.setAddress(addr);
        r.setNodeId("n-" + addr);
        r.setHandlers(Arrays.asList("h1"));
        return r;
    }
}

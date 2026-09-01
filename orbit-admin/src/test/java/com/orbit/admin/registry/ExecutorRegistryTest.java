package com.orbit.admin.registry;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RegistryRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 执行器内存注册表单元测试。
 * 测试用例：
 * 
 *   - 节点注册与轮询（ROUND）负载均衡路由测试
 *   - 无节点时路由返回 null 测试
 *   - 节点主动下线注销测试
 * 
 */
class ExecutorRegistryTest {

    private ExecutorRegistry registry;

    @BeforeEach
    void setUp() {
        AdminProperties p = new AdminProperties();
        p.setHeartbeatTimeoutSeconds(90);
        registry = new ExecutorRegistry(p);
    }

    /**
     * 测试多节点注册后，ROUND 轮询路由能够均匀命中所有存活节点
     */
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

    /**
     * 测试无在线执行器时路由返回 null
     */
    @Test
    void routeEmptyReturnsNull() {
        assertNull(registry.route("missing", "ROUND"));
    }

    /**
     * 测试执行器主动注销后从在线列表中移除
     */
    @Test
    void remove() {
        registry.register(req("demo", "http://a:1"));
        registry.remove("demo", "http://a:1");
        assertEquals(0, registry.listByApp("demo").size());
    }

    /**
     * 构造模拟的注册请求实体
     *
     * @param app  应用名称
     * @param addr 通信地址
     * @return 注册请求
     */
    private static RegistryRequest req(String app, String addr) {
        RegistryRequest r = new RegistryRequest();
        r.setAppName(app);
        r.setAddress(addr);
        r.setNodeId("n1");
        r.setHandlers(Arrays.asList("h1"));
        return r;
    }
}

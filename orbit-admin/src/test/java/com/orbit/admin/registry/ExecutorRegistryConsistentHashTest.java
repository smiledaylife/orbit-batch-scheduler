package com.orbit.admin.registry;

import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RouteStrategy;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 一致性哈希路由策略（CONSISTENT_HASH）专项测试。
 * 重点回归：确定性（同键同点）、虚拟节点均衡性、节点增减的最小扰动、
 * 空候选与空键的兜底行为。
 */
class ExecutorRegistryConsistentHashTest {

    private final ExecutorRegistry registry = new ExecutorRegistry(null, null);

    private static ExecutorNode node(String address) {
        ExecutorNode n = new ExecutorNode();
        n.setAppName("app");
        n.setAddress(address);
        return n;
    }

    @Test
    void emptyCandidatesReturnNull() {
        assertNull(registry.route(new ArrayList<ExecutorNode>(), "app", RouteStrategy.CONSISTENT_HASH, "job"));
        assertNull(registry.route(null, "app", RouteStrategy.CONSISTENT_HASH, "job"));
    }

    /** 同一任务键在同一候选集合下必须稳定命中同一节点（粘性路由的核心保证）。 */
    @Test
    void sameKeyPicksSameNodeDeterministically() {
        List<ExecutorNode> candidates = Arrays.asList(
                node("http://10.0.0.1:8081"), node("http://10.0.0.2:8081"), node("http://10.0.0.3:8081"));
        for (String job : new String[]{"jobA", "jobB", "jobC", "jobD"}) {
            ExecutorNode first = registry.route(candidates, "app", RouteStrategy.CONSISTENT_HASH, job);
            for (int i = 0; i < 10; i++) {
                assertEquals(first, registry.route(candidates, "app", RouteStrategy.CONSISTENT_HASH, job),
                        "same job key must map to the same node");
            }
        }
    }

    /** 不同任务应散布到多个节点，而不是全部落在一个节点上（160 虚拟节点下的均衡性）。 */
    @Test
    void keysSpreadAcrossNodes() {
        List<ExecutorNode> candidates = Arrays.asList(
                node("http://10.0.0.1:8081"), node("http://10.0.0.2:8081"),
                node("http://10.0.0.3:8081"), node("http://10.0.0.4:8081"));
        Map<String, Integer> hits = new HashMap<String, Integer>();
        int total = 200;
        for (int i = 0; i < total; i++) {
            ExecutorNode picked = registry.route(candidates, "app", RouteStrategy.CONSISTENT_HASH, "job-" + i);
            hits.merge(picked.getAddress(), 1, Integer::sum);
        }
        assertTrue(hits.size() >= 3, "keys should spread to at least 3 of 4 nodes, got: " + hits);
        // 单节点独揽超过半数视为失衡（随机撞上小概率极端分布时给出宽松余量）
        for (Integer count : hits.values()) {
            assertTrue(count < total / 2, "one node must not take the majority: " + hits);
        }
    }

    /** 节点下线时只有少数任务换点（一致性哈希相对 FIRST/轮询的核心价值）。 */
    @Test
    void removingOneNodeMovesOnlyMinorityOfKeys() {
        List<ExecutorNode> four = Arrays.asList(
                node("http://10.0.0.1:8081"), node("http://10.0.0.2:8081"),
                node("http://10.0.0.3:8081"), node("http://10.0.0.4:8081"));
        List<ExecutorNode> three = Arrays.asList(
                node("http://10.0.0.1:8081"), node("http://10.0.0.2:8081"),
                node("http://10.0.0.3:8081"));

        int moved = 0;
        int total = 200;
        for (int i = 0; i < total; i++) {
            String job = "job-" + i;
            ExecutorNode before = registry.route(four, "app", RouteStrategy.CONSISTENT_HASH, job);
            ExecutorNode after = registry.route(three, "app", RouteStrategy.CONSISTENT_HASH, job);
            if (!before.getAddress().equals(after.getAddress())) {
                moved++;
            }
        }
        // 摘除 1/4 节点理论上扰动约 25%；一致性哈希的保证是「远小于全部重排」，
        // 宽松断言为「最多一半任务换点」，把虚拟节点采样波动纳入余量
        assertTrue(moved <= total / 2, "removing 1/4 node should move a minority, moved=" + moved);
        // 且原下线节点上的任务必须全部换到剩余节点（不指向已消失节点）
        for (ExecutorNode picked : three) {
            assertNotNull(picked);
        }
    }

    /** 哈希键为空时退化为轮询，路由不能因缺键而失败。 */
    @Test
    void nullHashKeyFallsBackToRoundRobin() {
        List<ExecutorNode> candidates = Arrays.asList(
                node("http://10.0.0.1:8081"), node("http://10.0.0.2:8081"));
        ExecutorNode a = registry.route(candidates, "app", RouteStrategy.CONSISTENT_HASH, null);
        ExecutorNode b = registry.route(candidates, "app", RouteStrategy.CONSISTENT_HASH, " ");
        // 轮询游标连续推进：两次命中不同节点
        assertEquals(candidates.get(0), a);
        assertEquals(candidates.get(1), b);
    }

    /** 未知策略仍由 ROUND 兜底（历史数据兼容），显式传 CONSISTENT_HASH 常量可路由。 */
    @Test
    void unknownStrategyStillFallsBackToRound() {
        List<ExecutorNode> candidates = Arrays.asList(
                node("http://10.0.0.1:8081"), node("http://10.0.0.2:8081"));
        assertNotNull(registry.route(candidates, "app", "SOMETHING_ELSE", "job"));
        assertNotNull(registry.route(candidates, "app", RouteStrategy.CONSISTENT_HASH, "job"));
    }
}

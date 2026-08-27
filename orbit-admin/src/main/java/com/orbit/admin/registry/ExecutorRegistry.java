package com.orbit.admin.registry;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RegistryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 执行器在线注册表（内存）。执行器周期性心跳；超时自动摘除。
 *
 * <p>云原生场景：Pod 扩缩容后新地址通过心跳自动加入，宕机地址超时剔除。
 */
@Component
public class ExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutorRegistry.class);

    private final AdminProperties properties;
    /** key = appName|address */
    private final ConcurrentHashMap<String, ExecutorNode> nodes = new ConcurrentHashMap<String, ExecutorNode>();
    private final ConcurrentHashMap<String, AtomicInteger> roundRobin = new ConcurrentHashMap<String, AtomicInteger>();

    public ExecutorRegistry(AdminProperties properties) {
        this.properties = properties;
    }

    public void register(RegistryRequest req) {
        if (req.getAppName() == null || req.getAppName().trim().isEmpty()) {
            throw new IllegalArgumentException("appName required");
        }
        if (req.getAddress() == null || req.getAddress().trim().isEmpty()) {
            throw new IllegalArgumentException("address required");
        }
        String app = req.getAppName().trim();
        String addr = trimSlash(req.getAddress().trim());
        String key = keyOf(app, addr);
        ExecutorNode node = new ExecutorNode();
        node.setAppName(app);
        node.setAddress(addr);
        node.setNodeId(req.getNodeId());
        node.setHandlers(req.getHandlers() == null
                ? Collections.<String>emptyList()
                : new ArrayList<String>(req.getHandlers()));
        node.setLastHeartbeat(new Date());
        node.setOnline(true);
        nodes.put(key, node);
        log.debug("[orbit-admin] registry upsert app={} address={} handlers={}",
                app, addr, node.getHandlers().size());
    }

    public void remove(String appName, String address) {
        if (appName == null || address == null) {
            return;
        }
        nodes.remove(keyOf(appName.trim(), trimSlash(address.trim())));
    }

    /** 清理超时节点 */
    public int evictExpired() {
        long timeoutMs = properties.getHeartbeatTimeoutSeconds() * 1000L;
        long now = System.currentTimeMillis();
        int removed = 0;
        for (Map.Entry<String, ExecutorNode> e : nodes.entrySet()) {
            ExecutorNode n = e.getValue();
            if (n.getLastHeartbeat() == null || now - n.getLastHeartbeat().getTime() > timeoutMs) {
                if (nodes.remove(e.getKey(), n)) {
                    removed++;
                    log.info("[orbit-admin] executor offline (heartbeat timeout): {} @ {}",
                            n.getAppName(), n.getAddress());
                }
            }
        }
        return removed;
    }

    public List<ExecutorNode> listAll() {
        return new ArrayList<ExecutorNode>(nodes.values());
    }

    public List<ExecutorNode> listByApp(String appName) {
        List<ExecutorNode> list = new ArrayList<ExecutorNode>();
        if (appName == null) {
            return list;
        }
        String app = appName.trim();
        for (ExecutorNode n : nodes.values()) {
            if (app.equals(n.getAppName()) && n.isOnline()) {
                list.add(n);
            }
        }
        return list;
    }

    /**
     * 按路由策略选择一个执行器。
     *
     * @param strategy ROUND / RANDOM / FIRST
     */
    public ExecutorNode route(String appName, String strategy) {
        List<ExecutorNode> list = listByApp(appName);
        if (list.isEmpty()) {
            return null;
        }
        String s = strategy == null ? "ROUND" : strategy.trim().toUpperCase();
        if ("RANDOM".equals(s)) {
            return list.get(ThreadLocalRandom.current().nextInt(list.size()));
        }
        if ("FIRST".equals(s)) {
            return list.get(0);
        }
        // ROUND
        AtomicInteger cursor = roundRobin.computeIfAbsent(appName, k -> new AtomicInteger(0));
        int idx = Math.floorMod(cursor.getAndIncrement(), list.size());
        return list.get(idx);
    }

    public int onlineCount() {
        return nodes.size();
    }

    private static String keyOf(String app, String address) {
        return app + "|" + address;
    }

    private static String trimSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }
}

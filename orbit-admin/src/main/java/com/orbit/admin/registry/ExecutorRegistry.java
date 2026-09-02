package com.orbit.admin.registry;

import com.orbit.admin.config.AdminProperties;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RegistryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调度中心执行器在线注册表（内存管理）。
 * 核心机制：
 * 
 *   - 基于 {@code appName|address} 作为全局唯一主键，支持高并发线程安全的节点心跳注册与状态刷新（Upsert）；
 *   - 支持心跳超时自动失效剔除（Eviction）；
 *   - 支持多副本实例的高效路由策略：轮询（ROUND）、随机（RANDOM）、首节点（FIRST）；
 *   - 天然适配 K8s Pod 动态扩缩容，滚动更新时无缝上线新 Pod 并摘除老 Pod。
 * 
 */
@Component
public class ExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutorRegistry.class);

    private final AdminProperties properties;

    /**
     * 在线执行器节点集合（key: appName|address，value: ExecutorNode）
     */
    private final ConcurrentHashMap<String, ExecutorNode> nodes = new ConcurrentHashMap<String, ExecutorNode>();

    /**
     * 轮询路由策略的原子递增游标（key: appName，value: 轮询计数器）
     */
    private final ConcurrentHashMap<String, AtomicInteger> roundRobin = new ConcurrentHashMap<String, AtomicInteger>();

    /**
     * 节点排序器：按地址升序。
     * 用于消除 {@link ConcurrentHashMap} 遍历顺序不确定带来的影响。
     */
    private static final Comparator<ExecutorNode> BY_ADDRESS = new Comparator<ExecutorNode>() {
        @Override
        public int compare(ExecutorNode a, ExecutorNode b) {
            return a.getAddress().compareTo(b.getAddress());
        }
    };

    public ExecutorRegistry(AdminProperties properties) {
        this.properties = properties;
    }

    /**
     * 执行器上线注册与心跳刷新逻辑。
     * 根据 appName 和 address 判定节点：若不存在则新增，若已存在则更新其最近心跳时间和支持的 Handlers 列表。
     *
     * @param req 执行器注册/心跳请求数据
     */
    public void register(RegistryRequest req) {
        // 校验入参必要性
        if (req.getAppName() == null || req.getAppName().trim().isEmpty()) {
            throw new IllegalArgumentException("appName required");
        }
        if (req.getAddress() == null || req.getAddress().trim().isEmpty()) {
            throw new IllegalArgumentException("address required");
        }

        String app = req.getAppName().trim();
        // 校验并规范化地址：拒绝非 http(s)、无 host、以及链路本地等保留地址（防 SSRF）
        String addr = ExecutorAddressValidator.validateAndNormalize(
                req.getAddress(), properties.getExecutorAddressAllowPattern());
        String key = keyOf(app, addr);

        // 组装并更新节点状态
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

    /**
     * 执行器主动下线注销。
     *
     * @param appName 应用名
     * @param address 节点地址
     */
    public void remove(String appName, String address) {
        if (appName == null || address == null) {
            return;
        }
        nodes.remove(keyOf(appName.trim(), trimSlash(address.trim())));
    }

    /**
     * 扫描注册表并清理心跳超时的失联节点。
     *
     * @return 本次清理摘除的节点数量
     */
    public int evictExpired() {
        long timeoutMs = properties.getHeartbeatTimeoutSeconds() * 1000L;
        long now = System.currentTimeMillis();
        int removed = 0;

        for (Map.Entry<String, ExecutorNode> e : nodes.entrySet()) {
            ExecutorNode n = e.getValue();
            // 若节点上次心跳时间距今超过配置的阈值，则认为已离线
            if (isExpired(n, now, timeoutMs)) {
                if (nodes.remove(e.getKey(), n)) {
                    removed++;
                    log.info("[orbit-admin] executor offline (heartbeat timeout): {} @ {}",
                            n.getAppName(), n.getAddress());
                }
            }
        }
        return removed;
    }

    /**
     * 判断节点心跳是否已超时。
     *
     * @param n         节点
     * @param now       当前时间戳（毫秒）
     * @param timeoutMs 超时阈值（毫秒）
     * @return 是否已失联
     */
    private static boolean isExpired(ExecutorNode n, long now, long timeoutMs) {
        return n.getLastHeartbeat() == null || now - n.getLastHeartbeat().getTime() > timeoutMs;
    }

    /**
     * 列出当前所有在线的执行器节点
     *
     * @return 在线节点列表
     */
    public List<ExecutorNode> listAll() {
        List<ExecutorNode> list = new ArrayList<ExecutorNode>(nodes.values());
        Collections.sort(list, BY_ADDRESS);
        return list;
    }

    /**
     * 按应用名称筛选在线的执行器节点
     *
     * @param appName 应用名称
     * @return 该应用下的在线节点列表
     */
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
        // ConcurrentHashMap.values() 无顺序保证，且遍历顺序可能随时变化。
        // 若不排序，FIRST 策略取到的「第一个」实际是任意节点，
        // ROUND 的取模也建立在乱序列表上，负载会不均。此处按地址排序保证确定性。
        Collections.sort(list, BY_ADDRESS);
        return list;
    }

    /**
     * 按照指定的路由策略从该应用的所有在线节点中选择一个执行器实例。
     *
     * @param appName  执行器应用名
     * @param strategy 路由策略：ROUND（轮询）、RANDOM（随机）、FIRST（首个）
     * @return 选中的执行器节点，若无可用在线实例则返回 null
     */
    public ExecutorNode route(String appName, String strategy) {
        List<ExecutorNode> list = listByApp(appName);
        if (list.isEmpty()) {
            return null;
        }

        String s = strategy == null ? "ROUND" : strategy.trim().toUpperCase();

        // 1. 随机路由策略
        if ("RANDOM".equals(s)) {
            return list.get(ThreadLocalRandom.current().nextInt(list.size()));
        }

        // 2. 固定首节点路由策略
        if ("FIRST".equals(s)) {
            return list.get(0);
        }

        // 3. 默认轮询路由策略（ROUND），基于原子计数器无锁递增取模
        AtomicInteger cursor = roundRobin.computeIfAbsent(appName, k -> new AtomicInteger(0));
        int idx = Math.floorMod(cursor.getAndIncrement(), list.size());
        return list.get(idx);
    }

    /**
     * 获取当前所有在线节点的总数。
     * <p>
     * 注意：注册表中的节点要等后台 evict 任务扫描后才会被物理移除，
     * 因此不能直接用 {@code nodes.size()} —— 那会把「心跳已超时但尚未被剔除」的节点也算成在线。
     *
     * @return 心跳未超时的节点总数
     */
    public int onlineCount() {
        long timeoutMs = properties.getHeartbeatTimeoutSeconds() * 1000L;
        long now = System.currentTimeMillis();
        int count = 0;
        for (ExecutorNode n : nodes.values()) {
            if (n.isOnline() && !isExpired(n, now, timeoutMs)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 生成注册表的唯一复合主键：appName|address
     *
     * @param app     应用名
     * @param address 节点地址
     * @return 复合主键
     */
    private static String keyOf(String app, String address) {
        return app + "|" + address;
    }

    /**
     * 规范化地址，去除末尾可能多余的斜杠
     *
     * @param s 地址字符串
     * @return 规范化字符串
     */
    private static String trimSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }
}

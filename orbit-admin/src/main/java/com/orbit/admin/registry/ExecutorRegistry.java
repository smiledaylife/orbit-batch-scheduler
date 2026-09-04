package com.orbit.admin.registry;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.store.mapper.OrbitExecutorRegistryMapper;
import com.orbit.admin.store.po.OrbitExecutorRegistryPO;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RegistryRequest;
import com.orbit.core.model.RouteStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调度中心执行器在线注册表（共享库，对齐 XXL-JOB {@code xxl_job_registry}）。
 * <p>
 * 心跳 upsert 写入 {@code orbit_executor_registry}，任意 admin 副本读到同一份在线节点。
 * 因此调度中心可用无状态 Deployment + 普通 Service：执行器只需把心跳打到
 * {@code http://orbit-admin:8080}，不必 StatefulSet / Headless DNS 逐副本上报。
 * 轮询游标仍为本进程内存（负载略偏也可接受，各副本独立 ROUND）。
 * <p>
 * 性能设计：读路径（调度派发 / API 查询 / 在线计数）经<b>短 TTL 本地缓存</b>提供，
 * 由 {@code orbit.admin.registry-cache-ttl-ms} 控制（默认 3 秒，0 = 关闭）。
 * 写操作（注册 / 摘除 / 超时剔除）在变更数据库的同时<b>立即失效本进程缓存</b>；
 * 多副本间的一致性由 TTL 上界保证，命中已下线节点的派发由 failover 兜底。
 */
@Component
public class ExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutorRegistry.class);

    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<List<String>>() {
    };

    /** handlers 列宽（与 schema.sql 的 VARCHAR(2000) 一致），超出时收缩列表防溢出 */
    private static final int HANDLERS_JSON_MAX_LEN = 2000;

    private final AdminProperties properties;
    private final OrbitExecutorRegistryMapper mapper;
    private final ObjectMapper json = new ObjectMapper();

    /**
     * 轮询路由策略的原子递增游标（key: appName）。进程本地即可。
     */
    private final ConcurrentHashMap<String, AtomicInteger> roundRobin = new ConcurrentHashMap<String, AtomicInteger>();

    /** 地址比较器（null 视为最大，防御历史脏数据导致的 NPE） */
    private static final Comparator<ExecutorNode> BY_ADDRESS = new Comparator<ExecutorNode>() {
        @Override
        public int compare(ExecutorNode a, ExecutorNode b) {
            String x = a.getAddress();
            String y = b.getAddress();
            if (x == null && y == null) {
                return 0;
            }
            return x == null ? 1 : (y == null ? -1 : x.compareTo(y));
        }
    };

    /**
     * 注册表本地缓存（不可变快照）。volatile 单引用替换，读无锁。
     */
    private static final class RegistrySnapshot {
        final long expiresAtMs;
        final List<ExecutorNode> nodes;

        RegistrySnapshot(long expiresAtMs, List<ExecutorNode> nodes) {
            this.expiresAtMs = expiresAtMs;
            this.nodes = nodes;
        }
    }

    private volatile RegistrySnapshot cache;

    /** 缓存重建锁：防止过期瞬间的并发重建风暴 */
    private final Object cacheLock = new Object();

    public ExecutorRegistry(AdminProperties properties, OrbitExecutorRegistryMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
    }

    /**
     * 执行器上线注册与心跳刷新：按 (appName, address) upsert。
     * 写库后立即失效本地缓存，保证本进程随后的路由立刻可见新节点。
     */
    public void register(RegistryRequest req) {
        if (req.getAppName() == null || req.getAppName().trim().isEmpty()) {
            throw new IllegalArgumentException("appName required");
        }
        if (req.getAddress() == null || req.getAddress().trim().isEmpty()) {
            throw new IllegalArgumentException("address required");
        }

        String app = req.getAppName().trim();
        String addr = ExecutorAddressValidator.validateAndNormalize(
                req.getAddress(), properties.getExecutorAddressAllowPattern());
        Date now = new Date();
        String handlersJson = toHandlersJson(req.getHandlers());

        LambdaUpdateWrapper<OrbitExecutorRegistryPO> uw = new LambdaUpdateWrapper<OrbitExecutorRegistryPO>()
                .eq(OrbitExecutorRegistryPO::getAppName, app)
                .eq(OrbitExecutorRegistryPO::getAddress, addr)
                .set(OrbitExecutorRegistryPO::getNodeId, req.getNodeId())
                .set(OrbitExecutorRegistryPO::getHandlers, handlersJson)
                .set(OrbitExecutorRegistryPO::getLastHeartbeat, now);
        int updated = mapper.update(null, uw);
        if (updated > 0) {
            invalidateCache();
            log.debug("[orbit-admin] registry heartbeat app={} address={}", app, addr);
            return;
        }

        OrbitExecutorRegistryPO po = new OrbitExecutorRegistryPO();
        po.setAppName(app);
        po.setAddress(addr);
        po.setNodeId(req.getNodeId());
        po.setHandlers(handlersJson);
        po.setLastHeartbeat(now);
        try {
            mapper.insert(po);
            invalidateCache();
            log.debug("[orbit-admin] registry insert app={} address={}", app, addr);
        } catch (DataIntegrityViolationException dup) {
            // 并发首次注册：另一副本已插入，再刷一次心跳
            mapper.update(null, uw);
            invalidateCache();
        }
    }

    /**
     * 主动摘除执行器节点（下线通知或 failover 不可达摘除），并立即失效缓存。
     */
    public void remove(String appName, String address) {
        if (appName == null || address == null) {
            return;
        }
        String addr = trimSlash(address.trim());
        int deleted = mapper.delete(new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                .eq(OrbitExecutorRegistryPO::getAppName, appName.trim())
                .eq(OrbitExecutorRegistryPO::getAddress, addr));
        if (deleted > 0) {
            invalidateCache();
        }
    }

    /**
     * 物理删除心跳超时的失联节点。
     */
    public int evictExpired() {
        Date cutoff = new Date(System.currentTimeMillis() - properties.getHeartbeatTimeoutSeconds() * 1000L);
        int deleted = mapper.delete(new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                .lt(OrbitExecutorRegistryPO::getLastHeartbeat, cutoff));
        if (deleted > 0) {
            invalidateCache();
        }
        return deleted;
    }

    /**
     * 查询全部在线执行器节点（经 TTL 缓存）。
     * 返回列表为防御性副本，调用方可安全修改。
     */
    public List<ExecutorNode> listAll() {
        return new ArrayList<ExecutorNode>(snapshot().nodes);
    }

    /**
     * 按应用名查询在线执行器节点（经 TTL 缓存后过滤，保持地址序）。
     */
    public List<ExecutorNode> listByApp(String appName) {
        if (appName == null) {
            return new ArrayList<ExecutorNode>();
        }
        String target = appName.trim();
        List<ExecutorNode> all = snapshot().nodes;
        List<ExecutorNode> matched = new ArrayList<ExecutorNode>(all.size());
        for (ExecutorNode n : all) {
            if (target.equals(n.getAppName())) {
                matched.add(n);
            }
        }
        return matched;
    }

    /**
     * 便捷路由入口：自行查询候选列表并按策略选点（保留旧 API）。
     * 热路径（任务派发）请改用 {@link #route(List, String, String)} 复用已查出的候选列表，
     * 避免一次派发查两遍库。
     */
    public ExecutorNode route(String appName, String strategy) {
        return route(listByApp(appName), appName, strategy);
    }

    /**
     * 在<b>已查出的候选列表</b>上按路由策略选点。
     * 新增该方法使 {@code dispatch} 能以一次数据库（或缓存）查询完成「取候选 + 选起点」。
     *
     * @param candidates 候选节点列表（非空时生效）
     * @param appName    应用名（仅作为轮询游标 key）
     * @param strategy   路由策略：ROUND / RANDOM / FIRST
     * @return 选中的节点；候选为空时返回 null
     */
    public ExecutorNode route(List<ExecutorNode> candidates, String appName, String strategy) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }

        String s = strategy == null ? RouteStrategy.ROUND : strategy.trim().toUpperCase();

        if (RouteStrategy.RANDOM.equals(s)) {
            return candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
        }

        if (RouteStrategy.FIRST.equals(s)) {
            return candidates.get(0);
        }

        // ROUND（含未知策略的历史数据，与旧行为一致：按轮询处理）
        AtomicInteger cursor = roundRobin.computeIfAbsent(appName, k -> new AtomicInteger(0));
        int idx = Math.floorMod(cursor.getAndIncrement(), candidates.size());
        return candidates.get(idx);
    }

    /**
     * 在线执行器节点数（经 TTL 缓存，避免 overview/探针高频 count 查询）。
     */
    public int onlineCount() {
        return snapshot().nodes.size();
    }

    // ============================ 缓存内部实现 ============================

    /**
     * 读取当前有效快照：命中且未过期直接返回；否则（加锁）重建。
     * 重建采用全量查询 + 内存过滤，列表规模为在线节点数（通常几十到几百），成本可忽略。
     */
    private RegistrySnapshot snapshot() {
        long ttl = properties.getRegistryCacheTtlMs();
        RegistrySnapshot c = cache;
        if (ttl <= 0) {
            // 缓存关闭：每次直查数据库
            return loadSnapshot(0);
        }
        if (c != null && c.expiresAtMs > System.currentTimeMillis()) {
            return c;
        }
        synchronized (cacheLock) {
            c = cache;
            if (c != null && c.expiresAtMs > System.currentTimeMillis()) {
                return c;
            }
            c = loadSnapshot(ttl);
            cache = c;
            return c;
        }
    }

    /**
     * 从数据库加载快照（按地址升序，与既有排序语义一致）。
     *
     * @param ttl 缓存有效期（毫秒）；&lt;=0 表示不做缓存复用
     */
    private RegistrySnapshot loadSnapshot(long ttl) {
        List<OrbitExecutorRegistryPO> rows = mapper.selectList(
                new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                        .ge(OrbitExecutorRegistryPO::getLastHeartbeat, aliveSince())
                        .orderByAsc(OrbitExecutorRegistryPO::getAddress));
        List<ExecutorNode> nodes = new ArrayList<ExecutorNode>(rows.size());
        for (OrbitExecutorRegistryPO po : rows) {
            nodes.add(toNode(po));
        }
        Collections.sort(nodes, BY_ADDRESS);
        long expiresAt = ttl <= 0 ? 0 : System.currentTimeMillis() + ttl;
        return new RegistrySnapshot(expiresAt, Collections.unmodifiableList(nodes));
    }

    /**
     * 立即失效本地缓存（写路径调用）。
     */
    private void invalidateCache() {
        cache = null;
    }

    private Date aliveSince() {
        return new Date(System.currentTimeMillis() - properties.getHeartbeatTimeoutSeconds() * 1000L);
    }

    // ============================ 转换与工具 ============================

    private ExecutorNode toNode(OrbitExecutorRegistryPO po) {
        ExecutorNode node = new ExecutorNode();
        node.setAppName(po.getAppName());
        node.setAddress(po.getAddress());
        node.setNodeId(po.getNodeId());
        node.setHandlers(parseHandlers(po.getHandlers()));
        node.setLastHeartbeat(po.getLastHeartbeat());
        node.setOnline(true);
        return node;
    }

    /**
     * 序列化 handler 列表为 JSON，并确保不超过 {@code handlers} 列宽（2000）。
     * 超出时自尾部收缩列表直至可完整入库——否则海量 handler 的执行器心跳会因
     * DataIntegrityViolationException 而<b>永久注册失败</b>（每轮心跳都撞列宽）。
     */
    private String toHandlersJson(List<String> handlers) {
        List<String> src = handlers == null ? Collections.<String>emptyList() : handlers;
        try {
            String s = json.writeValueAsString(src);
            if (s.length() <= HANDLERS_JSON_MAX_LEN) {
                return s;
            }
            List<String> shrink = new ArrayList<String>(src);
            while (!shrink.isEmpty()) {
                shrink.remove(shrink.size() - 1);
                s = json.writeValueAsString(shrink);
                if (s.length() <= HANDLERS_JSON_MAX_LEN) {
                    log.warn("[orbit-admin] handlers json exceeds column width {}, truncated to {} entries",
                            HANDLERS_JSON_MAX_LEN, shrink.size());
                    return s;
                }
            }
            return "[]";
        } catch (Exception e) {
            return "[]";
        }
    }

    private List<String> parseHandlers(String raw) {
        if (raw == null || raw.trim().isEmpty()) {
            return Collections.emptyList();
        }
        try {
            List<String> list = json.readValue(raw, STRING_LIST);
            return list == null ? Collections.<String>emptyList() : list;
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    private static String trimSlash(String s) {
        return s.endsWith("/") && s.length() > 1 ? s.substring(0, s.length() - 1) : s;
    }
}

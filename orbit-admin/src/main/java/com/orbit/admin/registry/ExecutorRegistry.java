package com.orbit.admin.registry;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orbit.admin.config.AdminProperties;
import com.orbit.admin.store.ColumnLimits;
import com.orbit.admin.store.mapper.OrbitExecutorRegistryMapper;
import com.orbit.admin.store.po.OrbitExecutorRegistryPO;
import com.orbit.core.model.ExecutorNode;
import com.orbit.core.model.RegistryRequest;
import com.orbit.core.model.RouteStrategy;
import com.orbit.core.protocol.OrbitProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 调度中心执行器在线注册表（共享库，对齐 XXL-JOB {@code xxl_job_registry}）。
 *
 * 心跳 upsert 写入 {@code orbit_executor_registry}，任意 admin 副本读到同一份在线节点。
 * 因此调度中心可用无状态 Deployment + 普通 Service：执行器只需把心跳打到
 * {@code http://orbit-admin:8080}，不必 StatefulSet / Headless DNS 逐副本上报。
 * 轮询游标仍为本进程内存（负载略偏也可接受，各副本独立 ROUND）。
 *
 * 性能设计：读路径（调度派发 / API 查询 / 在线计数）经短 TTL 本地缓存提供，
 * 由 {@code orbit.admin.registry-cache-ttl-ms} 控制（默认 3 秒，0 = 关闭）。
 * 写操作（注册 / 摘除 / 超时剔除）在变更数据库的同时立即失效本进程缓存；
 * 多副本间的一致性由 TTL 上界保证，命中已下线节点的派发由 failover 兜底。
 */
@Component
public class ExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutorRegistry.class);

    /** handlers 列的 JSON 反序列化目标类型 */
    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<List<String>>() {
    };

    /** 调度中心配置：提供心跳超时与缓存 TTL */
    private final AdminProperties properties;

    /** 注册表 Mapper */
    private final OrbitExecutorRegistryMapper mapper;

    /** handlers 列的 JSON 编解码器（线程安全，单实例复用） */
    private final ObjectMapper json = new ObjectMapper();

    /**
     * 轮询路由策略的原子递增游标（key: appName）。进程本地即可。
     */
    private final ConcurrentHashMap<String, AtomicInteger> roundRobin = new ConcurrentHashMap<String, AtomicInteger>();

    /** 地址比较器（null 视为最大，防御历史脏数据导致的 NPE） */
    private static final Comparator<ExecutorNode> BY_ADDRESS =
            Comparator.comparing(ExecutorNode::getAddress, Comparator.nullsLast(String::compareTo));

    /**
     * 注册表本地缓存（不可变快照）。JDK 21 record：两个只读组件、无行为，
     * volatile 单引用替换，读无锁。
     */
    private record RegistrySnapshot(
            /** 快照失效时刻（epoch 毫秒），到点后下一次读会重建 */
            long expiresAtMs,
            /** 快照内容：按地址升序、已过滤失联节点的不可变列表 */
            List<ExecutorNode> nodes) {
    }

    /** 当前生效的缓存快照；null 表示需要重建 */
    private volatile RegistrySnapshot cache;

    /** 缓存重建锁：防止过期瞬间的并发重建风暴 */
    private final Object cacheLock = new Object();

    /**
     * @param properties 调度中心配置，提供心跳超时与缓存 TTL
     * @param mapper     注册表 Mapper
     */
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
        // 列宽前置校验：这三个值都来自执行器上报，超长会让每一轮心跳都以 SQL 异常失败，
        // 且日志里只有一句被截断的驱动报错。这里直接给出字段名与上限，便于定位到具体配置项。
        ColumnLimits.requireMaxLength("appName", app, ColumnLimits.REG_APP_NAME);
        ColumnLimits.requireMaxLength("address", addr, ColumnLimits.REG_ADDRESS);
        ColumnLimits.requireMaxLength("nodeId", req.getNodeId(), ColumnLimits.REG_NODE_ID);
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
        String addr = OrbitProtocol.trimTrailingSlash(address.trim());
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
        Date cutoff = new Date(System.currentTimeMillis() - heartbeatTimeoutMs());
        int deleted = mapper.delete(new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                .lt(OrbitExecutorRegistryPO::getLastHeartbeat, cutoff));
        if (deleted > 0) {
            invalidateCache();
        }
        return deleted;
    }

    /** 查询全部在线执行器节点（经 TTL 缓存）。返回列表为防御性副本，调用方可安全修改。 */
    public List<ExecutorNode> listAll() {
        return new ArrayList<ExecutorNode>(snapshot().nodes());
    }

    /**
     * 按应用名查询在线执行器节点（经 TTL 缓存后过滤，保持地址序）。
     */
    public List<ExecutorNode> listByApp(String appName) {
        if (appName == null) {
            return new ArrayList<ExecutorNode>();
        }
        String target = appName.trim();
        List<ExecutorNode> all = snapshot().nodes();
        List<ExecutorNode> matched = new ArrayList<ExecutorNode>(all.size());
        for (ExecutorNode n : all) {
            if (target.equals(n.getAppName())) {
                matched.add(n);
            }
        }
        return matched;
    }


    /**
     * 在已查出的候选列表上按路由策略选点。
     * 新增该方法使 {@code dispatch} 能以一次数据库（或缓存）查询完成「取候选 + 选起点」。
     *
     * @param candidates 候选节点列表（非空时生效）
     * @param appName    应用名（仅作为轮询游标 key）
     * @param strategy   路由策略：ROUND / RANDOM / FIRST / CONSISTENT_HASH
     * @return 选中的节点；候选为空时返回 null
     */
    public ExecutorNode route(List<ExecutorNode> candidates, String appName, String strategy) {
        return route(candidates, appName, strategy, null);
    }

    /**
     * 路由选点（完整版本，支持一致性哈希的哈希键）。
     *
     * JDK 21 箭头 switch（JEP 441）分发路由策略：
     * case 标签直接引用 {@link RouteStrategy} 常量（编译期常量），
     * ROUND 作为 default 分支兜底未知策略（兼容库里已有的历史数据）。
     *
     * CONSISTENT_HASH 以 {@code hashKey}（通常为任务名）对虚拟节点环做哈希：
     * 同一任务在同一候选集合下总是命中同一节点；节点增减时只有少数任务换点。
     * hashKey 为空时退化为 ROUND —— 一致性哈希没有键就没有意义，但路由不能因此失败。
     *
     * @param candidates 候选节点列表（非空时生效）
     * @param appName    应用名（仅作为轮询游标 key）
     * @param strategy   路由策略：ROUND / RANDOM / FIRST / CONSISTENT_HASH
     * @param hashKey    一致性哈希键（通常为任务名；可为空，为空时 CONSISTENT_HASH 退化为 ROUND）
     * @return 选中的节点；候选为空时返回 null
     */
    public ExecutorNode route(List<ExecutorNode> candidates, String appName, String strategy, String hashKey) {
        if (candidates == null || candidates.isEmpty()) {
            return null;
        }

        // Locale.ROOT：默认 toUpperCase 在土耳其语等 locale 下会把 "first" 变成 "FİRST"
        String s = strategy == null ? RouteStrategy.ROUND : strategy.trim().toUpperCase(Locale.ROOT);

        return switch (s) {
            case RouteStrategy.RANDOM ->
                    candidates.get(ThreadLocalRandom.current().nextInt(candidates.size()));
            // JDK 21 SequencedCollection：getFirst() 替代 get(0)，意图更明确
            case RouteStrategy.FIRST -> candidates.getFirst();
            case RouteStrategy.CONSISTENT_HASH ->
                    hashKey == null || hashKey.trim().isEmpty()
                            ? roundRobinPick(candidates, appName)
                            : consistentHashPick(candidates, hashKey);
            default -> roundRobinPick(candidates, appName);
        };
    }

    /**
     * 轮询选点：游标只在选中 ROUND（或一致性哈希的无键退化）时推进，
     * 避免与 RANDOM/FIRST 混用时游标漂移。
     */
    private ExecutorNode roundRobinPick(List<ExecutorNode> candidates, String appName) {
        AtomicInteger cursor = roundRobin.computeIfAbsent(appName, k -> new AtomicInteger(0));
        return candidates.get(Math.floorMod(cursor.getAndIncrement(), candidates.size()));
    }

    /** 一致性哈希每节点的虚拟节点数：XXL-JOB 同款量级，均衡性与环重建成本的折中 */
    private static final int VIRTUAL_NODES = 160;

    /**
     * 一致性哈希选点：虚拟节点环 + FNV-1a 哈希。
     *
     * 实现：每个候选地址在环上展开 {@link #VIRTUAL_NODES} 个虚拟节点
     * （键 = hash(地址 + "#VN" + 序号)），任务键命中环上第一个 ≥ hash(hashKey) 的节点。
     * 环在每次派发时重建 —— 候选量级为在线节点数（通常几十），160×N 次
     * TreeMap.put 的开销在派发热路径上可忽略（相对一次 HTTP 触发往返），
     * 换来的是无状态、无失效一致性问题。
     *
     * 哈希函数用 FNV-1a 64 位 + splitmix64 雪崩混淆：纯算术运算，跨 JVM 结果确定，
     * 不依赖任何库；顺序命名的任务键同样均匀分布（见 mix64 注释）。
     * 64 位空间下虚拟节点键的碰撞概率可忽略，碰撞时 TreeMap put 后写者覆盖，
     * 影响仅限单个虚拟节点的归属，不破坏整体均匀性。
     *
     * @param candidates 候选节点列表（非空）
     * @param hashKey    哈希键（非空）
     * @return 命中的节点
     */
    private static ExecutorNode consistentHashPick(List<ExecutorNode> candidates, String hashKey) {
        TreeMap<Long, ExecutorNode> ring = new TreeMap<Long, ExecutorNode>();
        for (ExecutorNode node : candidates) {
            String address = node.getAddress() == null
                    ? String.valueOf(node.getNodeId()) : node.getAddress();
            for (int i = 0; i < VIRTUAL_NODES; i++) {
                ring.put(fnv1a64(address + "#VN" + i), node);
            }
        }
        Map.Entry<Long, ExecutorNode> hit = ring.ceilingEntry(fnv1a64(hashKey));
        if (hit == null) {
            // 哈希值落在环尾：环绕到第一个节点
            hit = ring.firstEntry();
        }
        return hit.getValue();
    }

    /** FNV-1a 64 位哈希 + splitmix64 雪崩混淆（offset basis = 0xcbf29ce484222325，prime = 0x100000001b3） */
    private static long fnv1a64(String input) {
        long hash = 0xcbf29ce484222325L;
        for (int i = 0; i < input.length(); i++) {
            hash ^= input.charAt(i);
            hash *= 0x100000001b3L;
        }
        return mix64(hash);
    }

    /**
     * splitmix64 终末混淆：消除 FNV-1a 对顺序键的簇聚。
     *
     * 必须要有这一步：FNV-1a 逐字符乘素数，对仅末尾字符不同的顺序键（如 job-1 / job-2），
     * 哈希差值约等于素数本身的量级（~1e12），相对 2^64 环面是极小的一段弧 ——
     * 任务名连续编号时全部落在同一二三个节点上，一致性哈希失去均衡性。
     * 终末混淆把每一位的差异充分扩散到全 64 位，顺序键同样均匀分布。
     */
    private static long mix64(long x) {
        x ^= x >>> 33;
        x *= 0xff51afd7ed558ccdL;
        x ^= x >>> 33;
        x *= 0xc4ceb9fe1a85ec53L;
        x ^= x >>> 33;
        return x;
    }

    /**
     * 在线执行器节点数（经 TTL 缓存，避免 overview/探针高频 count 查询）。
     */
    public int onlineCount() {
        return snapshot().nodes().size();
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
        if (c != null && c.expiresAtMs() > System.currentTimeMillis()) {
            return c;
        }
        synchronized (cacheLock) {
            c = cache;
            if (c != null && c.expiresAtMs() > System.currentTimeMillis()) {
                return c;
            }
            c = loadSnapshot(ttl);
            cache = c;
            return c;
        }
    }

    /**
     * 从数据库加载快照，并在内存里按地址升序排列。
     *
     * 排序只在内存做一次：BY_ADDRESS 对 null 地址有确定语义，而各数据库 ASC 的 NULL 位置并不一致，
     * 交给 SQL 排反而要额外约束；在线节点数量级为几十到几百，内存排序成本可忽略。
     *
     * @param ttl 缓存有效期（毫秒）；小于等于 0 表示不做缓存复用
     */
    private RegistrySnapshot loadSnapshot(long ttl) {
        List<OrbitExecutorRegistryPO> rows = mapper.selectList(
                new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                        .ge(OrbitExecutorRegistryPO::getLastHeartbeat, aliveSince()));
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

    /**
     * 存活判定的心跳下界：早于该时刻的心跳一律视为失联。
     *
     * @return now − heartbeatTimeoutMs()
     */
    private Date aliveSince() {
        return new Date(System.currentTimeMillis() - heartbeatTimeoutMs());
    }

    /**
     * 心跳超时时长（毫秒），下限 5 秒。
     *
     * 该项直接来自配置且没有下限保护：配成 0 或负数时 cutoff 会落到当前时刻甚至未来，
     * evictExpired 会在每一轮扫描里删掉整张注册表，aliveSince 则让所有节点都查不出来 ——
     * 表现为「执行器明明在心跳，调度中心却说没有在线执行器」，且没有任何报错。
     */
    private long heartbeatTimeoutMs() {
        int seconds = properties.getHeartbeatTimeoutSeconds();
        return (seconds < 5 ? 5 : seconds) * 1000L;
    }

    // ============================ 转换与工具 ============================

    /**
     * 把注册表 PO 转成对外的节点模型，顺带把 handlers 列的 JSON 解成列表。
     *
     * @param po 数据库行
     * @return 节点模型
     */
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
     * DataIntegrityViolationException 而永久注册失败（每轮心跳都撞列宽）。
     */
    private String toHandlersJson(List<String> handlers) {
        List<String> src = handlers == null ? Collections.<String>emptyList() : handlers;
        try {
            String s = json.writeValueAsString(src);
            if (s.length() <= ColumnLimits.REG_HANDLERS_JSON) {
                return s;
            }
            List<String> shrink = new ArrayList<String>(src);
            while (!shrink.isEmpty()) {
                // JDK 21 SequencedCollection：removeLast() 替代 remove(size() - 1)
                shrink.removeLast();
                s = json.writeValueAsString(shrink);
                if (s.length() <= ColumnLimits.REG_HANDLERS_JSON) {
                    log.warn("[orbit-admin] handlers json exceeds column width {}, truncated to {} entries",
                            ColumnLimits.REG_HANDLERS_JSON, shrink.size());
                    return s;
                }
            }
            return "[]";
        } catch (Exception e) {
            return "[]";
        }
    }

    /**
     * 解析 handlers 列的 JSON 数组。
     *
     * 空值、空串与非法 JSON 都返回空列表而不是抛异常：注册表里可能有历史脏数据，
     * 一个坏节点不应该让整次路由查询失败。
     *
     * @param raw handlers 列原始字符串
     * @return handler 名称列表，解析不出时为空列表
     */
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


}

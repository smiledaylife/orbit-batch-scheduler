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
 *
 * 心跳 upsert 写入 {@code orbit_executor_registry}，任意 admin 副本读到同一份在线节点。
 * 因此调度中心可用无状态 Deployment + 普通 Service：执行器只需把心跳打到
 * {@code http://orbit-admin:8080}，不必 StatefulSet / Headless DNS 逐副本上报。
 * 轮询游标仍为本进程内存（负载略偏也可接受，各副本独立 ROUND）。
 */
@Component
public class ExecutorRegistry {

    private static final Logger log = LoggerFactory.getLogger(ExecutorRegistry.class);

    private static final TypeReference<List<String>> STRING_LIST = new TypeReference<List<String>>() {
    };

    private final AdminProperties properties;
    private final OrbitExecutorRegistryMapper mapper;
    private final ObjectMapper json = new ObjectMapper();

    /**
     * 轮询路由策略的原子递增游标（key: appName）。进程本地即可。
     */
    private final ConcurrentHashMap<String, AtomicInteger> roundRobin = new ConcurrentHashMap<String, AtomicInteger>();

    private static final Comparator<ExecutorNode> BY_ADDRESS = new Comparator<ExecutorNode>() {
        @Override
        public int compare(ExecutorNode a, ExecutorNode b) {
            return a.getAddress().compareTo(b.getAddress());
        }
    };

    public ExecutorRegistry(AdminProperties properties, OrbitExecutorRegistryMapper mapper) {
        this.properties = properties;
        this.mapper = mapper;
    }

    /**
     * 执行器上线注册与心跳刷新：按 (appName, address) upsert。
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
            log.debug("[orbit-admin] registry insert app={} address={}", app, addr);
        } catch (DataIntegrityViolationException dup) {
            // 并发首次注册：另一副本已插入，再刷一次心跳
            mapper.update(null, uw);
        }
    }

    public void remove(String appName, String address) {
        if (appName == null || address == null) {
            return;
        }
        String addr = trimSlash(address.trim());
        mapper.delete(new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                .eq(OrbitExecutorRegistryPO::getAppName, appName.trim())
                .eq(OrbitExecutorRegistryPO::getAddress, addr));
    }

    /**
     * 物理删除心跳超时的失联节点。
     */
    public int evictExpired() {
        Date cutoff = new Date(System.currentTimeMillis() - properties.getHeartbeatTimeoutSeconds() * 1000L);
        return mapper.delete(new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                .lt(OrbitExecutorRegistryPO::getLastHeartbeat, cutoff));
    }

    public List<ExecutorNode> listAll() {
        List<OrbitExecutorRegistryPO> rows = mapper.selectList(
                new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                        .ge(OrbitExecutorRegistryPO::getLastHeartbeat, aliveSince())
                        .orderByAsc(OrbitExecutorRegistryPO::getAddress));
        return toNodes(rows);
    }

    public List<ExecutorNode> listByApp(String appName) {
        if (appName == null) {
            return new ArrayList<ExecutorNode>();
        }
        List<OrbitExecutorRegistryPO> rows = mapper.selectList(
                new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                        .eq(OrbitExecutorRegistryPO::getAppName, appName.trim())
                        .ge(OrbitExecutorRegistryPO::getLastHeartbeat, aliveSince())
                        .orderByAsc(OrbitExecutorRegistryPO::getAddress));
        return toNodes(rows);
    }

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

        AtomicInteger cursor = roundRobin.computeIfAbsent(appName, k -> new AtomicInteger(0));
        int idx = Math.floorMod(cursor.getAndIncrement(), list.size());
        return list.get(idx);
    }

    public int onlineCount() {
        Long n = mapper.selectCount(new LambdaQueryWrapper<OrbitExecutorRegistryPO>()
                .ge(OrbitExecutorRegistryPO::getLastHeartbeat, aliveSince()));
        return n == null ? 0 : n.intValue();
    }

    private Date aliveSince() {
        return new Date(System.currentTimeMillis() - properties.getHeartbeatTimeoutSeconds() * 1000L);
    }

    private List<ExecutorNode> toNodes(List<OrbitExecutorRegistryPO> rows) {
        List<ExecutorNode> list = new ArrayList<ExecutorNode>(rows.size());
        for (OrbitExecutorRegistryPO po : rows) {
            list.add(toNode(po));
        }
        Collections.sort(list, BY_ADDRESS);
        return list;
    }

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

    private String toHandlersJson(List<String> handlers) {
        List<String> src = handlers == null ? Collections.<String>emptyList() : handlers;
        try {
            return json.writeValueAsString(src);
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

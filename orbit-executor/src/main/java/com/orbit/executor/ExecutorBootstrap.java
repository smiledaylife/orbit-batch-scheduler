package com.orbit.executor;

import com.orbit.core.model.RegistryRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.net.InetAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 启动后向调度中心注册，并周期性心跳；停机时主动下线。
 */
public class ExecutorBootstrap implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(ExecutorBootstrap.class);

    private final ExecutorProperties properties;
    private final JobHandlerRegistry handlerRegistry;
    private final AdminClient adminClient;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;
    private String resolvedAddress;
    private String resolvedNodeId;

    public ExecutorBootstrap(ExecutorProperties properties, JobHandlerRegistry handlerRegistry,
                             AdminClient adminClient) {
        this.properties = properties;
        this.handlerRegistry = handlerRegistry;
        this.adminClient = adminClient;
    }

    @Override
    public void start() {
        if (!properties.isEnabled() || !running.compareAndSet(false, true)) {
            return;
        }
        resolvedAddress = resolveAddress();
        resolvedNodeId = resolveNodeId();
        log.info("[orbit-executor] starting appName={} address={} admin={}",
                properties.getAppName(), resolvedAddress, properties.getAdminAddresses());
        heartbeatOnce();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "orbit-executor-heartbeat");
            t.setDaemon(true);
            return t;
        });
        long interval = Math.max(5000L, properties.getHeartbeatIntervalMs());
        scheduler.scheduleAtFixedRate(this::heartbeatOnce, interval, interval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        try {
            RegistryRequest req = buildRequest();
            adminClient.remove(req);
            log.info("[orbit-executor] unregistered from admin");
        } catch (Exception e) {
            log.warn("[orbit-executor] unregister failed: {}", e.getMessage());
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    private void heartbeatOnce() {
        try {
            boolean ok = adminClient.registry(buildRequest());
            if (ok) {
                log.debug("[orbit-executor] heartbeat ok handlers={}", handlerRegistry.listNames().size());
            }
        } catch (Exception e) {
            log.warn("[orbit-executor] heartbeat error: {}", e.getMessage());
        }
    }

    private RegistryRequest buildRequest() {
        RegistryRequest req = new RegistryRequest();
        req.setAppName(properties.getAppName());
        req.setAddress(resolvedAddress);
        req.setNodeId(resolvedNodeId);
        req.setHandlers(handlerRegistry.listNames());
        return req;
    }

    private String resolveAddress() {
        if (properties.getAddress() != null && !properties.getAddress().trim().isEmpty()) {
            String a = properties.getAddress().trim();
            return a.endsWith("/") ? a.substring(0, a.length() - 1) : a;
        }
        // K8s: 优先 POD_IP
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isEmpty()) {
            return "http://" + podIp + ":" + properties.getPort();
        }
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            if ("127.0.0.1".equals(host) || "0:0:0:0:0:0:0:1".equals(host)) {
                host = InetAddress.getLocalHost().getHostName();
            }
            return "http://" + host + ":" + properties.getPort();
        } catch (Exception e) {
            return "http://127.0.0.1:" + properties.getPort();
        }
    }

    private String resolveNodeId() {
        if (properties.getNodeId() != null && !properties.getNodeId().trim().isEmpty()) {
            return properties.getNodeId().trim();
        }
        String pod = System.getenv("POD_NAME");
        if (pod != null && !pod.isEmpty()) {
            return pod;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "executor-" + System.currentTimeMillis();
        }
    }

    public String getResolvedAddress() {
        return resolvedAddress;
    }

    public String getResolvedNodeId() {
        return resolvedNodeId;
    }
}

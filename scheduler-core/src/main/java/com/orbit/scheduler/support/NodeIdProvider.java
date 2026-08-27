package com.orbit.scheduler.support;

import java.net.InetAddress;
import java.util.UUID;

/**
 * 节点 ID 解析：配置项 &gt; POD_NAME 环境变量（K8s） &gt; 主机名 &gt; 随机串。
 *
 * @author orbit
 */
public final class NodeIdProvider {

    private NodeIdProvider() {
    }

    public static String resolve(String configured) {
        if (configured != null && !configured.trim().isEmpty()) {
            return configured.trim();
        }
        String podName = System.getenv("POD_NAME");
        if (podName != null && !podName.trim().isEmpty()) {
            return podName.trim();
        }
        try {
            String host = InetAddress.getLocalHost().getHostName();
            if (host != null && !host.trim().isEmpty()) {
                return host.trim();
            }
        } catch (Exception ignore) {
            // fall through
        }
        return "node-" + UUID.randomUUID().toString().substring(0, 8);
    }
}

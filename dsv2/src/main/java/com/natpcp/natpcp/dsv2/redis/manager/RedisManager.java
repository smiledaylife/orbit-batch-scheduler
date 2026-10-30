package com.natpcp.natpcp.dsv2.redis.manager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.RedisClusterConnection;
import org.springframework.data.redis.connection.RedisClusterNode;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.script.DefaultScriptExecutor;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.RedisSerializer;

import jakarta.annotation.PostConstruct;
import java.util.Set;
import java.util.Map;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * redis封装客户端
 */

public class RedisManager {

    /**
     * 允许配置自定义connector
     * 不得使用*?[]等能够触发通配判断的字符
     */
    @Value("${framework.cache.key.connector::}")
    private String cacheKeyConnector;

    private DefaultScriptExecutor<String> defaultScriptExecutor = null;

    @Autowired
    @Qualifier("objectRedisTemplate")
    protected RedisTemplate<String, Object> redisTemplate;

    @PostConstruct
    public void init() {
        this.defaultScriptExecutor = new DefaultScriptExecutor(this.redisTemplate);
    }

    /**
     * 指定前缀 + key 拼接提供给redis的key（可选）
     */
    public String buildCacheKey(String prefix, String cacheKey) {
        return prefix + cacheKeyConnector + cacheKey;
    }

    // -------------------- key操作 --------------------

    public Boolean expire(String prefix, String bizKey, Long seconds) {
        return this.expire(buildCacheKey(prefix, bizKey), seconds);
    }

    public Boolean expire(String key, Long seconds) {
        return redisTemplate.expire(key, seconds, TimeUnit.SECONDS);
    }

    public Long getExpireTime(String prefix, String bizKey) {
        return this.getExpireTime(buildCacheKey(prefix, bizKey));
    }

    public Long getExpireTime(String key) {
        return redisTemplate.getExpire(key, TimeUnit.SECONDS);
    }

    public Boolean persist(String prefix, String bizKey) {
        return this.persist(buildCacheKey(prefix, bizKey));
    }

    public Boolean persist(String key) {
        return redisTemplate.boundValueOps(key).persist();
    }

    public Boolean hasKey(String prefix, String bizKey) {
        return this.hasKey(buildCacheKey(prefix, bizKey));
    }

    public Boolean hasKey(String key) {
        return redisTemplate.hasKey(key);
    }

    public Set<String> keysInPrefix(String prefix) {
        return this.keys(prefix + cacheKeyConnector + "*");
    }

    public Set<String> keys(String pattern) {
        return scanKeys(pattern);
    }

    // -------------------- key-value操作 --------------------
    public Object get(String prefix, String bizKey) {
        return this.get(buildCacheKey(prefix, bizKey));
    }

    public Object get(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public void set(String prefix, String bizKey, Object value) {
        this.set(prefix, bizKey, value, -1L);
    }

    public void set(String prefix, String bizKey, Object value, Long cacheSeconds) {
        this.set(buildCacheKey(prefix, bizKey), value, cacheSeconds);
    }

    public void set(String key, Object value) {
        this.set(key, value, -1L);
    }

    // cacheSeconds为null或负数时，不过期，长期有效
    public void set(String key, Object value, Long cacheSeconds) {
        if (cacheSeconds == null || cacheSeconds <= 0) {
            redisTemplate.opsForValue().set(key, value);
            return;
        }
        redisTemplate.opsForValue().set(key, value, cacheSeconds, TimeUnit.SECONDS);
    }

    public Boolean setIfAbsent(String prefix, String bizKey, Object value) {
        return this.setIfAbsent(prefix, bizKey, value, -1L);
    }

    public Boolean setIfAbsent(String prefix, String bizKey, Object value, Long cacheSeconds) {
        return this.setIfAbsent(buildCacheKey(prefix, bizKey), value, cacheSeconds);
    }

    public Boolean setIfAbsent(String key, Object value) {
        return this.setIfAbsent(key, value, -1L);
    }

    // cacheSeconds为null或负数时，不过期，长期有效
    public Boolean setIfAbsent(String key, Object value, Long cacheSeconds) {
        if (cacheSeconds == null || cacheSeconds <= 0) {
            return redisTemplate.opsForValue().setIfAbsent(key, value);
        } else {
            return redisTemplate.opsForValue().setIfAbsent(key, value, cacheSeconds, TimeUnit.SECONDS);
        }
    }

    public void batchSet(Map<String, Object> keyAndValue) {
        redisTemplate.opsForValue().multiSet(keyAndValue);
    }

    public void batchSetIfAbsent(Map<String, Object> keyAndValue) {
        redisTemplate.opsForValue().multiSetIfAbsent(keyAndValue);
    }

    public void push(String channel, String message) {
        redisTemplate.convertAndSend(channel, message);
    }

    public Long incrementByNumber(String prefix, String bizKey, Long number) {
        return this.incrementByNumber(buildCacheKey(prefix, bizKey), number);
    }

    public Long incrementByNumber(String key, Long number) {
        return redisTemplate.opsForValue().increment(key, number);
    }

    public Boolean delete(String prefix, String bizKey) {
        return this.delete(buildCacheKey(prefix, bizKey));
    }

    public Boolean delete(String key) {
        return redisTemplate.delete(key);
    }

    // -------------------- 执行指定的lua脚本操作 --------------------
    public <T> T execute(final RedisScript<T> script, final List<String> keys, final Object... args) {
        return this.defaultScriptExecutor.execute(script, keys, args);
    }

    public RedisSerializer<String> getStringSerializer() {
        return redisTemplate.getStringSerializer();
    }

    /**
     * 集群各 master 节点 SCAN 匹配 key。
     *
     * <p>任何 Redis 访问异常都会向上抛出（而不是吞掉返回 null）：
     * 调用方是巡检/同步任务，静默返回空会让任务显示 COMPLETED 但实际什么都没检查。</p>
     *
     * @throws IllegalStateException 未配置连接工厂或非集群模式
     */
    public Set<String> scanKeys(String pattern) {
        Set<String> keys = new HashSet<>();
        RedisConnectionFactory factory = redisTemplate.getConnectionFactory();
        if (factory == null) {
            throw new IllegalStateException("RedisTemplate 未配置 RedisConnectionFactory");
        }
        ScanOptions options = ScanOptions.scanOptions().match(pattern).count(500).build();
        try (RedisClusterConnection clusterConnection = factory.getClusterConnection()) {
            for (RedisClusterNode node : clusterConnection.clusterGetNodes()) {
                if (node.isMaster() && node.isConnected()) {
                    try (Cursor<byte[]> cursor = clusterConnection.scan(node, options)) {
                        cursor.forEachRemaining(key -> keys.add(new String(key, StandardCharsets.UTF_8)));
                    }
                }
            }
        }
        return keys;
    }
}

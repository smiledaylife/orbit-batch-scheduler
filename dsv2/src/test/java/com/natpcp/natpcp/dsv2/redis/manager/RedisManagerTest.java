package com.natpcp.natpcp.dsv2.redis.manager;

import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * RedisManager unit test（核心缓存读写 API）。
 */
@DisplayName("RedisManager Test")
@ExtendWith(MockitoExtension.class)
class RedisManagerTest {

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ValueOperations<String, Object> valueOps;

    @Mock
    private org.springframework.data.redis.core.SetOperations<String, Object> setOps;

    @Mock
    private org.springframework.data.redis.core.convert.MappingRedisConverter converter;

    private RedisManager redisManager;

    @BeforeEach
    void setUp() {
        redisManager = new RedisManager();
        ReflectionTestUtils.setField(redisManager, "redisTemplate", redisTemplate);
        ReflectionTestUtils.setField(redisManager, "CACHE_KEY_CONNECTOR", ":");
    }

    @DisplayName("buildCacheKey: 前缀+连接符+key")
    @Test
    void buildCacheKey_shouldJoin() {
        assertThat(redisManager.buildCacheKey("pre", "biz")).isEqualTo("pre:biz");
    }

    @DisplayName("get(fullKey): 委托 valueOps.get")
    @Test
    void get_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get("k1")).thenReturn("v1");
        assertThat(redisManager.get("k1")).isEqualTo("v1");
        verify(valueOps).get("k1");
    }

    @DisplayName("get(prefix, bizKey): 拼接后读取")
    @Test
    void getWithPrefix_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.get("pre:biz")).thenReturn("v");
        assertThat(redisManager.get("pre", "biz")).isEqualTo("v");
    }

    @DisplayName("set(key, value): 委托 opsForValue().set")
    @Test
    void set_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        redisManager.set("k", "obj");
        verify(valueOps).set("k", "obj");
    }

    @DisplayName("set(key, value, seconds): 带过期时间")
    @Test
    void setWithExpire_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        String k = "k";
        Object obj = new Object();
        Long sec = 60L;
        redisManager.set(k, obj, sec);
        verify(valueOps).set(eq("k"), eq(obj), eq(60L), eq(TimeUnit.SECONDS));
    }

    @DisplayName("set(prefix, bizKey, value): 拼接 key 写入")
    @Test
    void setWithPrefix_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        redisManager.set("pre", "biz", "v");
        verify(valueOps).set("pre:biz", "v");
    }

    @DisplayName("expire(key, seconds): 委托 expire")
    @Test
    void expire_shouldDelegate() {
        when(redisTemplate.expire("k", 30L, TimeUnit.SECONDS)).thenReturn(true);
        assertThat(redisManager.expire("k", 30L)).isTrue();
    }

    @DisplayName("expire(prefix, bizKey, seconds): 拼接 key 设置过期")
    @Test
    void expireWithPrefix_shouldDelegate() {
        when(redisTemplate.expire("pre:biz", 30L, TimeUnit.SECONDS)).thenReturn(true);
        assertThat(redisManager.expire("pre", "biz", 30L)).isTrue();
    }

    @DisplayName("hasKey: 委托 hasKey")
    @Test
    void hasKey_shouldDelegate() {
        when(redisTemplate.hasKey("k1")).thenReturn(true);
        assertThat(redisManager.hasKey("k1")).isTrue();
    }

    @DisplayName("delete(key): 委托 delete")
    @Test
    void delete_shouldDelegate() {
        when(redisTemplate.delete("k1")).thenReturn(true);
        assertThat(redisManager.delete("k1")).isTrue();
        verify(redisTemplate).delete("k1");
    }

    @DisplayName("delete(prefix, bizKey): 拼接 key 删除")
    @Test
    void deleteWithPrefix_shouldDelegate() {
        when(redisTemplate.delete("pre:biz")).thenReturn(true);
        assertThat(redisManager.delete("pre", "biz")).isTrue();
    }

    @DisplayName("keysInPrefix: 集群 scan 收集 key 列表")
    @Test
    void keysInPrefix_shouldScanCluster() {
        org.springframework.data.redis.connection.RedisConnectionFactory factory =
                org.mockito.Mockito.mock(org.springframework.data.redis.connection.RedisConnectionFactory.class);
        org.springframework.data.redis.connection.RedisClusterConnection clusterConn =
                org.mockito.Mockito.mock(org.springframework.data.redis.connection.RedisClusterConnection.class);
        org.springframework.data.redis.connection.RedisClusterNode node =
                org.mockito.Mockito.mock(org.springframework.data.redis.connection.RedisClusterNode.class);
        java.util.Set<org.springframework.data.redis.connection.RedisClusterNode> nodes = new HashSet<>();
        nodes.add(node);
        when(redisTemplate.getConnectionFactory()).thenReturn(factory);
        when(factory.getClusterConnection()).thenReturn(clusterConn);
        when(clusterConn.clusterGetNodes()).thenReturn(nodes);
        when(node.isMaster()).thenReturn(true);
        when(node.isConnected()).thenReturn(true);
        // 模拟 scan 游标——clusterConnection.scan() 返回的 Cursor 迭代一个 key
        org.springframework.data.redis.core.Cursor<byte[]> cursor =
                org.mockito.Mockito.mock(org.springframework.data.redis.core.Cursor.class);
        when(clusterConn.scan(eq(node), any(org.springframework.data.redis.core.ScanOptions.class)))
                .thenReturn(cursor);
        org.mockito.Mockito.doAnswer(invocation -> {
            java.util.function.Consumer<byte[]> consumer = invocation.getArgument(0);
            consumer.accept("pre:k1".getBytes());
            consumer.accept("pre:k2".getBytes());
            return null;
        }).when(cursor).forEachRemaining(any());

        Set<String> result = redisManager.keysInPrefix("pre");
        assertThat(result).containsExactlyInAnyOrder("pre:k1", "pre:k2");
    }

    @DisplayName("keys(pattern): 走 scanKeys 集群扫描（无节点空集）")
    @Test
    void keys_shouldDelegateToScan() {
        org.springframework.data.redis.connection.RedisConnectionFactory factory =
                org.mockito.Mockito.mock(org.springframework.data.redis.connection.RedisConnectionFactory.class);
        org.springframework.data.redis.connection.RedisClusterConnection clusterConn =
                org.mockito.Mockito.mock(org.springframework.data.redis.connection.RedisClusterConnection.class);
        when(redisTemplate.getConnectionFactory()).thenReturn(factory);
        when(factory.getClusterConnection()).thenReturn(clusterConn);
        when(clusterConn.clusterGetNodes()).thenReturn(new HashSet<>());
        Set<String> result = redisManager.keys("x:*");
        assertThat(result).isNotNull().isEmpty();
    }

    @DisplayName("scanKeys: 集群连接异常返回 null")
    @Test
    void scanKeys_shouldReturnNull_whenConnectionFails() {
        when(redisTemplate.getConnectionFactory()).thenReturn(null);
        assertThat(redisManager.scanKeys("x:*")).isNull();
    }

    @DisplayName("incrementByNumber: 委托 increment")
    @Test
    void incrementByNumber_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.increment("k1", 5L)).thenReturn(6L);
        assertThat(redisManager.incrementByNumber("k1", 5L)).isEqualTo(6L);
    }

    @DisplayName("push: 发布消息")
    @Test
    void push_shouldConvertAndSend() {
        when(redisTemplate.convertAndSend("channel", "msg")).thenReturn(1L);
        redisManager.push("channel", "msg");
        verify(redisTemplate).convertAndSend("channel", "msg");
    }

    @DisplayName("setIfAbsent: 委托 setIfAbsent")
    @Test
    void setIfAbsent_shouldDelegate() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent("k", "v")).thenReturn(true);
        assertThat(redisManager.setIfAbsent("k", "v")).isTrue();
    }

    @DisplayName("batchSet: 委托 multiSet")
    @Test
    void batchSet_shouldWriteAll() {
        when(redisTemplate.opsForValue()).thenReturn(valueOps);
        Map<String, Object> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "v2");
        redisManager.batchSet(map);
        verify(valueOps).multiSet(map);
    }

    @DisplayName("getExpireTime: 委托 getExpire")
    @Test
    void getExpireTime_shouldDelegate() {
        when(redisTemplate.getExpire("k1", TimeUnit.SECONDS)).thenReturn(100L);
        assertThat(redisManager.getExpireTime("k1")).isEqualTo(100L);
    }

    @DisplayName("persist: 委托 boundValueOps().persist")
    @Test
    void persist_shouldDelegate() {
        org.springframework.data.redis.core.BoundValueOperations<String, Object> bound =
                org.mockito.Mockito.mock(org.springframework.data.redis.core.BoundValueOperations.class);
        when(redisTemplate.boundValueOps("k1")).thenReturn(bound);
        when(bound.persist()).thenReturn(true);
        assertThat(redisManager.persist("k1")).isTrue();
    }
}
/*
 * AICode End
 */

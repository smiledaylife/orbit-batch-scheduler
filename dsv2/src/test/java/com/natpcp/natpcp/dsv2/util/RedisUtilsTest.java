package com.natpcp.natpcp.dsv2.util;

import com.natpcp.natpcp.dsv2.common.CachePrefix;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.util.RedisUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * RedisUtils unit test.
 */
@DisplayName("RedisUtils Test")
@ExtendWith(MockitoExtension.class)
class RedisUtilsTest {

    @Mock
    private RedisManager redisManager;

    private RedisUtils redisUtils;

    private CachePrefix prefix;

    @BeforeEach
    void setUp() {
        redisUtils = new RedisUtils();
        org.springframework.test.util.ReflectionTestUtils.setField(redisUtils, "redisManager", redisManager);
        prefix = new CachePrefix() {
            @Override
            public String getPrefix() {
                return "URLS:";
            }
        };
    }

    @DisplayName("set: 委托 redisManager.set(prefix.getValue)")
    @Test
    void set_shouldDelegate() {
        Object value = new Object();
        redisUtils.set(prefix, value);
        verify(redisManager).set("URLS:", value);
    }

    @DisplayName("set(带过期): 委托 redisManager.set(三参数)")
    @Test
    void setWithExpire_shouldDelegate() {
        Object value = new Object();
        redisUtils.set(prefix, value, 300L);
        verify(redisManager).set("URLS:", value, 300L);
    }

    @DisplayName("get: 缓存命中且类型匹配返回")
    @Test
    void get_shouldReturn_whenMatch() {
        when(redisManager.get("URLS:")).thenReturn("hello");
        assertThat(redisUtils.get(prefix, String.class)).isEqualTo("hello");
    }

    @DisplayName("get: 缓存未命中返回 null")
    @Test
    void get_shouldReturnNull_whenMiss() {
        when(redisManager.get("URLS:")).thenReturn(null);
        assertThat(redisUtils.get(prefix, String.class)).isNull();
    }

    @DisplayName("get: 类型不匹配抛 IllegalStateException")
    @Test
    void get_shouldThrow_whenTypeMismatch() {
        when(redisManager.get("URLS:")).thenReturn(123);
        assertThatThrownBy(() -> redisUtils.get(prefix, String.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("缓存类型不匹配");
    }

    @DisplayName("delete: 委托 redisManager.delete")
    @Test
    void delete_shouldDelegate() {
        when(redisManager.delete("URLS:")).thenReturn(true);
        assertThat(redisUtils.delete(prefix)).isTrue();
        verify(redisManager).delete("URLS:");
    }
}
/*
 * AICode End
 */

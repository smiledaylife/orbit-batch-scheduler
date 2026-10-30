/*
 * AICode Begin
 * plt-common 等价类：RedisUtils（缓存工具类）。
 *
 * 等价于 plt-common-bo 的 com.natpcp.util.dsv2.natpcp.RedisUtils。
 * 封装 RedisManager，提供基于 CachePrefix 的业务缓存读写与发布订阅能力。
 */
package com.natpcp.natpcp.dsv2.util;

import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.common.CachePrefix;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 平台缓存工具类。
 */
@Component
public class RedisUtils {

    @Autowired
    private RedisManager redisManager;

    /**
     * 写入缓存（key = prefix + value 序列化，自定义对象序列化为 JSON 字符串）。
     *
     * @param cachePrefix 缓存前缀
     * @param value       缓存值
     */
    public void set(CachePrefix cachePrefix, Object value) {
        redisManager.set(cachePrefix.getPrefix(), value);
    }

    /**
     * 写入缓存并指定过期时间（秒）。
     *
     * @param cachePrefix  缓存前缀
     * @param value        缓存值
     * @param cacheSeconds 过期秒数
     */
    public void set(CachePrefix cachePrefix, Object value, Long cacheSeconds) {
        redisManager.set(cachePrefix.getPrefix(), value, cacheSeconds);
    }

    /**
     * 读取缓存。
     *
     * @param cachePrefix 缓存前缀
     * @param clazz       目标类型
     * @param <T>         目标类型
     * @return 缓存值
     */
    public <T> T get(CachePrefix cachePrefix, Class<T> clazz) {
        Object value = redisManager.get(cachePrefix.getPrefix());
        if (value == null) {
            return null;
        }
        if (clazz.isInstance(value)) {
            return clazz.cast(value);
        }
        throw new IllegalStateException("缓存类型不匹配: 期望 " + clazz.getName() + "，实际 " + value.getClass().getName());
    }

    /**
     * 删除缓存。
     *
     * @param cachePrefix 缓存前缀
     * @return 是否删除成功
     */
    public Boolean delete(CachePrefix cachePrefix) {
        return redisManager.delete(cachePrefix.getPrefix());
    }
}
/*
 * AICode End
 */

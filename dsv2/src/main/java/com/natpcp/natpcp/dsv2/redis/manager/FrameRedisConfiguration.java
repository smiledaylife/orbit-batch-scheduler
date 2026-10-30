package com.natpcp.natpcp.dsv2.redis.manager;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class FrameRedisConfiguration {

    /**
     * 对象 RedisTemplate。
     *
     * <p>安全说明：历史实现开启了 {@code activateDefaultTyping(LaissezFaireSubTypeValidator, NON_FINAL)}，
     * 即对 Redis 中的任意值做无白名单的多态反序列化（CWE-502）。gatewayRedis 为多系统共享集群，
     * 任何可写入 Redis 的一方都可借此触发反序列化 gadget。dsv2 实际只读写 String / 简单集合，
     * 因此改用 {@link GenericJackson2JsonRedisSerializer} + 仅允许本工程包与 JDK 基础类型的
     * {@link BasicPolymorphicTypeValidator} 白名单。</p>
     */
    @Bean
    public RedisTemplate<String, Object> objectRedisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        PolymorphicTypeValidator ptv = BasicPolymorphicTypeValidator.builder()
                .allowIfSubType("com.natpcp.natpcp.dsv2.")
                .allowIfSubType("java.util.")
                .allowIfSubType("java.lang.")
                .allowIfSubType("java.time.")
                .build();
        ObjectMapper om = JsonMapper.builder()
                .visibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY)
                // 保持与原实现相同的类型信息写法（NON_FINAL + 默认 WRAPPER_ARRAY），
                // 保证 Redis 中既有数据及其他读取方（网关）的兼容性，仅收紧反序列化白名单
                .activateDefaultTyping(ptv, ObjectMapper.DefaultTyping.NON_FINAL)
                .build();
        GenericJackson2JsonRedisSerializer jacksonSerializer = new GenericJackson2JsonRedisSerializer(om);

        template.setValueSerializer(jacksonSerializer);
        template.setKeySerializer(new StringRedisSerializer());
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(jacksonSerializer);
        template.afterPropertiesSet();
        return template;
    }

    @Bean
    public RedisManager redisManager() {
        return new RedisManager();
    }

}

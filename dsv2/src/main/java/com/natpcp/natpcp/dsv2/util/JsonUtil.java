/*
 * AICode Begin
 * JsonUtil：工程统一 JSON 工具（Jackson）。
 *
 * 背景：历史代码同时使用 fastjson 1.2.x（已 EOL，历史 autoType 漏洞多）与 Jackson，
 * 本工具用 Jackson 统一所有 JSON 读写，fastjson 依赖已移除。
 *
 * 配置：
 *   - 序列化忽略 null 字段（与 fastjson 默认行为一致，保证 JobParameters 等既有 JSON 形态不变）；
 *   - 反序列化忽略未知字段（调度中心下发参数允许比 BO 多）；
 *   - 支持 java.time（JavaTimeModule，LocalDateTime 以字符串输出）。
 */
package com.natpcp.natpcp.dsv2.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.util.Collections;
import java.util.Map;

/**
 * JSON 工具（Jackson）。
 */
public final class JsonUtil {

    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .serializationInclusion(JsonInclude.Include.NON_NULL)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build();

    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private JsonUtil() {
    }

    /**
     * 对象 → JSON 字符串。
     *
     * @throws IllegalArgumentException 序列化失败
     */
    public static String toJson(Object value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 序列化失败: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * JSON 字符串 → 指定类型；输入为空返回 null。
     *
     * @throws IllegalArgumentException 不是合法 JSON 或类型不匹配
     */
    public static <T> T fromJson(String json, Class<T> type) {
        if (json == null || json.isBlank()) {
            return null;
        }
        try {
            return MAPPER.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 解析失败: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * JSON 对象字符串 → Map；输入为空或非对象返回空 Map（不抛异常）。
     */
    public static Map<String, Object> toMap(String json) {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            Map<String, Object> map = MAPPER.readValue(json, MAP_TYPE);
            return map == null ? Collections.emptyMap() : map;
        } catch (JsonProcessingException e) {
            return Collections.emptyMap();
        }
    }

    /**
     * JSON 对象字符串 → Map（严格版）：输入为空或不是合法 JSON 对象时抛异常。
     *
     * @throws IllegalArgumentException 不是合法 JSON 对象
     */
    public static Map<String, Object> parseMap(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("JSON 解析失败: 输入为空");
        }
        try {
            Map<String, Object> map = MAPPER.readValue(json, MAP_TYPE);
            if (map == null) {
                throw new IllegalArgumentException("JSON 解析失败: 输入为 null");
            }
            return map;
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("JSON 解析失败: " + e.getOriginalMessage(), e);
        }
    }

    /**
     * 任意对象（通常是 Map）→ 指定类型 Bean（等价于先 toJson 再 fromJson，但无字符串中转）。
     */
    public static <T> T convert(Object value, Class<T> type) {
        if (value == null) {
            return null;
        }
        return MAPPER.convertValue(value, type);
    }
}
/*
 * AICode End
 */

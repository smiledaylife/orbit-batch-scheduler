package com.natpcp.natpcp.dsv2.service.impl;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.natpcp.natpcp.dsv2.dao.PageInfo;
import com.natpcp.natpcp.dsv2.dao.mapper.GenericMapper;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 通用的操作数据类，访问数据库和redis，保持数据库和redis的一致
 */
@Service
public class GenericDataAccessService {

    @Autowired
    private GenericMapper genericMapper;

    @Autowired
    private RedisManager redisManager;

    private ObjectMapper objectMapper;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    public GenericDataAccessService() {
        objectMapper = new ObjectMapper();
        JavaTimeModule module = new JavaTimeModule();
        module.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        module.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
        objectMapper.registerModule(module);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * 数据库数据变化后，全量写入redis
     *
     * @param clazz           操作的实体类型的class对象
     * @param data            插入或查询的数据项，为空时，查询默认查所有字段
     * @param queryConditions 查询条件，为空时，不为条件
     * @param <T>
     * @return
     */
    public <T> String manipulateAllData(Class<T> clazz, T data,T queryConditions) {

        try {
            Map<String, Object> entityMap = null;
            Map<String, Object> conditionsMap = null;
            //实体属性
            if (data != null) {
                entityMap = objectMapper.convertValue(data, Map.class);
            }
            //查询条件
            if (queryConditions != null) {
                conditionsMap = objectMapper.convertValue(queryConditions, Map.class);
            }
            List<Map<String, Object>> forwardInfoMap = convertColumn(genericMapper.query(clazz, entityMap, conditionsMap, null, null));
            for (Map<String, Object> map : forwardInfoMap) {
                String key = String.valueOf(map.get("key"));
                String value = String.valueOf(map.get("value"));
                String redisValue = redisTemplate.opsForValue().get(key);
                if (!value.equals(redisValue)){
                    redisTemplate.opsForValue().set(key, value);
                    //发布消息
                    redisManager.push(key, value);
                }
            }
            return null;
        } catch (Exception e) {
            // 处理异常
            throw new RuntimeException("数据库操作报错！", e);
        }
    }

    /**
     * 查询返回对象列表
     *
     * @param clazz
     * @param data
     * @param queryConditions
     * @param pageInfo
     * @param <T>
     * @return
     */
    public <T> List<T> queryForObjects(Class<T> clazz, T data, T queryConditions, PageInfo pageInfo) {
        //实体属性
        Map<String, Object> entityMap = objectMapper.convertValue(data, Map.class);
        //查询条件
        Map<String, Object> conditionsMap = objectMapper.convertValue(queryConditions, Map.class);

        JavaType type = objectMapper.getTypeFactory().constructCollectionType(List.class, clazz);

        List list = (pageInfo == null) ? objectMapper.convertValue(convertColumn(genericMapper.query(clazz, entityMap,
                conditionsMap, null, null)), type) :
                objectMapper.convertValue(convertColumn(genericMapper.query(clazz, entityMap, conditionsMap,
                                pageInfo.getPage(), pageInfo.getPageSize())),
                        type);

        return list;
    }

    public List<Map<String, Object>> convertColumn(List<Map<String, Object>> query) {
        List<Map<String, Object>> newList = new ArrayList<>();

        for (Map<String, Object> stringObjectMap : query) {
            Map<String, Object> subMap = new HashMap<>();
            Set<String> keySet = stringObjectMap.keySet();
            for (String key : keySet) {
                String newKey = underScoreToCamelCase(key);
                if (newKey.equals("createTime") || newKey.equals("lastModTime")) {
                    Timestamp timeStamp = Timestamp.valueOf(stringObjectMap.get(key).toString());
                    subMap.put(newKey, timeStamp.toLocalDateTime());
                } else {
                    subMap.put(newKey, stringObjectMap.get(key));
                }
            }
            newList.add(subMap);
        }
        return newList;
    }


    public String underScoreToCamelCase(String fieldName) {
        String[] words = fieldName.split("_");
        StringBuilder camelCase = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String word = words[i];
            if (i == 0) {
                camelCase.append(word);
            } else {
                camelCase.append(word.substring(0, 1).toUpperCase()).append(word.substring(1).toLowerCase());
            }
        }
        return camelCase.toString();
    }
}

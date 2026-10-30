package com.natpcp.natpcp.dsv2.batch.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.batch.processor.UpdateRedisItemProcessor;
import com.natpcp.natpcp.dsv2.dao.mapper.GenericMapper;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * UpdateRedisItemProcessor 单元测试：DB 全量数据 vs Redis 当前值比对，标记 needUpdate。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("UpdateRedisItemProcessor 测试")
class UpdateRedisItemProcessorTest {

    @Mock
    private GenericDataAccessService genericDataAccessService;

    @Mock
    private GenericMapper genericMapper;

    @Mock
    private RedisManager redisManager;

    private ObjectMapper objectMapper;

    private UpdateRedisItemProcessor processor;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        processor = new UpdateRedisItemProcessor(genericDataAccessService, genericMapper, redisManager, objectMapper);
    }

    private List<Map<String, Object>> rowsWith(Map<String, Object> row) {
        List<Map<String, Object>> rows = new ArrayList<>();
        rows.add(row);
        return rows;
    }

    @DisplayName("process: DB 与 Redis 不同 → needUpdate=true, dataString 已设置")
    @Test
    void process_shouldMarkNeedUpdate_whenDifferent() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        row.put("name", "mac");
        List<Map<String, Object>> rows = rowsWith(row);

        when(genericDataAccessService.convertColumn(any())).thenReturn(rows);
        when(redisManager.get("macRelated")).thenReturn("older-value");

        UpdateRedisSyncItem item = new UpdateRedisSyncItem("macRelated", String.class);
        UpdateRedisSyncItem result = processor.process(item);

        assertThat(result).isSameAs(item);
        assertThat(result.isNeedUpdate()).isTrue();
        // HashMap 序列化键序不保证，用 JSON 解析后比较
        java.util.List<Map<String, Object>> parsed =
                com.alibaba.fastjson.JSON.parseObject(result.getDataString(),
                        new com.alibaba.fastjson.TypeReference<java.util.List<Map<String, Object>>>() {
                        });
        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0)).containsEntry("id", 1).containsEntry("name", "mac");
    }

    @DisplayName("process: DB 与 Redis 相同 → needUpdate=false")
    @Test
    void process_shouldNotMarkNeedUpdate_whenSame() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        List<Map<String, Object>> rows = rowsWith(row);

        when(genericDataAccessService.convertColumn(any())).thenReturn(rows);
        // fastjson 序列化 '{"id":1}' 与 ObjectMapper 输出 '[{"id":1}]' 并不相同，
        // 这里直接 mock redis 返回与 DB 序列化一致的字符串
        when(redisManager.get("inRoute")).thenReturn("[{\"id\":1}]");

        UpdateRedisSyncItem item = new UpdateRedisSyncItem("inRoute", String.class);
        UpdateRedisSyncItem result = processor.process(item);

        assertThat(result.isNeedUpdate()).isFalse();
    }

    @DisplayName("process: Redis 值为 null 视为不同 → needUpdate=true")
    @Test
    void process_shouldMarkNeedUpdate_whenRedisNull() throws Exception {
        Map<String, Object> row = new HashMap<>();
        row.put("id", 1);
        when(genericDataAccessService.convertColumn(any())).thenReturn(rowsWith(row));
        when(redisManager.get("outRoute")).thenReturn(null);

        UpdateRedisSyncItem item = new UpdateRedisSyncItem("outRoute", String.class);
        assertThat(processor.process(item).isNeedUpdate()).isTrue();
    }

    @DisplayName("process: 查询抛异常时向外传播")
    @Test
    void process_shouldPropagate_whenQueryThrows() {
        when(genericDataAccessService.convertColumn(any()))
                .thenThrow(new RuntimeException("db error"));

        UpdateRedisSyncItem item = new UpdateRedisSyncItem("k", String.class);
        assertThatThrownBy(() -> processor.process(item))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("db error");
    }

    @DisplayName("process: 序列化时若 query 返回 null 也能完成（rows=null→JSON null）")
    @Test
    void process_shouldHandleNullRows() throws Exception {
        when(genericDataAccessService.convertColumn(any())).thenReturn(null);
        when(redisManager.get("k")).thenReturn("null");

        UpdateRedisSyncItem item = new UpdateRedisSyncItem("k", String.class);
        UpdateRedisSyncItem result = processor.process(item);
        assertThat(result.getDataString()).isEqualTo("null");
        assertThat(result.isNeedUpdate()).isFalse();
    }
}
/*
 * AICode End
 */

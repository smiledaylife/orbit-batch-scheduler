package com.natpcp.natpcp.dsv2.batch.writer;

import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.batch.writer.UpdateRedisItemWriter;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.item.Chunk;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * AICode Begin
 * UpdateRedisItemWriter 单元测试：仅对 needUpdate 项执行 set + push。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("UpdateRedisItemWriter 测试")
class UpdateRedisItemWriterTest {

    @Mock
    private RedisManager redisManager;

    private UpdateRedisItemWriter writer;

    @BeforeEach
    void setUp() {
        writer = new UpdateRedisItemWriter(redisManager);
    }

    @DisplayName("write: needUpdate=true 的项执行 set + push")
    @Test
    void write_shouldSetAndPush_whenNeedUpdate() throws Exception {
        UpdateRedisSyncItem item = new UpdateRedisSyncItem("macRelated", String.class);
        item.setNeedUpdate(true);
        item.setDataString("[{\"id\":1}]");

        Chunk<UpdateRedisSyncItem> chunk = new Chunk<>();
        chunk.add(item);
        writer.write(chunk);

        verify(redisManager).set("macRelated", "[{\"id\":1}]");
        verify(redisManager).push("macRelated", "[{\"id\":1}]");
    }

    @DisplayName("write: needUpdate=false 的项跳过 set/push")
    @Test
    void write_shouldSkip_whenNotNeedUpdate() throws Exception {
        UpdateRedisSyncItem item = new UpdateRedisSyncItem("inRoute", String.class);
        item.setNeedUpdate(false);
        item.setDataString("[{\"id\":2}]");

        Chunk<UpdateRedisSyncItem> chunk = new Chunk<>();
        chunk.add(item);
        writer.write(chunk);

        verify(redisManager, never()).set("inRoute", "[{\"id\":2}]");
        verify(redisManager, never()).push("inRoute", "[{\"id\":2}]");
    }

    @DisplayName("write: 混合列表，只处理差异项")
    @Test
    void write_shouldOnlyProcessChanged_whenMixed() throws Exception {
        UpdateRedisSyncItem changed = new UpdateRedisSyncItem("k1", String.class);
        changed.setNeedUpdate(true);
        changed.setDataString("v1");
        UpdateRedisSyncItem same = new UpdateRedisSyncItem("k2", String.class);
        same.setNeedUpdate(false);
        same.setDataString("v2");

        Chunk<UpdateRedisSyncItem> chunk = new Chunk<>();
        chunk.add(changed);
        chunk.add(same);
        writer.write(chunk);

        verify(redisManager).set("k1", "v1");
        verify(redisManager).push("k1", "v1");
        verify(redisManager, never()).set("k2", "v2");
        verify(redisManager, never()).push("k2", "v2");
    }
}
/*
 * AICode End
 */

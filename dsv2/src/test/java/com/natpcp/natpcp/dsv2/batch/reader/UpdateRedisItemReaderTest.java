package com.natpcp.natpcp.dsv2.batch.reader;

import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.batch.reader.UpdateRedisItemReader;
import com.natpcp.natpcp.dsv2.constant.UpdateRedisEnum;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AICode Begin
 * UpdateRedisItemReader 单元测试：chunk Reader 产出同步单元（排除 CONFIG_VERSION 两类）。
 */
@DisplayName("UpdateRedisItemReader 测试")
class UpdateRedisItemReaderTest {

    @DisplayName("read: 产出排除 CONFIG_VERSION 后的全部枚举项，耗尽返回 null")
    @Test
    void read_shouldYieldItemsThenNull() {
        UpdateRedisItemReader reader = new UpdateRedisItemReader();
        List<UpdateRedisSyncItem> items = new ArrayList<>();
        UpdateRedisSyncItem item;
        while ((item = reader.read()) != null) {
            items.add(item);
        }

        // 枚举共 10 项，排除 CONFIG_VERSION / CONFIG_VERSION_NG = 8 项
        assertThat(items).hasSize(UpdateRedisEnum.values().length - 2);
        assertThat(items).noneMatch(i -> i.getRedisKey().equals("configVersion"));
        assertThat(items).noneMatch(i -> i.getRedisKey().equals("configVersionNg"));
        // 每项的 clazz 与枚举映射一致
        for (UpdateRedisSyncItem it : items) {
            boolean matched = java.util.stream.Stream.of(UpdateRedisEnum.values())
                    .filter(e -> !e.equals(UpdateRedisEnum.CONFIG_VERSION)
                            && !e.equals(UpdateRedisEnum.CONFIG_VERSION_NG))
                    .anyMatch(e -> e.getRedisKey().equals(it.getRedisKey()) && e.getClazz().equals(it.getClazz()));
            assertThat(matched).as("redisKey=%s clazz=%s", it.getRedisKey(), it.getClazz()).isTrue();
        }
    }

    @DisplayName("read: 遍历结束后返回 null")
    @Test
    void read_shouldReturnNull_whenExhausted() {
        UpdateRedisItemReader reader = new UpdateRedisItemReader();
        int count = 0;
        while (reader.read() != null) {
            count++;
            if (count > 20) {
                break;
            }
        }
        assertThat(reader.read()).isNull();
    }
}
/*
 * AICode End
 */

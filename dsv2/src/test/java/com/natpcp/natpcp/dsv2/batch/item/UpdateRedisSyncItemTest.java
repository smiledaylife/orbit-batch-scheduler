package com.natpcp.natpcp.dsv2.batch.item;

import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * AICode Begin
 * UpdateRedisSyncItem 单元测试：chunk 处理单元（数据载体）。
 */
@DisplayName("UpdateRedisSyncItem 测试")
class UpdateRedisSyncItemTest {

    @DisplayName("构造器与 getter：redisKey/clazz 正确")
    @Test
    void constructor_shouldSetRedisKeyAndClazz() {
        UpdateRedisSyncItem item = new UpdateRedisSyncItem("macRelated", String.class);
        assertThat(item.getRedisKey()).isEqualTo("macRelated");
        assertThat(item.getClazz()).isEqualTo(String.class);
    }

    @DisplayName("needUpdate 默认为 false，set 后生效")
    @Test
    void needUpdate_shouldDefaultFalse_andUpdate() {
        UpdateRedisSyncItem item = new UpdateRedisSyncItem("k", Integer.class);
        assertThat(item.isNeedUpdate()).isFalse();
        item.setNeedUpdate(true);
        assertThat(item.isNeedUpdate()).isTrue();
    }

    @DisplayName("dataString set/get 往返")
    @Test
    void dataString_shouldRoundTrip() {
        UpdateRedisSyncItem item = new UpdateRedisSyncItem("k", Object.class);
        item.setDataString("{\"a\":1}");
        assertThat(item.getDataString()).isEqualTo("{\"a\":1}");
    }
}
/*
 * AICode End
 */

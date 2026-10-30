/*
 * AICode Begin
 * UpdateRedisItemReader：updateRedisInfo 任务 chunk 化 Reader。
 *
 * 语义与原文 updateRedisInfo() 完全一致：
 *   - 前导 LOOP_FORWARD_SAVE_REDIS 同步（转发规则增量落 redis）独立为前置 Tasklet（forwardRuleTasklet），
 *     不进入 chunk；
 *   - 本 Reader 按 UpdateRedisEnum 逐项产出"配置表 → Redis key"同步单元，
 *     排除 CONFIG_VERSION / CONFIG_VERSION_NG（这两个分别由后置 Tasklet 同步）。
 *
 * 依托 Spring Batch chunk 模型，每个 item（一个 Redis key 的同步）可独立
 * per-item 重试/跳过，故障不影响其余 key。
 */
package com.natpcp.natpcp.dsv2.batch.reader;

import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.constant.UpdateRedisEnum;
import org.springframework.batch.item.ItemReader;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 产出"配置表 → Redis key"同步单元列表。
 */
public class UpdateRedisItemReader implements ItemReader<UpdateRedisSyncItem> {

    private final Iterator<UpdateRedisSyncItem> iterator;

    public UpdateRedisItemReader() {
        List<UpdateRedisSyncItem> items = Stream.of(UpdateRedisEnum.values())
                // CONFIG_VERSION / CONFIG_VERSION_NG 由后置 Tasklet 专用同步，chunk 不做
                .filter(e -> !e.equals(UpdateRedisEnum.CONFIG_VERSION)
                        && !e.equals(UpdateRedisEnum.CONFIG_VERSION_NG))
                .map(e -> new UpdateRedisSyncItem(e.getRedisKey(), e.getClazz()))
                .collect(Collectors.toList());
        this.iterator = items.iterator();
    }

    @Override
    public UpdateRedisSyncItem read() {
        return iterator.hasNext() ? iterator.next() : null;
    }
}
/*
 * AICode End
 */

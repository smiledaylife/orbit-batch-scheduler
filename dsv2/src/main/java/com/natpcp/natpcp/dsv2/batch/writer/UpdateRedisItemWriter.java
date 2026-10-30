/*
 * AICode Begin
 * UpdateRedisItemWriter：updateRedisInfo 任务 chunk 化 Writer。
 *
 * 承接原文 setRedisData 的写入阶段：
 *   - 仅处理 Processor 标记为 needUpdate 的 item（DB 与 Redis 不一致）；
 *   - set Redis + 发布消息（redisManager.push）。
 *
 * 依赖注入方式：见 UpdateRedisInfoChunkComponents（batch/UpdateRedisInfoChunkComponents.java）。
 */
package com.natpcp.natpcp.dsv2.batch.writer;

import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemWriter;

/**
 * 将差异项写入 Redis 并发布变更消息。
 */
@Slf4j
public class UpdateRedisItemWriter implements ItemWriter<UpdateRedisSyncItem> {

    private final RedisManager redisManager;

    public UpdateRedisItemWriter(RedisManager redisManager) {
        this.redisManager = redisManager;
    }

    @Override
    public void write(Chunk<? extends UpdateRedisSyncItem> chunk) throws Exception {
        for (UpdateRedisSyncItem item : chunk.getItems()) {
            if (!item.isNeedUpdate()) {
                continue;
            }
            redisManager.set(item.getRedisKey(), item.getDataString());
            redisManager.push(item.getRedisKey(), item.getDataString());
            log.info("updateRedisInfo 已更新并推送 redisKey:{}", item.getRedisKey());
        }
    }
}
/*
 * AICode End
 */

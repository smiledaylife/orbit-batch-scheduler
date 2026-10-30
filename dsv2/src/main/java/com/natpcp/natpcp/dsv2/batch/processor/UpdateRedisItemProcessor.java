/*
 * AICode Begin
 * UpdateRedisItemProcessor：updateRedisInfo 任务 chunk 化 Processor。
 *
 * 承接原文 updateRedisInfo() 中"遍历 UpdateRedisEnum，将 DB 数据序列化并与 Redis 当前值比对"部分：
 *   - 查询 DB 全量数据（GenericMapper.query）→ convertColumn → ObjectMapper 序列化；
 *   - 与 Redis 当前值比对：不同才标记 needUpdate（原文 setRedisData 语义）。
 *
 * 纯函数式 Processor（无副作用写），若个别 key 查询/序列化失败，会由
 * faultTolerant chunk 的 skip/retry 策略接管，不影响其它 key。
 */
package com.natpcp.natpcp.dsv2.batch.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.batch.item.UpdateRedisSyncItem;
import com.natpcp.natpcp.dsv2.dao.mapper.GenericMapper;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.util.List;
import java.util.Map;

/**
 * 计算"数据库全量数据"与"Redis 当前值"的差异，标记待写单元。
 */
@Slf4j
public class UpdateRedisItemProcessor implements ItemProcessor<UpdateRedisSyncItem, UpdateRedisSyncItem> {

    private final GenericDataAccessService genericDataAccessService;

    private final GenericMapper genericMapper;

    private final RedisManager redisManager;

    private final ObjectMapper objectMapper;

    public UpdateRedisItemProcessor(GenericDataAccessService genericDataAccessService,
                                    GenericMapper genericMapper,
                                    RedisManager redisManager,
                                    ObjectMapper objectMapper) {
        this.genericDataAccessService = genericDataAccessService;
        this.genericMapper = genericMapper;
        this.redisManager = redisManager;
        this.objectMapper = objectMapper;
    }

    @Override
    public UpdateRedisSyncItem process(UpdateRedisSyncItem item) throws Exception {
        Class<?> clazz = item.getClazz();
        // 原文：genericMapper.query(clazz, null, null, null, null) + convertColumn
        List<Map<String, Object>> rows = genericDataAccessService.convertColumn(
                genericMapper.query(clazz, null, null, null, null));
        String dataStr = objectMapper.writeValueAsString(rows);
        item.setDataString(dataStr);

        // 原文 setRedisData 语义：与 redis 当前值比对，不同才写
        Object redisValue = redisManager.get(item.getRedisKey());
        boolean changed = !dataStr.equals(redisValue);
        item.setNeedUpdate(changed);
        if (changed) {
            log.info("updateRedisInfo 待更新 redisKey:{}", item.getRedisKey());
        }
        return item;
    }
}
/*
 * AICode End
 */

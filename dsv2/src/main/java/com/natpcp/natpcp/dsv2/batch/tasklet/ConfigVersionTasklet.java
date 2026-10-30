/*
 * AICode Begin
 * ConfigVersionTasklet：updateRedisInfo 任务步骤——配置版本表同步 Redis。
 *
 * 语义与原文 updateRedisInfo() 中 updateConfigVersion() 一致：
 *   查询 ConfigurationVersionPo 全量 -> JSON 序列化 -> setRedisData(CONFIGVERSION)。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.constant.Constant;
import com.natpcp.natpcp.dsv2.dao.mapper.GenericMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.HashMap;

/**
 * 配置版本表同步 Redis（updateRedisInfo 步骤）。
 */
@Slf4j
public class ConfigVersionTasklet implements Tasklet {

    private final GenericDataAccessService genericDataAccessService;

    private final GenericMapper genericMapper;

    private final RedisManager redisManager;

    private final ObjectMapper objectMapper;

    public ConfigVersionTasklet(GenericDataAccessService genericDataAccessService,
                                GenericMapper genericMapper,
                                RedisManager redisManager,
                                ObjectMapper objectMapper) {
        this.genericDataAccessService = genericDataAccessService;
        this.genericMapper = genericMapper;
        this.redisManager = redisManager;
        this.objectMapper = objectMapper;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        HashMap<String, Object> params = new HashMap<>();
        params.put("configName", null);
        params.put("version", null);
        String configVersionStr = objectMapper.writeValueAsString(genericDataAccessService.convertColumn(
                genericMapper.query(ConfigurationVersionPo.class, params, null, null, null)));
        setRedisData(Constant.CONFIGVERSION, configVersionStr);
        log.info("updateRedisInfo 配置版本表同步完成");
        return RepeatStatus.FINISHED;
    }

    /**
     * 对比 redis 当前值，仅在不同时更新并发布（同名源语义）。
     */
    private void setRedisData(String redisKey, String dbDataStr) {
        Object redisValue = redisManager.get(redisKey);
        if (!dbDataStr.equals(redisValue)) {
            log.info("更新redisKey:{},值为{}", redisKey, dbDataStr);
            redisManager.set(redisKey, dbDataStr);
            redisManager.push(redisKey, dbDataStr);
        }
    }
}
/*
 * AICode End
 */

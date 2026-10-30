/*
 * AICode Begin
 * NgVersionTasklet：updateRedisInfo 任务步骤——大网关配置版本同步 Redis。
 *
 * 语义与原文 updateRedisInfo() 中 updateNgVersion() 一致：
 *   configurationVersionService.selectByConfigName() -> covertTo map ->
 *   JSON 序列化 -> setRedisData(CONFIGVERSION_NG)（走 redisTemplate，与原文一致）。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.constant.Constant;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.ConfigurationVersionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.HashMap;
import java.util.List;

/**
 * 大网关版本同步 Redis（updateRedisInfo 步骤）。
 */
@Slf4j
public class NgVersionTasklet implements Tasklet {

    private final ConfigurationVersionService configurationVersionService;

    private final RedisManager redisManager;

    private final ObjectMapper objectMapper;

    private final RedisTemplate<String, String> redisTemplate;

    public NgVersionTasklet(ConfigurationVersionService configurationVersionService,
                            RedisManager redisManager,
                            ObjectMapper objectMapper,
                            RedisTemplate<String, String> redisTemplate) {
        this.configurationVersionService = configurationVersionService;
        this.redisManager = redisManager;
        this.objectMapper = objectMapper;
        this.redisTemplate = redisTemplate;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        List<HashMap<String, String>> versionMaps = configurationVersionService.selectByConfigName();
        HashMap<String, String> result = new HashMap<>();
        for (HashMap<String, String> map : versionMaps) {
            result.put(map.get("config_name"), map.get("version"));
        }
        String configVersionNgStr = objectMapper.writeValueAsString(result);

        Object redisValue = redisTemplate.opsForValue().get(Constant.CONFIGVERSION_NG);
        if (!configVersionNgStr.equals(redisValue)) {
            log.info("更新redisKey:{},值为{}", Constant.CONFIGVERSION_NG, configVersionNgStr);
            redisTemplate.opsForValue().set(Constant.CONFIGVERSION_NG, configVersionNgStr);
            // 大网关版本监听通道与普通 key 一致（原文 redisManager.push）
            redisManager.push(Constant.CONFIGVERSION_NG, configVersionNgStr);
        }
        log.info("updateRedisInfo 大网关版本同步完成");
        return RepeatStatus.FINISHED;
    }
}
/*
 * AICode End
 */

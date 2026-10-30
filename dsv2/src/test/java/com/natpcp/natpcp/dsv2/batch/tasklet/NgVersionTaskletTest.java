package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.batch.tasklet.NgVersionTasklet;
import com.natpcp.natpcp.dsv2.constant.Constant;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.ConfigurationVersionService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * NgVersionTasklet 单元测试。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("NgVersionTasklet 测试")
class NgVersionTaskletTest {

    @Mock
    private ConfigurationVersionService configurationVersionService;

    @Mock
    private RedisManager redisManager;

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private ObjectMapper objectMapper;

    private NgVersionTasklet tasklet;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        tasklet = new NgVersionTasklet(configurationVersionService, redisManager, objectMapper, redisTemplate);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    }

    @DisplayName("execute: 版本不同时 set + push")
    @Test
    void execute_shouldSetAndPush_whenDifferent() throws Exception {
        List<HashMap<String, String>> maps = new ArrayList<>();
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("config_name", "ngConfig");
        m1.put("version", "v1");
        maps.add(m1);
        when(configurationVersionService.selectByConfigName()).thenReturn(maps);
        when(valueOperations.get(Constant.CONFIGVERSION_NG)).thenReturn("old");

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        verify(valueOperations).set(eq(Constant.CONFIGVERSION_NG), any(String.class));
        verify(redisManager).push(eq(Constant.CONFIGVERSION_NG), any(String.class));
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
    }

    @DisplayName("execute: 版本相同时不写 Redis")
    @Test
    void execute_shouldSkipWrite_whenSame() throws Exception {
        List<HashMap<String, String>> maps = new ArrayList<>();
        HashMap<String, String> m1 = new HashMap<>();
        m1.put("config_name", "ngConfig");
        m1.put("version", "v1");
        maps.add(m1);
        when(configurationVersionService.selectByConfigName()).thenReturn(maps);
        when(valueOperations.get(Constant.CONFIGVERSION_NG)).thenReturn("{\"ngConfig\":\"v1\"}");

        tasklet.execute(contribution, chunkContext);

        verify(valueOperations, never()).set(eq(Constant.CONFIGVERSION_NG), any(String.class));
        verify(redisManager, never()).push(eq(Constant.CONFIGVERSION_NG), any(String.class));
    }
}
/*
 * AICode End
 */

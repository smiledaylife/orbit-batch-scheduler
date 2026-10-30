package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.natpcp.natpcp.dsv2.batch.tasklet.ConfigVersionTasklet;
import com.natpcp.natpcp.dsv2.constant.Constant;
import com.natpcp.natpcp.dsv2.dao.mapper.GenericMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * ConfigVersionTasklet 单元测试。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ConfigVersionTasklet 测试")
class ConfigVersionTaskletTest {

    @Mock
    private GenericDataAccessService genericDataAccessService;

    @Mock
    private GenericMapper genericMapper;

    @Mock
    private RedisManager redisManager;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private ObjectMapper objectMapper;

    private ConfigVersionTasklet tasklet;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        tasklet = new ConfigVersionTasklet(genericDataAccessService, genericMapper, redisManager, objectMapper);
    }

    private void stubQueryWith(List<Map<String, Object>> rows) {
        when(genericDataAccessService.convertColumn(any())).thenReturn(rows);
    }

    @DisplayName("execute: DB 与 Redis 不同时 set + push")
    @Test
    void execute_shouldUpdateRedis_whenDifferent() throws Exception {
        stubQueryWith(new ArrayList<>());
        when(redisManager.get(Constant.CONFIGVERSION)).thenReturn("old");

        tasklet.execute(contribution, chunkContext);

        verify(genericMapper).query(eq(ConfigurationVersionPo.class), any(), any(), any(), any());
        verify(redisManager).set(eq(Constant.CONFIGVERSION), any(String.class));
        verify(redisManager).push(eq(Constant.CONFIGVERSION), any(String.class));
    }

    @DisplayName("execute: DB 与 Redis 相同时只查询不写 Redis")
    @Test
    void execute_shouldSkipWrite_whenSame() throws Exception {
        stubQueryWith(new ArrayList<>());
        when(redisManager.get(Constant.CONFIGVERSION)).thenReturn("[]");

        tasklet.execute(contribution, chunkContext);

        verify(redisManager, never()).set(eq(Constant.CONFIGVERSION), any(String.class));
        verify(redisManager, never()).push(eq(Constant.CONFIGVERSION), any(String.class));
    }

    @DisplayName("execute: 返回 FINISHED")
    @Test
    void execute_shouldReturnFinished() throws Exception {
        stubQueryWith(new ArrayList<>());
        when(redisManager.get(Constant.CONFIGVERSION)).thenReturn(null);
        RepeatStatus status = tasklet.execute(contribution, chunkContext);
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
    }
}
/*
 * AICode End
 */

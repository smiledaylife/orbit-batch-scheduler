package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.batch.tasklet.VersionNoticeTasklet;
import com.natpcp.natpcp.dsv2.common.BizException;
import com.natpcp.natpcp.dsv2.constant.GatewayForwardConstants;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.NoticeService;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * VersionNoticeTasklet unit test: instance / big-gateway version compare & notice.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("VersionNoticeTasklet Test")
class VersionNoticeTaskletTest {

    @Mock
    private RedisTemplate<String, String> redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private RedisManager redisManager;

    @Mock
    private GenericDataAccessService genericDataAccessService;

    @Mock
    private NoticeService noticeService;

    @Mock
    private ConfigurableApplicationContext context;

    @Mock
    private ConfigurableEnvironment environment;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private VersionNoticeTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new VersionNoticeTasklet(redisTemplate, redisManager,
                genericDataAccessService, noticeService, context);
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(context.getEnvironment()).thenReturn(environment);
    }

    private void stubConfigVersions(List<ConfigurationVersionPo> pos) {
        when(genericDataAccessService.queryForObjects(
                any(Class.class), any(Object.class), any(), any())).thenReturn(pos);
    }

    private ConfigurationVersionPo po(String configName, String version) {
        return new ConfigurationVersionPo(configName, version, null, null);
    }

    @DisplayName("execute: no instances -> finish, no notice")
    @Test
    void execute_shouldFinish_whenNoInstances() {
        stubConfigVersions(Collections.singletonList(po("cfg1", "v1")));
        when(redisManager.keysInPrefix("instance")).thenReturn(Collections.emptySet());
        when(redisManager.scanKeys("*_ng:*")).thenReturn(Collections.emptySet());

        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
    }

    @DisplayName("execute: instance version same -> no notice")
    @Test
    void execute_shouldNotNotice_whenInstanceVersionSame() {
        stubConfigVersions(Collections.singletonList(po("cfg1", "v1")));
        String instanceKey = "instance:svcA/1/1.2.3.4/8080/H";
        when(redisManager.keysInPrefix("instance"))
                .thenReturn(new HashSet<>(Collections.singletonList(instanceKey)));
        when(redisManager.get(instanceKey)).thenReturn("{\"cfg1\":\"v1\"}");
        when(redisManager.scanKeys("*_ng:*")).thenReturn(Collections.emptySet());

        tasklet.execute(contribution, chunkContext);

        verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
    }

    @DisplayName("execute: instance version diff -> notice sent")
    @Test
    void execute_shouldNotice_whenInstanceVersionDiff() {
        stubConfigVersions(Collections.singletonList(po("cfg1", "v2")));
        String instanceKey = "instance:svcA/1/1.2.3.4/8080/H";
        when(redisManager.keysInPrefix("instance"))
                .thenReturn(new HashSet<>(Collections.singletonList(instanceKey)));
        when(redisManager.get(instanceKey)).thenReturn("{\"cfg1\":\"v1\"}");
        when(redisManager.scanKeys("*_ng:*")).thenReturn(Collections.emptySet());
        when(environment.getProperty("gatewayName.H")).thenReturn("Hefei");
        when(environment.getProperty("gatewayName.svcA")).thenReturn("ServiceA");

        tasklet.execute(contribution, chunkContext);
        // sendNotice 内 insert 两次（0101/0104 角色）
        verify(noticeService).sendToAdminAndOps(anyString(), anyString());
    }

    @DisplayName("execute: instance value not json -> BizException")
    @Test
    void execute_shouldThrowBizException_whenInstanceValueNotJson() {
        stubConfigVersions(Collections.singletonList(po("cfg1", "v1")));
        String instanceKey = "instance:svcA/1/1.2.3.4/8080/H";
        lenient().when(redisManager.keysInPrefix("instance"))
                .thenReturn(new HashSet<>(Collections.singletonList(instanceKey)));
        lenient().when(redisManager.get(instanceKey)).thenReturn("not-json{{{");

        // execute 在 checkVersionAndSendNotice 阶段即抛 BizException，scanKeys 不会被调用
        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(BizException.class);
    }

    @DisplayName("execute: ng version same -> no notice")
    @Test
    void execute_shouldNotNotice_whenNgVersionSame() {
        stubConfigVersions(Arrays.asList(
                po(GatewayForwardConstants.FORWARD_OBJ_KEY, "objV1"),
                po("web_hf_forward", "ruleV1")));
        when(redisManager.keysInPrefix("instance")).thenReturn(null);
        when(redisManager.scanKeys("*_ng:*"))
                .thenReturn(new HashSet<>(Collections.singletonList("web_hf:2")));
        when(valueOperations.get("web_hf:2")).thenReturn("prefix:objV1,ruleV1");

        tasklet.execute(contribution, chunkContext);

        verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
    }

    @DisplayName("execute: ng version diff -> notice")
    @Test
    void execute_shouldNotice_whenNgVersionDiff() {
        stubConfigVersions(Arrays.asList(
                po(GatewayForwardConstants.FORWARD_OBJ_KEY, "objV1"),
                po("web_hf_forward", "ruleV1")));
        when(redisManager.keysInPrefix("instance")).thenReturn(null);
        when(redisManager.scanKeys("*_ng:*"))
                .thenReturn(new HashSet<>(Collections.singletonList("web_hf:2")));
        when(valueOperations.get("web_hf:2")).thenReturn("prefix:objV2,ruleV2");

        tasklet.execute(contribution, chunkContext);

        // sendNotice 内 insert 两次
        verify(noticeService).sendToAdminAndOps(anyString(), anyString());
    }

    @DisplayName("execute: ng value empty -> skip")
    @Test
    void execute_shouldSkip_whenNgValueEmpty() {
        stubConfigVersions(Collections.singletonList(
                po(GatewayForwardConstants.FORWARD_OBJ_KEY, "objV1")));
        when(redisManager.keysInPrefix("instance")).thenReturn(null);
        when(redisManager.scanKeys("*_ng:*"))
                .thenReturn(new HashSet<>(Collections.singletonList("web_hf:2")));
        when(valueOperations.get("web_hf:2")).thenReturn("");

        tasklet.execute(contribution, chunkContext);

        verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
    }
}
/*
 * AICode End
 */

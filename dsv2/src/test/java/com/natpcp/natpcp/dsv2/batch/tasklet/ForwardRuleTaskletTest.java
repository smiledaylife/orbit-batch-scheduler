package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.batch.tasklet.ForwardRuleTasklet;
import com.natpcp.natpcp.dsv2.dao.po.ForwardRulePO;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;

/**
 * AICode Begin
 * ForwardRuleTasklet 单元测试。
 *
 * 适配 GenericDataAccessService.manipulateAllData 新 3 参数签名
 * （clazz, data, queryConditions）：
 * 转发规则增量同步只需全量遍历 forward_rule 表，data 与 queryConditions 均为空实例/空。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ForwardRuleTasklet 测试")
class ForwardRuleTaskletTest {

    @Mock
    private GenericDataAccessService genericDataAccessService;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private ForwardRuleTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new ForwardRuleTasklet(genericDataAccessService);
    }

    @DisplayName("execute: 调用 manipulateAllData(ForwardRulePO, 空实例, null) 返回 FINISHED")
    @Test
    void execute_shouldInvokeManipulateAllDataAndFinish() {
        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        verify(genericDataAccessService).manipulateAllData(
                eq(ForwardRulePO.class), any(ForwardRulePO.class),
                isNull());
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
    }
}
/*
 * AICode End
 */

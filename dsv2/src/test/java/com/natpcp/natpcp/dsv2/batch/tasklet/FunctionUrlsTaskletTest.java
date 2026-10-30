package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.batch.tasklet.FunctionUrlsTasklet;
import com.natpcp.natpcp.dsv2.service.impl.FunctionServiceImpl;
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
import static org.mockito.Mockito.verify;

/**
 * AICode Begin
 * FunctionUrlsTasklet 单元测试。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("FunctionUrlsTasklet 测试")
class FunctionUrlsTaskletTest {

    @Mock
    private FunctionServiceImpl functionService;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private FunctionUrlsTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new FunctionUrlsTasklet(functionService);
    }

    @DisplayName("execute: 调用 updateRedisFunctionUrls 返回 FINISHED")
    @Test
    void execute_shouldInvokeServiceAndFinish() {
        RepeatStatus status = tasklet.execute(contribution, chunkContext);
        verify(functionService).updateRedisFunctionUrls();
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
    }
}
/*
 * AICode End
 */

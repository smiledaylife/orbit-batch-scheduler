package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.batch.tasklet.ClearNoticeTasklet;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;

import java.time.LocalDate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * AICode Begin
 * ClearNoticeTasklet 单元测试。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ClearNoticeTasklet 测试")
class ClearNoticeTaskletTest {

    @Mock
    private NoticeService noticeService;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private ClearNoticeTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new ClearNoticeTasklet(noticeService);
    }

    @DisplayName("execute: 以 3 天前日期调用 clearNotice 返回 FINISHED")
    @Test
    void execute_shouldClearThreeDaysAgoAndFinish() {
        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        LocalDate expected = LocalDate.now().minusDays(3);
        verify(noticeService).clearNotice(expected);
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
    }
}
/*
 * AICode End
 */

/*
 * AICode Begin
 * ClearNoticeTasklet：clearNotice 任务 Tasklet。
 *
 * 语义与原文 handler 一致：删除消息通知表中 3 天以前的记录。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.service.NoticeService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.time.LocalDate;

/**
 * 清理历史通知（Tasklet）。
 */
@Slf4j
public class ClearNoticeTasklet implements Tasklet {

    private final NoticeService noticeService;

    public ClearNoticeTasklet(NoticeService noticeService) {
        this.noticeService = noticeService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        LocalDate localDate = LocalDate.now().minusDays(3);
        noticeService.clearNotice(localDate);
        log.info("clearNotice 清理历史通知完成, 截止日期={}", localDate);
        return RepeatStatus.FINISHED;
    }
}
/*
 * AICode End
 */

/*
 * AICode Begin
 * FunctionUrlsTasklet：updateRedisFunctionUrls 任务 Tasklet。
 *
 * 全量刷新语义：一次查询全部有效 func_action 写入 Redis 缓存。
 * 说明：本任务为"全量覆写"模型（一个 key 整体 set），拆分 chunk 无收益，
 *       因此以 Tasklet 承载；同样受 Spring Batch 事务/状态/重启管理。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.service.impl.FunctionServiceImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * 更新功能 URL 缓存（Tasklet）。
 */
@Slf4j
public class FunctionUrlsTasklet implements Tasklet {

    private final FunctionServiceImpl functionService;

    public FunctionUrlsTasklet(FunctionServiceImpl functionService) {
        this.functionService = functionService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        functionService.updateRedisFunctionUrls();
        return RepeatStatus.FINISHED;
    }
}
/*
 * AICode End
 */

/*
 * AICode Begin
 * ForwardRuleTasklet：updateRedisInfo 任务前置步骤——转发规则增量同步 Redis。
 *
 * 语义与原文 updateRedisInfo() 第一步一致：
 *   manipulateAllData(ForwardRulePO, new ForwardRulePO(), "loopForwardSaveRedis", ...)
 * 即遍历 forward_rule 表，若 Redis 当前值不同则 set + push。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.dao.po.ForwardRulePO;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * 转发规则增量同步 Redis（updateRedisInfo 前置步骤）。
 */
@Slf4j
public class ForwardRuleTasklet implements Tasklet {

    private final GenericDataAccessService genericDataAccessService;

    public ForwardRuleTasklet(GenericDataAccessService genericDataAccessService) {
        this.genericDataAccessService = genericDataAccessService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        //网关转发配置pg同步到redis
        genericDataAccessService.manipulateAllData(ForwardRulePO.class, new ForwardRulePO(),null);
        log.info("updateRedisInfo 转发规则增量同步完成");
        return RepeatStatus.FINISHED;
    }
}
/*
 * AICode End
 */

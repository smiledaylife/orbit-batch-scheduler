/*
 * AICode Begin
 * FunctionServiceImpl 精简版（仅保留 updateRedisFunctionUrls 任务能力）。
 *
 * 原实现依赖 MenuServiceImpl / RoleMenuFunctionServiceImpl / ApprovalWorkflowConfigService
 * / PltStringUtil 等页内模块；迁移版去除全部 CRUD 接口，仅保留任务入口：
 * 从 tb_pmctl_auth_function 拉取全部有效 func_action 写入 Redis 缓存
 * （key = MgCachePrefixEnum.FUNCTION_URLS）。
 */
package com.natpcp.natpcp.dsv2.service.impl;

import com.natpcp.natpcp.dsv2.util.RedisUtils;
import com.natpcp.natpcp.dsv2.constant.MgCachePrefixEnum;
import com.natpcp.natpcp.dsv2.dao.mapper.FunctionMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * 功能模块任务服务实现（ds-V2 精简版）。
 */
@Service
@Slf4j
public class FunctionServiceImpl {

    @Autowired
    private RedisUtils redisUtils;

    @Autowired
    private FunctionMapper functionMapper;

    /**
     * 更新缓存中的功能 URL 信息。
     *
     * <p>将数据库中的有效功能入口 URL 集合全量刷新到 Redis，
     * 保持与 plt-mgmt 原文 execute/service 一致的行为。</p>
     */
    public void updateRedisFunctionUrls() {
        Set<String> urlSet = functionMapper.getUrls();
        redisUtils.set(MgCachePrefixEnum.FUNCTION_URLS, urlSet);
        log.info("更新功能URL缓存完成，共 {} 条", urlSet == null ? 0 : urlSet.size());
    }
}
/*
 * AICode End
 */

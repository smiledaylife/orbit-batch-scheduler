/*
 * AICode Begin
 * FunctionMapper 精简版（仅保留 getUrls）。
 *
 * 原接口包含 insertFunction/updateFunction 等页内 CRUD；迁移版仅保留
 * updateRedisFunctionUrls 任务使用的 getUrls（返回全部有效 func_action）。
 */
package com.natpcp.natpcp.dsv2.dao.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Repository;

import java.util.Set;

/**
 * 功能模块 Mapper（ds-V2 精简版）。
 */
@Mapper
@Repository
public interface FunctionMapper {

    /**
     * 查询全部有效的功能入口 URL。
     *
     * @return 去重后的 URL 集合
     */
    Set<String> getUrls();
}
/*
 * AICode End
 */

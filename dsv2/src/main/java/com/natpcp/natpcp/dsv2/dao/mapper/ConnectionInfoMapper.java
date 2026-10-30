/*
 * AICode Begin
 * ConnectionInfoMapper 精简版（仅保留任务需要的 selectById）。
 *
 * 原接口包含 insert/queryConnectionInfo/editConnectionInfo/deleteConnectionInfo 等
 * 页面 CRUD（依赖 api.vo.ConfigInfoQueryVO / ConnectionInfoVO，属于未复制页面层）；
 * 迁移版仅保留 TableCheck / ConfigCheck 使用的 selectById，结合 XML 提供 SQL。
 */
package com.natpcp.natpcp.dsv2.dao.mapper;

import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import org.apache.ibatis.annotations.Mapper;

/**
 * 连接信息 Mapper（ds-V2 精简版）。
 */
@Mapper
public interface ConnectionInfoMapper {

    /**
     * 查询连接信息。
     *
     * @param id 主键
     * @return 连接信息
     */
    ConnectionInfoPO selectById(String id);
}
/*
 * AICode End
 */

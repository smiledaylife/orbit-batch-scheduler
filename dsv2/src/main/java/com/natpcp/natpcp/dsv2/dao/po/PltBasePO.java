/*
 * AICode Begin
 * plt-common 等价类：PltBasePO（实体基类）。
 *
 * 等价于 plt-common-bo 的 com.natpcp.po.dao.dsv2.natpcp.PltBasePO。
 * 字段依据 NoticeMapper.xml resultMap / FunctionMapper.xml 使用情况确定：
 * create_teller_no / create_time / last_mod_teller_no / last_mod_time；
 * 扩展 del_status、createTellerName 等公共审计字段（按需补充）。
 */
package com.natpcp.natpcp.dsv2.dao.po;

import lombok.Data;

import java.time.LocalDateTime;

/**
 * 平台实体基类。
 */
@Data
public class PltBasePO {

    /**
     * 创建柜员号
     */
    private String createTellerNo;

    /**
     * 创建时间
     */
    private LocalDateTime createTime;

    /**
     * 最近修改柜员号
     */
    private String lastModTellerNo;

    /**
     * 最近修改时间
     */
    private LocalDateTime lastModTime;

    /**
     * 逻辑删除标志：1-有效，0-已删除
     */
    private String delStatus;
}
/*
 * AICode End
 */

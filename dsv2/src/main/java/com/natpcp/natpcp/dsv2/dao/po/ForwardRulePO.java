package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.baomidou.mybatisplus.annotation.TableName;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@TableName("tb_forward_rule")
public class ForwardRulePO {
    /**
     * 主键id
     */
    @TableId
    private String key;
    /**
     * 存储的值
     */
    private String value;

    /**
     * 创建柜员号
     */
    @TableField("create_teller_no")
    private String createTellerNo;

    /**
     * 创建时间
     */
    @TableField("create_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime createTime;
    /**
     * 最后修改柜员号
     */
    @TableField("last_mod_teller_no")
    private String lastModTellerNo;

    /**
     * 最后修改时间
     */
    @TableField("last_mod_time")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime lastModTime;

    /**
     * 删除标志
     */
    @TableField("del_status")
    private String delStatus;
}

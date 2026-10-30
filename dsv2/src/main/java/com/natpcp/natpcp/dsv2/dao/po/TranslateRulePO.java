package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

/**
 * @Auther wangl
 * @Date : 2024/8/14  9:26
 */
@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@TableName("tb_translate_rule")
public class TranslateRulePO {
    @TableId
    private String id;
    //阶段类型（0.接入 1接出）
    @TableField("stage_type")
    private Integer stageType;
    //交易码
    @TableField("tx_code")
    private String txCode;
    //子交易码
    @TableField("subtx_no")
    private String subtxNo;
    //扩展字段
    @TableField("extfld_name")
    private String extfldName;
    //接收方系统号
    @TableField("acceptor_sys_no")
    private String acceptorSysNo;
    //全局业务跟踪号
    @TableField("global_busi_track_no")
    private String globalBusiTrackNo;
    @TableField("del_status")
    private String delStatus;
    @TableField("last_mod_time")
    private LocalDateTime lastModTime;
}

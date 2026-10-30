package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
@TableName("tb_macrelated_txcode")
public class MacrelatedTxcodePo implements Serializable {
    @TableId
    private String id;
    @TableField("tx_code")
    private String txCode;
    @TableField("separator")
    private String separator;
    @TableField("secret_id")
    private String secretId;
    @TableField("gv_flag")
    private String gvFlag;
    @TableField("generate_field")
    private String generateField;
    @TableField("req_generate_mac")
    private String reqGenerateMac;
    @TableField("validate_field")
    private String validateField;
    @TableField("req_validate_mac")
    private String reqValidateMac;
    @TableField("del_status")
    private String delStatus;
    @TableField("create_teller_no")
    private String createTellerNo;
    @TableField("create_time")
    private LocalDateTime createTime;
    @TableField("last_mod_teller_no")
    private String lastModTellerNo;
    @TableField("last_mod_time")
    private LocalDateTime lastModTime;


}
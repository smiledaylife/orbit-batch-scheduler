package com.natpcp.natpcp.dsv2.dao.po;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@TableName("tb_pmctl_auth_notice")
public class NoticePO extends PltBasePO {

    @TableId(type = IdType.ASSIGN_UUID)
    private String id;

    private String title;

    private String content;

    /**
     * 接收柜员号
     */
    @TableField("receive_teller_no")
    private String receiveTellerNo;

    /**
     * 0未读,1已读
     */
    private String status = "0";

    /**
     * 业务ID
     */
    @TableField("business_id")
    private String businessId;

    @TableField(value = "teller_name",exist = false)
    private String tellerName;
    /**
     * 接收通知角色
     */
    private String receiveRole;

}

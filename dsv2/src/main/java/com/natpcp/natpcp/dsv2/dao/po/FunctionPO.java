package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @author zhangxiaolong0114
 * @version 1.0
 * @className FunctionPo
 * @description 功能
 * @date 2022/9/13 15:53
 */
@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@AllArgsConstructor
@TableName("tb_pmctl_auth_function")
public class FunctionPO extends PltBasePO {

    // 菜单编码
    @TableField("menu_id")
    private String 	menuId;
    // 功能id
    @TableId(type = IdType.ASSIGN_UUID)
    private String 	id;
    // 功能编码
    @TableField(value = "func_code")
    private String 	funcCode;
    //功能名称
    @TableField("func_name")
    private String funcName;
    //功能入口URL
    @TableField("func_action")
    private String funcAction;
    //功能描述
    @TableField("func_desc")
    private String funcDesc;
    //功能添加说明
    @TableField("func_tips")
    private String funcTips;
    //功能授权/审批标识：0-不需要，1-授权，2-审批工作流
    @TableField("authorize_flag")
    private String authorizeFlag;
    //功能的知识库链接
    @TableField("doc_url")
    private String docUrl;
    //是否审批可编辑
    @TableField("can_edit_on_approve")
    private String canEditOnApprove;

}

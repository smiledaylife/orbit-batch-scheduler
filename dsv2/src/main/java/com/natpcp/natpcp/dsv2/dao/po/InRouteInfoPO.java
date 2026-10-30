package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

import java.time.LocalDateTime;

@Data
@Accessors(chain = true)
@AllArgsConstructor
@NoArgsConstructor
@TableName("tb_route_in")
public class InRouteInfoPO {
    /**
     * Id
     */
    @TableId
    private String id;
    /**
     * 路由Id
     */
    @TableField("route_id")
    private String routeId;
    /**
     * 断言
     */
    @TableField("predicates_rule")
    private String predicatesRule;
    /**
     * 转发路径
     */
    @TableField("target_url")
    private String targetUrl;
    /**
     * 备用转发路径
     */
    @TableField("standby_url")
    private String standbyUrl;
    /**
     * api处理
     */
    @TableField("strip_prefix")
    private Integer stripPrefix;
    /**
     * 优先级
     */
    @TableField("route_order")
    private Integer routeOrder;
    /**
     * 删除标志
     */
    @TableField("del_status")
    private Integer delStatus;

    @TableField("create_teller_no")
    private String createTellerNo;

    @TableField("create_time")
    private LocalDateTime createTime;

    @TableField("last_mod_teller_no")
    private String lastModTellerNo;

    @TableField("last_mod_time")
    private LocalDateTime lastModTime;
}
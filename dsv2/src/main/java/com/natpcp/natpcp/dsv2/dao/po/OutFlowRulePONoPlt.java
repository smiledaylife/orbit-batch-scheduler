package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("tb_flow_rule_out")
public class OutFlowRulePONoPlt {


    @TableId
    private String id;
    /**
     * app名称
     */
    private String app;

    /**
     * 熔断资源名称
     */
    private String resource;

    /**
     * 限流应用类型
     * 0：ROUTE ID 1：API分组
     */
    @TableField("resource_mode")
    private String resourceMode;

    /**
     * 针对请求类型
     */
    @TableField("param_item")
    private String paramItem;

    /**
     * 阈值类型
     * 0：QPS  1：线程数
     */
    private String grade;

    /**
     * 针对grage不同，代表含义不同
     * QPS：QPS阈值
     * 线程数：线程数
     */
    private String count;

    /**
     * 间隔时间
     */
    private String interval;

    /**
     * 间隔时间单位
     * 0：秒 1：分 2：时 3：天
     */
    @TableField("interval_unit")
    private String intervalUnit;

    /**
     * 流控方式
     * 0：快速失败 2：匀速排队
     */
    @TableField("control_behavior")
    private String controlBehavior;

    /**
     * 选择快速失败时，代表突发流量的数量，表示超过QPS限制后可以容忍的数量
     */
    private String burst;

    @TableField("del_status")
    private String delStatus;

    /**
     * 选择匀速排队时，代表超时时间
     */
    @TableField("max_queueing_timeout_ms")
    private String maxQueueingTimeoutMs;

    @TableField("last_mod_time")
    private LocalDateTime lastModTime;

    @TableField("last_mod_teller_no")
    private String lastModTellerNo;

    @TableField("create_time")
    private LocalDateTime createTime;

    @TableField("create_teller_no")
    private String createTellerNo;
}

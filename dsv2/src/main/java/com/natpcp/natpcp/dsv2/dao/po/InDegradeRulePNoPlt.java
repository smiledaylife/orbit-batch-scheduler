package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("tb_degrade_rule_in")
public class InDegradeRulePNoPlt {

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
     * 熔断策略
     * 0：慢调用比例 1：异常比例 2：异常数
     */
    private String grade;

    /**
     * 不同熔断策略对应不同的涵义：
     * 1、慢调用比例：最大RT
     * 2、异常比例：比例阈值
     * 3、异常数：异常数
     */
    private String count;

    /**
     * 选择慢调用比例时，表示比例阈值
     */
    @TableField("slow_ratio_threshold")
    private String slowRatioThreshold;

    /**
     * 熔断时长
     */
    @TableField("time_window")
    private String timeWindow;

    /**
     * 最小请求数
     */
    @TableField("min_request_amount")
    private String minRequestAmount;

    @TableField("last_mod_time")
    private LocalDateTime lastModTime;

    @TableField("create_time")
    private LocalDateTime createTime;

    @TableField("last_mod_teller_no")
    private String lastModTellerNo;

    @TableField("create_teller_no")
    private String createTellerNo;
    /**
     * 统计时长
     */
    @TableField("stat_interval_ms")
    private String statIntervalMs;

    @TableField("del_status")
    private String delStatus;
}

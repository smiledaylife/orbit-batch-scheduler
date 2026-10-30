package com.natpcp.natpcp.dsv2.jobs.bo;


import lombok.Data;

/**
 * @Auther wangl
 * @Date : 2024/3/13  15:32
 */
@Data
public class TableCheckBO {
    //连接任务id
    private String connectionJobId;
    //检测表名
    private String tableName;
    //是否检查分区表
    private Boolean onlyExist;
    //新生成表名规则
    private String newNameRule;
    //新生成表时间周期
    private Integer createPeriod;
    //新生成表时间周期单位
    private Integer createPeriodUnit;
    //最新已存在表名
    private String latestTableName;

}

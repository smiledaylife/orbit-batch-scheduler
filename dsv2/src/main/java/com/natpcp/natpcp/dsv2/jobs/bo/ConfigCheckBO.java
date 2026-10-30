package com.natpcp.natpcp.dsv2.jobs.bo;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @Auther wangl
 * @Date : 2024/3/13  15:32
 */
@Data
public class ConfigCheckBO {
    private String connectionJobId;
    //应用类型
    private String config;
    private String appName;
    //配置项检测
    private List<ConfigBO> configs =new ArrayList<>();
}

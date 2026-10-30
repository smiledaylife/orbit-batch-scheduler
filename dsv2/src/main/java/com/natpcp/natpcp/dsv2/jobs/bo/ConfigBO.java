package com.natpcp.natpcp.dsv2.jobs.bo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

/**
 * @Auther wangl
 * @Date : 2024/4/2  10:40
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
public class ConfigBO {
    //配置项名称
    private String name;
    //配置项值
    private String value;
}

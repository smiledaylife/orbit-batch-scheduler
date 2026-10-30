package com.natpcp.natpcp.dsv2.dao.po;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@TableName("tb_configuration_version")
public class ConfigurationVersionPo  {

    @TableField("config_name")
    private String configName;
    @TableField("version")
    private String version;

    @TableField("last_mode_time")
    private LocalDateTime lastModeTime;

    @TableField("del_status")
    private String delStatus;
}


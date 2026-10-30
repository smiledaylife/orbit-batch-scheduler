package com.natpcp.natpcp.dsv2.dao.po;

import lombok.Data;

/**
 * @Auther wangl
 * @Date : 2024/6/20  9:30
 */
@Data
public class ConnectionInfoPO {
    private String id;
    //配置名
    private String config;
    //检测应用名
    private String name;
    //账号
    private String account;
    //密码
    private String password;
    //url
    private String url;
    //端口
    private String port;
    //path
    private String path;

}

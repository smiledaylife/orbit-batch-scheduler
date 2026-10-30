package com.natpcp.natpcp.dsv2.constant;

public class GatewayForwardConstants {

    /**
     * 服务名code后缀
     */
    public static final String SERVICE_SUFFIX = "ng";
    /**
     * 规则后缀
     */
    public static final String RULE_SUFFIX = "forward";
    /**
     * 转发目标key值
     */
    public static final String FORWARD_OBJ_KEY = "ng_forward_obj";

    /**
     * 最大重试次数
     */
    public static final int MAX_RETRIES = 3;

    /**
     * 匹配类型
     */
    public static final String BODY = "body";

    /**
     * 数据的字段名
     */
    public static final String FIELD_NAME = "value";

    /**
     * 服务名中文后缀
     */
    public static final String SERVICE_CHINESE_SUFFIX = "大网关";

    /**
     * 默认转发
     */
    public static final String DEFAULT_FORWARD = "默认转发";

    /**
     * 默认转发code
     */
    public static final String DEFAULT_FORWARD_CODE = "default";
    /**
     * 匹配字段key的层级(最多支持9层)
     */
    public static final int MAX_TIER = 9;

}

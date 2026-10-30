package com.natpcp.natpcp.dsv2.constant;

import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.dao.po.MacrelatedTxcodePo;
import com.natpcp.natpcp.dsv2.dao.po.InDegradeRulePNoPlt;
import com.natpcp.natpcp.dsv2.dao.po.InFlowRulePONoPlt;
import com.natpcp.natpcp.dsv2.dao.po.InRouteInfoPO;
import com.natpcp.natpcp.dsv2.dao.po.OutDegradeRulePNoPlt;
import com.natpcp.natpcp.dsv2.dao.po.OutFlowRulePONoPlt;
import com.natpcp.natpcp.dsv2.dao.po.OutRouteInfoPO;
import com.natpcp.natpcp.dsv2.dao.po.TranslateRulePO;
import lombok.Getter;

@Getter
public enum UpdateRedisEnum {
    /**
     * 更新redis中数据库枚举
     */
    MAC_RELATED(Constant.MACRELATED, MacrelatedTxcodePo.class,"MAC校验", "安全管理/MAC校验"),
    IN_ROUTE(Constant.INROUTE, InRouteInfoPO.class, "接入路由", "流量配置/路由配置/接入网关"),
    OUT_ROUTE(Constant.OUTROUTE, OutRouteInfoPO.class, "接出路由", "流量配置/路由配置/接出网关"),
    IN_FLOW_RULE(Constant.INFLOWRULE, InFlowRulePONoPlt.class, "接入限流规则", "流量配置/流控规则/接入网关"),
    OUT_FLOW_RULE(Constant.OUTFLOWRULE, OutFlowRulePONoPlt.class, "接出限流规则", "流量配置/流控规则/接出网关"),
    IN_DEGRADE_RULE(Constant.INDEGRADERULE, InDegradeRulePNoPlt.class, "接入熔断规则", "流量配置/降级规则/接入网关"),
    OUT_DEGRADE_RULE(Constant.OUTDEGRADERULE, OutDegradeRulePNoPlt.class, "接出熔断规则", "流量配置/降级规则/接出网关"),
    TRANSLATE_RULE(Constant.TRANSLATE_RULE, TranslateRulePO.class, "流水落库规则", "系统管理/流水落库"),
    CONFIG_VERSION(Constant.CONFIGVERSION, ConfigurationVersionPo.class, "配置版本刷新", "系统管理/流水落库"),
    CONFIG_VERSION_NG(Constant.CONFIGVERSION_NG, TranslateRulePO.class, "大网关", "流量配置/路由配置/大网关");
    private final String redisKey;
    private final Class clazz;
    private final String desc;
    private final String menu;

    UpdateRedisEnum(String redisKey, Class clazz, String desc, String menu) {
        this.redisKey = redisKey;
        this.clazz = clazz;
        this.desc = desc;
        this.menu = menu;
    }
}

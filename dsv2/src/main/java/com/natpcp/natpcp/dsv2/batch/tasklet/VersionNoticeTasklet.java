/*
 * AICode Begin
 * VersionNoticeTasklet：versionNotice 任务（配置版本一致性巡检）。
 *
 * 迁移自 plt-mgmt TaskSchedulingNoticeJobs.versionNotice()，业务语义保持不变：
 *   1. 普通实例：Redis 中 key 形如 "instance:<serviceId>/<version>/<host>/<port>/<center>"，
 *      value 为 JSON {配置名: 版本}；逐项与 tb_configuration_version 比对，不一致则通知。
 *   2. 大网关实例：Redis 中 key 形如 "<location>_<center>_ng:<n>"，
 *      value 形如 "<ip信息>:<forwardObj版本>,<forwardRule版本>"；两项分别与库内比对，不一致则通知。
 *
 * 说明：本任务原为 1 分钟周期热更新任务，迁移后由调度中心 cron 触发；
 *       实例遍历基于 Redis SCAN，逻辑为"比对 + 插库通知"，无数据量放大。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.common.BizException;
import com.natpcp.natpcp.dsv2.common.ExceptionCodeMsg;
import com.natpcp.natpcp.dsv2.common.PltResponseStatus;
import com.natpcp.natpcp.dsv2.constant.CenterEnum;
import com.natpcp.natpcp.dsv2.constant.GatewayForwardConstants;
import com.natpcp.natpcp.dsv2.constant.LocationEnum;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import com.natpcp.natpcp.dsv2.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 版本消息通知（Tasklet）。
 */
@Slf4j
public class VersionNoticeTasklet implements Tasklet {

    /** 普通实例 key 前缀 */
    private static final String INSTANCE_PREFIX = "instance";
    /** 大网关实例 key 模式 */
    private static final String GATEWAY_KEY_PATTERN = "*_ng:*";
    /** 配置文件中服务/中心的中文名前缀，如 gatewayName.H=合肥 */
    private static final String GATEWAY_NAME_PROPERTY_PREFIX = "gatewayName.";

    private final RedisTemplate<String, String> redisTemplate;
    private final RedisManager redisManager;
    private final GenericDataAccessService genericDataAccessService;
    private final NoticeService noticeService;
    private final ConfigurableApplicationContext context;

    public VersionNoticeTasklet(RedisTemplate<String, String> redisTemplate,
                                RedisManager redisManager,
                                GenericDataAccessService genericDataAccessService,
                                NoticeService noticeService,
                                ConfigurableApplicationContext context) {
        this.redisTemplate = redisTemplate;
        this.redisManager = redisManager;
        this.genericDataAccessService = genericDataAccessService;
        this.noticeService = noticeService;
        this.context = context;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.info("versionNotice 任务执行开始");
        // 库内版本只读一次，两类实例共用
        Map<String, String> dbVersions = loadConfigVersions();
        checkInstanceVersions(dbVersions);
        checkGatewayForwardVersions(dbVersions);
        log.info("versionNotice 任务执行结束");
        return RepeatStatus.FINISHED;
    }

    // ---------------------------------------------------------------- 普通实例

    private void checkInstanceVersions(Map<String, String> dbVersions) {
        Set<String> instances = redisManager.keysInPrefix(INSTANCE_PREFIX);
        if (instances == null || instances.isEmpty()) {
            return;
        }
        for (String instanceKey : instances) {
            Object value = redisManager.get(instanceKey);
            String json = value == null ? "" : value.toString();
            // 与原实现一致：value 是 key 的一部分时视为无需比对
            if (instanceKey.contains(json)) {
                continue;
            }
            Map<String, Object> instanceVersions;
            try {
                instanceVersions = JsonUtil.parseMap(json);
            } catch (IllegalArgumentException e) {
                throw new BizException(ExceptionCodeMsg.TRADE_NOTICE_ERROR,
                        "实例配置版本数据非JSON: " + instanceKey + "，" + e.getMessage());
            }
            for (Map.Entry<String, Object> entry : instanceVersions.entrySet()) {
                String configName = entry.getKey();
                String instanceVersion = entry.getValue() == null ? null : String.valueOf(entry.getValue());
                String dbVersion = dbVersions.get(configName);
                if (!StringUtils.equals(instanceVersion, dbVersion)) {
                    notifyInstanceOutdated(InstanceKey.parse(instanceKey), configName, instanceVersion, dbVersion);
                }
            }
        }
    }

    private void notifyInstanceOutdated(InstanceKey instance, String configName,
                                        String instanceVersion, String dbVersion) {
        Environment env = context.getEnvironment();
        String centerName = env.getProperty(GATEWAY_NAME_PROPERTY_PREFIX + instance.center);
        String serviceName = env.getProperty(GATEWAY_NAME_PROPERTY_PREFIX + instance.serviceId);
        String address = instance.hostAddress + ":" + instance.port;

        String title = serviceName + "(" + instance.hostAddress + ")实例未更新到" + configName + "配置！";
        String content = centerName + "的" + serviceName + "服务地址: " + address + "; "
                + "配置参数 " + configName + "在redis缓存中的数据是：" + instanceVersion + ","
                + configName + "在库里的数据是：" + dbVersion;
        noticeService.sendToAdminAndOps(title, content);
    }

    /** 解析 "instance:<serviceId>/<version>/<host>/<port>/<center>" */
    private static final class InstanceKey {
        final String serviceId;
        final String hostAddress;
        final String port;
        final String center;

        private InstanceKey(String serviceId, String hostAddress, String port, String center) {
            this.serviceId = serviceId;
            this.hostAddress = hostAddress;
            this.port = port;
            this.center = center;
        }

        static InstanceKey parse(String key) {
            if (StringUtils.isEmpty(key) || "null".equals(key) || !key.contains(":")) {
                throw new BizException(PltResponseStatus.PARAMETER_ERROR, "Redis中的版本实例数据异常");
            }
            String body = key.split(":")[1];
            if (StringUtils.isEmpty(body) || "null".equals(body)) {
                throw new BizException(PltResponseStatus.PARAMETER_ERROR, "Redis中的版本实例数据异常");
            }
            String[] parts = body.split("/");
            if (parts.length < 5) {
                throw new BizException(PltResponseStatus.PARAMETER_ERROR, "Redis中的版本实例数据异常: " + key);
            }
            // parts[1] 为实例版本号，通知文案不使用
            return new InstanceKey(parts[0], parts[2], parts[3], parts[4]);
        }
    }

    // ---------------------------------------------------------------- 大网关实例

    private void checkGatewayForwardVersions(Map<String, String> dbVersions) {
        Set<String> instances = redisManager.scanKeys(GATEWAY_KEY_PATTERN);
        if (instances == null || instances.isEmpty()) {
            return;
        }
        for (String instanceKey : instances) {
            String value = redisTemplate.opsForValue().get(instanceKey);
            if (StringUtils.isEmpty(value)) {
                log.warn("大网关实例 {} 配置版本为空，跳过", instanceKey);
                continue;
            }
            // value 形如 "<ip信息>:<objVersion>,<ruleVersion>"
            int lastColon = value.lastIndexOf(':');
            String ipInfo = value.substring(0, Math.max(lastColon, 0));
            String[] versions = value.substring(lastColon + 1).split(",");
            if (versions.length < 2) {
                continue;
            }
            String instanceObjVersion = versions[0];
            String instanceRuleVersion = versions[1];

            // key 形如 "<location>_<center>_ng:<n>"，取 ':' 前部分作为规则名前缀
            String gatewayId = instanceKey.split(":")[0];
            String ruleConfigName = gatewayId + "_" + GatewayForwardConstants.RULE_SUFFIX;
            String dbObjVersion = dbVersions.get(GatewayForwardConstants.FORWARD_OBJ_KEY);
            String dbRuleVersion = dbVersions.get(ruleConfigName);
            if (dbObjVersion == null || dbRuleVersion == null) {
                log.warn("大网关实例 {} 对应的库内版本缺失（{}={}, {}={}），跳过比对",
                        instanceKey, GatewayForwardConstants.FORWARD_OBJ_KEY, dbObjVersion,
                        ruleConfigName, dbRuleVersion);
                continue;
            }
            boolean objOutdated = !dbObjVersion.equals(instanceObjVersion);
            boolean ruleOutdated = !dbRuleVersion.equals(instanceRuleVersion);
            if (objOutdated || ruleOutdated) {
                notifyGatewayOutdated(gatewayId, ipInfo, ruleConfigName,
                        objOutdated ? instanceObjVersion : null, dbObjVersion,
                        ruleOutdated ? instanceRuleVersion : null, dbRuleVersion);
            }
        }
    }

    /**
     * 大网关通知。标题形如 "合肥xx服务(ip)实例未更新到forwardObj和xx_rule配置！"，
     * 正文逐项列出 redis 与库内版本；instance*Version 为 null 表示该项未过期、不写入文案。
     */
    private void notifyGatewayOutdated(String gatewayId, String ipInfo, String ruleConfigName,
                                       String instanceObjVersion, String dbObjVersion,
                                       String instanceRuleVersion, String dbRuleVersion) {
        String[] idParts = gatewayId.split("_");
        String serviceName = LocationEnum.getDescByValue(idParts[0]) + CenterEnum.getDescByValue(idParts[1])
                + GatewayForwardConstants.SERVICE_CHINESE_SUFFIX;

        StringBuilder outdatedNames = new StringBuilder();
        StringBuilder content = new StringBuilder(serviceName).append("(").append(ipInfo).append(")配置参数");
        if (instanceObjVersion != null) {
            outdatedNames.append(GatewayForwardConstants.FORWARD_OBJ_KEY);
            content.append(GatewayForwardConstants.FORWARD_OBJ_KEY)
                    .append("在redis缓存中的数据是：").append(instanceObjVersion).append(",")
                    .append(GatewayForwardConstants.FORWARD_OBJ_KEY)
                    .append("在库里的数据是：").append(dbObjVersion).append(";");
        }
        if (instanceRuleVersion != null) {
            if (outdatedNames.length() > 0) {
                outdatedNames.append("和");
            }
            outdatedNames.append(ruleConfigName);
            content.append(ruleConfigName)
                    .append("在redis缓存中的数据是：").append(instanceRuleVersion).append(",")
                    .append(ruleConfigName)
                    .append("在库里的数据是：").append(dbRuleVersion);
        }
        String title = serviceName + "(" + ipInfo + ")实例未更新到" + outdatedNames + "配置！";
        noticeService.sendToAdminAndOps(title, content.toString());
    }

    // ---------------------------------------------------------------- 库内版本

    /**
     * 读取 tb_configuration_version 为 Map&lt;configName, version&gt;。
     * 容忍 version 为 null 与 config_name 重复（后者覆盖并告警），避免一条脏数据让任务持续 FAILED。
     */
    private Map<String, String> loadConfigVersions() {
        List<ConfigurationVersionPo> rows = genericDataAccessService.queryForObjects(
                ConfigurationVersionPo.class, new ConfigurationVersionPo(), null, null);
        Map<String, String> map = new HashMap<>();
        if (rows == null) {
            return map;
        }
        for (ConfigurationVersionPo row : rows) {
            if (row == null || row.getConfigName() == null) {
                continue;
            }
            if (map.containsKey(row.getConfigName())) {
                log.warn("tb_configuration_version 存在重复 config_name={}，以后者为准", row.getConfigName());
            }
            map.put(row.getConfigName(), row.getVersion());
        }
        return map;
    }
}
/*
 * AICode End
 */

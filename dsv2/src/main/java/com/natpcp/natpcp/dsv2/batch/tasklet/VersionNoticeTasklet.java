/*
 * AICode Begin
 * VersionNoticeTasklet：versionNotice 任务 Tasklet。
 *
 * 语义与原文 TaskSchedulingNoticeJobs.versionNotice() 一致：
 *   1. checkVersionAndSendNotice()：普通实例版本比对并发送通知；
 *   2. checkGatewayForwardVersionAndSendNotice()：大网关版本比对并发送通知。
 *
 * 说明：本任务原为 1 分钟周期热更新任务，迁移后由调度中心 cron 触发；
 *       实例遍历基于 Redis SCAN，逻辑为"比对 + 插库通知"，无数据量放大。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.alibaba.fastjson.JSONObject;
import com.natpcp.natpcp.dsv2.common.BizException;
import com.natpcp.natpcp.dsv2.common.ExceptionCodeMsg;
import com.natpcp.natpcp.dsv2.common.PltResponseStatus;
import com.natpcp.natpcp.dsv2.constant.CenterEnum;
import com.natpcp.natpcp.dsv2.constant.GatewayForwardConstants;
import com.natpcp.natpcp.dsv2.constant.LocationEnum;
import com.natpcp.natpcp.dsv2.dao.po.ConfigurationVersionPo;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import com.natpcp.natpcp.dsv2.redis.manager.RedisManager;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.service.impl.GenericDataAccessService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.data.redis.core.RedisTemplate;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 版本消息通知（Tasklet）。
 */
@Slf4j
public class VersionNoticeTasklet implements Tasklet {

    private static final String INSTANCE_PREFIX = "instance";
    private static final String HOST_ADDRESS = "hostAddress";
    private static final String SERVICE_ID = "serviceId";
    private static final String VERSION = "version";
    private static final String CENTER = "center";
    private static final String PORT = "port";
    private static final String SPLIT1 = ":";
    private static final String SPLIT2 = "\\/";
    private static final String GATEWAY_NAME = "gatewayName.";

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
        checkVersionAndSendNotice();
        checkGatewayForwardVersionAndSendNotice();
        log.info("versionNotice 任务执行结束");
        return RepeatStatus.FINISHED;
    }

    private void checkVersionAndSendNotice() {
        Map<String, String> configVersionsMap = loadConfigVersions();
        Set<String> instances = redisManager.keysInPrefix(INSTANCE_PREFIX);
        NoticePO po = new NoticePO();
        if (instances != null && !instances.isEmpty()) {
            instances.forEach(instance -> {
                Object value = redisManager.get(instance);
                String instanceConfigVersionStr = value == null ? "" : value.toString();
                if (!instance.contains(instanceConfigVersionStr)) {
                    try {
                        JSONObject instanceConfigVersions = JSONObject.parseObject(instanceConfigVersionStr);
                        if (instanceConfigVersions == null) {
                            throw new BizException(ExceptionCodeMsg.TRADE_NOTICE_ERROR,
                                    "实例配置版本数据非JSON: " + instance);
                        }
                        Set<String> instanceConfigs = instanceConfigVersions.keySet();
                        for (String instanceConfig : instanceConfigs) {
                            String instanceConfigVersion = instanceConfigVersions.getString(instanceConfig);
                            String configVersion = configVersionsMap.get(instanceConfig);
                            if (!StringUtils.equals(instanceConfigVersion, configVersion)) {
                                sendNotice(po, instance, instanceConfig, instanceConfigVersion, configVersion);
                            }
                        }
                    } catch (BizException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new BizException(ExceptionCodeMsg.TRADE_NOTICE_ERROR, e.getMessage());
                    }
                }
            });
        }
    }

    /**
     * 读取 tb_configuration_version 为 Map。
     * 容忍 version 为 null（Collectors.toMap 对 null 值抛 NPE）与 config_name 重复（后者覆盖并告警），
     * 避免一条脏数据让分钟级高频任务持续 FAILED。
     */
    private Map<String, String> loadConfigVersions() {
        List<ConfigurationVersionPo> configVersions = genericDataAccessService.
                queryForObjects(ConfigurationVersionPo.class,
                        new ConfigurationVersionPo(), null, null);
        Map<String, String> map = new HashMap<>();
        if (configVersions == null) {
            return map;
        }
        for (ConfigurationVersionPo po : configVersions) {
            if (po == null || po.getConfigName() == null) {
                continue;
            }
            if (map.containsKey(po.getConfigName())) {
                log.warn("tb_configuration_version 存在重复 config_name={}，以后者为准", po.getConfigName());
            }
            map.put(po.getConfigName(), po.getVersion());
        }
        return map;
    }

    private void sendNotice(NoticePO po, String instance, String config,
                            String instanceConfigVersion, String configVersion) {
        JSONObject instanceValue = analysisRedisKey(instance);
        ConfigurableEnvironment environment = context.getEnvironment();

        String content = environment.getProperty(GATEWAY_NAME + instanceValue.getString(CENTER)) + "的" +
                environment.getProperty(GATEWAY_NAME + instanceValue.getString(SERVICE_ID)) + "服务地址: " +
                instanceValue.get(HOST_ADDRESS) + ":" + instanceValue.get(PORT) + "; " +
                "配置参数 " + config + "在redis缓存中的数据是：" + instanceConfigVersion + "," +
                config + "在库里的数据是：" + configVersion;
        po.setId(UUID.randomUUID().toString());
        po.setBusinessId("");
        po.setContent(content);
        po.setStatus("0");
        po.setTitle(environment.getProperty(GATEWAY_NAME + instanceValue.getString(SERVICE_ID)) +
                "(" + instanceValue.get(HOST_ADDRESS) + ")实例未更新到" + config + "配置！");
        po.setReceiveTellerNo("");
        po.setReceiveRole("0101");//系统管理员
        po.setTellerName("");
        po.setCreateTime(LocalDateTime.now());
        noticeService.insert(po);
        po.setId(UUID.randomUUID().toString());
        po.setReceiveRole("0104");//运维员
        noticeService.insert(po);
    }

    private void checkGatewayForwardVersionAndSendNotice() {
        log.debug("大网关消息通知");
        Map<String, String> configVersionsMap = loadConfigVersions();
        Set<String> instances = redisManager.scanKeys("*_ng:*");
        if (instances == null || instances.isEmpty()) {
            return;
        }
        instances.forEach(instance -> {
            String instanceConfigVersion = redisTemplate.opsForValue().get(instance);
            if (StringUtils.isEmpty(instanceConfigVersion)) {
                log.warn("大网关实例 {} 配置版本为空，跳过", instance);
                return;
            }
            int lastIndex = instanceConfigVersion.lastIndexOf(":");
            String versionObjAndForwardStr = instanceConfigVersion.substring(lastIndex + 1);
            String[] versionArr = versionObjAndForwardStr.split(",");

            if (versionArr.length >= 2) {
                String key = instance.split(":")[0];
                String instanceForwardObjVersion = versionArr[0];
                String instanceForwardRuleVersion = versionArr[1];

                String forwardRule = key + "_" + GatewayForwardConstants.RULE_SUFFIX;
                String forwardObjVersion = configVersionsMap.get(GatewayForwardConstants.FORWARD_OBJ_KEY);
                String forwardRuleVersion = configVersionsMap.get(forwardRule);
                if (forwardObjVersion == null || forwardRuleVersion == null) {
                    log.warn("大网关实例 {} 对应的库内版本缺失（{}={}, {}={}），跳过比对",
                            instance, GatewayForwardConstants.FORWARD_OBJ_KEY, forwardObjVersion,
                            forwardRule, forwardRuleVersion);
                    return;
                }

                if (!forwardObjVersion.equals(instanceForwardObjVersion)
                        || !forwardRuleVersion.equals(instanceForwardRuleVersion)) {
                    sendNotice(forwardObjVersion, instanceConfigVersion, instanceForwardObjVersion,
                            instanceForwardRuleVersion, forwardRule, key, forwardRuleVersion);
                }
            }
        });
    }

    private void sendNotice(String forwardObjVersion, String instanceConfigVersion, String instanceForwardObjVersion,
                            String instanceForwardRuleVersion, String forwardRule,
                            String key, String version) {
        StringBuilder titleSb = new StringBuilder();
        StringBuilder contentSb = new StringBuilder();
        NoticePO noticePo = new NoticePO();

        String[] split = key.split("_");
        String serviceName = LocationEnum.getDescByValue(split[0]) + CenterEnum.getDescByValue(split[1]) +
                GatewayForwardConstants.SERVICE_CHINESE_SUFFIX;
        String ipInfo = getIpInfo(instanceConfigVersion);

        titleSb.append(serviceName).append("(").append(ipInfo).append(")实例未更新到");
        contentSb.append(serviceName).append("(").append(ipInfo).append(")配置参数");
        if (!forwardObjVersion.equals(instanceForwardObjVersion)) {
            titleSb.append(GatewayForwardConstants.FORWARD_OBJ_KEY).append("配置！");
            contentSb.append(GatewayForwardConstants.FORWARD_OBJ_KEY).append("在redis缓存中的数据是：")
                    .append(instanceForwardObjVersion).append(",").append(GatewayForwardConstants
                            .FORWARD_OBJ_KEY).append("在库里的数据是：").append(forwardObjVersion).append(";");
        }
        if (!version.equals(instanceForwardRuleVersion)) {
            if (titleSb.toString().endsWith("！")) {
                titleSb.deleteCharAt(titleSb.indexOf("！")).append("和").append(forwardRule).append("配置！");
            } else {
                titleSb.append(forwardRule).append("配置！");
            }
            contentSb.append(forwardRule).append("在redis缓存中的数据是：").append(instanceForwardRuleVersion)
                    .append(",").append(forwardRule).append("在库里的数据是：").append(version);
        }
        noticePo.setId(UUID.randomUUID().toString());
        noticePo.setBusinessId("");
        noticePo.setContent(contentSb.toString());
        noticePo.setStatus("0");
        noticePo.setTitle(titleSb.toString());
        noticePo.setReceiveTellerNo("");
        noticePo.setReceiveRole("0101");//系统管理员
        noticePo.setTellerName("");
        noticePo.setCreateTime(LocalDateTime.now());
        noticeService.insert(noticePo);
        noticePo.setId(UUID.randomUUID().toString());
        noticePo.setReceiveRole("0104");//运维员
        noticeService.insert(noticePo);
    }

    private JSONObject analysisRedisKey(String str) {
        JSONObject json = new JSONObject();
        if (StringUtils.isNotEmpty(str) && !str.equals("null") && str.contains(SPLIT1)) {
            String splitStr = str.split(SPLIT1)[1];
            if (StringUtils.isNotEmpty(splitStr) && !splitStr.equals("null")) {
                String[] key = splitStr.split(SPLIT2);
                if (key.length < 5) {
                    throw new BizException(PltResponseStatus.PARAMETER_ERROR, "Redis中的版本实例数据异常: " + str);
                }
                json.put(SERVICE_ID, key[0]);
                json.put(VERSION, key[1]);
                json.put(HOST_ADDRESS, key[2]);
                json.put(PORT, key[3]);
                json.put(CENTER, key[4]);
                return json;
            }
        }
        throw new BizException(PltResponseStatus.PARAMETER_ERROR, "Redis中的版本实例数据异常");
    }

    private String getIpInfo(String redisKey) {
        int lastIndex = redisKey.lastIndexOf(":");
        return redisKey.substring(0, lastIndex);
    }
}
/*
 * AICode End
 */

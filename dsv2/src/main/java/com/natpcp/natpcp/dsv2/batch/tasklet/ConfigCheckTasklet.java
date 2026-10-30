/*
 * AICode Begin
 * ConfigCheckTasklet：ConfigCheck 任务 Tasklet（配置检测）。
 *
 * 语义与原文 ConfigCheckJob.execute() 一致，参数来源改为 JobParameters（jobParams JSON）：
 *   - 按 config 类型分发：LOG_LEVEL / JVM_MEMORY / JVM_THREAD / REDIS(jedis) / POSTGRES / ROCKET_MQ(jsch)
 *   - 比对配置项差异并发通知
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.alibaba.fastjson.JSON;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import com.natpcp.natpcp.dsv2.constant.ConfigEnum;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import com.natpcp.natpcp.dsv2.jobs.bo.ConfigBO;
import com.natpcp.natpcp.dsv2.jobs.bo.ConfigCheckBO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.ObjectUtils;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * 配置检测（Tasklet）。
 */
@Slf4j
public class ConfigCheckTasklet implements Tasklet {

    private final ConnectionInfoMapper connectionInfoMapper;
    private final NoticeService noticeService;

    public ConfigCheckTasklet(ConnectionInfoMapper connectionInfoMapper,
                              NoticeService noticeService) {
        this.connectionInfoMapper = connectionInfoMapper;
        this.noticeService = noticeService;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Map<String, Object> jobParams = JobParamsHelper.getParamsMap(
                chunkContext.getStepContext().getJobParameters());
        ConfigCheckBO configCheckBO = JSON.parseObject(JSON.toJSONString(jobParams), ConfigCheckBO.class);
        if (configCheckBO == null || configCheckBO.getConnectionJobId() == null) {
            throw new IllegalArgumentException("配置检测任务参数缺失，请检查任务参数配置");
        }
        ConfigEnum configEnum = ConfigEnum.getByName(configCheckBO.getConfig());
        if (configEnum == null) {
            throw new IllegalArgumentException("未知检测类型: " + configCheckBO.getConfig());
        }
        ConnectionInfoPO connectionInfoPO = connectionInfoMapper.selectById(configCheckBO.getConnectionJobId());
        if (connectionInfoPO == null) {
            throw new IllegalArgumentException("连接信息不存在 connectionJobId=" + configCheckBO.getConnectionJobId());
        }
        String password = EncoderUtil.decrypt(connectionInfoPO.getPassword());
        List<ConfigBO> configs = configCheckBO.getConfigs();
        List<String> differentList;
        switch (configEnum) {
            case LOG_LEVEL:
                LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
                ch.qos.logback.classic.Logger currentLog = loggerContext.getLogger("root");
                Level level = currentLog.getLevel();
                break;
            case JVM_MEMORY:
                break;
            case JVM_THREAD:
                break;
            case REDIS:
                HashMap<String, String> redisConfigMap = getRedisConfigMap(connectionInfoPO, password);
                differentList = new ArrayList<>();
                configs.forEach(s -> {
                    String value = redisConfigMap.get(s.getName());
                    if (ObjectUtils.isEmpty(value) || !value.equals(s.getValue())) {
                        differentList.add(s.getName() + "配置项检测值为：" + s.getValue() + ",实际值为：" + value);
                    }
                });
                if (!ObjectUtils.isEmpty(differentList)) {
                    sendNotice("Redis配置异常：" + differentList, "Redis配置异常：", connectionInfoPO);
                }
                break;
            case POSTGRES:
                Class.forName("org.postgresql.Driver");
                try (Connection connection = DriverManager.getConnection(connectionInfoPO.getUrl(),
                        connectionInfoPO.getAccount(), password);
                     Statement statement = connection.createStatement()) {
                    String sql = "select name,setting from pg_settings";
                    ResultSet resultSet = statement.executeQuery(sql);
                    HashMap<String, String> postgresConfigMap = new HashMap<>();
                    while (resultSet.next()) {
                        postgresConfigMap.put(resultSet.getString("name"), resultSet.getString("setting"));
                    }
                    differentList = new ArrayList<>();
                    configs.forEach(s -> {
                        String value = postgresConfigMap.get(s.getName());
                        if (ObjectUtils.isEmpty(value) || !value.equals(s.getValue())) {
                            differentList.add(s.getName() + "配置项检测值为：" + s.getValue() + ",实际值为：" + value);
                        }
                    });
                    if (!ObjectUtils.isEmpty(differentList)) {
                        sendNotice("Postgres配置异常：" + differentList, "Postgres配置异常：", connectionInfoPO);
                    }
                } catch (Exception e) {
                    sendNotice("数据库连接失败，请确认配置是否正确", "数据库连接失败", connectionInfoPO);
                    log.info("{}，数据库连接失败已发送通知", connectionInfoPO.getName());
                }
                break;
            case ROCKET_MQ:
                ChannelExec channelExec = null;
                Session session = null;
                try {
                    JSch jSch = new JSch();
                    session = jSch.getSession(connectionInfoPO.getAccount(), connectionInfoPO.getUrl(),
                            Integer.parseInt(connectionInfoPO.getPort()));
                    session.setPassword(password);

                    Properties properties = new Properties();
                    properties.put("StrictHostKeyChecking", "no");
                    session.setConfig(properties);

                    session.connect();

                    channelExec = (ChannelExec) session.openChannel("exec");
                    InputStream in = channelExec.getInputStream();
                    InputStream err = channelExec.getErrStream();
                    channelExec.setCommand("cat" + " " + connectionInfoPO.getPath());
                    channelExec.connect();

                    String result = getResultByInputStream(in);
                    HashMap<String, String> configMap = new HashMap<>();
                    if (result.length() > 0) {
                        getCnfigMap(result, configMap);
                    } else {
                        result = getResultByInputStream(err);
                        log.error("执行脚本获取配置文件失败：{}", result);
                    }
                    differentList = new ArrayList<>();
                    configs.forEach(s -> {
                        String value = configMap.get(s.getName());
                        if (ObjectUtils.isEmpty(value) || !value.equals(s.getValue())) {
                            differentList.add(s.getName() + "配置项检测值为：" + s.getValue() + ",实际值为：" + value);
                        }
                    });
                    if (!ObjectUtils.isEmpty(differentList)) {
                        sendNotice("RocketMQ配置异常：" + differentList, "RocketMQ配置异常：", connectionInfoPO);
                    }
                } catch (Exception e) {
                    sendNotice("RocketMQ连接失败，请确认配置是否正确", "RocketMQ连接失败", connectionInfoPO);
                    log.info("{}，RocketMQ连接失败已发送通知", connectionInfoPO.getName());
                } finally {
                    if (!ObjectUtils.isEmpty(channelExec)) {
                        channelExec.disconnect();
                    }
                    if (!ObjectUtils.isEmpty(session)) {
                        session.disconnect();
                    }
                }
                break;
            default:
                log.error("未知检测项，请检查");
        }
        return RepeatStatus.FINISHED;
    }

    private void sendNotice(String content, String x, ConnectionInfoPO connectionInfoPO) {
        NoticePO noticePo = new NoticePO();
        noticePo.setId(UUID.randomUUID().toString());
        noticePo.setBusinessId("");
        noticePo.setContent(content);
        noticePo.setStatus("0");
        noticePo.setTitle(x + connectionInfoPO.getName());
        noticePo.setReceiveTellerNo("");
        noticePo.setReceiveRole("0101");
        noticePo.setTellerName("");
        noticePo.setCreateTime(LocalDateTime.now());
        noticeService.insert(noticePo);
        noticePo.setId(UUID.randomUUID().toString());
        noticePo.setReceiveRole("0104");
        noticeService.insert(noticePo);
    }

    private static void getCnfigMap(final String result, final HashMap<String, String> configMap) {
        String[] split = result.split("\n");
        for (String s : split) {
            if (s.contains("=")) {
                String[] split1 = s.split("=", 2);
                configMap.put(split1[0].trim(), split1[1].trim());
            } else if (s.contains(":")) {
                String[] split1 = s.split(":", 2);
                configMap.put(split1[0].trim(), split1[1].trim());
            }
        }
    }

    private HashMap<String, String> getRedisConfigMap(ConnectionInfoPO connectionInfoPO, String password) {
        try {
            Jedis jedis = new Jedis(connectionInfoPO.getUrl(), Integer.parseInt(connectionInfoPO.getPort()));
            jedis.auth(password);
            List<String> x = jedis.configGet("*");
            HashMap<String, String> configMap = new HashMap<>();
            for (int i = 0; i + 1 < x.size(); i += 2) {
                configMap.put(x.get(i), x.get(i + 1));
            }
            return configMap;
        } catch (Exception e) {
            sendNotice("redis连接失败，请确认配置是否正确", "redis连接失败", connectionInfoPO);
            log.info("{}，redis连接失败已发送通知", connectionInfoPO.getName());
            throw e;
        }
    }

    private static String getResultByInputStream(final InputStream in) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(in));
        String str;
        while ((str = bufferedReader.readLine()) != null) {
            stringBuilder.append(str).append("\n");
        }
        return stringBuilder.toString();
    }
}
/*
 * AICode End
 */

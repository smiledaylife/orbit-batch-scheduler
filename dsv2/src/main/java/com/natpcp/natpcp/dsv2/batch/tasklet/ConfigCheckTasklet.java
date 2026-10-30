/*
 * AICode Begin
 * ConfigCheckTasklet：ConfigCheck 任务（外部中间件配置项巡检）。
 *
 * 迁移自 plt-mgmt ConfigCheckJob，业务语义保持不变：
 *   1. 任务参数（ConfigCheckBO）指定连接信息 id、检测类型（ConfigEnum）与期望配置项列表；
 *   2. 按类型获取目标系统的实际配置：
 *        REDIS     → Jedis CONFIG GET *
 *        POSTGRES  → select name,setting from pg_settings
 *        ROCKET_MQ → SSH 到目标机 cat 配置文件（key=value / key: value）
 *        LOG_LEVEL / JVM_MEMORY / JVM_THREAD → 原系统即未实现，仅记录日志
 *   3. 期望值与实际值不一致 → 发送"配置异常"通知；连接失败 → 发送"连接失败"通知。
 *
 * 安全：JDBC URL / SSH 路径 / 端口均来自库表录入，统一经 ConnectionInfoValidator 白名单校验。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import com.natpcp.natpcp.dsv2.constant.ConfigEnum;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import com.natpcp.natpcp.dsv2.jobs.bo.ConfigBO;
import com.natpcp.natpcp.dsv2.jobs.bo.ConfigCheckBO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.util.ConnectionInfoValidator;
import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import com.natpcp.natpcp.dsv2.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 配置检测（Tasklet）。
 */
@Slf4j
public class ConfigCheckTasklet implements Tasklet {

    /** 外部连接（SSH / Redis）超时，避免巡检卡死在不可达的目标上 */
    private static final int SSH_TIMEOUT_MS = 10_000;
    private static final int REDIS_TIMEOUT_MS = 5_000;

    private final ConnectionInfoMapper connectionInfoMapper;
    private final NoticeService noticeService;
    /** JSch StrictHostKeyChecking 取值（yes / no / ask） */
    private final String strictHostKeyChecking;

    public ConfigCheckTasklet(ConnectionInfoMapper connectionInfoMapper, NoticeService noticeService) {
        this(connectionInfoMapper, noticeService, "no");
    }

    public ConfigCheckTasklet(ConnectionInfoMapper connectionInfoMapper,
                              NoticeService noticeService,
                              String strictHostKeyChecking) {
        this.connectionInfoMapper = connectionInfoMapper;
        this.noticeService = noticeService;
        this.strictHostKeyChecking = strictHostKeyChecking == null ? "no" : strictHostKeyChecking;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Map<String, Object> params = JobParamsHelper.getParamsMap(chunkContext.getStepContext().getJobParameters());
        ConfigCheckBO bo = params.isEmpty() ? null : JsonUtil.convert(params, ConfigCheckBO.class);
        if (bo == null || bo.getConnectionJobId() == null) {
            throw new IllegalArgumentException("配置检测任务参数缺失，请检查任务参数配置");
        }
        ConfigEnum type = ConfigEnum.getByName(bo.getConfig());
        if (type == null) {
            throw new IllegalArgumentException("未知检测类型: " + bo.getConfig());
        }
        ConnectionInfoPO conn = connectionInfoMapper.selectById(bo.getConnectionJobId());
        if (conn == null) {
            throw new IllegalArgumentException("连接信息不存在 connectionJobId=" + bo.getConnectionJobId());
        }
        String password = EncoderUtil.decrypt(conn.getPassword());
        List<ConfigBO> expected = bo.getConfigs() == null ? Collections.emptyList() : bo.getConfigs();

        switch (type) {
            case REDIS:
                checkRedis(conn, password, expected);
                break;
            case POSTGRES:
                checkPostgres(conn, password, expected);
                break;
            case ROCKET_MQ:
                checkRocketMq(conn, password, expected);
                break;
            case LOG_LEVEL:
            case JVM_MEMORY:
            case JVM_THREAD:
                // 与原 plt-mgmt 实现一致：这三类检测尚未实现。显式记录，避免被误认为"检测通过"。
                log.warn("配置检测类型 {} 尚未实现，本次跳过（connection={}）", type, conn.getName());
                break;
            default:
                log.error("未支持的检测类型 {}，请检查任务参数", type);
        }
        return RepeatStatus.FINISHED;
    }

    // ---------------------------------------------------------------- 各类型检测

    /**
     * Redis：CONFIG GET * 返回 [k1, v1, k2, v2, ...]。
     * 与原实现一致：连接失败发通知后仍抛出异常（任务 FAILED）。
     */
    private void checkRedis(ConnectionInfoPO conn, String password, List<ConfigBO> expected) {
        Map<String, String> actual;
        // try-with-resources：历史实现从不 close()，每次巡检泄露一条 Redis 连接
        try (Jedis jedis = new Jedis(conn.getUrl(), ConnectionInfoValidator.requirePort(conn.getPort()),
                REDIS_TIMEOUT_MS)) {
            jedis.auth(password);
            List<String> kv = jedis.configGet("*");
            actual = new HashMap<>();
            for (int i = 0; i + 1 < kv.size(); i += 2) {
                actual.put(kv.get(i), kv.get(i + 1));
            }
        } catch (RuntimeException e) {
            log.warn("{}，redis连接失败，已发送通知: {}", conn.getName(), e.getMessage(), e);
            notify(conn, "redis连接失败", "redis连接失败，请确认配置是否正确");
            throw e;
        }
        reportDifferences(conn, "Redis配置异常：", expected, actual);
    }

    /** PostgreSQL：pg_settings 全量参数 */
    private void checkPostgres(ConnectionInfoPO conn, String password, List<ConfigBO> expected) {
        Map<String, String> actual = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(
                ConnectionInfoValidator.requireSafeJdbcUrl(conn.getUrl()), conn.getAccount(), password);
             Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("select name,setting from pg_settings")) {
            while (rs.next()) {
                actual.put(rs.getString("name"), rs.getString("setting"));
            }
        } catch (Exception e) {
            log.warn("{}，数据库连接/检测失败，已发送通知: {}", conn.getName(), e.getMessage(), e);
            notify(conn, "数据库连接失败", "数据库连接失败，请确认配置是否正确");
            return;
        }
        reportDifferences(conn, "Postgres配置异常：", expected, actual);
    }

    /** RocketMQ：SSH 到目标机读取配置文件 */
    private void checkRocketMq(ConnectionInfoPO conn, String password, List<ConfigBO> expected) {
        Map<String, String> actual;
        try {
            actual = parseKeyValueText(readRemoteFile(conn, password));
        } catch (Exception e) {
            log.warn("{}，RocketMQ连接/检测失败，已发送通知: {}", conn.getName(), e.getMessage(), e);
            notify(conn, "RocketMQ连接失败", "RocketMQ连接失败，请确认配置是否正确");
            return;
        }
        reportDifferences(conn, "RocketMQ配置异常：", expected, actual);
    }

    // ---------------------------------------------------------------- 比对与通知

    /** 期望值与实际值不一致（或实际缺失）时发送一条汇总通知 */
    private void reportDifferences(ConnectionInfoPO conn, String titlePrefix,
                                   List<ConfigBO> expected, Map<String, String> actual) {
        List<String> differences = new ArrayList<>();
        for (ConfigBO item : expected) {
            String actualValue = actual.get(item.getName());
            if (actualValue == null || actualValue.isEmpty() || !actualValue.equals(item.getValue())) {
                differences.add(item.getName() + "配置项检测值为：" + item.getValue() + ",实际值为：" + actualValue);
            }
        }
        if (!differences.isEmpty()) {
            notify(conn, titlePrefix, titlePrefix + differences);
        }
    }

    private void notify(ConnectionInfoPO conn, String titlePrefix, String content) {
        noticeService.sendToAdminAndOps(titlePrefix + conn.getName(), content);
    }

    // ---------------------------------------------------------------- SSH

    /**
     * SSH 执行 {@code cat <path>} 读取远程文件内容；stdout 为空时记录 stderr 并返回空串。
     * 路径先经白名单校验再拼入命令（防命令注入）。
     */
    private String readRemoteFile(ConnectionInfoPO conn, String password) throws Exception {
        String remotePath = ConnectionInfoValidator.requireSafeAbsolutePath(conn.getPath());
        Session session = null;
        ChannelExec channel = null;
        try {
            session = new JSch().getSession(conn.getAccount(), conn.getUrl(),
                    ConnectionInfoValidator.requirePort(conn.getPort()));
            session.setPassword(password);
            Properties config = new Properties();
            // 默认 "no" 与原实现兼容；生产配好 known_hosts 后建议通过 dsv2.ssh.strict-host-key-checking 改为 "yes"
            config.put("StrictHostKeyChecking", strictHostKeyChecking);
            session.setConfig(config);
            session.setTimeout(SSH_TIMEOUT_MS);
            session.connect(SSH_TIMEOUT_MS);

            channel = (ChannelExec) session.openChannel("exec");
            InputStream stdout = channel.getInputStream();
            InputStream stderr = channel.getErrStream();
            channel.setCommand("cat '" + remotePath + "'");
            channel.connect(SSH_TIMEOUT_MS);

            String content = readFully(stdout);
            if (content.isEmpty()) {
                log.error("执行脚本获取配置文件失败：{}", readFully(stderr));
            }
            return content;
        } finally {
            if (channel != null) {
                channel.disconnect();
            }
            if (session != null) {
                session.disconnect();
            }
        }
    }

    /** 解析 {@code key=value} 或 {@code key: value} 行（'=' 优先） */
    static Map<String, String> parseKeyValueText(String text) {
        Map<String, String> map = new HashMap<>();
        for (String line : text.split("\n")) {
            String separator = line.contains("=") ? "=" : line.contains(":") ? ":" : null;
            if (separator == null) {
                continue;
            }
            String[] kv = line.split(separator, 2);
            map.put(kv[0].trim(), kv[1].trim());
        }
        return map;
    }

    private static String readFully(InputStream in) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line).append('\n');
        }
        return sb.toString();
    }
}
/*
 * AICode End
 */

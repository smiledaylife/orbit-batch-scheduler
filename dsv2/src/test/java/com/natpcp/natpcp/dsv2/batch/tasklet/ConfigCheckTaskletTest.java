package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.util.JsonUtil;
import com.natpcp.natpcp.dsv2.batch.tasklet.ConfigCheckTasklet;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import com.natpcp.natpcp.dsv2.jobs.bo.ConfigBO;
import com.natpcp.natpcp.dsv2.jobs.bo.ConfigCheckBO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * ConfigCheckTasklet 单元测试：配置检测分发（REDIS/POSTGRES/ROCKET_MQ/LOG_LEVEL 等分支）。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("ConfigCheckTasklet 测试")
class ConfigCheckTaskletTest {

    @Mock
    private ConnectionInfoMapper connectionInfoMapper;

    @Mock
    private NoticeService noticeService;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    @Mock
    private StepContext stepContext;

    private ConfigCheckTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new ConfigCheckTasklet(connectionInfoMapper, noticeService);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
    }

    private Map<String, Object> jobParamsWith(ConfigCheckBO bo) {
        Map<String, Object> params = new HashMap<>();
        params.put(JobParamsHelper.KEY_JOB_PARAMS, JsonUtil.toJson(bo));
        return params;
    }

    private ConnectionInfoPO po() {
        ConnectionInfoPO po = new ConnectionInfoPO();
        po.setId("c1");
        po.setUrl("127.0.0.1");
        po.setAccount("redis-user");
        po.setPassword("ENC(x)");
        po.setPort("6379");
        po.setPath("/etc/rocketmq/conf/broker.conf");
        po.setName("cfg-conn");
        return po;
    }

    @DisplayName("execute: 参数缺失抛 IllegalArgumentException")
    @Test
    void execute_shouldThrow_whenParamsMissing() {
        Map<String, Object> empty = new HashMap<>();
        when(stepContext.getJobParameters()).thenReturn(empty);
        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("参数缺失");
    }

    @DisplayName("execute: connectionJobId 为空抛 IllegalArgumentException")
    @Test
    void execute_shouldThrow_whenConnectionJobIdNull() {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId(null);
        bo.setConfig("redis");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("参数缺失");
    }

    @DisplayName("execute: 未知检测类型抛 IllegalArgumentException")
    @Test
    void execute_shouldThrow_whenUnknownConfig() {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("UNKNOWN_TYPE");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("未知检测类型");
    }

    @DisplayName("execute: 连接信息不存在抛 IllegalArgumentException")
    @Test
    void execute_shouldThrow_whenConnectionInfoMissing() {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("no-such");
        bo.setConfig("redis");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        when(connectionInfoMapper.selectById("no-such")).thenReturn(null);
        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("连接信息不存在");
    }

    @DisplayName("execute: REDIS 分支配置项不同发通知")
    @Test
    void execute_shouldSendNotice_whenRedisConfigDifferent() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("redis");
        List<ConfigBO> configs = new ArrayList<>();
        configs.add(new ConfigBO("maxmemory", "1g"));
        bo.setConfigs(configs);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedConstruction<redis.clients.jedis.Jedis> mc = mockConstruction(
                     redis.clients.jedis.Jedis.class,
                     (jedis, ctx) -> {
                         when(jedis.configGet("*")).thenReturn(java.util.Arrays.asList("maxmemory", "512m"));
                         try {
                             when(jedis.auth("pwd")).thenReturn("OK");
                         } catch (Exception ignored) {
                         }
                     })) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: REDIS 分支配置项一致不发通知")
    @Test
    void execute_shouldNotNotice_whenRedisConfigSame() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("redis");
        List<ConfigBO> configs = new ArrayList<>();
        configs.add(new ConfigBO("maxmemory", "1g"));
        bo.setConfigs(configs);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedConstruction<redis.clients.jedis.Jedis> mc = mockConstruction(
                     redis.clients.jedis.Jedis.class,
                     (jedis, ctx) -> {
                         when(jedis.configGet("*")).thenReturn(java.util.Arrays.asList("maxmemory", "1g"));
                         try {
                             when(jedis.auth("pwd")).thenReturn("OK");
                         } catch (Exception ignored) {
                         }
                     })) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");

            tasklet.execute(contribution, chunkContext);

            verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: LOG_LEVEL 分支正常返回 FINISHED")
    @Test
    void execute_shouldFinish_whenLogLevel() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("logLevel");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            RepeatStatus status = tasklet.execute(contribution, chunkContext);
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        }
    }

    @DisplayName("execute: POSTGRES 分支连接失败发通知")
    @Test
    void execute_shouldSendNotice_whenPostgresConnectionFails() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("postgres");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        po.setUrl("jdbc:postgresql://127.0.0.1:5432/db");
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);
        // SQLException 构造内部会调用 DriverManager.getLogWriter，必须在 mockStatic 作用域之外构造
        java.sql.SQLException dbDown = new java.sql.SQLException("db down");

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<java.sql.DriverManager> dm = mockStatic(java.sql.DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            dm.when(() -> java.sql.DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenThrow(dbDown);

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: POSTGRES 分支 JDBC URL 含危险参数时不建连，直接发通知")
    @Test
    void execute_shouldRejectDangerousJdbcUrl_whenPostgres() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("postgres");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        po.setUrl("jdbc:postgresql://127.0.0.1:5432/db?socketFactory=evil.Factory");
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<java.sql.DriverManager> dm = mockStatic(java.sql.DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            dm.verifyNoInteractions();
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: ROCKET_MQ 路径含 shell 元字符时不建连，直接发通知")
    @Test
    void execute_shouldRejectUnsafePath_whenRocketMq() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("rocketMQ");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        po.setPath("/etc/broker.conf; rm -rf /");
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedConstruction<com.jcraft.jsch.JSch> js = mockConstruction(com.jcraft.jsch.JSch.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            assertThat(js.constructed()).isEmpty();
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: ROCKET_MQ 分支创建连接失败发通知且不抛")
    @Test
    void execute_shouldSendNotice_whenMqConnectionFails() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("rocketMQ");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedConstruction<com.jcraft.jsch.JSch> js = mockConstruction(
                     com.jcraft.jsch.JSch.class,
                     (jsch, ctx) -> {
                         try {
                             when(jsch.getSession(anyString(), anyString(), any(Integer.class)))
                                     .thenThrow(new com.jcraft.jsch.JSchException("ssh refused"));
                         } catch (Exception e) {
                             throw new RuntimeException(e);
                         }
                     })) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: JVM_MEMORY 分支返回 FINISHED")
    @Test
    void execute_shouldFinish_whenJvmMemory() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("jvmMemory");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            RepeatStatus status = tasklet.execute(contribution, chunkContext);
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        }
    }

    @DisplayName("execute: JVM_THREAD 分支返回 FINISHED")
    @Test
    void execute_shouldFinish_whenJvmThread() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("jvmThread");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            RepeatStatus status = tasklet.execute(contribution, chunkContext);
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
        }
    }

    @DisplayName("execute: REDIS 连接失败发送通知并抛异常")
    @Test
    void execute_shouldThrow_whenRedisConnectFails() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("redis");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedConstruction<redis.clients.jedis.Jedis> js = mockConstruction(
                     redis.clients.jedis.Jedis.class,
                     (jedis, ctx) -> {
                         when(jedis.auth("pwd")).thenThrow(new RuntimeException("redis down"));
                     })) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessageContaining("redis down");
            // 失败时发送 redis 连接失败通知（2 条：0101/0104）
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: POSTGRES 正常连接并比对一致不发通知")
    @Test
    void execute_shouldNoNotice_whenPostgresSame() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("postgres");
        List<ConfigBO> configs = new ArrayList<>();
        configs.add(new ConfigBO("max_connections", "100"));
        bo.setConfigs(configs);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        // Postgres 分支要求合法 JDBC URL（ConnectionInfoValidator 白名单校验）
        po.setUrl("jdbc:postgresql://127.0.0.1:5432/db");
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<java.sql.DriverManager> dm = mockStatic(java.sql.DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            java.sql.Connection conn = mock(java.sql.Connection.class);
            java.sql.Statement stmt = mock(java.sql.Statement.class);
            java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
            dm.when(() -> java.sql.DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenReturn(conn);
            when(conn.createStatement()).thenReturn(stmt);
            when(stmt.executeQuery("select name,setting from pg_settings")).thenReturn(rs);
            // 首次 next() 返回记录，第二次 false 结束循环
            when(rs.next()).thenReturn(true, false);
            when(rs.getString("name")).thenReturn("max_connections");
            when(rs.getString("setting")).thenReturn("100");

            RepeatStatus status = tasklet.execute(contribution, chunkContext);
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: ROCKET_MQ 正常输出比对配置不同发送通知")
    @Test
    void execute_shouldNotice_whenRocketMqDiff() throws Exception {
        ConfigCheckBO bo = new ConfigCheckBO();
        bo.setConnectionJobId("c1");
        bo.setConfig("rocketMQ");
        List<ConfigBO> configs = new ArrayList<>();
        configs.add(new ConfigBO("brokerClusterName", "cluster1"));
        bo.setConfigs(configs);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("c1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedConstruction<com.jcraft.jsch.JSch> js = mockConstruction(
                     com.jcraft.jsch.JSch.class,
                     (jsch, ctx) -> {
                         try {
                             com.jcraft.jsch.Session sess = mock(com.jcraft.jsch.Session.class);
                             com.jcraft.jsch.ChannelExec ch = mock(com.jcraft.jsch.ChannelExec.class);
                             java.io.ByteArrayInputStream in = new java.io.ByteArrayInputStream(
                                     "brokerClusterName=cluster2\n".getBytes());
                             when(sess.openChannel("exec")).thenReturn(ch);
                             when(ch.getInputStream()).thenReturn(in);
                             when(ch.getErrStream()).thenReturn(new java.io.ByteArrayInputStream(new byte[0]));
                             when(jsch.getSession(anyString(), anyString(), any(Integer.class))).thenReturn(sess);
                         } catch (Exception e) {
                             throw new RuntimeException(e);
                         }
                     })) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            RepeatStatus status = tasklet.execute(contribution, chunkContext);
            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }
}
/*
 * AICode End
 */

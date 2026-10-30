package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.util.JsonUtil;
import com.natpcp.natpcp.dsv2.batch.tasklet.TableCheckTasklet;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import com.natpcp.natpcp.dsv2.jobs.AdminJobClient;
import com.natpcp.natpcp.dsv2.jobs.bo.TableCheckBO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.scope.context.StepContext;
import org.springframework.batch.repeat.RepeatStatus;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * AICode Begin
 * TableCheckTasklet unit test: plain/partition table detection, notice & callback.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("TableCheckTasklet Test")
class TableCheckTaskletTest {

    private static final String HANDLER = "TableCheck";

    @Mock
    private ConnectionInfoMapper connectionInfoMapper;

    @Mock
    private NoticeService noticeService;

    @Mock
    private AdminJobClient adminJobClient;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    @Mock
    private StepContext stepContext;

    private TableCheckTasklet tasklet;

    @BeforeEach
    void setUp() {
        tasklet = new TableCheckTasklet(connectionInfoMapper, noticeService, adminJobClient);
        when(chunkContext.getStepContext()).thenReturn(stepContext);
    }

    private Map<String, Object> jobParamsWith(TableCheckBO bo) {
        Map<String, Object> params = new HashMap<>();
        params.put(JobParamsHelper.KEY_JOB_PARAMS, JsonUtil.toJson(bo));
        return params;
    }

    private ConnectionInfoPO po() {
        ConnectionInfoPO po = new ConnectionInfoPO();
        po.setId("conn1");
        po.setUrl("jdbc:postgresql://localhost:5432/db");
        po.setAccount("user");
        po.setPassword("ENC(x)");
        po.setName("conn-name");
        return po;
    }

    @DisplayName("execute: params missing -> IllegalArgumentException")
    @Test
    void execute_shouldThrow_whenParamsMissing() {
        Map<String, Object> empty = new HashMap<>();
        when(stepContext.getJobParameters()).thenReturn(empty);
        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("表检测任务参数缺失");
    }

    @DisplayName("execute: connection info not found -> IllegalArgumentException")
    @Test
    void execute_shouldThrow_whenConnectionInfoMissing() {
        TableCheckBO bo = new TableCheckBO();
        bo.setConnectionJobId("nope");
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));
        when(connectionInfoMapper.selectById("nope")).thenReturn(null);

        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("connectionJobId");
    }

    @DisplayName("execute: onlyExist=false & table missing -> notice sent")
    @Test
    void execute_shouldSendNotice_whenTableNotExists() throws Exception {
        TableCheckBO bo = new TableCheckBO();
        bo.setConnectionJobId("conn1");
        bo.setTableName("t_missing");
        bo.setOnlyExist(false);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("conn1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            Connection conn = mock(Connection.class);
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
            when(conn.getMetaData()).thenReturn(meta);
            ResultSet rs = mock(ResultSet.class);
lenient().when(rs.next()).thenReturn(false);
            lenient().when(meta.getTables(any(), any(), any(), any())).thenReturn(rs);

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: onlyExist=false & table exists -> no notice")
    @Test
    void execute_shouldNotNotice_whenTableExists() throws Exception {
        TableCheckBO bo = new TableCheckBO();
        bo.setConnectionJobId("conn1");
        bo.setTableName("t_exists");
        bo.setOnlyExist(false);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("conn1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            Connection conn = mock(Connection.class);
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
            when(conn.getMetaData()).thenReturn(meta);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getString("TABLE_NAME")).thenReturn("t_exists");
            lenient().when(meta.getTables(any(), any(), any(), any())).thenReturn(rs);

            tasklet.execute(contribution, chunkContext);

            verify(noticeService, never()).sendToAdminAndOps(anyString(), anyString());
        }
    }

    @DisplayName("execute: partition table exists -> updateJobParam called")
    @Test
    void execute_shouldUpdateLatestTableName_whenPartitionExists() throws Exception {
        TableCheckBO bo = new TableCheckBO();
        bo.setConnectionJobId("conn1");
        bo.setTableName("t_part");
        bo.setOnlyExist(true);
        bo.setNewNameRule("t_part_+yyyyMMdd");
        bo.setLatestTableName("t_part_20261013");
        bo.setCreatePeriod(1);
        bo.setCreatePeriodUnit(3); // DAY
        Map<String, Object> stepParams = jobParamsWith(bo);
        stepParams.put("handlerName", HANDLER);
        when(stepContext.getJobParameters()).thenReturn(stepParams);

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("conn1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            Connection conn = mock(Connection.class);
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
            when(conn.getMetaData()).thenReturn(meta);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true, false);
            when(rs.getString("TABLE_NAME")).thenReturn("t_part_20261014");
            lenient().when(meta.getTables(any(), any(), any(), any())).thenReturn(rs);

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
            verify(adminJobClient).updateJobParam(HANDLER, "latestTableName", "t_part_20261014");
        }
    }

    @DisplayName("execute: partition missing -> notice, no callback")
    @Test
    void execute_shouldSendNotice_whenPartitionMissing() throws Exception {
        TableCheckBO bo = new TableCheckBO();
        bo.setConnectionJobId("conn1");
        bo.setTableName("t_part");
        bo.setOnlyExist(true);
        bo.setNewNameRule("t_part_+yyyyMMdd");
        bo.setLatestTableName("t_part_20261013");
        bo.setCreatePeriod(1);
        bo.setCreatePeriodUnit(3);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("conn1")).thenReturn(po);

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            Connection conn = mock(Connection.class);
            DatabaseMetaData meta = mock(DatabaseMetaData.class);
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(conn);
            when(conn.getMetaData()).thenReturn(meta);
            ResultSet rs = mock(ResultSet.class);
lenient().when(rs.next()).thenReturn(false);
            lenient().when(meta.getTables(any(), any(), any(), any())).thenReturn(rs);

            tasklet.execute(contribution, chunkContext);

            // sendNotice 内对 insert 调用 2 次（0101 + 0104 角色）
            verify(noticeService).sendToAdminAndOps(anyString(), anyString());
            verify(adminJobClient, never()).updateJobParam(anyString(), anyString(), any());
        }
    }

    @DisplayName("execute: connection failure -> notice, no exception")
    @Test
    void execute_shouldSendDbFailNotice_whenConnectionFails() throws Exception {
        TableCheckBO bo = new TableCheckBO();
        bo.setConnectionJobId("conn1");
        bo.setTableName("t_x");
        bo.setOnlyExist(false);
        when(stepContext.getJobParameters()).thenReturn(jobParamsWith(bo));

        ConnectionInfoPO po = po();
        when(connectionInfoMapper.selectById("conn1")).thenReturn(po);
        // SQLException 构造会调用 DriverManager 静态方法，需在 mockStatic 作用域外构造
        java.sql.SQLException connRefused = new java.sql.SQLException("conn refused");

        try (MockedStatic<EncoderUtil> enc = mockStatic(EncoderUtil.class);
             MockedStatic<DriverManager> dm = mockStatic(DriverManager.class)) {
            enc.when(() -> EncoderUtil.decrypt("ENC(x)")).thenReturn("pwd");
            dm.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
                    .thenThrow(connRefused);

            RepeatStatus status = tasklet.execute(contribution, chunkContext);

            assertThat(status).isEqualTo(RepeatStatus.FINISHED);
verify(noticeService).sendToAdminAndOps(anyString(), anyString());
        }
    }
}
/*
 * AICode End
 */

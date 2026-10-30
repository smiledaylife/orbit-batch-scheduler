/*
 * AICode Begin
 * TableCheckTasklet：TableCheck 任务 Tasklet（表检测）。
 *
 * 语义与原文 TableCheckJob.execute() 一致，参数来源改为 JobParameters（jobParams JSON）：
 *   - connectionJobId 经 ConnectionInfo（加密密码）连外部库
 *   - 检测表是否存在 / 分区表按时间规则提前检测
 *   - 回写最新表名到调度中心（AdminJobClient.updateJobParam）
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.alibaba.fastjson.JSON;
import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import com.natpcp.natpcp.dsv2.constant.TimeUnitEnum;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import com.natpcp.natpcp.dsv2.dao.po.NoticePO;
import com.natpcp.natpcp.dsv2.jobs.AdminJobClient;
import com.natpcp.natpcp.dsv2.jobs.bo.TableCheckBO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.util.ConnectionInfoValidator;
import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;

/**
 * 表检测（Tasklet）。
 */
@Slf4j
public class TableCheckTasklet implements Tasklet {

    private final ConnectionInfoMapper connectionInfoMapper;
    private final NoticeService noticeService;
    private final AdminJobClient adminJobClient;

    public TableCheckTasklet(ConnectionInfoMapper connectionInfoMapper,
                             NoticeService noticeService,
                             AdminJobClient adminJobClient) {
        this.connectionInfoMapper = connectionInfoMapper;
        this.noticeService = noticeService;
        this.adminJobClient = adminJobClient;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        // Spring Batch 5: StepContext.getJobParameters() 返回 Map<String,Object>（key=jobParams）
        Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();
        Map<String, Object> params = JobParamsHelper.getParamsMap(jobParameters);
        log.info("TableCheck 参数：{}", params);
        TableCheckBO tableCheckBO = resolveTableCheckBO(params);
        if (tableCheckBO == null) {
            throw new IllegalArgumentException("表检测任务参数缺失或格式错误，请检查任务参数配置");
        }
        ConnectionInfoPO connectionInfoPO = connectionInfoMapper.selectById(tableCheckBO.getConnectionJobId());
        if (connectionInfoPO == null) {
            throw new IllegalArgumentException("连接信息不存在 connectionJobId=" + tableCheckBO.getConnectionJobId());
        }
        String password = EncoderUtil.decrypt(connectionInfoPO.getPassword());
        // JDBC URL 来自库表录入，先做白名单校验（仅 PostgreSQL、禁止 socketFactory 等危险参数）
        String jdbcUrl = ConnectionInfoValidator.requireSafeJdbcUrl(connectionInfoPO.getUrl());
        try (Connection connection = DriverManager.getConnection(jdbcUrl,
                connectionInfoPO.getAccount(), password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            String tableName = tableCheckBO.getTableName();
            if (Boolean.FALSE.equals(tableCheckBO.getOnlyExist())) {
                try (ResultSet table = metaData.getTables(null, null, tableName, null)) {
                    if (!table.next()) {
                        sendNotice(connectionInfoPO, "检测表" + tableName + "不存在，请确认配置是否正确",
                                "【巡检异常】数据库：" + connectionInfoPO.getName());
                    }
                }
            } else {
                String nextTableName = null;
                LocalDate now = LocalDate.now();
                LocalDate nextDateTime = null;
                String format = null;
                try {
                    String latestTableName = tableCheckBO.getLatestTableName();
                    String newNameRule = tableCheckBO.getNewNameRule();
                    String[] split = newNameRule.split("\\+");
                    String pattern = split[1];
                    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
                    String latestDateStr = latestTableName.substring(split[0].length());

                    LocalDate latestDate = getLocalDate(pattern, dateTimeFormatter, latestDateStr);
                    TimeUnitEnum byUnit = TimeUnitEnum.getByUnit(tableCheckBO.getCreatePeriodUnit());
                    Integer createPeriod = tableCheckBO.getCreatePeriod();
                    if (byUnit == null) {
                        throw new IllegalArgumentException("非法的时间单位: " + tableCheckBO.getCreatePeriodUnit());
                    }
                    nextDateTime = getLocalDate(latestDate, byUnit, createPeriod);

                    if (nextDateTime == null) {
                        throw new IllegalArgumentException("下次表生成日期计算为空");
                    }
                    String datePattern = pattern;
                    if (pattern.contains("HH")) {
                        datePattern = pattern.split("HH")[0];
                    }
                    DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(datePattern);
                    format = nextDateTime.format(dateFormatter);
                    nextTableName = split[0] + format;
                } catch (Exception e) {
                    log.warn("分区表 {} 命名规则/日期解析失败: {}", tableName, e.getMessage(), e);
                    sendNotice(connectionInfoPO, "检测分区表" + tableName + "日期格式有误，请确认配置是否正确",
                            "【巡检异常】数据库：" + connectionInfoPO.getName());
                }
                if (nextDateTime != null && now.plusDays(4).isAfter(nextDateTime)) {
                    try (ResultSet table = metaData.getTables(null, null, nextTableName, null)) {
                        if (!table.next()) {
                            sendNotice(connectionInfoPO, "检测分区表" + tableName + "不存在" + format + "日期，请确认配置是否正确",
                                    "【巡检异常】数据库：" + connectionInfoPO.getName());
                        } else {
                            final String table_name = table.getString("TABLE_NAME");
                            tableCheckBO.setLatestTableName(table_name);
                            Object handlerName = chunkContext.getStepContext().getJobParameters().get("handlerName");
                            adminJobClient.updateJobParam(String.valueOf(handlerName), "latestTableName", table_name);
                            log.info("最新表名更新：{}", table_name);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("{}，数据库连接/元数据查询失败，已发送通知: {}", connectionInfoPO.getName(), e.getMessage(), e);
            sendNotice(connectionInfoPO, "数据库连接失败，请确认配置是否正确",
                    "【巡检异常】数据库：" + connectionInfoPO.getName());
        }
        return RepeatStatus.FINISHED;
    }

    private TableCheckBO resolveTableCheckBO(Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return null;
        }
        return JSON.parseObject(JSON.toJSONString(params), TableCheckBO.class);
    }

    private void sendNotice(ConnectionInfoPO connectionInfoPO, String content, String title) {
        NoticePO noticePo = new NoticePO();
        noticePo.setId(UUID.randomUUID().toString());
        noticePo.setBusinessId("");
        noticePo.setContent(content);
        noticePo.setStatus("0");
        noticePo.setTitle(title);
        noticePo.setReceiveTellerNo("");
        noticePo.setReceiveRole("0101");
        noticePo.setTellerName("");
        noticePo.setCreateTime(LocalDateTime.now());
        noticeService.insert(noticePo);
        noticePo.setId(UUID.randomUUID().toString());
        noticePo.setReceiveRole("0104");
        noticeService.insert(noticePo);
    }

    private static LocalDate getLocalDate(LocalDate latestDate, TimeUnitEnum byUnit, Integer createPeriod) {
        LocalDate nextDateTime = null;
        switch (byUnit) {
            case DAY:
                nextDateTime = latestDate.plusDays(createPeriod);
                break;
            case WEEK:
                nextDateTime = latestDate.plusWeeks(createPeriod).with(DayOfWeek.MONDAY);
                break;
            case MONTH:
                nextDateTime = latestDate.plusMonths(createPeriod).withDayOfMonth(1);
                break;
            case YEAR:
                nextDateTime = latestDate.plusYears(createPeriod).withDayOfYear(1);
                break;
            default:
        }
        return nextDateTime;
    }

    private static LocalDate getLocalDate(String pattern, DateTimeFormatter dateTimeFormatter, String latestDateStr) {
        LocalDate latestDate;
        try {
            latestDate = LocalDateTime.parse(latestDateStr, dateTimeFormatter).toLocalDate();
        } catch (DateTimeException e) {
            try {
                latestDate = LocalDate.parse(latestDateStr, dateTimeFormatter);
            } catch (DateTimeException e1) {
                int year = Integer.parseInt(latestDateStr.substring(pattern.indexOf("y"),
                        pattern.lastIndexOf("y") + 1));
                if (pattern.contains("y") && pattern.contains("M")) {
                    int month = Integer.parseInt(latestDateStr.substring(pattern.indexOf("M"), pattern.lastIndexOf("M") + 1));
                    latestDate = LocalDate.of(year, month, 1);
                } else {
                    latestDate = LocalDate.of(year, 1, 1);
                }
            }
        }
        return latestDate;
    }
}
/*
 * AICode End
 */

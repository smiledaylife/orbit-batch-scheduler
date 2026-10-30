/*
 * AICode Begin
 * TableCheckTasklet：TableCheck 任务（外部数据库表巡检）。
 *
 * 迁移自 plt-mgmt TableCheckJob，业务语义保持不变：
 *   1. 任务参数（TableCheckBO，由调度中心下发、经 JobParameters.jobParams 传入）指定连接信息 id 与表名；
 *   2. 用 tb_connection_info 中的 JDBC URL / 账号 / 加密密码直连目标库；
 *   3. onlyExist=false：仅检测表是否存在；
 *      onlyExist=true/null：按命名规则（前缀+日期格式）与建表周期推算"下一张分区表"，
 *      若其建表日期已进入 4 天预警窗口则检测是否已存在，存在时把最新表名回写调度中心；
 *   4. 任一检测失败发送巡检异常通知（系统管理员 + 运维员）。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.batch.util.JobParamsHelper;
import com.natpcp.natpcp.dsv2.constant.TimeUnitEnum;
import com.natpcp.natpcp.dsv2.dao.mapper.ConnectionInfoMapper;
import com.natpcp.natpcp.dsv2.dao.po.ConnectionInfoPO;
import com.natpcp.natpcp.dsv2.jobs.AdminJobClient;
import com.natpcp.natpcp.dsv2.jobs.bo.TableCheckBO;
import com.natpcp.natpcp.dsv2.service.NoticeService;
import com.natpcp.natpcp.dsv2.util.ConnectionInfoValidator;
import com.natpcp.natpcp.dsv2.util.EncoderUtil;
import com.natpcp.natpcp.dsv2.util.JsonUtil;
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
import java.util.Locale;
import java.util.Map;

/**
 * 表检测（Tasklet）。
 */
@Slf4j
public class TableCheckTasklet implements Tasklet {

    /** 分区表预警窗口：下一张分区表建表日期距今不足该天数时开始检测 */
    private static final int PARTITION_WARN_DAYS = 4;

    /** 回写到调度中心任务参数中的键 */
    private static final String PARAM_LATEST_TABLE_NAME = "latestTableName";

    private static final String NOTICE_TITLE_PREFIX = "【巡检异常】数据库：";

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
        Map<String, Object> jobParameters = chunkContext.getStepContext().getJobParameters();
        Map<String, Object> params = JobParamsHelper.getParamsMap(jobParameters);
        log.info("TableCheck 参数：{}", params);

        TableCheckBO bo = params.isEmpty() ? null : JsonUtil.convert(params, TableCheckBO.class);
        if (bo == null || bo.getConnectionJobId() == null) {
            throw new IllegalArgumentException("表检测任务参数缺失或格式错误，请检查任务参数配置");
        }
        ConnectionInfoPO conn = connectionInfoMapper.selectById(bo.getConnectionJobId());
        if (conn == null) {
            throw new IllegalArgumentException("连接信息不存在 connectionJobId=" + bo.getConnectionJobId());
        }
        if (bo.getTableName() == null) {
            throw new IllegalArgumentException("表检测任务参数缺少 tableName，请检查任务参数配置");
        }

        // JDBC URL 来自库表录入，先做白名单校验（仅 PostgreSQL、禁止 socketFactory 等危险参数）
        String jdbcUrl = ConnectionInfoValidator.requireSafeJdbcUrl(conn.getUrl());
        String password = EncoderUtil.decrypt(conn.getPassword());
        try (Connection connection = DriverManager.getConnection(jdbcUrl, conn.getAccount(), password)) {
            DatabaseMetaData metaData = connection.getMetaData();
            if (Boolean.FALSE.equals(bo.getOnlyExist())) {
                checkTableExists(metaData, conn, bo.getTableName());
            } else {
                checkNextPartitionTable(metaData, conn, bo, String.valueOf(jobParameters.get("handlerName")));
            }
        } catch (SQLException e) {
            log.warn("{}，数据库连接/元数据查询失败，已发送通知: {}", conn.getName(), e.getMessage(), e);
            notify(conn, "数据库连接失败，请确认配置是否正确");
        }
        return RepeatStatus.FINISHED;
    }

    // ---------------------------------------------------------------- 检测分支

    /** 普通表：仅检测存在性 */
    private void checkTableExists(DatabaseMetaData metaData, ConnectionInfoPO conn, String tableName)
            throws SQLException {
        if (findTable(metaData, tableName) == null) {
            notify(conn, "检测表" + tableName + "不存在，请确认配置是否正确");
        }
    }

    /**
     * 分区表：根据最新已存在表名推算下一张表，进入预警窗口后检测其是否已创建。
     * 推算失败（命名规则/日期格式错误）发通知后结束，不中断任务。
     */
    private void checkNextPartitionTable(DatabaseMetaData metaData, ConnectionInfoPO conn,
                                         TableCheckBO bo, String handlerName) throws SQLException {
        NextPartition next;
        try {
            next = resolveNextPartition(bo);
        } catch (RuntimeException e) {
            log.warn("分区表 {} 命名规则/日期解析失败: {}", bo.getTableName(), e.getMessage(), e);
            notify(conn, "检测分区表" + bo.getTableName() + "日期格式有误，请确认配置是否正确");
            return;
        }
        if (!LocalDate.now().plusDays(PARTITION_WARN_DAYS).isAfter(next.date)) {
            log.info("分区表 {} 下一张表 {} 尚未进入预警窗口", bo.getTableName(), next.tableName);
            return;
        }
        String actualTableName = findTable(metaData, next.tableName);
        if (actualTableName == null) {
            notify(conn, "检测分区表" + bo.getTableName() + "不存在" + next.dateText + "日期，请确认配置是否正确");
            return;
        }
        // 以数据库元数据返回的真实表名（大小写）回写
        adminJobClient.updateJobParam(handlerName, PARAM_LATEST_TABLE_NAME, actualTableName);
        log.info("最新表名更新：{}", actualTableName);
    }

    // ---------------------------------------------------------------- 分区表推算

    /** 下一张分区表的推算结果 */
    private static final class NextPartition {
        final String tableName;
        final LocalDate date;
        final String dateText;

        NextPartition(String tableName, LocalDate date, String dateText) {
            this.tableName = tableName;
            this.date = date;
            this.dateText = dateText;
        }
    }

    /**
     * 命名规则形如 {@code prefix_+yyyyMM}：'+' 前为前缀，后为日期格式。
     * 最新表名去掉前缀即为最新日期，按周期单位推进得到下一张表日期。
     */
    private static NextPartition resolveNextPartition(TableCheckBO bo) {
        String[] rule = bo.getNewNameRule().split("\\+", 2);
        if (rule.length != 2) {
            throw new IllegalArgumentException("命名规则缺少 '+' 分隔的日期格式: " + bo.getNewNameRule());
        }
        String prefix = rule[0];
        String pattern = rule[1];
        String latestDateText = bo.getLatestTableName().substring(prefix.length());
        LocalDate latestDate = parseLatestDate(pattern, latestDateText);

        TimeUnitEnum unit = TimeUnitEnum.getByUnit(bo.getCreatePeriodUnit());
        if (unit == null) {
            throw new IllegalArgumentException("非法的时间单位: " + bo.getCreatePeriodUnit());
        }
        LocalDate nextDate = advance(latestDate, unit, bo.getCreatePeriod());
        if (nextDate == null) {
            throw new IllegalArgumentException("不支持的建表周期单位: " + unit);
        }
        // 表名中的日期只取到"日"级（规则含时分时截掉 HH 及其后部分）
        String datePattern = pattern.contains("HH") ? pattern.split("HH")[0] : pattern;
        String dateText = nextDate.format(DateTimeFormatter.ofPattern(datePattern));
        return new NextPartition(prefix + dateText, nextDate, dateText);
    }

    private static LocalDate advance(LocalDate from, TimeUnitEnum unit, int period) {
        switch (unit) {
            case DAY:
                return from.plusDays(period);
            case WEEK:
                return from.plusWeeks(period).with(DayOfWeek.MONDAY);
            case MONTH:
                return from.plusMonths(period).withDayOfMonth(1);
            case YEAR:
                return from.plusYears(period).withDayOfYear(1);
            default:
                return null;
        }
    }

    /**
     * 解析最新表名中的日期：依次尝试 LocalDateTime、LocalDate；
     * 格式只有年/年月（如 yyyyMM）时退化为手工截取年、月。
     */
    private static LocalDate parseLatestDate(String pattern, String dateText) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        try {
            return LocalDateTime.parse(dateText, formatter).toLocalDate();
        } catch (DateTimeException ignore) {
            // 继续尝试
        }
        try {
            return LocalDate.parse(dateText, formatter);
        } catch (DateTimeException ignore) {
            // 继续尝试
        }
        int year = Integer.parseInt(dateText.substring(pattern.indexOf('y'), pattern.lastIndexOf('y') + 1));
        if (pattern.contains("M")) {
            int month = Integer.parseInt(dateText.substring(pattern.indexOf('M'), pattern.lastIndexOf('M') + 1));
            return LocalDate.of(year, month, 1);
        }
        return LocalDate.of(year, 1, 1);
    }

    // ---------------------------------------------------------------- 工具

    /**
     * 按名称查表，返回数据库元数据中的真实表名；不存在返回 null。
     * <p>
     * {@link DatabaseMetaData#getTables} 对表名大小写敏感，而各数据库对未加引号标识符的存储习惯不同
     * （PostgreSQL 转小写、Oracle 转大写、MySQL 保持原样）。为避免运维填参大小写与库内不一致导致误报，
     * 先按数据库声明的存储习惯归一化表名；若仍未命中，再以原始写法兜底查询一次。
     */
    private static String findTable(DatabaseMetaData metaData, String tableName) throws SQLException {
        String normalized = normalizeIdentifier(metaData, tableName);
        String found = queryTable(metaData, normalized);
        if (found == null && !normalized.equals(tableName)) {
            found = queryTable(metaData, tableName);
        }
        return found;
    }

    /** 依据 storesLowerCaseIdentifiers / storesUpperCaseIdentifiers 归一化未加引号标识符的大小写 */
    static String normalizeIdentifier(DatabaseMetaData metaData, String identifier) throws SQLException {
        if (metaData.storesLowerCaseIdentifiers()) {
            return identifier.toLowerCase(Locale.ROOT);
        }
        if (metaData.storesUpperCaseIdentifiers()) {
            return identifier.toUpperCase(Locale.ROOT);
        }
        return identifier;
    }

    private static String queryTable(DatabaseMetaData metaData, String tableName) throws SQLException {
        try (ResultSet rs = metaData.getTables(null, null, tableName, null)) {
            return rs.next() ? rs.getString("TABLE_NAME") : null;
        }
    }

    private void notify(ConnectionInfoPO conn, String content) {
        noticeService.sendToAdminAndOps(NOTICE_TITLE_PREFIX + conn.getName(), content);
    }
}
/*
 * AICode End
 */

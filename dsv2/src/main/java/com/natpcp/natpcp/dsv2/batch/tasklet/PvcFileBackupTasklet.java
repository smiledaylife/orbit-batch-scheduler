/*
 * AICode Begin
 * PvcFileBackupTasklet：pvcFileBackup 任务 Tasklet。
 *
 * 语义与原文 handler 一致：
 *   1. backupPvcFile() 失败发监控（无监控 agent，手动推 Kafka monitor_common_bus 主题）并抛出异常
 *      （中断 Job，触发重启/告警）；
 *   2. 成功后才执行 deleteFilePvcTow() 清理。
 */
package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.alibaba.fastjson.JSON;
import com.natpcp.natpcp.dsv2.common.MessageDTO;
import com.natpcp.natpcp.dsv2.common.MonitorCommonTargetErrDTO;
import com.natpcp.natpcp.dsv2.common.ServiceTool;
import com.natpcp.natpcp.dsv2.service.PvcService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * pvc 文件备份与清理（Tasklet）。
 */
@Slf4j
public class PvcFileBackupTasklet implements Tasklet {

    /** 监控目标名称（与旧 MonitorUtil 上报一致）。 */
    private static final String MONITOR_TARGET_NAME = "管理端定时并备份失败";

    private final PvcService pvcService;
    private final KafkaProducer<String, String> kafkaProducer;

    /** 监控消息推送主题（无监控 agent，手动直推 Kafka，由配置 spring.kafka.topic 注入）。 */
    private final String monitorTopic;

    public PvcFileBackupTasklet(PvcService pvcService, KafkaProducer<String, String> kafkaProducer,
                                String dataCenter, String monitorTopic) {
        this.pvcService = pvcService;
        this.kafkaProducer = kafkaProducer;
        this.monitorTopic = monitorTopic;
        // 无 agent 环境：由配置中心标志初始化 ServiceTool（幂等，重复调用无副作用）
        ServiceTool.initCenterFlag(dataCenter);
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        log.info("pvcFileBackup 任务执行开始");
        try {
            pvcService.backupPvcFile();
        } catch (Exception e) {
            String errorMsg = e.getMessage();
            log.error("pvcFileBackup 备份失败：{}", errorMsg, e);
            pushMonitorErr(errorMsg, MONITOR_TARGET_NAME, UUID.randomUUID().toString());
            throw new RuntimeException(e.getMessage());
        }
        pvcService.deleteFilePvcTow();
        log.info("pvcFileBackup 任务执行结束");
        return RepeatStatus.FINISHED;
    }

    /**
     * 手动推送监控告警消息到 Kafka {@code monitorTopic}（默认 monitor_common_bus）主题。
     * 消息结构：MessageDTO 信封 + MonitorCommonTargetErrDTO 业务体。
     * 推送失败仅记录日志，不阻断主流程异常抛出。
     *
     * @param errMsg     告警信息
     * @param targetName 事件名称
     * @param targetCode 事件编码
     */
    private void pushMonitorErr(String errMsg, String targetName, String targetCode) {
        try {
            MonitorCommonTargetErrDTO dto = new MonitorCommonTargetErrDTO();
            dto.setTargetCode(targetCode);
            dto.setTargetName(targetName);
            dto.setCenterFlag(ServiceTool.getCenterFlag());
            dto.setSubitemCode(targetCode);
            dto.setClrDate(LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE));
            dto.setCollectDateTime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")));
            dto.setErrMsg(errMsg);
            dto.setAlarmStatus(true);

            MessageDTO<MonitorCommonTargetErrDTO> message =
                    new MessageDTO<>(MonitorCommonTargetErrDTO.TABLE_NAME, dto);
            kafkaProducer.send(new ProducerRecord<>(monitorTopic, JSON.toJSONString(message)));
            log.info("监控消息已推送 Kafka topic={}, targetCode={}", monitorTopic, targetCode);
        } catch (Exception ex) {
            log.error("推送监控消息到 Kafka topic={} 失败", monitorTopic, ex);
        }
    }
}

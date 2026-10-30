package com.natpcp.natpcp.dsv2.batch.tasklet;

import com.natpcp.natpcp.dsv2.batch.tasklet.PvcFileBackupTasklet;
import com.natpcp.natpcp.dsv2.service.PvcService;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * AICode Begin
 * PvcFileBackupTasklet 单元测试。
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("PvcFileBackupTasklet 测试")
class PvcFileBackupTaskletTest {

    @Mock
    private PvcService pvcService;

    @Mock
    private KafkaProducer<String, String> kafkaProducer;

    @Mock
    private StepContribution contribution;

    @Mock
    private ChunkContext chunkContext;

    private PvcFileBackupTasklet tasklet;

    /** 测试注入的监控主题（模拟 spring.kafka.topic 配置）。 */
    private static final String MONITOR_TOPIC = "monitor_common_bus";

    @BeforeEach
    void setUp() {
        tasklet = new PvcFileBackupTasklet(pvcService, kafkaProducer, "H", MONITOR_TOPIC);
    }

    @DisplayName("execute: 备份成功后执行清理并返回 FINISHED，不推送监控")
    @Test
    void execute_shouldBackupThenCleanup_whenSuccess() {
        RepeatStatus status = tasklet.execute(contribution, chunkContext);

        verify(pvcService).backupPvcFile();
        verify(pvcService).deleteFilePvcTow();
        verify(kafkaProducer, never()).send(any(ProducerRecord.class));
        assertThat(status).isEqualTo(RepeatStatus.FINISHED);
    }

    @DisplayName("execute: 备份失败时推送监控到 Kafka 并抛 RuntimeException，不执行清理")
    @Test
    void execute_shouldPushMonitorAndThrow_whenBackupFails() {
        doThrow(new RuntimeException("backup disk full")).when(pvcService).backupPvcFile();

        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("backup disk full");
        verify(kafkaProducer).send(any(ProducerRecord.class));
        verify(pvcService, never()).deleteFilePvcTow();
    }

    @DisplayName("execute: 推送的 ProducerRecord 使用配置注入的主题")
    @Test
    void execute_shouldUseConfiguredTopic_whenBackupFails() {
        doThrow(new RuntimeException("backup disk full")).when(pvcService).backupPvcFile();

        assertThatThrownBy(() -> tasklet.execute(contribution, chunkContext))
                .isInstanceOf(RuntimeException.class);
        org.mockito.ArgumentCaptor<ProducerRecord<String, String>> captor =
                org.mockito.ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaProducer).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(MONITOR_TOPIC);
    }
}
/*
 * AICode End
 */

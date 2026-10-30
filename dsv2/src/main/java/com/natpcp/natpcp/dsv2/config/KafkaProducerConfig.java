/*
 * AICode Begin
 * KafkaProducerConfig：轻量 Kafka 生产者配置（原生 kafka-clients，无 spring-kafka）。
 *
 * 用途：无监控 agent 环境下，业务手动推送监控消息到 Kafka（monitor_common_bus 主题）。
 * 配置键：spring.kafka.bootstrap-servers（与 dsv2-application-dev.yml 对齐）。
 */
package com.natpcp.natpcp.dsv2.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * Kafka 生产者配置：提供线程安全的 {@link KafkaProducer} Bean（String/String）。
 */
@Configuration
public class KafkaProducerConfig {

    /** Kafka 地址：必须由外部配置（ConfigMap）提供，不在代码中内置任何环境地址。 */
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /** 单条消息投递总超时（含重试），避免 broker 不可达时阻塞业务线程过久。 */
    @Value("${spring.kafka.producer.delivery-timeout-ms:15000}")
    private int deliveryTimeoutMs;

    /** send() 元数据获取/缓冲区满时的最大阻塞时间。 */
    @Value("${spring.kafka.producer.max-block-ms:5000}")
    private int maxBlockMs;

    /**
     * 单例 Kafka 生产者。kafka-clients 的 Producer 线程安全，进程内全局共享一个即可。
     *
     * @return KafkaProducer（String key / String value）
     */
    @Bean(destroyMethod = "close")
    public KafkaProducer<String, String> kafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 可靠投递：acks=all + 重试 3 次，保证监控消息尽量不丢
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, Math.min(deliveryTimeoutMs, 10000));
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        return new KafkaProducer<>(props);
    }
}
/*
 * AICode End
 */

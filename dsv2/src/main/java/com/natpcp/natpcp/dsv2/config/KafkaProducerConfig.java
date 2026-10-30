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

    /** 默认 Kafka 地址（与本工程 dev 环境地址一致，可被配置覆盖）。 */
    static final String DEFAULT_BOOTSTRAP_SERVERS = "20.240.137.55:9092";

    @Value("${spring.kafka.bootstrap-servers:" + DEFAULT_BOOTSTRAP_SERVERS + "}")
    private String bootstrapServers;

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
        return new KafkaProducer<>(props);
    }
}
/*
 * AICode End
 */

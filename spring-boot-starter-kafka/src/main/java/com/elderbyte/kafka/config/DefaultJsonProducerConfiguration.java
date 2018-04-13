package com.elderbyte.kafka.config;

import com.elderbyte.kafka.serialisation.SpringKafkaJsonSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ConditionalOnProperty(value = "kafka.client.producer.enabled", havingValue = "true", matchIfMissing = true)
public class DefaultJsonProducerConfiguration {

    @Autowired
    private KafkaClientConfig config;

    @Autowired
    private SpringKafkaJsonSerializer springKafkaJsonSerializer;

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(producerConfigs());
        factory.setKeySerializer(new StringSerializer());
        factory.setValueSerializer(springKafkaJsonSerializer);
        config.getProducerTransactionId().ifPresent(factory::setTransactionIdPrefix);
        return factory;
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    private Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        // See https://kafka.apache.org/documentation/#producerconfigs for more properties
        return props;
    }

}

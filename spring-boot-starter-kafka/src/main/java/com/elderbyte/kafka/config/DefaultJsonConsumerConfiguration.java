package com.elderbyte.kafka.config;

import com.elderbyte.kafka.serialisation.Json;
import com.elderbyte.kafka.serialisation.SpringKafkaJsonDeserializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class DefaultJsonConsumerConfiguration {

    @Autowired
    private KafkaClientConfig config;

    @Autowired
    private SpringKafkaJsonDeserializer springKafkaJsonDeserializer;

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<String, Json>>
    kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Json> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setConcurrency(3);
        factory.getContainerProperties().setPollTimeout(3000);
        return factory;
    }

    @Bean
    public ConsumerFactory<String, Json> consumerFactory() {
        DefaultKafkaConsumerFactory<String, Json> factory = new DefaultKafkaConsumerFactory<>(consumerConfigs());
        factory.setKeyDeserializer(new StringDeserializer());
        factory.setValueDeserializer(springKafkaJsonDeserializer);

        return factory;
    }

    @Bean
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaServers());
        return props;
    }
}

package com.elderbyte.kafka.streams;

import com.elderbyte.kafka.config.KafkaClientProperties;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextFactoryImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configure Kafka Streams Support
 *
 * (Only when Kafka Streams is on the class path)
 */
@Configuration
@ConditionalOnClass(KafkaStreams.class)
public class ElderKafkaStreamsConfiguration {

    @Autowired
    private KafkaClientProperties properties;

    @Autowired
    private ObjectMapper mapper;

    @Bean
    public KafkaStreamsContextBuilderFactory kafkaStreamsBuilderFactory(){
        return new KafkaStreamsContextFactoryImpl(properties, mapper);
    }

}

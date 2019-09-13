package com.elderbyte.kafka.streams;

import com.elderbyte.kafka.config.KafkaClientProperties;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextBuilderFactory;
import com.elderbyte.kafka.streams.factory.KafkaStreamsContextFactoryImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
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
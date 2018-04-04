package com.elderbyte.kafka.config;

import org.springframework.beans.factory.annotation.Value;


public class KafkaClientConfig {

    @Value("${kafka.client.servers}")
    private String kafkaServers;


    public String getKafkaServers(){
        return kafkaServers;
    }
}

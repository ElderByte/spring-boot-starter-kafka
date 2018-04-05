package com.elderbyte.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import java.util.Optional;

public class KafkaClientConfig {

    @Value("${kafka.client.enabled:true}")
    private boolean kafkaEnabled;

    @Value("${kafka.client.servers}")
    private String kafkaServers;

    @Value("${kafka.client.consumer.concurrency:}")
    private Integer consumerConcurrency;

    @Value("${kafka.client.consumer.pollTimeout:}")
    private Integer consumerPollTimeout;

    @Value("${kafka.client.consumer.enableBatch:false}")
    private boolean consumerEnableBatch;

    @Value("${kafka.client.consumer.enableBatch:}")
    private Integer consumerMaxPollRecords;


    public String getKafkaServers(){
        return kafkaServers;
    }

    public boolean isKafkaEnabled(){
        return kafkaEnabled;
    }

    public Optional<Integer> getConsumerConcurrency() {
        return Optional.ofNullable(consumerConcurrency);
    }

    public Optional<Integer> getConsumerPollTimeout() {
        return Optional.ofNullable(consumerPollTimeout);
    }

    public Optional<Integer> getConsumerMaxPollRecords() {
        return Optional.ofNullable(consumerMaxPollRecords);
    }

    public boolean isConsumerEnableBatch() {
        return consumerEnableBatch;
    }
}

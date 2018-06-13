package com.elderbyte.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import java.util.Optional;

public class KafkaClientConfig {

    @Value("${kafka.client.enabled:true}")
    private boolean kafkaEnabled;

    @Value("${kafka.client.servers:}")
    private String kafkaServers;

    @Value("${kafka.client.consumer.concurrency:}")
    private Integer consumerConcurrency;

    @Value("${kafka.client.consumer.pollTimeout:}")
    private Integer consumerPollTimeout;

    @Value("${kafka.client.consumer.maxPollRecords:}")
    private Integer consumerMaxPollRecords;

    @Value("${kafka.client.consumer.autoCommit:true}")
    private boolean consumerAutoCommit;

    @Value("${kafka.client.consumer.autoOffsetReset:earliest}")
    private String consumerAutoOffsetReset;

    @Value("${kafka.client.producer.transaction.id:}")
    private String producerTransactionId;


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

    public boolean isConsumerAutoCommit() {
        return consumerAutoCommit;
    }

    public String getConsumerAutoOffsetReset(){
        return consumerAutoOffsetReset;
    }


    public Optional<String> getProducerTransactionId() {
        return Optional.ofNullable(producerTransactionId);
    }
}

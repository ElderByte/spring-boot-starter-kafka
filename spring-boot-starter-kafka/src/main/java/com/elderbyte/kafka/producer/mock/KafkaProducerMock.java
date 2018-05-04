package com.elderbyte.kafka.producer.mock;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.SendResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class KafkaProducerMock<K,V> implements KafkaProducer<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final Logger logger = LoggerFactory.getLogger(getClass());

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/


    @Override
    public CompletableFuture<SendResult<K, V>> send(String topic, K key, V data) {
        logger.debug("Mocking Kafka Send! topic: {}, key: {}", topic, key);
        return CompletableFuture.completedFuture(mockResult(topic, key, data));
    }

    @Override
    public CompletableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data) {
        logger.debug("Mocking Kafka Send! topic: {}, key: {}, partition: {}", topic, key, partition);
        return CompletableFuture.completedFuture(mockResult(topic, key, data));
    }

    @Override
    public CompletableFuture<SendResult<K, V>> send(String topic, KafkaMessage<K, V> message) {
        logger.debug("Mocking Kafka Send! topic: {}, message: {}", topic, message);
        return CompletableFuture.completedFuture(mockResult(topic, message.getKey(), message.getValue()));
    }

    @Override
    public void sendAll(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        logger.debug("Mocking Kafka Send! topic: {}, messages: {}", topic, kafkaMessages.size());
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return new ArrayList<>();
    }

    @Override
    public void flush() {
        // NOP
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/


    private SendResult<K,V> mockResult(String topic, K key, V data){
        return new SendResult<>(
                new ProducerRecord<>(topic, key, data),
                mockMetadata(topic));
    }

    private RecordMetadata mockMetadata(String topic){
        return new RecordMetadata(
                new TopicPartition(topic, 1),
                0,
                0,
                0,
                0,
                0,
                0
        );
    }

}

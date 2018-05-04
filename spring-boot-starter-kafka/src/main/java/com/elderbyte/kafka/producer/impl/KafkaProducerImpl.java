package com.elderbyte.kafka.producer.impl;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

public class KafkaProducerImpl<K,V> implements KafkaProducer<K,V> {

    /***************************************************************************
     *                                                                         *
     * Fields                                                                  *
     *                                                                         *
     **************************************************************************/

    private final KafkaOperations<K,V> kafkaOperations;

    /***************************************************************************
     *                                                                         *
     * Constructors                                                            *
     *                                                                         *
     **************************************************************************/

    public KafkaProducerImpl(KafkaOperations<K,V> kafkaOperations){
        if(kafkaOperations == null) throw new IllegalArgumentException("kafkaOperations");
        this.kafkaOperations = kafkaOperations;
    }

    /***************************************************************************
     *                                                                         *
     * Public API                                                              *
     *                                                                         *
     **************************************************************************/

    @Override
    public CompletableFuture<SendResult<K, V>> send(String topic, K key, V data) {
        return kafkaOperations.send(topic, key, data).completable();
    }

    @Override
    public CompletableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data) {
        return kafkaOperations.send(topic, partition, key, data).completable();
    }

    @Override
    public CompletableFuture<SendResult<K, V>> send(String topic, KafkaMessage<K, V> message) {
        return kafkaOperations.send(message.toRecord(topic)).completable();
    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return kafkaOperations.partitionsFor(topic);
    }

    @Override
    public void flush() {
        kafkaOperations.flush();
    }

    @Override
    public List<CompletableFuture<SendResult<K, V>>> sendAll(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        return kafkaMessages.stream()
                .map(m -> send(topic, m))
                .collect(toList());
    }

    /***************************************************************************
     *                                                                         *
     * Internal methods                                                        *
     *                                                                         *
     **************************************************************************/

    KafkaOperations<K,V> getKafkaOperations(){
        return kafkaOperations;
    }

}

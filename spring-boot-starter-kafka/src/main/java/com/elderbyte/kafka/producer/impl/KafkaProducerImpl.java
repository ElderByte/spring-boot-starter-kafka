package com.elderbyte.kafka.producer.impl;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;

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
    public ListenableFuture<SendResult<K, V>> send(String topic, K key, V data) {
        return kafkaOperations.send(topic, key, data);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data) {
        return kafkaOperations.send(topic, partition, key, data);
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, KafkaMessage<K, V> message) {
        return kafkaOperations.send(message.toRecord(topic));
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
    public void sendAll(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {
        kafkaMessages.forEach(m -> send(topic, m));
    }

    /***************************************************************************
     *                                                                         *
     * Private methods                                                         *
     *                                                                         *
     **************************************************************************/

    protected KafkaOperations<K,V> getKafkaOperations(){
        return kafkaOperations;
    }

}

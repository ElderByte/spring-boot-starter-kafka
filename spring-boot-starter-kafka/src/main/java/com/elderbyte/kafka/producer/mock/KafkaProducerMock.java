package com.elderbyte.kafka.producer.mock;

import com.elderbyte.kafka.producer.KafkaMessage;
import com.elderbyte.kafka.producer.KafkaProducer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KafkaProducerMock<K,V> implements KafkaProducer<K,V> {

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, K key, V data) {
        return null;
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data) {
        return null;
    }

    @Override
    public ListenableFuture<SendResult<K, V>> send(String topic, KafkaMessage<K, V> message) {
        return null;
    }

    @Override
    public void sendAll(String topic, Collection<KafkaMessage<K, V>> kafkaMessages) {

    }

    @Override
    public List<PartitionInfo> partitionsFor(String topic) {
        return new ArrayList<>();
    }

    @Override
    public void flush() {
        // NOP
    }
}

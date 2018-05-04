package com.elderbyte.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;

public interface KafkaProducer<K,V> {

    /**
     * Send the data to the provided topic with the provided key and no partition.
     * @param topic the topic.
     * @param key the key.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> send(String topic, K key, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param partition the partition.
     * @param key the key.
     * @param data the data.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param message the partition.
     * @return a Future for the {@link SendResult}.
     */
    ListenableFuture<SendResult<K, V>> send(String topic, KafkaMessage<K, V> message);

    void sendAll(String topic, Collection<KafkaMessage<K, V>> messages);


    /**
     * See {@link Producer#partitionsFor(String)}.
     * @param topic the topic.
     * @return the partition info.
     */
    List<PartitionInfo> partitionsFor(String topic);


    void flush();
}

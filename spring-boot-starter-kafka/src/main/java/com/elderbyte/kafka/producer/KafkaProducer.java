package com.elderbyte.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.PartitionInfo;
import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

/**
 * Provides the ability to send messages / records to a kafka broker.
 * @param <K> The message key
 * @param <V> The message content
 */
public interface KafkaProducer<K,V> {

    /***************************************************************************
     *                                                                         *
     * Message Producer API                                                    *
     *                                                                         *
     **************************************************************************/

    default
            <
            MK,
            MV
            >
    CompletableFuture<SendResult<MK, V>> sendMessage(String topic, MV messageBody) {
        var message = KafkaMessage.fromMessage(messageBody);
        return send(topic, (KafkaMessage) message);
    }

    default
            <
            MK,
            MV
            >
    List<CompletableFuture<SendResult<MK, V>>> sendAllMessages(String topic, Collection<? extends MV> messageBodys) {
        var messages = messageBodys.stream()
                .map(KafkaMessage::fromMessage)
                .collect(toList());
        return sendAll(topic, (Collection) messages);
    }

    /***************************************************************************
     *                                                                         *
     * Kafka Producer API                                                      *
     *                                                                         *
     **************************************************************************/

    /**
     * Send the data to the provided topic with the provided key and no partition.
     * @param topic the topic.
     * @param key the key.
     * @param data The data.
     * @return a Future for the {@link SendResult}.
     */
    CompletableFuture<SendResult<K, V>> send(String topic, K key, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param partition the partition.
     * @param key the key.
     * @param data the data.
     * @return a Future for the {@link SendResult}.
     */
    CompletableFuture<SendResult<K, V>> send(String topic, Integer partition, K key, V data);

    /**
     * Send the data to the provided topic with the provided key and partition.
     * @param topic the topic.
     * @param message the partition.
     * @return a Future for the {@link SendResult}.
     */
    CompletableFuture<SendResult<K, V>> send(String topic, KafkaMessage<K, V> message);

    /**
     * Send all messages to to given topic
     * @param topic The topic name
     * @param messages The messages to send
     */
    List<CompletableFuture<SendResult<K, V>>> sendAll(String topic, Collection<KafkaMessage<K, V>> messages);


    /**
     * See {@link Producer#partitionsFor(String)}.
     * @param topic the topic.
     * @return the partition info.
     */
    List<PartitionInfo> partitionsFor(String topic);

    /**
     * Flush all pending messages to the broker
     */
    void flush();
}

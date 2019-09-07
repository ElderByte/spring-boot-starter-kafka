package com.elderbyte.kafka.producer;

import org.springframework.kafka.support.SendResult;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.stream.Collectors.toList;

/**
 * This producer supports transactional operations on top of the default ones.
 * @param <K>
 * @param <V>
 */
public interface KafkaProducerTx<K,V> extends KafkaProducer<K,V> {

    List<CompletableFuture<SendResult<K, V>>> sendAllTransactionally(String topic, Collection<KafkaMessage<K, V>> messages);

    default List<CompletableFuture<SendResult<String, V>>> sendAllMessagesTransactionally(String topic, Collection<V> messageBodys) {
        var messages = messageBodys.stream()
                .map(KafkaMessage::fromMessage)
                .collect(toList());
        return sendAllTransactionally(topic, (Collection) messages);
    }
}
